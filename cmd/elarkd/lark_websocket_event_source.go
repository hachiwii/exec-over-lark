package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	gorillaws "github.com/gorilla/websocket"
	"github.com/hachiwii/exec-over-lark/internal/bootstrap"
	"github.com/hachiwii/exec-over-lark/internal/daemon"
	"github.com/hachiwii/exec-over-lark/internal/lark"
	sdkws "github.com/larksuite/oapi-sdk-go/v3/ws"
)

const defaultLarkWSReconnectInterval = 5 * time.Second

type websocketEndpointProvider interface {
	PersistentConnectionEndpoint(ctx context.Context) (lark.WSEndpoint, error)
}

type larkWebSocketEventSource struct {
	client          websocketEndpointProvider
	dialer          *gorillaws.Dialer
	stderr          io.Writer
	bootstrapSender bootstrap.Sender
}

type wsReconnectError struct {
	err error
}

func (e wsReconnectError) Error() string {
	return e.err.Error()
}

func (e wsReconnectError) Unwrap() error {
	return e.err
}

type wsChunkSet struct {
	sum    int
	chunks [][]byte
}

func newLarkWebSocketEventSource(client websocketEndpointProvider, stderr io.Writer) *larkWebSocketEventSource {
	return &larkWebSocketEventSource{
		client: client,
		dialer: gorillaws.DefaultDialer,
		stderr: stderr,
	}
}

func (s *larkWebSocketEventSource) Run(ctx context.Context, selfBotOpenID string, handler daemon.EventHandler) error {
	if s.client == nil {
		return errors.New("lark websocket event source client is nil")
	}
	if handler == nil {
		return errors.New("lark websocket event source handler is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	for {
		endpoint, err := s.client.PersistentConnectionEndpoint(ctx)
		if err != nil {
			return err
		}
		err = s.runConnection(ctx, endpoint, selfBotOpenID, handler)
		if err == nil {
			return nil
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		var reconnectErr wsReconnectError
		if !errors.As(err, &reconnectErr) {
			return err
		}
		s.log("lark websocket disconnected: %v", reconnectErr.err)

		delay := firstMainDuration(endpoint.ClientConfig.ReconnectInterval, defaultLarkWSReconnectInterval)
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func (s *larkWebSocketEventSource) runConnection(ctx context.Context, endpoint lark.WSEndpoint, selfBotOpenID string, handler daemon.EventHandler) error {
	connURL, err := url.Parse(endpoint.URL)
	if err != nil {
		return fmt.Errorf("parse lark websocket URL: %w", err)
	}
	serviceID, _ := strconv.ParseInt(connURL.Query().Get("service_id"), 10, 32)
	dialer := s.dialer
	if dialer == nil {
		dialer = gorillaws.DefaultDialer
	}
	conn, resp, err := dialer.DialContext(ctx, endpoint.URL, nil)
	if err != nil {
		if resp != nil && resp.StatusCode != http.StatusSwitchingProtocols {
			return fmt.Errorf("connect lark websocket: status=%s: %w", resp.Status, err)
		}
		return wsReconnectError{err: fmt.Errorf("connect lark websocket: %w", err)}
	}
	defer conn.Close()
	s.log("lark websocket connected host=%s service_id=%s", connURL.Host, connURL.Query().Get("service_id"))

	connCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		<-connCtx.Done()
		_ = conn.Close()
	}()

	var writeMu sync.Mutex
	pingDone := make(chan struct{})
	go func() {
		defer close(pingDone)
		s.pingLoop(connCtx, conn, &writeMu, int32(serviceID), endpoint.ClientConfig.PingInterval)
	}()
	defer func() {
		cancel()
		<-pingDone
	}()

	chunks := map[string]wsChunkSet{}
	for {
		messageType, raw, err := conn.ReadMessage()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return wsReconnectError{err: fmt.Errorf("read lark websocket frame: %w", err)}
		}
		if messageType != gorillaws.BinaryMessage {
			continue
		}

		var frame sdkws.Frame
		if err := frame.Unmarshal(raw); err != nil {
			return fmt.Errorf("decode lark websocket frame: %w", err)
		}
		if err := s.handleFrame(ctx, conn, &writeMu, chunks, &frame, selfBotOpenID, handler); err != nil {
			return err
		}
	}
}

func (s *larkWebSocketEventSource) pingLoop(ctx context.Context, conn *gorillaws.Conn, writeMu *sync.Mutex, serviceID int32, interval time.Duration) {
	if interval <= 0 {
		interval = 90 * time.Second
	}
	timer := time.NewTimer(interval)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			frame := sdkws.NewPingFrame(serviceID)
			if err := writeWSFrame(conn, writeMu, frame); err != nil {
				s.log("lark websocket ping failed: %v", err)
				return
			}
			timer.Reset(interval)
		}
	}
}

func (s *larkWebSocketEventSource) handleFrame(
	ctx context.Context,
	conn *gorillaws.Conn,
	writeMu *sync.Mutex,
	chunks map[string]wsChunkSet,
	frame *sdkws.Frame,
	selfBotOpenID string,
	handler daemon.EventHandler,
) error {
	if sdkws.FrameType(frame.Method) == sdkws.FrameTypeControl {
		return nil
	}
	if sdkws.FrameType(frame.Method) != sdkws.FrameTypeData {
		return nil
	}

	headers := sdkws.Headers(frame.Headers)
	if headers.GetString(sdkws.HeaderType) != string(sdkws.MessageTypeEvent) {
		return nil
	}

	payload := frame.Payload
	sum := firstMainPositive(headers.GetInt(sdkws.HeaderSum), 1)
	if sum > 1 {
		msgID := headers.GetString(sdkws.HeaderMessageID)
		seq := headers.GetInt(sdkws.HeaderSeq)
		set := chunks[msgID]
		if set.chunks == nil {
			set.sum = sum
			set.chunks = make([][]byte, sum)
		}
		if seq >= 0 && seq < len(set.chunks) {
			set.chunks[seq] = append([]byte(nil), payload...)
		}
		chunks[msgID] = set
		payload = combineWSChunks(set)
		if payload == nil {
			return nil
		}
		delete(chunks, msgID)
	}

	start := time.Now()
	status := http.StatusOK
	err := s.handlePayload(ctx, payload, selfBotOpenID, handler)
	if err != nil {
		status = http.StatusInternalServerError
	}
	ackErr := ackWSFrame(conn, writeMu, frame, status, time.Since(start))
	if ackErr != nil {
		return wsReconnectError{err: ackErr}
	}
	if err != nil {
		return err
	}
	return nil
}

func (s *larkWebSocketEventSource) handlePayload(ctx context.Context, payload []byte, selfBotOpenID string, handler daemon.EventHandler) error {
	if s.bootstrapSender != nil {
		err := bootstrap.HandleEventJSON(ctx, s.bootstrapSender, payload, selfBotOpenID)
		if err == nil {
			return nil
		}
		if !errors.Is(err, bootstrap.ErrIgnoredEvent) {
			return err
		}
	}

	event, err := lark.ParseMessageReceiveEvent(payload, selfBotOpenID)
	if errors.Is(err, lark.ErrIgnoredEvent) {
		return nil
	}
	if err != nil {
		return err
	}
	if err := handler(ctx, event); err != nil && !errors.Is(err, lark.ErrIgnoredEvent) && !errors.Is(err, daemon.ErrIgnoredEvent) {
		return err
	}
	return nil
}

func combineWSChunks(set wsChunkSet) []byte {
	if set.sum <= 0 || len(set.chunks) != set.sum {
		return nil
	}
	total := 0
	for _, chunk := range set.chunks {
		if len(chunk) == 0 {
			return nil
		}
		total += len(chunk)
	}
	payload := make([]byte, 0, total)
	for _, chunk := range set.chunks {
		payload = append(payload, chunk...)
	}
	return payload
}

func ackWSFrame(conn *gorillaws.Conn, writeMu *sync.Mutex, frame *sdkws.Frame, statusCode int, elapsed time.Duration) error {
	headers := sdkws.Headers(frame.Headers)
	headers.Add(sdkws.HeaderBizRt, strconv.FormatInt(elapsed.Milliseconds(), 10))
	resp := sdkws.NewResponseByCode(statusCode)
	payload, err := json.Marshal(resp)
	if err != nil {
		return fmt.Errorf("marshal lark websocket ack: %w", err)
	}
	frame.Headers = []sdkws.Header(headers)
	frame.Payload = payload
	return writeWSFrame(conn, writeMu, frame)
}

func writeWSFrame(conn *gorillaws.Conn, writeMu *sync.Mutex, frame *sdkws.Frame) error {
	raw, err := frame.Marshal()
	if err != nil {
		return fmt.Errorf("marshal lark websocket frame: %w", err)
	}
	writeMu.Lock()
	defer writeMu.Unlock()
	if err := conn.WriteMessage(gorillaws.BinaryMessage, raw); err != nil {
		return fmt.Errorf("write lark websocket frame: %w", err)
	}
	return nil
}

func (s *larkWebSocketEventSource) log(format string, args ...any) {
	if s.stderr == nil {
		return
	}
	msg := fmt.Sprintf(format, args...)
	if strings.TrimSpace(msg) == "" {
		return
	}
	fmt.Fprintf(s.stderr, "lark event: %s\n", msg)
}

func firstMainDuration(values ...time.Duration) time.Duration {
	for _, value := range values {
		if value > 0 {
			return value
		}
	}
	return 0
}

func firstMainPositive(values ...int) int {
	for _, value := range values {
		if value > 0 {
			return value
		}
	}
	return 0
}
