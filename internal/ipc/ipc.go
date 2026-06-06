package ipc

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const (
	defaultMaxHeaderBytes  = 1 << 20
	defaultMaxPayloadBytes = 64 << 20
)

var (
	ErrClosed          = errors.New("ipc closed")
	ErrInvalidMessage  = errors.New("invalid ipc message")
	ErrPayloadTooLarge = errors.New("ipc payload too large")
	ErrSocketInUse     = errors.New("ipc socket already in use")
)

// MessageType is the local elark CLI <-> elarkd IPC message type.
type MessageType string

const (
	TypeStartSession MessageType = "start_session"
	TypeStdin        MessageType = "stdin"
	TypeResize       MessageType = "resize"
	TypeSignal       MessageType = "signal"
	TypeClose        MessageType = "close"

	TypeStdout MessageType = "stdout"
	TypeStderr MessageType = "stderr"
	TypeExit   MessageType = "exit"
	TypeError  MessageType = "error"
)

const (
	ErrorCodeBadRequest  = "bad_request"
	ErrorCodeCanceled    = "canceled"
	ErrorCodeHandler     = "handler_error"
	ErrorCodeInternal    = "internal"
	ErrorCodeProtocol    = "protocol_error"
	ErrorCodeUnavailable = "unavailable"
)

// Message is one IPC message. Bytes is transported outside the JSON header as
// an optional raw byte payload.
type Message struct {
	Type      MessageType       `json:"type"`
	RequestID string            `json:"request_id,omitempty"`
	Host      string            `json:"host,omitempty"`
	Cmd       string            `json:"cmd,omitempty"`
	Pty       bool              `json:"pty,omitempty"`
	Cwd       string            `json:"cwd,omitempty"`
	Env       map[string]string `json:"env,omitempty"`
	Shell     string            `json:"shell,omitempty"`
	Rows      int               `json:"rows,omitempty"`
	Cols      int               `json:"cols,omitempty"`
	Name      string            `json:"name,omitempty"`
	Reason    string            `json:"reason,omitempty"`
	Code      int               `json:"code,omitempty"`

	ErrorCode string `json:"error_code,omitempty"`
	Message   string `json:"message,omitempty"`
	Detail    string `json:"detail,omitempty"`

	Bytes []byte `json:"-"`
}

type messageHeader struct {
	Type      MessageType       `json:"type"`
	RequestID string            `json:"request_id,omitempty"`
	Host      string            `json:"host,omitempty"`
	Cmd       string            `json:"cmd,omitempty"`
	Pty       bool              `json:"pty,omitempty"`
	Cwd       string            `json:"cwd,omitempty"`
	Env       map[string]string `json:"env,omitempty"`
	Shell     string            `json:"shell,omitempty"`
	Rows      int               `json:"rows,omitempty"`
	Cols      int               `json:"cols,omitempty"`
	Name      string            `json:"name,omitempty"`
	Reason    string            `json:"reason,omitempty"`
	Code      int               `json:"code,omitempty"`
	ErrorCode string            `json:"error_code,omitempty"`
	Message   string            `json:"message,omitempty"`
	Detail    string            `json:"detail,omitempty"`
	BytesLen  int               `json:"bytes_len,omitempty"`
}

// StartSessionRequest is sent by elark CLI to open one daemon-managed session.
type StartSessionRequest struct {
	RequestID string
	Host      string
	Cmd       string
	Pty       bool
	Cwd       string
	Env       map[string]string
	Shell     string
	Rows      int
	Cols      int
}

type StdinRequest struct {
	RequestID string
	Bytes     []byte
}

type ResizeRequest struct {
	RequestID string
	Rows      int
	Cols      int
}

type SignalRequest struct {
	RequestID string
	Name      string
}

type CloseRequest struct {
	RequestID string
	Reason    string
}

// Handler is implemented by local elarkd. Handler methods should register or
// update daemon state quickly and return; output can be written through Session.
type Handler interface {
	StartSession(context.Context, *Session, StartSessionRequest) error
	Stdin(context.Context, StdinRequest) error
	Resize(context.Context, ResizeRequest) error
	Signal(context.Context, SignalRequest) error
	Close(context.Context, CloseRequest) error
}

// HandlerFuncs lets tests and small integrations implement Handler without a
// named type.
type HandlerFuncs struct {
	StartSessionFunc func(context.Context, *Session, StartSessionRequest) error
	StdinFunc        func(context.Context, StdinRequest) error
	ResizeFunc       func(context.Context, ResizeRequest) error
	SignalFunc       func(context.Context, SignalRequest) error
	CloseFunc        func(context.Context, CloseRequest) error
}

func (h HandlerFuncs) StartSession(ctx context.Context, sess *Session, req StartSessionRequest) error {
	if h.StartSessionFunc == nil {
		return nil
	}
	return h.StartSessionFunc(ctx, sess, req)
}

func (h HandlerFuncs) Stdin(ctx context.Context, req StdinRequest) error {
	if h.StdinFunc == nil {
		return nil
	}
	return h.StdinFunc(ctx, req)
}

func (h HandlerFuncs) Resize(ctx context.Context, req ResizeRequest) error {
	if h.ResizeFunc == nil {
		return nil
	}
	return h.ResizeFunc(ctx, req)
}

func (h HandlerFuncs) Signal(ctx context.Context, req SignalRequest) error {
	if h.SignalFunc == nil {
		return nil
	}
	return h.SignalFunc(ctx, req)
}

func (h HandlerFuncs) Close(ctx context.Context, req CloseRequest) error {
	if h.CloseFunc == nil {
		return nil
	}
	return h.CloseFunc(ctx, req)
}

// RPCError is the stable error envelope used on the wire for TypeError.
type RPCError struct {
	Code      string
	RequestID string
	Message   string
	Detail    string
	Err       error
}

func (e *RPCError) Error() string {
	if e == nil {
		return ""
	}
	msg := e.Message
	if msg == "" {
		msg = "ipc error"
	}
	if e.Code != "" {
		msg = e.Code + ": " + msg
	}
	if e.Detail != "" {
		msg += ": " + e.Detail
	}
	if e.Err != nil {
		msg += ": " + e.Err.Error()
	}
	return msg
}

func (e *RPCError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

func NewRPCError(code, message, detail string) *RPCError {
	if code == "" {
		code = ErrorCodeInternal
	}
	if message == "" {
		message = "ipc error"
	}
	return &RPCError{Code: code, Message: message, Detail: detail}
}

func ErrorMessage(requestID, code, message, detail string) Message {
	err := NewRPCError(code, message, detail)
	return Message{
		Type:      TypeError,
		RequestID: requestID,
		ErrorCode: err.Code,
		Message:   err.Message,
		Detail:    err.Detail,
	}
}

func (m Message) AsError() *RPCError {
	if m.Type != TypeError {
		return nil
	}
	return &RPCError{
		Code:      firstNonEmpty(m.ErrorCode, ErrorCodeInternal),
		RequestID: m.RequestID,
		Message:   m.Message,
		Detail:    m.Detail,
	}
}

func StartSessionMessage(req StartSessionRequest) Message {
	return Message{
		Type:      TypeStartSession,
		RequestID: req.RequestID,
		Host:      req.Host,
		Cmd:       req.Cmd,
		Pty:       req.Pty,
		Cwd:       req.Cwd,
		Env:       cloneEnv(req.Env),
		Shell:     req.Shell,
		Rows:      req.Rows,
		Cols:      req.Cols,
	}
}

func StdinMessage(requestID string, data []byte) Message {
	return Message{Type: TypeStdin, RequestID: requestID, Bytes: append([]byte(nil), data...)}
}

func ResizeMessage(requestID string, rows, cols int) Message {
	return Message{Type: TypeResize, RequestID: requestID, Rows: rows, Cols: cols}
}

func SignalMessage(requestID, name string) Message {
	return Message{Type: TypeSignal, RequestID: requestID, Name: name}
}

func CloseMessage(requestID, reason string) Message {
	return Message{Type: TypeClose, RequestID: requestID, Reason: reason}
}

func StdoutMessage(requestID string, data []byte) Message {
	return Message{Type: TypeStdout, RequestID: requestID, Bytes: append([]byte(nil), data...)}
}

func StderrMessage(requestID string, data []byte) Message {
	return Message{Type: TypeStderr, RequestID: requestID, Bytes: append([]byte(nil), data...)}
}

func ExitMessage(requestID string, code int) Message {
	return Message{Type: TypeExit, RequestID: requestID, Code: code}
}

// Conn is a framed IPC connection. Concurrent writes are safe; callers should
// keep reads to a single goroutine.
type Conn struct {
	conn            net.Conn
	writeMu         sync.Mutex
	maxHeaderBytes  int
	maxPayloadBytes int
	closed          chan struct{}
	closeOnce       sync.Once
}

func NewConn(conn net.Conn) *Conn {
	return &Conn{
		conn:            conn,
		maxHeaderBytes:  defaultMaxHeaderBytes,
		maxPayloadBytes: defaultMaxPayloadBytes,
		closed:          make(chan struct{}),
	}
}

func (c *Conn) Close() error {
	var err error
	c.closeOnce.Do(func() {
		close(c.closed)
		err = c.conn.Close()
	})
	return err
}

func (c *Conn) Done() <-chan struct{} {
	return c.closed
}

func (c *Conn) Write(ctx context.Context, msg Message) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := msg.validate(c.maxPayloadBytes); err != nil {
		return err
	}

	header := msg.toHeader()
	header.BytesLen = len(msg.Bytes)
	rawHeader, err := json.Marshal(header)
	if err != nil {
		return fmt.Errorf("marshal ipc header: %w", err)
	}
	if len(rawHeader) > c.maxHeaderBytes {
		return fmt.Errorf("%w: header has %d bytes, limit is %d", ErrPayloadTooLarge, len(rawHeader), c.maxHeaderBytes)
	}

	var prefix [4]byte
	binary.BigEndian.PutUint32(prefix[:], uint32(len(rawHeader)))

	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	cleanupDeadline, err := setWriteDeadlineOnCancel(ctx, c.conn)
	if err != nil {
		return err
	}
	defer cleanupDeadline()

	if _, err := c.conn.Write(prefix[:]); err != nil {
		return normalizeNetErr(ctx, err)
	}
	if _, err := c.conn.Write(rawHeader); err != nil {
		return normalizeNetErr(ctx, err)
	}
	if len(msg.Bytes) > 0 {
		if _, err := c.conn.Write(msg.Bytes); err != nil {
			return normalizeNetErr(ctx, err)
		}
	}
	return nil
}

func (c *Conn) Read(ctx context.Context) (Message, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return Message{}, err
	}

	cleanupDeadline, err := setReadDeadlineOnCancel(ctx, c.conn)
	if err != nil {
		return Message{}, err
	}
	defer cleanupDeadline()

	var prefix [4]byte
	if _, err := io.ReadFull(c.conn, prefix[:]); err != nil {
		return Message{}, normalizeNetErr(ctx, err)
	}
	headerLen := int(binary.BigEndian.Uint32(prefix[:]))
	if headerLen <= 0 {
		return Message{}, fmt.Errorf("%w: empty header", ErrInvalidMessage)
	}
	if headerLen > c.maxHeaderBytes {
		return Message{}, fmt.Errorf("%w: header has %d bytes, limit is %d", ErrPayloadTooLarge, headerLen, c.maxHeaderBytes)
	}

	rawHeader := make([]byte, headerLen)
	if _, err := io.ReadFull(c.conn, rawHeader); err != nil {
		return Message{}, normalizeNetErr(ctx, err)
	}

	var header messageHeader
	if err := json.Unmarshal(rawHeader, &header); err != nil {
		return Message{}, fmt.Errorf("%w: decode header: %w", ErrInvalidMessage, err)
	}
	if header.BytesLen < 0 {
		return Message{}, fmt.Errorf("%w: negative bytes_len", ErrInvalidMessage)
	}
	if header.BytesLen > c.maxPayloadBytes {
		return Message{}, fmt.Errorf("%w: payload has %d bytes, limit is %d", ErrPayloadTooLarge, header.BytesLen, c.maxPayloadBytes)
	}

	msg := messageFromHeader(header)
	if header.BytesLen > 0 {
		msg.Bytes = make([]byte, header.BytesLen)
		if _, err := io.ReadFull(c.conn, msg.Bytes); err != nil {
			return Message{}, normalizeNetErr(ctx, err)
		}
	}
	if err := msg.validate(c.maxPayloadBytes); err != nil {
		return Message{}, err
	}
	return msg, nil
}

// Client is the short-lived elark side of the local IPC connection.
type Client struct {
	conn *Conn
}

func Dial(ctx context.Context, socketPath string) (*Client, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	dialer := net.Dialer{}
	raw, err := dialer.DialContext(ctx, "unix", socketPath)
	if err != nil {
		return nil, fmt.Errorf("dial ipc socket %s: %w", socketPath, err)
	}
	return &Client{conn: NewConn(raw)}, nil
}

func (c *Client) Close() error {
	if c == nil || c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

func (c *Client) StartSession(ctx context.Context, req StartSessionRequest) error {
	return c.conn.Write(ctx, StartSessionMessage(req))
}

func (c *Client) SendStdin(ctx context.Context, requestID string, data []byte) error {
	return c.conn.Write(ctx, StdinMessage(requestID, data))
}

func (c *Client) Resize(ctx context.Context, requestID string, rows, cols int) error {
	return c.conn.Write(ctx, ResizeMessage(requestID, rows, cols))
}

func (c *Client) Signal(ctx context.Context, requestID, name string) error {
	return c.conn.Write(ctx, SignalMessage(requestID, name))
}

func (c *Client) CloseSession(ctx context.Context, requestID, reason string) error {
	return c.conn.Write(ctx, CloseMessage(requestID, reason))
}

func (c *Client) Cancel(ctx context.Context, requestID, reason string) error {
	return c.CloseSession(ctx, requestID, reason)
}

func (c *Client) Receive(ctx context.Context) (Message, error) {
	return c.conn.Read(ctx)
}

// Server listens on one local Unix socket and serves every CLI connection in a
// separate goroutine.
type Server struct {
	listener  net.Listener
	handler   Handler
	done      chan struct{}
	closeOnce sync.Once
	wg        sync.WaitGroup
	connMu    sync.Mutex
	conns     map[*Conn]struct{}
}

func Listen(socketPath string, handler Handler) (*Server, error) {
	if handler == nil {
		return nil, errors.New("ipc handler is nil")
	}
	if strings.TrimSpace(socketPath) == "" {
		return nil, errors.New("ipc socket path is empty")
	}

	if err := os.MkdirAll(filepath.Dir(socketPath), 0o700); err != nil {
		return nil, fmt.Errorf("create ipc socket directory: %w", err)
	}
	if err := removeStaleSocket(socketPath); err != nil {
		return nil, err
	}

	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		return nil, fmt.Errorf("listen ipc socket %s: %w", socketPath, err)
	}
	if err := os.Chmod(socketPath, 0o600); err != nil {
		_ = listener.Close()
		return nil, fmt.Errorf("chmod ipc socket %s: %w", socketPath, err)
	}

	return &Server{
		listener: listener,
		handler:  handler,
		done:     make(chan struct{}),
		conns:    make(map[*Conn]struct{}),
	}, nil
}

func (s *Server) SocketPath() string {
	if s == nil || s.listener == nil {
		return ""
	}
	if addr := s.listener.Addr(); addr != nil {
		return addr.String()
	}
	return ""
}

func (s *Server) Serve() error {
	for {
		raw, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.done:
				return nil
			default:
			}
			return fmt.Errorf("accept ipc connection: %w", err)
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.serveConn(raw)
		}()
	}
}

func (s *Server) Close() error {
	var err error
	s.closeOnce.Do(func() {
		close(s.done)
		err = s.listener.Close()
		s.connMu.Lock()
		for conn := range s.conns {
			_ = conn.Close()
		}
		s.connMu.Unlock()
		s.wg.Wait()
	})
	return err
}

type Session struct {
	conn *Conn
}

func (s *Session) SendStdout(ctx context.Context, requestID string, data []byte) error {
	return s.conn.Write(ctx, StdoutMessage(requestID, data))
}

func (s *Session) SendStderr(ctx context.Context, requestID string, data []byte) error {
	return s.conn.Write(ctx, StderrMessage(requestID, data))
}

func (s *Session) SendExit(ctx context.Context, requestID string, code int) error {
	return s.conn.Write(ctx, ExitMessage(requestID, code))
}

func (s *Session) SendError(ctx context.Context, requestID string, err *RPCError) error {
	if err == nil {
		err = NewRPCError(ErrorCodeInternal, "ipc error", "")
	}
	return s.conn.Write(ctx, ErrorMessage(requestID, err.Code, err.Message, err.Detail))
}

func (s *Server) serveConn(raw net.Conn) {
	conn := NewConn(raw)
	s.connMu.Lock()
	s.conns[conn] = struct{}{}
	s.connMu.Unlock()
	defer func() {
		_ = conn.Close()
		s.connMu.Lock()
		delete(s.conns, conn)
		s.connMu.Unlock()
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	session := &Session{conn: conn}
	for {
		msg, err := conn.Read(ctx)
		if err != nil {
			if !isExpectedClose(err) {
				_ = conn.Write(context.Background(), ErrorMessage("", ErrorCodeProtocol, "invalid ipc message", err.Error()))
			}
			return
		}

		if err := s.dispatch(ctx, session, msg); err != nil {
			_ = conn.Write(context.Background(), errorMessageFromError(msg.RequestID, err))
			if msg.Type == TypeClose {
				return
			}
		}
	}
}

func (s *Server) dispatch(ctx context.Context, session *Session, msg Message) error {
	switch msg.Type {
	case TypeStartSession:
		return s.handler.StartSession(ctx, session, StartSessionRequest{
			RequestID: msg.RequestID,
			Host:      msg.Host,
			Cmd:       msg.Cmd,
			Pty:       msg.Pty,
			Cwd:       msg.Cwd,
			Env:       cloneEnv(msg.Env),
			Shell:     msg.Shell,
			Rows:      msg.Rows,
			Cols:      msg.Cols,
		})
	case TypeStdin:
		return s.handler.Stdin(ctx, StdinRequest{RequestID: msg.RequestID, Bytes: append([]byte(nil), msg.Bytes...)})
	case TypeResize:
		return s.handler.Resize(ctx, ResizeRequest{RequestID: msg.RequestID, Rows: msg.Rows, Cols: msg.Cols})
	case TypeSignal:
		return s.handler.Signal(ctx, SignalRequest{RequestID: msg.RequestID, Name: msg.Name})
	case TypeClose:
		return s.handler.Close(ctx, CloseRequest{RequestID: msg.RequestID, Reason: msg.Reason})
	default:
		return NewRPCError(ErrorCodeBadRequest, "unsupported client message type", string(msg.Type))
	}
}

func errorMessageFromError(requestID string, err error) Message {
	if err == nil {
		return Message{}
	}
	var rpcErr *RPCError
	if errors.As(err, &rpcErr) {
		if rpcErr.RequestID != "" {
			requestID = rpcErr.RequestID
		}
		return ErrorMessage(requestID, rpcErr.Code, rpcErr.Message, rpcErr.Detail)
	}
	if errors.Is(err, context.Canceled) {
		return ErrorMessage(requestID, ErrorCodeCanceled, "request canceled", err.Error())
	}
	return ErrorMessage(requestID, ErrorCodeHandler, "handler returned error", err.Error())
}

func (m Message) toHeader() messageHeader {
	return messageHeader{
		Type:      m.Type,
		RequestID: m.RequestID,
		Host:      m.Host,
		Cmd:       m.Cmd,
		Pty:       m.Pty,
		Cwd:       m.Cwd,
		Env:       cloneEnv(m.Env),
		Shell:     m.Shell,
		Rows:      m.Rows,
		Cols:      m.Cols,
		Name:      m.Name,
		Reason:    m.Reason,
		Code:      m.Code,
		ErrorCode: m.ErrorCode,
		Message:   m.Message,
		Detail:    m.Detail,
	}
}

func messageFromHeader(h messageHeader) Message {
	return Message{
		Type:      h.Type,
		RequestID: h.RequestID,
		Host:      h.Host,
		Cmd:       h.Cmd,
		Pty:       h.Pty,
		Cwd:       h.Cwd,
		Env:       cloneEnv(h.Env),
		Shell:     h.Shell,
		Rows:      h.Rows,
		Cols:      h.Cols,
		Name:      h.Name,
		Reason:    h.Reason,
		Code:      h.Code,
		ErrorCode: h.ErrorCode,
		Message:   h.Message,
		Detail:    h.Detail,
	}
}

func (m Message) validate(maxPayloadBytes int) error {
	if m.Type == "" {
		return fmt.Errorf("%w: missing type", ErrInvalidMessage)
	}
	switch m.Type {
	case TypeStartSession:
		if strings.TrimSpace(m.RequestID) == "" {
			return fmt.Errorf("%w: start_session request_id is required", ErrInvalidMessage)
		}
		if strings.TrimSpace(m.Host) == "" {
			return fmt.Errorf("%w: start_session host is required", ErrInvalidMessage)
		}
		if (m.Rows < 0) || (m.Cols < 0) {
			return fmt.Errorf("%w: start_session rows/cols must be non-negative", ErrInvalidMessage)
		}
	case TypeStdin, TypeStdout, TypeStderr:
		if strings.TrimSpace(m.RequestID) == "" {
			return fmt.Errorf("%w: %s request_id is required", ErrInvalidMessage, m.Type)
		}
	case TypeResize:
		if strings.TrimSpace(m.RequestID) == "" {
			return fmt.Errorf("%w: resize request_id is required", ErrInvalidMessage)
		}
		if m.Rows <= 0 || m.Cols <= 0 {
			return fmt.Errorf("%w: resize rows and cols must be positive", ErrInvalidMessage)
		}
	case TypeSignal:
		if strings.TrimSpace(m.RequestID) == "" {
			return fmt.Errorf("%w: signal request_id is required", ErrInvalidMessage)
		}
		if strings.TrimSpace(m.Name) == "" {
			return fmt.Errorf("%w: signal name is required", ErrInvalidMessage)
		}
	case TypeClose:
		if strings.TrimSpace(m.RequestID) == "" {
			return fmt.Errorf("%w: close request_id is required", ErrInvalidMessage)
		}
	case TypeExit:
		if strings.TrimSpace(m.RequestID) == "" {
			return fmt.Errorf("%w: exit request_id is required", ErrInvalidMessage)
		}
	case TypeError:
		if strings.TrimSpace(m.ErrorCode) == "" {
			return fmt.Errorf("%w: error_code is required", ErrInvalidMessage)
		}
		if strings.TrimSpace(m.Message) == "" {
			return fmt.Errorf("%w: error message is required", ErrInvalidMessage)
		}
	default:
		return fmt.Errorf("%w: unknown type %q", ErrInvalidMessage, m.Type)
	}
	if len(m.Bytes) > maxPayloadBytes {
		return fmt.Errorf("%w: payload has %d bytes, limit is %d", ErrPayloadTooLarge, len(m.Bytes), maxPayloadBytes)
	}
	return nil
}

func removeStaleSocket(socketPath string) error {
	info, err := os.Lstat(socketPath)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("stat ipc socket %s: %w", socketPath, err)
	}
	if info.Mode()&os.ModeSocket == 0 {
		return fmt.Errorf("%w: %s exists and is not a socket", ErrSocketInUse, socketPath)
	}

	conn, err := net.DialTimeout("unix", socketPath, 100*time.Millisecond)
	if err == nil {
		_ = conn.Close()
		return fmt.Errorf("%w: %s", ErrSocketInUse, socketPath)
	}
	if err := os.Remove(socketPath); err != nil {
		return fmt.Errorf("remove stale ipc socket %s: %w", socketPath, err)
	}
	return nil
}

func setReadDeadlineOnCancel(ctx context.Context, conn net.Conn) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if ctx.Done() == nil {
		return func() {}, nil
	}
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = conn.SetReadDeadline(time.Now())
		case <-done:
		}
	}()
	return func() {
		close(done)
		clearReadDeadline(conn)
	}, nil
}

func setWriteDeadlineOnCancel(ctx context.Context, conn net.Conn) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if ctx.Done() == nil {
		return func() {}, nil
	}
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = conn.SetWriteDeadline(time.Now())
		case <-done:
		}
	}()
	return func() {
		close(done)
		clearWriteDeadline(conn)
	}, nil
}

func clearReadDeadline(conn net.Conn) {
	_ = conn.SetReadDeadline(time.Time{})
}

func clearWriteDeadline(conn net.Conn) {
	_ = conn.SetWriteDeadline(time.Time{})
}

func normalizeNetErr(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	if errors.Is(err, net.ErrClosed) {
		return ErrClosed
	}
	return err
}

func isExpectedClose(err error) bool {
	return errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) || errors.Is(err, ErrClosed) || errors.Is(err, context.Canceled)
}

func cloneEnv(env map[string]string) map[string]string {
	if len(env) == 0 {
		return nil
	}
	out := make(map[string]string, len(env))
	for key, value := range env {
		out[key] = value
	}
	return out
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
