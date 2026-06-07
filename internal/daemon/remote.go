package daemon

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/hachiwii/exec-over-lark/internal/config"
	"github.com/hachiwii/exec-over-lark/internal/lark"
	"github.com/hachiwii/exec-over-lark/internal/outbound"
	"github.com/hachiwii/exec-over-lark/internal/protocol"
	"github.com/hachiwii/exec-over-lark/internal/pty"
	"github.com/hachiwii/exec-over-lark/internal/remoteexec"
	"github.com/hachiwii/exec-over-lark/internal/session"
)

const defaultRemoteEventBuffer = 128

var (
	ErrRemoteExecDisabled  = errors.New("remote exec is disabled")
	ErrRemoteMissingStream = errors.New("remote event stream is nil")
	ErrRemoteMissingSender = errors.New("remote sender is nil")
	ErrRemoteMissingBotID  = errors.New("remote self bot open_id is required")
	ErrRemoteMissingExec   = errors.New("remote executor is nil")
)

type RemoteSender interface {
	ReplyRootMessage(ctx context.Context, chatID, rootMessageID, mentionOpenID, text string) (messageID string, err error)
}

type RemoteExecutor interface {
	Start(ctx context.Context, req remoteexec.Request) (RemoteProcess, error)
}

type RemoteProcess interface {
	Stdin() io.WriteCloser
	Stdout() io.ReadCloser
	Stderr() io.ReadCloser
	Wait() (remoteexec.Result, error)
	Signal(os.Signal) error
	Kill() error
}

type RemoteConfig struct {
	ExecEnabled      bool
	DefaultShell     string
	MaxSessions      int
	StreamChunkBytes int
	AllowedChatIDs   []string

	SendCooldown              time.Duration
	LarkTextRequestLimitBytes int
	HeartbeatInterval         time.Duration
	HeartbeatTimeout          time.Duration
	SequenceGapTimeout        time.Duration
}

type RemoteOptions struct {
	Config        RemoteConfig
	EventStream   io.Reader
	EventSource   EventSource
	SelfBotOpenID string
	Sender        RemoteSender
	Executor      RemoteExecutor
}

type RemoteDaemon struct {
	cfg           RemoteConfig
	eventStream   io.Reader
	eventSource   EventSource
	selfBotOpenID string
	sender        RemoteSender
	executor      RemoteExecutor

	queue   *outbound.Queue
	manager *session.Manager

	allowedChats map[string]struct{}

	mu    sync.Mutex
	tasks map[string]*remoteTask
	wg    sync.WaitGroup
}

func RemoteConfigFromConfig(cfg *config.Config) RemoteConfig {
	if cfg == nil {
		return RemoteConfig{}
	}
	return RemoteConfig{
		ExecEnabled:               cfg.Exec.Enabled,
		DefaultShell:              cfg.Exec.DefaultShell,
		MaxSessions:               cfg.Exec.MaxSessions,
		StreamChunkBytes:          cfg.Exec.StreamChunkBytes,
		AllowedChatIDs:            cfg.Exec.AllowedChatIDs,
		SendCooldown:              cfg.Lark.SendCooldown.Duration(),
		LarkTextRequestLimitBytes: cfg.Lark.LarkTextRequestLimitBytes,
		HeartbeatInterval:         cfg.Connection.HeartbeatInterval.Duration(),
		HeartbeatTimeout:          cfg.Connection.HeartbeatTimeout.Duration(),
		SequenceGapTimeout:        cfg.Connection.SequenceGapTimeout.Duration(),
	}
}

func NewRemoteDaemon(opts RemoteOptions) (*RemoteDaemon, error) {
	cfg := normalizeRemoteConfig(opts.Config)
	if !cfg.ExecEnabled {
		return nil, ErrRemoteExecDisabled
	}
	if opts.EventStream == nil && opts.EventSource == nil {
		return nil, ErrRemoteMissingStream
	}
	if strings.TrimSpace(opts.SelfBotOpenID) == "" {
		return nil, ErrRemoteMissingBotID
	}
	if opts.Sender == nil {
		return nil, ErrRemoteMissingSender
	}
	executor := opts.Executor
	if executor == nil {
		if strings.TrimSpace(cfg.DefaultShell) == "" {
			return nil, ErrRemoteMissingExec
		}
		executor = remoteexecAdapter{executor: remoteexec.New(cfg.DefaultShell)}
	}

	queue := outbound.New(
		replyOnlyOutboundSender{sender: opts.Sender},
		outbound.WithSendCooldown(cfg.SendCooldown),
		outbound.WithRequestLimitBytes(cfg.LarkTextRequestLimitBytes),
	)
	manager := session.New(
		session.WithOutboundQueue(queue),
		session.WithHeartbeatInterval(cfg.HeartbeatInterval),
		session.WithHeartbeatTimeout(cfg.HeartbeatTimeout),
		session.WithSequenceGapTimeout(cfg.SequenceGapTimeout),
		session.WithMaxRemoteSessions(cfg.MaxSessions),
	)

	return &RemoteDaemon{
		cfg:           cfg,
		eventStream:   opts.EventStream,
		eventSource:   opts.EventSource,
		selfBotOpenID: strings.TrimSpace(opts.SelfBotOpenID),
		sender:        opts.Sender,
		executor:      executor,
		queue:         queue,
		manager:       manager,
		allowedChats:  stringSet(cfg.AllowedChatIDs),
		tasks:         make(map[string]*remoteTask),
	}, nil
}

func (d *RemoteDaemon) Run(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	errCh := make(chan error, 2)
	var loops sync.WaitGroup
	loops.Add(1)
	go func() {
		defer loops.Done()
		if err := d.flushLoop(runCtx); err != nil && !errors.Is(err, context.Canceled) && runCtx.Err() == nil {
			errCh <- err
		}
	}()
	loops.Add(1)
	go func() {
		defer loops.Done()
		if err := d.tickLoop(runCtx); err != nil && !errors.Is(err, context.Canceled) && runCtx.Err() == nil {
			errCh <- err
		}
	}()

	var err error
	if d.eventSource != nil {
		err = d.eventSource.Run(runCtx, d.selfBotOpenID, d.HandleMessageEvent)
	} else {
		err = d.consumeEventStream(runCtx)
	}
	if err != nil {
		cancel()
	}
	d.wg.Wait()
	if err == nil {
		err = d.flushPending(ctx)
	}
	cancel()
	loops.Wait()
	if err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	select {
	case loopErr := <-errCh:
		return loopErr
	default:
		return nil
	}
}

func (d *RemoteDaemon) HandleEventJSON(ctx context.Context, data []byte) error {
	event, err := d.parseEventJSON(data)
	if err != nil {
		return err
	}
	return d.HandleMessageEvent(ctx, event)
}

func (d *RemoteDaemon) HandleMessageEvent(ctx context.Context, event lark.MessageEvent) error {
	if !d.cfg.ExecEnabled {
		return ErrRemoteExecDisabled
	}
	if !mentionsContain(event.Mentions, d.selfBotOpenID) {
		return lark.ErrIgnoredEvent
	}
	if !d.allowedChat(event.ChatID) {
		return lark.ErrIgnoredEvent
	}

	inbound := session.InboundMessage{
		ConnID:        session.ConnID(event.RootMessageID, event.MessageID),
		RootMessageID: event.RootMessageID,
		MessageID:     event.MessageID,
		ChatID:        event.ChatID,
		SenderOpenID:  event.SenderOpenID,
		IsRoot:        event.MessageID == event.RootMessageID,
		Frames:        event.Frames,
	}

	if inbound.IsRoot {
		if !hasStartFrame(event.Frames) {
			return lark.ErrIgnoredEvent
		}
		task := newRemoteTask(d, inbound.ConnID)
		if _, err := d.manager.AcceptRemoteStart(ctx, inbound, task); err != nil {
			return err
		}
		d.registerTask(task)
		d.wg.Add(1)
		go func() {
			defer d.wg.Done()
			defer d.unregisterTask(task.connID)
			task.run(ctx)
		}()
		return nil
	}

	err := d.manager.ReceiveRemote(ctx, inbound)
	if errors.Is(err, session.ErrSessionNotFound) {
		return lark.ErrIgnoredEvent
	}
	return err
}

func (d *RemoteDaemon) consumeEventStream(ctx context.Context) error {
	scanner := bufio.NewScanner(d.eventStream)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	for scanner.Scan() {
		if err := ctx.Err(); err != nil {
			return err
		}
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		if err := d.HandleEventJSON(ctx, []byte(line)); err != nil && !errors.Is(err, lark.ErrIgnoredEvent) {
			return err
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("read remote lark event stream: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}

func (d *RemoteDaemon) parseEventJSON(data []byte) (lark.MessageEvent, error) {
	if !d.cfg.ExecEnabled {
		return lark.MessageEvent{}, ErrRemoteExecDisabled
	}

	meta, err := parseRemoteEventMeta(data)
	if err != nil {
		return lark.MessageEvent{}, err
	}
	if meta.ignored {
		return lark.MessageEvent{}, lark.ErrIgnoredEvent
	}
	if !mentionsContain(meta.mentions, d.selfBotOpenID) {
		return lark.MessageEvent{}, lark.ErrIgnoredEvent
	}
	if !d.allowedChat(meta.chatID) {
		return lark.MessageEvent{}, lark.ErrIgnoredEvent
	}
	return lark.ParseMessageReceiveEvent(data, d.selfBotOpenID)
}

func (d *RemoteDaemon) allowedChat(chatID string) bool {
	if len(d.allowedChats) == 0 {
		return true
	}
	_, ok := d.allowedChats[chatID]
	return ok
}

func (d *RemoteDaemon) registerTask(task *remoteTask) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.tasks[task.connID] = task
}

func (d *RemoteDaemon) unregisterTask(connID string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.tasks, connID)
}

func (d *RemoteDaemon) flushLoop(ctx context.Context) error {
	timer := time.NewTimer(100 * time.Millisecond)
	defer timer.Stop()

	for {
		flushed, err := d.queue.FlushReady(ctx)
		if err != nil {
			return err
		}
		if flushed {
			continue
		}

		wait := 100 * time.Millisecond
		if next, ok := d.queue.NextFlushAt(); ok {
			wait = time.Until(next)
			if wait < 0 {
				wait = 0
			}
		}
		if wait <= 0 {
			wait = 100 * time.Millisecond
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(wait)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func (d *RemoteDaemon) tickLoop(ctx context.Context) error {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			_ = d.manager.Tick(ctx)
		}
	}
}

func (d *RemoteDaemon) flushPending(ctx context.Context) error {
	for d.queue.PendingLen() > 0 {
		flushed, err := d.queue.FlushReady(ctx)
		if err != nil {
			return err
		}
		if flushed {
			continue
		}
		next, ok := d.queue.NextFlushAt()
		if !ok {
			return nil
		}
		wait := time.Until(next)
		if wait < 0 {
			wait = 0
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return nil
}

type remoteTask struct {
	daemon *RemoteDaemon
	connID string

	events chan session.RemoteEvent

	mu       sync.Mutex
	closed   bool
	closeCh  chan struct{}
	start    protocol.StartPayload
	hasStart bool
}

func newRemoteTask(daemon *RemoteDaemon, connID string) *remoteTask {
	return &remoteTask{
		daemon:  daemon,
		connID:  connID,
		events:  make(chan session.RemoteEvent, defaultRemoteEventBuffer),
		closeCh: make(chan struct{}),
	}
}

func (t *remoteTask) Deliver(ctx context.Context, event session.RemoteEvent) error {
	if event.Type == session.RemoteEventStart {
		t.mu.Lock()
		t.start = event.Start
		t.hasStart = true
		t.mu.Unlock()
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.closeCh:
		return session.ErrSessionClosed
	case t.events <- event:
		return nil
	}
}

func (t *remoteTask) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return nil
	}
	t.closed = true
	close(t.closeCh)
	return nil
}

func (t *remoteTask) run(parent context.Context) {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	defer t.Close()

	if _, ok := t.daemon.manager.RemoteSnapshot(t.connID); !ok {
		return
	}
	start, ok := t.startPayload()
	if !ok {
		_ = t.daemon.manager.SendRemoteError(ctx, t.connID, "remote session start not found", "")
		return
	}
	proc, ptyMode, err := t.startProcess(ctx, start)
	if err != nil {
		t.sendStartError(ctx, err)
		return
	}

	done := make(chan struct{})
	go t.controlLoop(ctx, cancel, proc, done)

	var wg sync.WaitGroup
	errCh := make(chan error, 3)
	wg.Add(1)
	go t.copyOutput(ctx, &wg, errCh, proc.Stdout(), protocol.TypeStdout)
	if !ptyMode {
		wg.Add(1)
		go t.copyOutput(ctx, &wg, errCh, proc.Stderr(), protocol.TypeStderr)
	}

	result, waitErr := proc.Wait()
	close(done)
	cancel()
	wg.Wait()
	close(errCh)

	if ctx.Err() != nil && result.Canceled {
		return
	}
	if waitErr != nil {
		_ = t.daemon.manager.SendRemoteError(context.Background(), t.connID, "remote command wait failed", waitErr.Error())
		return
	}
	for copyErr := range errCh {
		if copyErr != nil {
			_ = t.daemon.manager.SendRemoteError(context.Background(), t.connID, "remote command output failed", copyErr.Error())
			return
		}
	}
	_ = t.daemon.manager.SendRemoteExit(context.Background(), t.connID, result.ExitCode)
}

func (t *remoteTask) startProcess(ctx context.Context, start protocol.StartPayload) (RemoteProcess, bool, error) {
	shell := firstRemoteNonEmpty(start.Shell, t.daemon.cfg.DefaultShell)
	if start.Pty || strings.TrimSpace(start.Cmd) == "" {
		session, err := pty.New(shell).Start(ctx, pty.Request{
			Command: start.Cmd,
			Shell:   shell,
			Cwd:     start.Cwd,
			Env:     cloneStringMap(start.Env),
			Rows:    start.Rows,
			Cols:    start.Cols,
		})
		if err != nil {
			return nil, true, err
		}
		return ptyProcess{session: session}, true, nil
	}

	proc, err := t.daemon.executor.Start(ctx, remoteexec.Request{
		Command: start.Cmd,
		Shell:   shell,
		Cwd:     start.Cwd,
		Env:     cloneStringMap(start.Env),
	})
	return proc, false, err
}

func (t *remoteTask) startPayload() (protocol.StartPayload, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return cloneStartPayload(t.start), t.hasStart
}

func (t *remoteTask) controlLoop(ctx context.Context, cancel context.CancelFunc, proc RemoteProcess, done <-chan struct{}) {
	for {
		select {
		case <-ctx.Done():
			_ = proc.Kill()
			return
		case <-done:
			return
		case <-t.closeCh:
			cancel()
			_ = proc.Kill()
			return
		case event := <-t.events:
			switch event.Type {
			case session.RemoteEventStdin:
				if _, err := proc.Stdin().Write(event.Bytes); err != nil {
					_ = t.daemon.manager.SendRemoteError(context.Background(), t.connID, "remote stdin failed", err.Error())
					cancel()
					_ = proc.Kill()
					return
				}
			case session.RemoteEventSignal:
				if sig, ok := signalByName(event.Name); ok {
					_ = proc.Signal(sig)
				}
			case session.RemoteEventResize:
				if resizable, ok := proc.(interface{ Resize(int, int) error }); ok {
					_ = resizable.Resize(event.Rows, event.Cols)
				}
			case session.RemoteEventClose, session.RemoteEventError, session.RemoteEventSequenceGapTimeout, session.RemoteEventPeerTimeout:
				cancel()
				_ = proc.Kill()
				return
			}
		}
	}
}

func (t *remoteTask) copyOutput(ctx context.Context, wg *sync.WaitGroup, errCh chan<- error, r io.ReadCloser, typ protocol.FrameType) {
	defer wg.Done()
	if r == nil {
		return
	}
	defer r.Close()

	chunkSize := t.daemon.cfg.StreamChunkBytes
	if chunkSize <= 0 {
		chunkSize = config.DefaultStreamChunkBytes
	}
	buf := make([]byte, chunkSize)
	for {
		n, err := r.Read(buf)
		if n > 0 {
			data := append([]byte(nil), buf[:n]...)
			var sendErr error
			switch typ {
			case protocol.TypeStdout:
				sendErr = t.daemon.manager.SendRemoteStdout(context.Background(), t.connID, data)
			case protocol.TypeStderr:
				sendErr = t.daemon.manager.SendRemoteStderr(context.Background(), t.connID, data)
			}
			if sendErr != nil && !errors.Is(sendErr, session.ErrSessionNotFound) && ctx.Err() == nil {
				errCh <- sendErr
				return
			}
		}
		if err == nil {
			continue
		}
		if remoteOutputEOF(err) {
			return
		}
		if ctx.Err() == nil {
			errCh <- err
		}
		return
	}
}

func remoteOutputEOF(err error) bool {
	return err == nil ||
		errors.Is(err, io.EOF) ||
		errors.Is(err, os.ErrClosed) ||
		errors.Is(err, io.ErrClosedPipe) ||
		errors.Is(err, syscall.EPIPE)
}

func (t *remoteTask) sendStartError(ctx context.Context, err error) {
	var startErr *remoteexec.StartError
	if errors.As(err, &startErr) {
		detail := ""
		if startErr.Err != nil {
			detail = startErr.Err.Error()
		}
		_ = t.daemon.manager.SendRemoteError(ctx, t.connID, firstRemoteNonEmpty(startErr.Message, "remote command start failed"), detail)
		return
	}
	var ptyErr *pty.StartError
	if errors.As(err, &ptyErr) {
		detail := ""
		if ptyErr.Err != nil {
			detail = ptyErr.Err.Error()
		}
		_ = t.daemon.manager.SendRemoteError(ctx, t.connID, firstRemoteNonEmpty(ptyErr.Message, "remote pty start failed"), detail)
		return
	}
	_ = t.daemon.manager.SendRemoteError(ctx, t.connID, "remote command start failed", err.Error())
}

type ptyProcess struct {
	session *pty.Session
}

func (p ptyProcess) Stdin() io.WriteCloser {
	return p.session.Stdin()
}

func (p ptyProcess) Stdout() io.ReadCloser {
	return p.session.Stdout()
}

func (p ptyProcess) Stderr() io.ReadCloser {
	return nil
}

func (p ptyProcess) Wait() (remoteexec.Result, error) {
	result, err := p.session.Wait()
	return remoteexec.Result{ExitCode: result.ExitCode, Canceled: result.Canceled}, err
}

func (p ptyProcess) Signal(sig os.Signal) error {
	return p.session.Signal(sig)
}

func (p ptyProcess) Kill() error {
	return p.session.Kill()
}

func (p ptyProcess) Resize(rows, cols int) error {
	return p.session.Resize(rows, cols)
}

type remoteexecAdapter struct {
	executor *remoteexec.Executor
}

func (a remoteexecAdapter) Start(ctx context.Context, req remoteexec.Request) (RemoteProcess, error) {
	return a.executor.Start(ctx, req)
}

type replyOnlyOutboundSender struct {
	sender RemoteSender
}

func (s replyOnlyOutboundSender) SendRootMessage(context.Context, string, string, string) (outbound.RootMessage, error) {
	return outbound.RootMessage{}, errors.New("remote daemon cannot send root messages")
}

func (s replyOnlyOutboundSender) ReplyRootMessage(ctx context.Context, chatID, rootMessageID, mentionOpenID, text string) (string, error) {
	return s.sender.ReplyRootMessage(ctx, chatID, rootMessageID, mentionOpenID, text)
}

type remoteEventMeta struct {
	ignored      bool
	chatID       string
	senderOpenID string
	mentions     []lark.Mention
}

func parseRemoteEventMeta(data []byte) (remoteEventMeta, error) {
	var envelope struct {
		Type   string `json:"type"`
		Header struct {
			EventType string `json:"event_type"`
		} `json:"header"`
		Event struct {
			Sender struct {
				SenderID lark.Identifier `json:"sender_id"`
			} `json:"sender"`
			Message struct {
				ChatID      string `json:"chat_id"`
				MessageType string `json:"message_type"`
				Mentions    []struct {
					Key     string          `json:"key"`
					Name    string          `json:"name"`
					ID      lark.Identifier `json:"id"`
					OpenID  string          `json:"open_id"`
					UserID  string          `json:"user_id"`
					UnionID string          `json:"union_id"`
				} `json:"mentions"`
			} `json:"message"`
		} `json:"event"`
	}
	if err := json.Unmarshal(data, &envelope); err != nil {
		return remoteEventMeta{}, fmt.Errorf("decode remote lark event metadata: %w", err)
	}
	if envelope.Type == "url_verification" || envelope.Type == "challenge" {
		return remoteEventMeta{ignored: true}, nil
	}
	if envelope.Header.EventType != "" && envelope.Header.EventType != lark.MessageReceiveEventType {
		return remoteEventMeta{ignored: true}, nil
	}
	if envelope.Event.Message.MessageType != lark.MessageTypeText {
		return remoteEventMeta{ignored: true}, nil
	}

	mentions := make([]lark.Mention, 0, len(envelope.Event.Message.Mentions))
	for _, mention := range envelope.Event.Message.Mentions {
		mentions = append(mentions, lark.Mention{
			Key:     mention.Key,
			Name:    mention.Name,
			OpenID:  firstRemoteNonEmpty(mention.ID.OpenID, mention.OpenID),
			UserID:  firstRemoteNonEmpty(mention.ID.UserID, mention.UserID),
			UnionID: firstRemoteNonEmpty(mention.ID.UnionID, mention.UnionID),
		})
	}
	return remoteEventMeta{
		chatID:       envelope.Event.Message.ChatID,
		senderOpenID: envelope.Event.Sender.SenderID.OpenID,
		mentions:     mentions,
	}, nil
}

func normalizeRemoteConfig(cfg RemoteConfig) RemoteConfig {
	if cfg.MaxSessions <= 0 {
		cfg.MaxSessions = config.DefaultMaxSessions
	}
	if cfg.StreamChunkBytes <= 0 {
		cfg.StreamChunkBytes = config.DefaultStreamChunkBytes
	}
	if cfg.LarkTextRequestLimitBytes <= 0 {
		cfg.LarkTextRequestLimitBytes = config.DefaultLarkTextRequestLimitBytes
	}
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = config.DefaultHeartbeatInterval
	}
	if cfg.HeartbeatTimeout <= 0 {
		cfg.HeartbeatTimeout = config.DefaultHeartbeatTimeout
	}
	if cfg.SequenceGapTimeout <= 0 {
		cfg.SequenceGapTimeout = config.DefaultSequenceGapTimeout
	}
	return cfg
}

func hasStartFrame(frames []protocol.Frame) bool {
	for _, frame := range frames {
		if frame.Type == protocol.TypeStart {
			return true
		}
	}
	return false
}

func mentionsContain(mentions []lark.Mention, openID string) bool {
	openID = strings.TrimSpace(openID)
	for _, mention := range mentions {
		if strings.TrimSpace(mention.OpenID) == openID {
			return true
		}
	}
	return false
}

func signalByName(name string) (os.Signal, bool) {
	switch strings.ToUpper(strings.TrimPrefix(strings.TrimSpace(name), "SIG")) {
	case "HUP":
		return syscall.SIGHUP, true
	case "INT":
		return syscall.SIGINT, true
	case "QUIT":
		return syscall.SIGQUIT, true
	case "KILL":
		return syscall.SIGKILL, true
	case "TERM":
		return syscall.SIGTERM, true
	default:
		return nil, false
	}
}

func stringSet(values []string) map[string]struct{} {
	if len(values) == 0 {
		return nil
	}
	out := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			out[value] = struct{}{}
		}
	}
	return out
}

func firstRemoteNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func cloneStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func cloneStartPayload(in protocol.StartPayload) protocol.StartPayload {
	out := in
	out.Env = cloneStringMap(in.Env)
	return out
}
