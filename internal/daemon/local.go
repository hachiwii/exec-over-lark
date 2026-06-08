package daemon

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/hachiwii/exec-over-lark/internal/config"
	"github.com/hachiwii/exec-over-lark/internal/ipc"
	"github.com/hachiwii/exec-over-lark/internal/lark"
	"github.com/hachiwii/exec-over-lark/internal/outbound"
	"github.com/hachiwii/exec-over-lark/internal/protocol"
	"github.com/hachiwii/exec-over-lark/internal/session"
)

var (
	ErrIgnoredEvent    = errors.New("ignored daemon event")
	ErrLocalNotStarted = errors.New("local daemon is not started")
)

type ConfigLoader func(path string) (*config.Config, error)

type LarkClient interface {
	BotOpenID(ctx context.Context) (string, error)
	SendRootMessage(ctx context.Context, chatID, mentionOpenID, text string) (lark.RootMessage, error)
	ReplyRootMessage(ctx context.Context, chatID, rootMessageID, mentionOpenID, text string) (string, error)
}

type LarkFactory func(cfg *config.Config) (LarkClient, error)

type EventHandler func(ctx context.Context, event lark.MessageEvent) error

type EventSource interface {
	Run(ctx context.Context, selfBotOpenID string, handler EventHandler) error
}

type EventSourceFunc func(ctx context.Context, selfBotOpenID string, handler EventHandler) error

func (f EventSourceFunc) Run(ctx context.Context, selfBotOpenID string, handler EventHandler) error {
	return f(ctx, selfBotOpenID, handler)
}

type NoopEventSource struct{}

func (NoopEventSource) Run(ctx context.Context, _ string, _ EventHandler) error {
	<-ctx.Done()
	return ctx.Err()
}

type IPCServer interface {
	Serve() error
	Close() error
	SocketPath() string
}

type IPCListenFunc func(socketPath string, handler ipc.Handler) (IPCServer, error)

type LocalOptions struct {
	Config       *config.Config
	ConfigPath   string
	ConfigLoader ConfigLoader

	LarkClient  LarkClient
	LarkFactory LarkFactory

	EventSource EventSource
	IPCListen   IPCListenFunc
	Logger      *slog.Logger
	Outbound    *outbound.Manager

	TickInterval time.Duration
}

type Local struct {
	cfg         *config.Config
	client      LarkClient
	eventSource EventSource
	ipcListen   IPCListenFunc
	logger      *slog.Logger

	tickInterval time.Duration

	outbound *outbound.Manager
	sessions *session.Manager

	selfBotOpenID string
	ipcServer     IPCServer

	mu      sync.RWMutex
	started bool
}

func NewLocal(opts LocalOptions) (*Local, error) {
	loader := opts.ConfigLoader
	if loader == nil {
		loader = config.Load
	}

	cfg := opts.Config
	if cfg == nil {
		loaded, err := loader(opts.ConfigPath)
		if err != nil {
			return nil, err
		}
		cfg = loaded
	} else {
		config.ApplyDefaults(cfg)
		if err := config.Validate(cfg); err != nil {
			return nil, err
		}
	}

	client := opts.LarkClient
	if client == nil {
		factory := opts.LarkFactory
		if factory == nil {
			factory = defaultLarkFactory
		}
		created, err := factory(cfg)
		if err != nil {
			return nil, err
		}
		client = created
	}
	if client == nil {
		return nil, errors.New("lark client is nil")
	}

	eventSource := opts.EventSource
	if eventSource == nil {
		eventSource = NoopEventSource{}
	}

	ipcListen := opts.IPCListen
	if ipcListen == nil {
		ipcListen = defaultIPCListen
	}

	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	if opts.Outbound == nil {
		return nil, errors.New("outbound manager is nil")
	}

	tickInterval := opts.TickInterval
	if tickInterval <= 0 {
		tickInterval = time.Second
	}
	sessions := session.New(
		session.WithOutbound(opts.Outbound),
		session.WithHeartbeatInterval(cfg.Connection.HeartbeatInterval.Duration()),
		session.WithHeartbeatTimeout(cfg.Connection.HeartbeatTimeout.Duration()),
		session.WithSequenceGapTimeout(cfg.Connection.SequenceGapTimeout.Duration()),
		session.WithMaxRemoteSessions(cfg.Exec.MaxSessions),
	)

	return &Local{
		cfg:          cfg,
		client:       client,
		eventSource:  eventSource,
		ipcListen:    ipcListen,
		logger:       logger,
		tickInterval: tickInterval,
		outbound:     opts.Outbound,
		sessions:     sessions,
	}, nil
}

func (d *Local) Run(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}

	selfOpenID, err := d.client.BotOpenID(ctx)
	if err != nil {
		return fmt.Errorf("resolve self bot open_id: %w", err)
	}
	return d.run(ctx, selfOpenID, true)
}

func (d *Local) RunServices(ctx context.Context, selfOpenID string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	selfOpenID = strings.TrimSpace(selfOpenID)
	if selfOpenID == "" {
		return errors.New("local self bot open_id is required")
	}
	return d.run(ctx, selfOpenID, false)
}

func (d *Local) run(ctx context.Context, selfOpenID string, runEventSource bool) error {
	d.mu.Lock()
	if d.started {
		d.mu.Unlock()
		return errors.New("local daemon is already running")
	}
	d.selfBotOpenID = selfOpenID
	d.started = true
	d.mu.Unlock()
	defer func() {
		d.mu.Lock()
		d.started = false
		d.ipcServer = nil
		d.mu.Unlock()
	}()

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	errCh := make(chan error, 4)
	var wg sync.WaitGroup

	if d.cfg.IPC.Enabled {
		socketPath, err := config.ExpandUserPath(d.cfg.IPC.SocketPath)
		if err != nil {
			return err
		}
		server, err := d.ipcListen(socketPath, d)
		if err != nil {
			return err
		}
		d.mu.Lock()
		d.ipcServer = server
		d.mu.Unlock()

		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := server.Serve(); err != nil && runCtx.Err() == nil {
				errCh <- err
			}
		}()
	}

	if runEventSource {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := d.eventSource.Run(runCtx, selfOpenID, d.HandleLarkEvent); err != nil && !errors.Is(err, context.Canceled) && runCtx.Err() == nil {
				errCh <- err
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := d.tickLoop(runCtx); err != nil && !errors.Is(err, context.Canceled) && runCtx.Err() == nil {
			errCh <- err
		}
	}()

	var runErr error
	select {
	case <-ctx.Done():
		runErr = ctx.Err()
	case runErr = <-errCh:
	}
	cancel()

	if d.ipcServer != nil {
		_ = d.ipcServer.Close()
	}
	wg.Wait()

	if errors.Is(runErr, context.Canceled) {
		return nil
	}
	return runErr
}

func (d *Local) StartSession(ctx context.Context, sess *ipc.Session, req ipc.StartSessionRequest) error {
	_, err := d.StartLocalSession(ctx, req, ipcSubscriber{session: sess})
	return err
}

func (d *Local) Stdin(ctx context.Context, req ipc.StdinRequest) error {
	return d.sessions.SendLocalStdin(ctx, req.RequestID, req.Bytes)
}

func (d *Local) Resize(ctx context.Context, req ipc.ResizeRequest) error {
	return d.sessions.SendLocalResize(ctx, req.RequestID, req.Rows, req.Cols)
}

func (d *Local) Signal(ctx context.Context, req ipc.SignalRequest) error {
	return d.sessions.SendLocalSignal(ctx, req.RequestID, req.Name)
}

func (d *Local) Close(ctx context.Context, req ipc.CloseRequest) error {
	return d.sessions.CloseLocal(ctx, req.RequestID, req.Reason)
}

func (d *Local) Status(_ context.Context, _ ipc.StatusRequest) (ipc.DaemonStatus, error) {
	d.mu.RLock()
	started := d.started
	selfBotOpenID := d.selfBotOpenID
	socketPath := ""
	if d.ipcServer != nil {
		socketPath = d.ipcServer.SocketPath()
	}
	d.mu.RUnlock()

	return ipc.DaemonStatus{
		Running:       started,
		SocketPath:    socketPath,
		SelfBotOpenID: selfBotOpenID,
		Event: ipc.EventConnectionStatus{
			Checked:   true,
			Connected: started,
		},
		Outbound: outboundStatusFromManager(d.outbound),
	}, nil
}

func (d *Local) StartLocalSession(ctx context.Context, req ipc.StartSessionRequest, subscriber session.Subscriber) (string, error) {
	hostName := strings.TrimSpace(req.Host)
	if hostName == "" {
		hostName = strings.TrimSpace(d.cfg.DefaultHost)
	}
	if hostName == "" {
		return "", ipc.NewRPCError(ipc.ErrorCodeBadRequest, "host is required", "")
	}

	host, ok := d.cfg.Hosts[hostName]
	if !ok {
		return "", ipc.NewRPCError(ipc.ErrorCodeBadRequest, "unknown host", hostName)
	}
	if strings.TrimSpace(req.RequestID) == "" {
		return "", ipc.NewRPCError(ipc.ErrorCodeBadRequest, "request_id is required", "")
	}
	host = mergeIPCStartHost(host, req.HostConfig)

	cwd := firstLocalNonEmpty(req.Cwd, host.DefaultCWD)
	shell := firstLocalNonEmpty(req.Shell, host.Shell)
	startPayload := protocol.StartPayload{
		Cmd:       req.Cmd,
		Pty:       req.Pty,
		Cwd:       cwd,
		Env:       cloneEnv(req.Env),
		Shell:     shell,
		Rows:      req.Rows,
		Cols:      req.Cols,
		Heartbeat: d.sessions.HeartbeatConfig(),
	}
	root, err := d.outbound.OpenRoot(ctx, outbound.OpenRootRequest{
		ChatID:            host.ChatID,
		MentionOpenID:     host.PeerBotOpenID,
		Role:              outbound.RoleLocal,
		RequestID:         req.RequestID,
		InitialType:       protocol.TypeStart,
		InitialPayload:    startPayload,
		HeartbeatInterval: d.cfg.Connection.HeartbeatInterval.Duration(),
	})
	if err != nil {
		return "", fmt.Errorf("send start root message: %w", err)
	}
	connID := strings.TrimSpace(root.RootMessageID)
	if connID == "" {
		return "", errors.New("lark root message response missing message_id")
	}

	if err := d.sessions.RegisterLocal(session.LocalStart{
		RequestID:     req.RequestID,
		Host:          hostName,
		ConnID:        connID,
		RootMessageID: connID,
		ChatID:        host.ChatID,
		PeerBotOpenID: host.PeerBotOpenID,
		NextSendSeq:   2,
	}, subscriber); err != nil {
		return "", err
	}
	return connID, nil
}

func (d *Local) HandleLarkEvent(ctx context.Context, event lark.MessageEvent) error {
	err := d.sessions.ReceiveLocal(ctx, session.InboundMessage{
		ConnID:        session.ConnID(event.RootMessageID, event.MessageID),
		RootMessageID: event.RootMessageID,
		MessageID:     event.MessageID,
		ChatID:        event.ChatID,
		SenderOpenID:  event.SenderOpenID,
		IsRoot:        event.RootMessageID == event.MessageID,
		Frames:        cloneFrames(event.Frames),
	})
	if errors.Is(err, session.ErrSessionNotFound) || errors.Is(err, session.ErrUnauthorizedPeer) {
		return ErrIgnoredEvent
	}
	return err
}

func (d *Local) SelfBotOpenID() string {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.selfBotOpenID
}

func (d *Local) ConnIDForRequest(requestID string) (string, bool) {
	return d.sessions.ConnIDForRequest(requestID)
}

func (d *Local) LocalSessions() []session.Snapshot {
	return d.sessions.LocalSessions()
}

func (d *Local) tickLoop(ctx context.Context) error {
	ticker := time.NewTicker(d.tickInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := d.sessions.Tick(ctx); err != nil {
				d.logger.Debug("local session tick returned error", "error", err)
			}
		}
	}
}

type ipcSubscriber struct {
	session *ipc.Session
}

func (s ipcSubscriber) Deliver(ctx context.Context, event session.LocalEvent) error {
	if s.session == nil {
		return nil
	}
	switch event.Type {
	case session.LocalEventStartAck:
		return s.session.SendStartAck(ctx, event.RequestID)
	case session.LocalEventStdout:
		return s.session.SendStdout(ctx, event.RequestID, event.Bytes)
	case session.LocalEventStderr:
		return s.session.SendStderr(ctx, event.RequestID, event.Bytes)
	case session.LocalEventExit:
		return s.session.SendExit(ctx, event.RequestID, event.Code)
	case session.LocalEventError:
		return s.session.SendError(ctx, event.RequestID, ipc.NewRPCError(ipc.ErrorCodeProtocol, firstLocalNonEmpty(event.Message, "remote error"), event.Detail))
	case session.LocalEventClose:
		return s.session.SendError(ctx, event.RequestID, ipc.NewRPCError(ipc.ErrorCodeCanceled, firstLocalNonEmpty(event.Message, "remote closed session"), event.Detail))
	default:
		return s.session.SendError(ctx, event.RequestID, ipc.NewRPCError(ipc.ErrorCodeProtocol, "unknown local event", string(event.Type)))
	}
}

func defaultLarkFactory(cfg *config.Config) (LarkClient, error) {
	return lark.NewClient(lark.ClientConfig{
		AppID:     cfg.Lark.AppID,
		AppSecret: cfg.Lark.AppSecret,
	})
}

func defaultIPCListen(socketPath string, handler ipc.Handler) (IPCServer, error) {
	return ipc.Listen(socketPath, handler)
}

func firstLocalNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func mergeIPCStartHost(base config.HostConfig, override ipc.HostConfig) config.HostConfig {
	if strings.TrimSpace(override.ChatID) != "" {
		base.ChatID = override.ChatID
	}
	if strings.TrimSpace(override.PeerBotOpenID) != "" {
		base.PeerBotOpenID = override.PeerBotOpenID
	}
	if strings.TrimSpace(override.Shell) != "" {
		base.Shell = override.Shell
	}
	if strings.TrimSpace(override.DefaultCWD) != "" {
		base.DefaultCWD = override.DefaultCWD
	}
	if override.StreamChunkBytes > 0 {
		base.StreamChunkBytes = override.StreamChunkBytes
	}
	return base
}

func outboundStatusFromManager(manager *outbound.Manager) ipc.OutboundQueueStatus {
	if manager == nil {
		return ipc.OutboundQueueStatus{}
	}
	status := manager.Status()
	out := ipc.OutboundQueueStatus{
		Checked:        true,
		PendingFrames:  status.PendingFrames,
		PendingTargets: make([]ipc.OutboundTarget, 0, len(status.PendingTargets)),
		LastSentAt:     status.LastAttemptAt,
		HasLastSent:    status.HasLastAttempt,
		NextFlushAt:    status.NextFlushAt,
		HasNextFlush:   status.HasNextFlush,
	}
	for _, target := range status.PendingTargets {
		out.PendingTargets = append(out.PendingTargets, ipc.OutboundTarget{
			ChatID:        target.ChatID,
			RootMessageID: target.RootMessageID,
			MentionOpenID: target.MentionOpenID,
		})
	}
	return out
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

func cloneFrames(frames []protocol.Frame) []protocol.Frame {
	if len(frames) == 0 {
		return nil
	}
	out := make([]protocol.Frame, len(frames))
	for i, frame := range frames {
		out[i] = protocol.Frame{
			Seq:     frame.Seq,
			Type:    frame.Type,
			Payload: append([]byte(nil), frame.Payload...),
		}
	}
	return out
}
