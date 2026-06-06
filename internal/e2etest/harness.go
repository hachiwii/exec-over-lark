package e2etest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/hachiwii/exec-over-lark/internal/bootstrap"
	"github.com/hachiwii/exec-over-lark/internal/config"
	"github.com/hachiwii/exec-over-lark/internal/daemon"
	"github.com/hachiwii/exec-over-lark/internal/ipc"
	"github.com/hachiwii/exec-over-lark/internal/lark"
	"github.com/hachiwii/exec-over-lark/internal/protocol"
)

const (
	DefaultHostName        = "macmini"
	DefaultChatID          = "oc_e2e"
	DefaultClientBotOpenID = "ou_e2e_client_bot"
	DefaultServerBotOpenID = "ou_e2e_server_bot"
)

type Options struct {
	RepoRoot string

	HostName        string
	ChatID          string
	ClientBotOpenID string
	ServerBotOpenID string
	SocketPath      string

	LocalConfig  *config.Config
	RemoteConfig daemon.RemoteConfig

	RemoteExecutor daemon.RemoteExecutor

	LocalTickInterval  time.Duration
	LocalFlushInterval time.Duration
	DeliveryDelay      time.Duration
}

type Harness struct {
	repoRoot string
	tempDir  string

	hostName        string
	chatID          string
	clientBotOpenID string
	serverBotOpenID string
	socketPath      string

	localCfg       *config.Config
	remoteCfg      daemon.RemoteConfig
	remoteExec     daemon.RemoteExecutor
	localTick      time.Duration
	localFlush     time.Duration
	fakeLark       *FakeLark
	localSource    *localEventSource
	remoteReader   *io.PipeReader
	remoteWriter   *io.PipeWriter
	localDaemon    *daemon.Local
	remoteDaemon   *daemon.RemoteDaemon
	requestCounter atomic.Uint64

	ctx    context.Context
	cancel context.CancelFunc

	mu         sync.Mutex
	localDone  chan error
	remoteDone chan error
	cliPath    string
	closed     bool
}

type RunRequest struct {
	Host      string
	Command   string
	Stdin     []byte
	Pty       bool
	Cwd       string
	Shell     string
	RequestID string
}

type RunResult struct {
	RequestID    string
	Stdout       []byte
	Stderr       []byte
	ExitCode     int
	ErrorCode    string
	ErrorMessage string
	ErrorDetail  string
}

type MessageRecord struct {
	MessageID     string
	RootMessageID string
	ChatID        string
	SenderOpenID  string
	MentionOpenID string
	Text          string
	Frames        []protocol.Frame
	IsRoot        bool
}

type BootstrapRecord struct {
	ChatID       string
	SenderOpenID string
	Text         string
}

func NewHarness(opts Options) (*Harness, error) {
	hostName := firstNonEmpty(opts.HostName, DefaultHostName)
	chatID := firstNonEmpty(opts.ChatID, DefaultChatID)
	clientOpenID := firstNonEmpty(opts.ClientBotOpenID, DefaultClientBotOpenID)
	serverOpenID := firstNonEmpty(opts.ServerBotOpenID, DefaultServerBotOpenID)

	tempDir := ""
	socketPath := strings.TrimSpace(opts.SocketPath)
	if socketPath == "" {
		dir, err := os.MkdirTemp("", "exec-over-lark-e2e-*")
		if err != nil {
			return nil, err
		}
		tempDir = dir
		if err := os.Chmod(dir, 0o700); err != nil {
			_ = os.RemoveAll(dir)
			return nil, err
		}
		socketPath = filepath.Join(dir, "elarkd.sock")
	} else if err := os.MkdirAll(filepath.Dir(socketPath), 0o700); err != nil {
		return nil, err
	}

	repoRoot := strings.TrimSpace(opts.RepoRoot)
	if repoRoot == "" {
		var err error
		repoRoot, err = findRepoRoot()
		if err != nil {
			if tempDir != "" {
				_ = os.RemoveAll(tempDir)
			}
			return nil, err
		}
	}

	localCfg := opts.LocalConfig
	if localCfg == nil {
		localCfg = defaultLocalConfig(hostName, chatID, clientOpenID, serverOpenID, socketPath)
	} else {
		localCfg = cloneConfig(localCfg)
		if localCfg.IPC.SocketPath == "" {
			localCfg.IPC.SocketPath = socketPath
		}
	}

	remoteCfg := opts.RemoteConfig
	if !remoteCfg.ExecEnabled {
		remoteCfg.ExecEnabled = true
	}
	if remoteCfg.DefaultShell == "" {
		remoteCfg.DefaultShell = "/bin/sh"
	}
	if remoteCfg.MaxSessions == 0 {
		remoteCfg.MaxSessions = config.DefaultMaxSessions
	}
	if remoteCfg.StreamChunkBytes == 0 {
		remoteCfg.StreamChunkBytes = config.DefaultStreamChunkBytes
	}
	if remoteCfg.LarkTextRequestLimitBytes == 0 {
		remoteCfg.LarkTextRequestLimitBytes = config.DefaultLarkTextRequestLimitBytes
	}
	if len(remoteCfg.AllowedChatIDs) == 0 {
		remoteCfg.AllowedChatIDs = []string{chatID}
	}
	if len(remoteCfg.AllowedSenderOpenIDs) == 0 {
		remoteCfg.AllowedSenderOpenIDs = []string{clientOpenID}
	}

	deliveryDelay := opts.DeliveryDelay
	if deliveryDelay == 0 {
		deliveryDelay = time.Millisecond
	}

	ctx, cancel := context.WithCancel(context.Background())
	fakeLark := NewFakeLark(FakeLarkOptions{
		ClientBotOpenID: clientOpenID,
		ServerBotOpenID: serverOpenID,
		DeliveryDelay:   deliveryDelay,
	})

	h := &Harness{
		repoRoot:        repoRoot,
		tempDir:         tempDir,
		hostName:        hostName,
		chatID:          chatID,
		clientBotOpenID: clientOpenID,
		serverBotOpenID: serverOpenID,
		socketPath:      socketPath,
		localCfg:        localCfg,
		remoteCfg:       remoteCfg,
		remoteExec:      opts.RemoteExecutor,
		localTick:       opts.LocalTickInterval,
		localFlush:      opts.LocalFlushInterval,
		fakeLark:        fakeLark,
		ctx:             ctx,
		cancel:          cancel,
	}
	return h, nil
}

func (h *Harness) Start(ctx context.Context) error {
	if err := h.StartRemote(ctx); err != nil {
		return err
	}
	return h.StartLocal(ctx)
}

func (h *Harness) StartLocal(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	h.mu.Lock()
	if h.localDone != nil {
		h.mu.Unlock()
		return nil
	}
	source := &localEventSource{
		bus:   h.fakeLark,
		ready: make(chan struct{}),
	}
	local, err := daemon.NewLocal(daemon.LocalOptions{
		Config:        cloneConfig(h.localCfg),
		LarkClient:    h.fakeLark.Client(h.clientBotOpenID),
		EventSource:   source,
		TickInterval:  h.localTick,
		FlushInterval: h.localFlush,
	})
	if err != nil {
		h.mu.Unlock()
		return err
	}
	h.localSource = source
	h.localDaemon = local
	h.localDone = make(chan error, 1)
	h.mu.Unlock()

	go func() {
		h.localDone <- local.Run(h.ctx)
	}()

	select {
	case <-source.ready:
	case err := <-h.localDone:
		return fmt.Errorf("local daemon exited before event source was ready: %w", err)
	case <-ctx.Done():
		return ctx.Err()
	}
	if err := waitForSocket(ctx, h.socketPath); err != nil {
		return err
	}
	return nil
}

func (h *Harness) StartRemote(ctx context.Context) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.remoteDone != nil {
		return nil
	}
	reader, writer := io.Pipe()
	h.remoteReader = reader
	h.remoteWriter = writer
	h.fakeLark.SetRemoteWriter(writer)

	remote, err := daemon.NewRemoteDaemon(daemon.RemoteOptions{
		Config:        h.remoteCfg,
		EventStream:   reader,
		SelfBotOpenID: h.serverBotOpenID,
		Sender:        h.fakeLark.RemoteSender(h.serverBotOpenID),
		Executor:      h.remoteExec,
	})
	if err != nil {
		return err
	}
	h.remoteDaemon = remote
	h.remoteDone = make(chan error, 1)

	go func() {
		h.remoteDone <- remote.Run(h.ctx)
	}()
	return nil
}

func (h *Harness) Close() error {
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return nil
	}
	h.closed = true
	cancel := h.cancel
	localDone := h.localDone
	remoteDone := h.remoteDone
	remoteReader := h.remoteReader
	remoteWriter := h.remoteWriter
	tempDir := h.tempDir
	h.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	h.fakeLark.Close()
	if remoteWriter != nil {
		_ = remoteWriter.Close()
	}
	if remoteReader != nil {
		_ = remoteReader.Close()
	}

	var errOut error
	errOut = firstErr(errOut, waitRun("local daemon", localDone))
	errOut = firstErr(errOut, waitRun("remote daemon", remoteDone))
	if tempDir != "" {
		errOut = firstErr(errOut, os.RemoveAll(tempDir))
	}
	return errOut
}

func (h *Harness) RunIPC(ctx context.Context, req RunRequest) (RunResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	requestID := strings.TrimSpace(req.RequestID)
	if requestID == "" {
		requestID = h.nextRequestID()
	}
	host := firstNonEmpty(req.Host, h.hostName)

	client, err := ipc.Dial(ctx, h.socketPath)
	if err != nil {
		return RunResult{RequestID: requestID}, err
	}
	defer client.Close()

	if err := client.StartSession(ctx, ipc.StartSessionRequest{
		RequestID: requestID,
		Host:      host,
		Cmd:       req.Command,
		Pty:       req.Pty,
		Cwd:       req.Cwd,
		Shell:     req.Shell,
	}); err != nil {
		return RunResult{RequestID: requestID}, err
	}
	if len(req.Stdin) > 0 {
		if err := client.SendStdin(ctx, requestID, req.Stdin); err != nil {
			return RunResult{RequestID: requestID}, err
		}
	}

	result := RunResult{RequestID: requestID, ExitCode: -1}
	for {
		msg, err := client.Receive(ctx)
		if err != nil {
			return result, err
		}
		if msg.RequestID != "" && msg.RequestID != requestID {
			continue
		}
		switch msg.Type {
		case ipc.TypeStdout:
			result.Stdout = append(result.Stdout, msg.Bytes...)
		case ipc.TypeStderr:
			result.Stderr = append(result.Stderr, msg.Bytes...)
		case ipc.TypeExit:
			result.ExitCode = msg.Code
			return result, nil
		case ipc.TypeError:
			result.ErrorCode = msg.ErrorCode
			result.ErrorMessage = msg.Message
			result.ErrorDetail = msg.Detail
			return result, nil
		default:
			return result, fmt.Errorf("unexpected ipc message type %q", msg.Type)
		}
	}
}

func (h *Harness) RunCLI(ctx context.Context, req RunRequest) (RunResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	cliPath, err := h.ensureCLIBinary(ctx)
	if err != nil {
		return RunResult{}, err
	}

	host := firstNonEmpty(req.Host, h.hostName)
	args := []string{"--socket", h.socketPath}
	if req.Cwd != "" {
		args = append(args, "--cwd", req.Cwd)
	}
	if req.Pty {
		args = append(args, "-t")
	}
	args = append(args, host)
	if req.Command != "" {
		args = append(args, req.Command)
	}

	cmd := exec.CommandContext(ctx, cliPath, args...)
	cmd.Dir = h.repoRoot
	cmd.Stdin = bytes.NewReader(req.Stdin)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err = cmd.Run()
	result := RunResult{
		Stdout:   stdout.Bytes(),
		Stderr:   stderr.Bytes(),
		ExitCode: exitStatus(err),
	}
	if err != nil && result.ExitCode < 0 {
		return result, err
	}
	return result, nil
}

func (h *Harness) BootstrapServer(ctx context.Context, chatID string) error {
	if chatID == "" {
		chatID = h.chatID
	}
	return bootstrap.HandleAddedToChatEvent(ctx, h.fakeLark.BootstrapSender(h.serverBotOpenID), bootstrap.AddedToChatEvent{
		EventType:      bootstrap.BotAddedEventType,
		ChatID:         chatID,
		AddedBotOpenID: h.serverBotOpenID,
	}, h.serverBotOpenID)
}

func (h *Harness) Messages() []MessageRecord {
	return h.fakeLark.Messages()
}

func (h *Harness) BootstrapMessages() []BootstrapRecord {
	return h.fakeLark.BootstrapMessages()
}

func (h *Harness) SocketPath() string {
	return h.socketPath
}

func (h *Harness) nextRequestID() string {
	return fmt.Sprintf("e2e-%d", h.requestCounter.Add(1))
}

func (h *Harness) ensureCLIBinary(ctx context.Context) (string, error) {
	h.mu.Lock()
	if h.cliPath != "" {
		path := h.cliPath
		h.mu.Unlock()
		return path, nil
	}
	buildDir := h.tempDir
	if buildDir == "" {
		var err error
		buildDir, err = os.MkdirTemp("", "exec-over-lark-e2e-cli-*")
		if err != nil {
			h.mu.Unlock()
			return "", err
		}
	}
	cliPath := filepath.Join(buildDir, "elark")
	h.mu.Unlock()

	cmd := exec.CommandContext(ctx, "go", "build", "-o", cliPath, "./cmd/elark")
	cmd.Dir = h.repoRoot
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("build elark CLI: %w: %s", err, strings.TrimSpace(string(out)))
	}

	h.mu.Lock()
	if h.cliPath == "" {
		h.cliPath = cliPath
	}
	path := h.cliPath
	h.mu.Unlock()
	return path, nil
}

type FakeLarkOptions struct {
	ClientBotOpenID string
	ServerBotOpenID string
	DeliveryDelay   time.Duration
}

type FakeLark struct {
	clientBotOpenID string
	serverBotOpenID string
	deliveryDelay   time.Duration

	ctx    context.Context
	cancel context.CancelFunc

	deliverCh chan MessageRecord

	mu                 sync.Mutex
	nextMessage        uint64
	messages           []MessageRecord
	bootstrapMessages  []BootstrapRecord
	localSelfOpenID    string
	localHandler       daemon.EventHandler
	localHandlerCtx    context.Context
	remoteWriter       *io.PipeWriter
	deliveryErrs       []error
	dispatcherFinished chan struct{}
}

func NewFakeLark(opts FakeLarkOptions) *FakeLark {
	clientOpenID := firstNonEmpty(opts.ClientBotOpenID, DefaultClientBotOpenID)
	serverOpenID := firstNonEmpty(opts.ServerBotOpenID, DefaultServerBotOpenID)
	ctx, cancel := context.WithCancel(context.Background())
	bus := &FakeLark{
		clientBotOpenID:    clientOpenID,
		serverBotOpenID:    serverOpenID,
		deliveryDelay:      opts.DeliveryDelay,
		ctx:                ctx,
		cancel:             cancel,
		deliverCh:          make(chan MessageRecord, 256),
		dispatcherFinished: make(chan struct{}),
	}
	go bus.dispatchLoop()
	return bus
}

func (b *FakeLark) Client(openID string) *BotClient {
	return &BotClient{bus: b, openID: strings.TrimSpace(openID)}
}

func (b *FakeLark) RemoteSender(openID string) *BotClient {
	return b.Client(openID)
}

func (b *FakeLark) BootstrapSender(openID string) *BootstrapSender {
	return &BootstrapSender{bus: b, openID: strings.TrimSpace(openID)}
}

func (b *FakeLark) Close() {
	b.cancel()
	<-b.dispatcherFinished
}

func (b *FakeLark) RegisterLocalHandler(ctx context.Context, selfOpenID string, handler daemon.EventHandler) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.localSelfOpenID = strings.TrimSpace(selfOpenID)
	b.localHandler = handler
	b.localHandlerCtx = ctx
}

func (b *FakeLark) SetRemoteWriter(writer *io.PipeWriter) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.remoteWriter = writer
}

func (b *FakeLark) Messages() []MessageRecord {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]MessageRecord, len(b.messages))
	for i, msg := range b.messages {
		out[i] = cloneMessageRecord(msg)
	}
	return out
}

func (b *FakeLark) BootstrapMessages() []BootstrapRecord {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]BootstrapRecord(nil), b.bootstrapMessages...)
}

func (b *FakeLark) sendRoot(ctx context.Context, senderOpenID, chatID, mentionOpenID, text string) (lark.RootMessage, error) {
	msg := b.newMessage(senderOpenID, chatID, "", mentionOpenID, text)
	if err := b.enqueue(ctx, msg); err != nil {
		return lark.RootMessage{}, err
	}
	return lark.RootMessage{MessageID: msg.MessageID}, nil
}

func (b *FakeLark) sendReply(ctx context.Context, senderOpenID, chatID, rootMessageID, mentionOpenID, text string) (string, error) {
	msg := b.newMessage(senderOpenID, chatID, rootMessageID, mentionOpenID, text)
	if err := b.enqueue(ctx, msg); err != nil {
		return "", err
	}
	return msg.MessageID, nil
}

func (b *FakeLark) recordBootstrap(senderOpenID, chatID, text string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.bootstrapMessages = append(b.bootstrapMessages, BootstrapRecord{
		ChatID:       chatID,
		SenderOpenID: senderOpenID,
		Text:         text,
	})
}

func (b *FakeLark) newMessage(senderOpenID, chatID, rootMessageID, mentionOpenID, text string) MessageRecord {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.nextMessage++
	messageID := fmt.Sprintf("om_e2e_%06d", b.nextMessage)
	frames, _ := protocol.DecodeFrames(text)
	msg := MessageRecord{
		MessageID:     messageID,
		RootMessageID: rootMessageID,
		ChatID:        chatID,
		SenderOpenID:  senderOpenID,
		MentionOpenID: mentionOpenID,
		Text:          text,
		Frames:        cloneFrames(frames),
		IsRoot:        strings.TrimSpace(rootMessageID) == "",
	}
	if msg.IsRoot {
		msg.RootMessageID = messageID
	}
	b.messages = append(b.messages, cloneMessageRecord(msg))
	return msg
}

func (b *FakeLark) enqueue(ctx context.Context, msg MessageRecord) error {
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-b.ctx.Done():
		return b.ctx.Err()
	case b.deliverCh <- cloneMessageRecord(msg):
		return nil
	}
}

func (b *FakeLark) dispatchLoop() {
	defer close(b.dispatcherFinished)
	for {
		select {
		case <-b.ctx.Done():
			return
		case msg := <-b.deliverCh:
			if b.deliveryDelay > 0 {
				timer := time.NewTimer(b.deliveryDelay)
				select {
				case <-b.ctx.Done():
					timer.Stop()
					return
				case <-timer.C:
				}
			}
			if err := b.deliver(msg); err != nil {
				b.mu.Lock()
				b.deliveryErrs = append(b.deliveryErrs, err)
				b.mu.Unlock()
			}
		}
	}
}

func (b *FakeLark) deliver(msg MessageRecord) error {
	switch strings.TrimSpace(msg.MentionOpenID) {
	case b.serverBotOpenID:
		return b.deliverRemote(msg)
	case b.clientBotOpenID:
		return b.deliverLocal(msg)
	default:
		return nil
	}
}

func (b *FakeLark) deliverRemote(msg MessageRecord) error {
	b.mu.Lock()
	writer := b.remoteWriter
	b.mu.Unlock()
	if writer == nil {
		return nil
	}
	raw, err := larkEventJSON(msg)
	if err != nil {
		return err
	}
	_, err = writer.Write(append(raw, '\n'))
	return err
}

func (b *FakeLark) deliverLocal(msg MessageRecord) error {
	b.mu.Lock()
	selfOpenID := b.localSelfOpenID
	handler := b.localHandler
	ctx := b.localHandlerCtx
	b.mu.Unlock()
	if handler == nil {
		return nil
	}
	raw, err := larkEventJSON(msg)
	if err != nil {
		return err
	}
	event, err := lark.ParseMessageReceiveEvent(raw, selfOpenID)
	if errors.Is(err, lark.ErrIgnoredEvent) {
		return nil
	}
	if err != nil {
		return err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	err = handler(ctx, event)
	if errors.Is(err, daemon.ErrIgnoredEvent) || errors.Is(err, lark.ErrIgnoredEvent) {
		return nil
	}
	return err
}

type BotClient struct {
	bus    *FakeLark
	openID string
}

func (c *BotClient) BotOpenID(context.Context) (string, error) {
	return c.openID, nil
}

func (c *BotClient) SendRootMessage(ctx context.Context, chatID, mentionOpenID, text string) (lark.RootMessage, error) {
	return c.bus.sendRoot(ctx, c.openID, chatID, mentionOpenID, text)
}

func (c *BotClient) ReplyRootMessage(ctx context.Context, chatID, rootMessageID, mentionOpenID, text string) (string, error) {
	return c.bus.sendReply(ctx, c.openID, chatID, rootMessageID, mentionOpenID, text)
}

type BootstrapSender struct {
	bus    *FakeLark
	openID string
}

func (s *BootstrapSender) SendTextMessage(_ context.Context, chatID, text string) error {
	s.bus.recordBootstrap(s.openID, chatID, text)
	return nil
}

type localEventSource struct {
	bus   *FakeLark
	ready chan struct{}
	once  sync.Once
}

func (s *localEventSource) Run(ctx context.Context, selfBotOpenID string, handler daemon.EventHandler) error {
	s.bus.RegisterLocalHandler(ctx, selfBotOpenID, handler)
	s.once.Do(func() { close(s.ready) })
	<-ctx.Done()
	return ctx.Err()
}

func larkEventJSON(msg MessageRecord) ([]byte, error) {
	rootID := ""
	if !msg.IsRoot {
		rootID = msg.RootMessageID
	}
	content, err := lark.TextContent(lark.BuildMentionedText(msg.MentionOpenID, msg.Text))
	if err != nil {
		return nil, err
	}
	return json.Marshal(map[string]any{
		"schema": "2.0",
		"header": map[string]any{
			"event_id":   "evt_" + msg.MessageID,
			"event_type": lark.MessageReceiveEventType,
		},
		"event": map[string]any{
			"sender": map[string]any{
				"sender_type": "app",
				"sender_id": map[string]any{
					"open_id": msg.SenderOpenID,
				},
			},
			"message": map[string]any{
				"message_id":   msg.MessageID,
				"root_id":      rootID,
				"chat_id":      msg.ChatID,
				"message_type": lark.MessageTypeText,
				"content":      content,
				"mentions": []map[string]any{
					{
						"key":  "@_user_1",
						"name": "peer bot",
						"id": map[string]any{
							"open_id": msg.MentionOpenID,
						},
					},
				},
			},
		},
	})
}

func defaultLocalConfig(hostName, chatID, clientOpenID, serverOpenID, socketPath string) *config.Config {
	return &config.Config{
		NodeName:    "local-e2e",
		DefaultHost: hostName,
		IPC: config.IPCConfig{
			Enabled:    true,
			SocketPath: socketPath,
		},
		Lark: config.LarkConfig{
			AppID:                     "cli_e2e_client",
			AppSecret:                 "not-a-real-secret",
			SendCooldown:              config.Duration(time.Millisecond),
			LarkTextRequestLimitBytes: config.DefaultLarkTextRequestLimitBytes,
		},
		Connection: config.ConnectionConfig{
			HeartbeatInterval:  config.Duration(config.DefaultHeartbeatInterval),
			HeartbeatTimeout:   config.Duration(config.DefaultHeartbeatTimeout),
			SequenceGapTimeout: config.Duration(config.DefaultSequenceGapTimeout),
		},
		Exec: config.ExecConfig{Enabled: false},
		Hosts: map[string]config.HostConfig{
			hostName: {
				ChatID:           chatID,
				PeerBotOpenID:    serverOpenID,
				Shell:            "/bin/sh",
				StreamChunkBytes: config.DefaultStreamChunkBytes,
			},
		},
	}
}

func waitForSocket(ctx context.Context, socketPath string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	deadline, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if _, err := os.Stat(socketPath); err == nil {
			var dialer net.Dialer
			conn, err := dialer.DialContext(deadline, "unix", socketPath)
			if err == nil {
				_ = conn.Close()
				return nil
			}
		}
		select {
		case <-deadline.Done():
			return fmt.Errorf("wait for ipc socket %s: %w", socketPath, deadline.Err())
		case <-ticker.C:
		}
	}
}

func waitRun(name string, done <-chan error) error {
	if done == nil {
		return nil
	}
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	select {
	case err := <-done:
		if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, io.ErrClosedPipe) {
			return fmt.Errorf("%s: %w", name, err)
		}
		return nil
	case <-timer.C:
		return fmt.Errorf("%s did not stop", name)
	}
}

func findRepoRoot() (string, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		modPath := filepath.Join(cwd, "go.mod")
		if raw, err := os.ReadFile(modPath); err == nil && strings.Contains(string(raw), "module github.com/hachiwii/exec-over-lark") {
			return cwd, nil
		}
		parent := filepath.Dir(cwd)
		if parent == cwd {
			return "", errors.New("could not locate exec-over-lark repository root")
		}
		cwd = parent
	}
}

func exitStatus(err error) int {
	if err == nil {
		return 0
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		if status, ok := exitErr.Sys().(syscall.WaitStatus); ok {
			if status.Exited() {
				return status.ExitStatus()
			}
			if status.Signaled() {
				return 128 + int(status.Signal())
			}
		}
		return exitErr.ExitCode()
	}
	return -1
}

func cloneConfig(in *config.Config) *config.Config {
	if in == nil {
		return nil
	}
	out := *in
	if in.Hosts != nil {
		out.Hosts = make(map[string]config.HostConfig, len(in.Hosts))
		for key, value := range in.Hosts {
			out.Hosts[key] = value
		}
	}
	out.Exec.AllowedChatIDs = append([]string(nil), in.Exec.AllowedChatIDs...)
	out.Exec.AllowedSenderOpenIDs = append([]string(nil), in.Exec.AllowedSenderOpenIDs...)
	return &out
}

func cloneMessageRecord(in MessageRecord) MessageRecord {
	out := in
	out.Frames = cloneFrames(in.Frames)
	return out
}

func cloneFrames(in []protocol.Frame) []protocol.Frame {
	if len(in) == 0 {
		return nil
	}
	out := make([]protocol.Frame, len(in))
	for i, frame := range in {
		out[i] = protocol.Frame{
			Seq:     frame.Seq,
			Type:    frame.Type,
			Payload: append([]byte(nil), frame.Payload...),
		}
	}
	return out
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func firstErr(existing, next error) error {
	if existing != nil {
		return existing
	}
	return next
}
