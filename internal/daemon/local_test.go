package daemon

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/hachiwii/exec-over-lark/internal/config"
	"github.com/hachiwii/exec-over-lark/internal/ipc"
	"github.com/hachiwii/exec-over-lark/internal/lark"
	"github.com/hachiwii/exec-over-lark/internal/outbound"
	"github.com/hachiwii/exec-over-lark/internal/protocol"
	"github.com/hachiwii/exec-over-lark/internal/session"
)

func TestLocalRunLoadsConfigStartsIPCAndEventSource(t *testing.T) {
	cfg := testLocalConfig(t)
	cfg.IPC.Enabled = true
	cfg.IPC.SocketPath = t.TempDir() + "/elarkd.sock"

	fakeLark := &fakeLarkClient{botOpenID: "ou_self_bot", nextRootID: "om_root"}
	fakeIPC := newFakeIPCServer()
	eventSource := &fakeEventSource{started: make(chan string, 1)}
	var loadedPath string

	local, err := NewLocal(LocalOptions{
		ConfigPath: "test-config.toml",
		ConfigLoader: func(path string) (*config.Config, error) {
			loadedPath = path
			return cfg, nil
		},
		LarkClient:  fakeLark,
		EventSource: eventSource,
		IPCListen: func(socketPath string, handler ipc.Handler) (IPCServer, error) {
			fakeIPC.socketPath = socketPath
			fakeIPC.handler = handler
			return fakeIPC, nil
		},
		Outbound:     newTestOutboundManager(t, fakeLark),
		TickInterval: time.Hour,
	})
	if err != nil {
		t.Fatalf("NewLocal returned error: %v", err)
	}
	if loadedPath != "test-config.toml" {
		t.Fatalf("loaded config path = %q, want test-config.toml", loadedPath)
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- local.Run(ctx)
	}()

	select {
	case got := <-eventSource.started:
		if got != "ou_self_bot" {
			t.Fatalf("event source self open_id = %q, want ou_self_bot", got)
		}
	case <-time.After(time.Second):
		t.Fatal("event source was not started")
	}
	select {
	case <-fakeIPC.started:
	case <-time.After(time.Second):
		t.Fatal("ipc server was not started")
	}
	if fakeIPC.socketPath != cfg.IPC.SocketPath {
		t.Fatalf("ipc socket path = %q, want %q", fakeIPC.socketPath, cfg.IPC.SocketPath)
	}
	if fakeIPC.handler != local {
		t.Fatalf("ipc handler = %#v, want local daemon", fakeIPC.handler)
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Run returned error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run did not stop after context cancellation")
	}
	if !fakeIPC.closed {
		t.Fatal("ipc server was not closed")
	}
	if got := local.SelfBotOpenID(); got != "ou_self_bot" {
		t.Fatalf("SelfBotOpenID = %q, want ou_self_bot", got)
	}
}

func TestStartLocalSessionCreatesRootMessageAndSendsControls(t *testing.T) {
	cfg := testLocalConfig(t)
	fakeLark := &fakeLarkClient{botOpenID: "ou_self_bot", nextRootID: "om_root"}
	local := newTestLocal(t, cfg, fakeLark)
	sub := &fakeSubscriber{}

	connID, err := local.StartLocalSession(context.Background(), ipc.StartSessionRequest{
		RequestID: "req-1",
		Host:      "macmini",
		Cmd:       "uname -a",
		Pty:       true,
		Env:       map[string]string{"TERM": "xterm-256color"},
		Rows:      24,
		Cols:      80,
	}, sub)
	if err != nil {
		t.Fatalf("StartLocalSession returned error: %v", err)
	}
	if connID != "om_root" {
		t.Fatalf("connID = %q, want om_root", connID)
	}
	if mapped, ok := local.ConnIDForRequest("req-1"); !ok || mapped != "om_root" {
		t.Fatalf("ConnIDForRequest = %q/%v, want om_root/true", mapped, ok)
	}
	if len(fakeLark.roots) != 1 {
		t.Fatalf("root messages = %d, want 1", len(fakeLark.roots))
	}
	root := fakeLark.roots[0]
	if root.chatID != "oc_chat" || root.mentionOpenID != "ou_peer_bot" {
		t.Fatalf("root target = %#v, want oc_chat/ou_peer_bot", root)
	}
	frames := decodeFrames(t, root.text)
	assertFrame(t, frames[0], 1, protocol.TypeStart)
	start, err := protocol.DecodeJSONPayload[protocol.StartPayload](frames[0])
	if err != nil {
		t.Fatalf("DecodeJSONPayload returned error: %v", err)
	}
	if start.Cmd != "uname -a" || !start.Pty || start.Cwd != "/srv/app" || start.Shell != "/bin/zsh" {
		t.Fatalf("start payload = %#v", start)
	}
	if start.Env["TERM"] != "xterm-256color" {
		t.Fatalf("start env = %#v", start.Env)
	}
	if start.Heartbeat.Interval != "10s" || start.Heartbeat.Timeout != "30s" {
		t.Fatalf("heartbeat = %#v", start.Heartbeat)
	}

	if err := local.Stdin(context.Background(), ipc.StdinRequest{RequestID: "req-1", Bytes: []byte("hello\n")}); err != nil {
		t.Fatalf("Stdin returned error: %v", err)
	}
	waitRepliesLen(t, fakeLark, 1)
	if len(fakeLark.replies) != 1 {
		t.Fatalf("replies = %d, want 1", len(fakeLark.replies))
	}
	reply := fakeLark.replies[0]
	if reply.chatID != "oc_chat" || reply.rootMessageID != "om_root" || reply.mentionOpenID != "ou_peer_bot" {
		t.Fatalf("reply target = %#v", reply)
	}
	replyFrames := decodeFrames(t, reply.text)
	assertFrame(t, replyFrames[0], 2, protocol.TypeStdin)
	if string(replyFrames[0].Payload) != "hello\n" {
		t.Fatalf("stdin payload = %q, want hello newline", replyFrames[0].Payload)
	}
}

func TestStartLocalSessionUsesHostConfigFromIPCRequest(t *testing.T) {
	cfg := testLocalConfig(t)
	fakeLark := &fakeLarkClient{botOpenID: "ou_self_bot", nextRootID: "om_root"}
	local := newTestLocal(t, cfg, fakeLark)

	_, err := local.StartLocalSession(context.Background(), ipc.StartSessionRequest{
		RequestID: "req-override",
		Host:      "macmini",
		Cmd:       "pwd",
		HostConfig: ipc.HostConfig{
			ChatID:           "oc_override",
			PeerBotOpenID:    "ou_override_bot",
			Shell:            "/bin/bash",
			StreamChunkBytes: 2048,
			DefaultCWD:       "/override/cwd",
		},
	}, &fakeSubscriber{})
	if err != nil {
		t.Fatalf("StartLocalSession returned error: %v", err)
	}
	if len(fakeLark.roots) != 1 {
		t.Fatalf("root messages = %d, want 1", len(fakeLark.roots))
	}
	root := fakeLark.roots[0]
	if root.chatID != "oc_override" || root.mentionOpenID != "ou_override_bot" {
		t.Fatalf("root target = %#v, want override target", root)
	}
	frames := decodeFrames(t, root.text)
	start, err := protocol.DecodeJSONPayload[protocol.StartPayload](frames[0])
	if err != nil {
		t.Fatalf("DecodeJSONPayload returned error: %v", err)
	}
	if start.Cwd != "/override/cwd" || start.Shell != "/bin/bash" {
		t.Fatalf("start payload = %#v, want override cwd/shell", start)
	}
}

func TestHandleLarkEventDistributesRemoteFramesToSubscriber(t *testing.T) {
	cfg := testLocalConfig(t)
	fakeLark := &fakeLarkClient{botOpenID: "ou_self_bot", nextRootID: "om_root"}
	local := newTestLocal(t, cfg, fakeLark)
	sub := &fakeSubscriber{}

	if _, err := local.StartLocalSession(context.Background(), ipc.StartSessionRequest{
		RequestID: "req-1",
		Host:      "macmini",
		Cmd:       "printf hello",
	}, sub); err != nil {
		t.Fatalf("StartLocalSession returned error: %v", err)
	}

	err := local.HandleLarkEvent(context.Background(), lark.MessageEvent{
		MessageID:     "om_reply",
		RootMessageID: "om_root",
		ChatID:        "oc_chat",
		SenderOpenID:  "cli_peer_app",
		Frames: []protocol.Frame{
			jsonFrame(t, 1, protocol.TypeStartAck, protocol.StartAckPayload{Heartbeat: protocol.HeartbeatConfig{Interval: "10s", Timeout: "45s"}}),
			{Seq: 2, Type: protocol.TypeStdout, Payload: []byte("hello\n")},
			jsonFrame(t, 3, protocol.TypeExit, protocol.ExitPayload{Code: 0}),
		},
	})
	if err != nil {
		t.Fatalf("HandleLarkEvent returned error: %v", err)
	}

	gotTypes := localEventTypes(sub.events)
	wantTypes := []session.LocalEventType{session.LocalEventStartAck, session.LocalEventStdout, session.LocalEventExit}
	if !reflect.DeepEqual(gotTypes, wantTypes) {
		t.Fatalf("event types = %v, want %v", gotTypes, wantTypes)
	}
	if sub.events[1].RequestID != "req-1" || string(sub.events[1].Bytes) != "hello\n" {
		t.Fatalf("stdout event = %#v", sub.events[1])
	}
	if sub.events[2].RequestID != "req-1" || sub.events[2].Code != 0 {
		t.Fatalf("exit event = %#v", sub.events[2])
	}
	if !sub.closed {
		t.Fatal("subscriber was not closed after exit")
	}
	if len(local.LocalSessions()) != 0 {
		t.Fatalf("LocalSessions = %#v, want none after exit", local.LocalSessions())
	}
}

func TestHandleLarkEventIgnoresUnconfiguredPeer(t *testing.T) {
	cfg := testLocalConfig(t)
	fakeLark := &fakeLarkClient{botOpenID: "ou_self_bot", nextRootID: "om_root"}
	local := newTestLocal(t, cfg, fakeLark)

	err := local.HandleLarkEvent(context.Background(), lark.MessageEvent{
		MessageID:     "om_reply",
		RootMessageID: "om_root",
		ChatID:        "oc_chat",
		SenderOpenID:  "ou_other_bot",
		Frames:        []protocol.Frame{{Seq: 1, Type: protocol.TypeStdout, Payload: []byte("ignored")}},
	})
	if !errors.Is(err, ErrIgnoredEvent) {
		t.Fatalf("HandleLarkEvent error = %v, want ErrIgnoredEvent", err)
	}
}

func newTestLocal(t *testing.T, cfg *config.Config, client *fakeLarkClient) *Local {
	t.Helper()
	local, err := NewLocal(LocalOptions{
		Config:       cfg,
		LarkClient:   client,
		EventSource:  NoopEventSource{},
		Outbound:     newTestOutboundManager(t, client),
		TickInterval: time.Hour,
	})
	if err != nil {
		t.Fatalf("NewLocal returned error: %v", err)
	}
	return local
}

func testLocalConfig(t *testing.T) *config.Config {
	t.Helper()
	return &config.Config{
		NodeName:    "local",
		DefaultHost: "macmini",
		IPC: config.IPCConfig{
			Enabled:    false,
			SocketPath: t.TempDir() + "/elarkd.sock",
		},
		Lark: config.LarkConfig{
			AppID:                     "cli_test",
			AppSecret:                 "secret_test",
			LarkTextRequestLimitBytes: config.DefaultLarkTextRequestLimitBytes,
		},
		Exec: config.ExecConfig{Enabled: false},
		Hosts: map[string]config.HostConfig{
			"macmini": {
				ChatID:           "oc_chat",
				PeerBotOpenID:    "ou_peer_bot",
				Shell:            "/bin/zsh",
				StreamChunkBytes: 12000,
				DefaultCWD:       "/srv/app",
			},
		},
	}
}

type sentMessage struct {
	chatID        string
	rootMessageID string
	mentionOpenID string
	text          string
}

type fakeLarkClient struct {
	botOpenID  string
	nextRootID string
	roots      []sentMessage
	replies    []sentMessage
}

func (c *fakeLarkClient) BotOpenID(context.Context) (string, error) {
	return c.botOpenID, nil
}

func (c *fakeLarkClient) SendRootMessage(_ context.Context, chatID, mentionOpenID, text string) (lark.RootMessage, error) {
	c.roots = append(c.roots, sentMessage{chatID: chatID, mentionOpenID: mentionOpenID, text: text})
	return lark.RootMessage{MessageID: c.nextRootID}, nil
}

func (c *fakeLarkClient) ReplyRootMessage(_ context.Context, chatID, rootMessageID, mentionOpenID, text string) (string, error) {
	c.replies = append(c.replies, sentMessage{chatID: chatID, rootMessageID: rootMessageID, mentionOpenID: mentionOpenID, text: text})
	return "om_reply", nil
}

type testOutboundSender struct {
	client *fakeLarkClient
}

func (s testOutboundSender) SendRootMessage(ctx context.Context, _ outbound.Role, chatID, mentionOpenID, text string) (outbound.RootMessage, error) {
	root, err := s.client.SendRootMessage(ctx, chatID, mentionOpenID, text)
	if err != nil {
		return outbound.RootMessage{}, err
	}
	return outbound.RootMessage{MessageID: root.MessageID}, nil
}

func (s testOutboundSender) ReplyRootMessage(ctx context.Context, _ outbound.Role, chatID, rootMessageID, mentionOpenID, text string) (string, error) {
	return s.client.ReplyRootMessage(ctx, chatID, rootMessageID, mentionOpenID, text)
}

func newTestOutboundManager(t *testing.T, client *fakeLarkClient) *outbound.Manager {
	t.Helper()
	manager, err := outbound.NewManager(outbound.ManagerOptions{
		Sender:            testOutboundSender{client: client},
		SendCooldown:      time.Millisecond,
		RequestLimitBytes: config.DefaultLarkTextRequestLimitBytes,
	})
	if err != nil {
		t.Fatalf("NewManager returned error: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() {
		_ = manager.Run(ctx)
	}()
	return manager
}

func waitRepliesLen(t *testing.T, client *fakeLarkClient, want int) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		if len(client.replies) >= want {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("replies did not reach %d, got %d", want, len(client.replies))
		case <-ticker.C:
		}
	}
}

func waitReplyFramesLen(t *testing.T, client *fakeLarkClient, want int) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		if len(remoteReplyFrames(t, client)) >= want {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("reply frames did not reach %d, got %d", want, len(remoteReplyFrames(t, client)))
		case <-ticker.C:
		}
	}
}

type fakeEventSource struct {
	started chan string
}

func (s *fakeEventSource) Run(ctx context.Context, selfBotOpenID string, _ EventHandler) error {
	s.started <- selfBotOpenID
	<-ctx.Done()
	return ctx.Err()
}

type fakeIPCServer struct {
	socketPath string
	handler    ipc.Handler
	started    chan struct{}
	done       chan struct{}
	closeOnce  sync.Once
	closed     bool
}

func newFakeIPCServer() *fakeIPCServer {
	return &fakeIPCServer{
		started: make(chan struct{}),
		done:    make(chan struct{}),
	}
}

func (s *fakeIPCServer) Serve() error {
	close(s.started)
	<-s.done
	return nil
}

func (s *fakeIPCServer) Close() error {
	s.closeOnce.Do(func() {
		s.closed = true
		close(s.done)
	})
	return nil
}

func (s *fakeIPCServer) SocketPath() string {
	return s.socketPath
}

type fakeSubscriber struct {
	events []session.LocalEvent
	closed bool
}

func (s *fakeSubscriber) Deliver(_ context.Context, event session.LocalEvent) error {
	event.Bytes = append([]byte(nil), event.Bytes...)
	s.events = append(s.events, event)
	return nil
}

func (s *fakeSubscriber) Close() error {
	s.closed = true
	return nil
}

func decodeFrames(t *testing.T, text string) []protocol.Frame {
	t.Helper()
	frames, err := protocol.DecodeFrames(text)
	if err != nil {
		t.Fatalf("DecodeFrames returned error: %v", err)
	}
	return frames
}

func jsonFrame(t *testing.T, seq uint64, typ protocol.FrameType, payload any) protocol.Frame {
	t.Helper()
	frame, err := protocol.NewJSONFrame(seq, typ, payload)
	if err != nil {
		t.Fatalf("NewJSONFrame returned error: %v", err)
	}
	return frame
}

func assertFrame(t *testing.T, frame protocol.Frame, seq uint64, typ protocol.FrameType) {
	t.Helper()
	if frame.Seq != seq || frame.Type != typ {
		t.Fatalf("frame = %#v, want seq %d type %s", frame, seq, typ)
	}
}

func localEventTypes(events []session.LocalEvent) []session.LocalEventType {
	out := make([]session.LocalEventType, 0, len(events))
	for _, event := range events {
		out = append(out, event.Type)
	}
	return out
}
