package daemon

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hachiwii/exec-over-lark/internal/config"
	"github.com/hachiwii/exec-over-lark/internal/lark"
	"github.com/hachiwii/exec-over-lark/internal/outbound"
	"github.com/hachiwii/exec-over-lark/internal/protocol"
	"github.com/hachiwii/exec-over-lark/internal/remoteexec"
)

func TestRemoteRunConsumesStartExecutesAndRepliesFrames(t *testing.T) {
	start := protocol.StartPayload{
		Cmd:   "printf hello",
		Cwd:   "/srv/app",
		Shell: "/bin/sh",
		Env:   map[string]string{"LANG": "C"},
		Heartbeat: protocol.HeartbeatConfig{
			Interval: "10s",
			Timeout:  "40s",
		},
	}
	event := remoteEventJSON(t, remoteEventOptions{
		EventID:      "evt_start",
		MessageID:    "om_root",
		ChatID:       "oc_chat",
		SenderOpenID: "ou_client",
		SelfOpenID:   "ou_self_bot",
		Text:         lark.BuildMentionedText("ou_self_bot", encodeRemoteFrames(t, jsonFrame(t, 1, protocol.TypeStart, start))),
	})

	fakeLark := &fakeLarkClient{}
	executor := &fakeRemoteExecutor{
		process: &fakeRemoteProcess{
			stdout: []byte("hello\n"),
			stderr: []byte("warn\n"),
			result: remoteexec.Result{ExitCode: 7},
		},
	}
	remote := newTestRemote(t, strings.NewReader(string(event)+"\n"), fakeLark, executor, RemoteConfig{
		ExecEnabled:      true,
		DefaultShell:     "/bin/zsh",
		StreamChunkBytes: 4,
	})

	if err := remote.Run(context.Background()); err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if len(executor.requests) != 1 {
		t.Fatalf("executor requests = %d, want 1", len(executor.requests))
	}
	gotReq := executor.requests[0]
	if gotReq.Command != "printf hello" || gotReq.Shell != "/bin/sh" || gotReq.Cwd != "/srv/app" || gotReq.Env["LANG"] != "C" {
		t.Fatalf("executor request = %#v", gotReq)
	}
	if len(fakeLark.replies) < 4 {
		t.Fatalf("replies = %d, want start_ack, output, and exit frames", len(fakeLark.replies))
	}
	for _, reply := range fakeLark.replies {
		if reply.chatID != "oc_chat" || reply.rootMessageID != "om_root" || reply.mentionOpenID != "ou_client" {
			t.Fatalf("reply target = %#v, want oc_chat/om_root/ou_client", reply)
		}
	}

	frames := remoteReplyFrames(t, fakeLark)
	assertFrame(t, frames[0], 1, protocol.TypeStartAck)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	var exitCode *int
	for _, frame := range frames[1:] {
		switch frame.Type {
		case protocol.TypeStdout:
			stdout.Write(frame.Payload)
		case protocol.TypeStderr:
			stderr.Write(frame.Payload)
		case protocol.TypeExit:
			payload, err := protocol.DecodeJSONPayload[protocol.ExitPayload](frame)
			if err != nil {
				t.Fatalf("DecodeJSONPayload exit returned error: %v", err)
			}
			exitCode = &payload.Code
		}
	}
	if stdout.String() != "hello\n" {
		t.Fatalf("stdout frames = %q, want hello newline", stdout.String())
	}
	if stderr.String() != "warn\n" {
		t.Fatalf("stderr frames = %q, want warn newline", stderr.String())
	}
	if exitCode == nil || *exitCode != 7 {
		t.Fatalf("exit code = %v, want 7", exitCode)
	}
}

func TestRemoteRunFiltersMentionsAndAllowlistsBeforeProtocolParsing(t *testing.T) {
	events := []string{
		string(remoteEventJSON(t, remoteEventOptions{
			EventID:      "evt_wrong_chat",
			MessageID:    "om_wrong_chat",
			ChatID:       "oc_other",
			SenderOpenID: "ou_allowed",
			SelfOpenID:   "ou_self_bot",
			Text:         lark.BuildMentionedText("ou_self_bot", "EOL1 not-a-valid-frame"),
		})),
		string(remoteEventJSON(t, remoteEventOptions{
			EventID:      "evt_wrong_sender",
			MessageID:    "om_wrong_sender",
			ChatID:       "oc_allowed",
			SenderOpenID: "ou_other",
			SelfOpenID:   "ou_self_bot",
			Text:         lark.BuildMentionedText("ou_self_bot", "EOL1 also-invalid"),
		})),
		string(remoteEventJSON(t, remoteEventOptions{
			EventID:      "evt_unmentioned",
			MessageID:    "om_unmentioned",
			ChatID:       "oc_allowed",
			SenderOpenID: "ou_allowed",
			SelfOpenID:   "ou_other_bot",
			Text:         lark.BuildMentionedText("ou_other_bot", encodeRemoteFrames(t, jsonFrame(t, 1, protocol.TypeStart, protocol.StartPayload{Cmd: "ignored"}))),
		})),
	}

	fakeLark := &fakeLarkClient{}
	executor := &fakeRemoteExecutor{}
	remote := newTestRemote(t, strings.NewReader(strings.Join(events, "\n")+"\n"), fakeLark, executor, RemoteConfig{
		ExecEnabled:          true,
		DefaultShell:         "/bin/zsh",
		AllowedChatIDs:       []string{"oc_allowed"},
		AllowedSenderOpenIDs: []string{"ou_allowed"},
	})

	if err := remote.Run(context.Background()); err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if len(executor.requests) != 0 {
		t.Fatalf("executor requests = %#v, want none", executor.requests)
	}
	if len(fakeLark.replies) != 0 {
		t.Fatalf("replies = %#v, want none", fakeLark.replies)
	}
}

func TestRemoteRunRepliesErrorWhenExecutorCannotStart(t *testing.T) {
	start := protocol.StartPayload{Cmd: "missing", Heartbeat: protocol.HeartbeatConfig{Timeout: "30s"}}
	event := remoteEventJSON(t, remoteEventOptions{
		EventID:      "evt_start_error",
		MessageID:    "om_root",
		ChatID:       "oc_chat",
		SenderOpenID: "ou_client",
		SelfOpenID:   "ou_self_bot",
		Text:         lark.BuildMentionedText("ou_self_bot", encodeRemoteFrames(t, jsonFrame(t, 1, protocol.TypeStart, start))),
	})

	fakeLark := &fakeLarkClient{}
	executor := &fakeRemoteExecutor{
		err: &remoteexec.StartError{
			ExitCode: remoteexec.ExitCodeCommandStartError,
			Message:  "command start failed",
			Err:      errors.New("no such file"),
		},
	}
	remote := newTestRemote(t, strings.NewReader(string(event)+"\n"), fakeLark, executor, RemoteConfig{
		ExecEnabled:  true,
		DefaultShell: "/bin/zsh",
	})

	if err := remote.Run(context.Background()); err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	frames := remoteReplyFrames(t, fakeLark)
	gotTypes := remoteFrameTypes(frames)
	wantTypes := []protocol.FrameType{protocol.TypeStartAck, protocol.TypeError}
	if !reflect.DeepEqual(gotTypes, wantTypes) {
		t.Fatalf("reply frame types = %v, want %v", gotTypes, wantTypes)
	}
	payload, err := protocol.DecodeJSONPayload[protocol.ErrorPayload](frames[1])
	if err != nil {
		t.Fatalf("DecodeJSONPayload error returned error: %v", err)
	}
	if payload.Message != "command start failed" || payload.Detail != "no such file" {
		t.Fatalf("error payload = %#v", payload)
	}
}

func TestRemoteFlushLoopRetriesAfterSendFailure(t *testing.T) {
	wantErr := errors.New("lark unavailable")
	sender := &retryRemoteSender{calls: make(chan error, 4)}
	queue := outbound.New(
		replyOnlyOutboundSender{sender: sender},
		outbound.WithSendCooldown(10*time.Millisecond),
	)
	target := outbound.Target{
		ChatID:        "oc_chat",
		RootMessageID: "om_root",
		MentionOpenID: "ou_client",
	}

	if err := queue.Enqueue(context.Background(), target, protocol.Frame{Seq: 1, Type: protocol.TypeStdout, Payload: []byte("prime")}); err != nil {
		t.Fatalf("prime Enqueue returned error: %v", err)
	}
	if err := waitRetrySenderCall(sender.calls); err != nil {
		t.Fatalf("prime send returned error: %v", err)
	}
	if err := queue.Enqueue(context.Background(), target, protocol.Frame{Seq: 2, Type: protocol.TypeStdout, Payload: []byte("retry me")}); err != nil {
		t.Fatalf("queued Enqueue returned error: %v", err)
	}
	if got := queue.PendingLen(); got != 1 {
		t.Fatalf("PendingLen before flushLoop = %d, want 1", got)
	}

	sender.setErrors(wantErr, nil)
	remote := &RemoteDaemon{
		queue:  queue,
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() {
		errCh <- remote.flushLoop(ctx)
	}()

	if err := waitRetrySenderCall(sender.calls); !errors.Is(err, wantErr) {
		t.Fatalf("first flush error = %v, want %v", err, wantErr)
	}
	if got := queue.PendingLen(); got != 1 {
		t.Fatalf("PendingLen after failed flush = %d, want 1", got)
	}
	if err := waitRetrySenderCall(sender.calls); err != nil {
		t.Fatalf("retry flush returned error: %v", err)
	}
	waitQueuePendingLen(t, queue, 0)

	cancel()
	if err := <-errCh; !errors.Is(err, context.Canceled) {
		t.Fatalf("flushLoop exit error = %v, want context.Canceled", err)
	}
}

func newTestRemote(t *testing.T, stream io.Reader, sender *fakeLarkClient, executor *fakeRemoteExecutor, cfg RemoteConfig) *RemoteDaemon {
	t.Helper()
	if cfg.LarkTextRequestLimitBytes == 0 {
		cfg.LarkTextRequestLimitBytes = config.DefaultLarkTextRequestLimitBytes
	}
	remote, err := NewRemoteDaemon(RemoteOptions{
		Config:        cfg,
		EventStream:   stream,
		SelfBotOpenID: "ou_self_bot",
		Sender:        sender,
		Executor:      executor,
	})
	if err != nil {
		t.Fatalf("NewRemoteDaemon returned error: %v", err)
	}
	return remote
}

type fakeRemoteExecutor struct {
	requests []remoteexec.Request
	process  *fakeRemoteProcess
	err      error
}

func (e *fakeRemoteExecutor) Start(_ context.Context, req remoteexec.Request) (RemoteProcess, error) {
	e.requests = append(e.requests, remoteexec.Request{
		Command: req.Command,
		Shell:   req.Shell,
		Cwd:     req.Cwd,
		Env:     cloneStringMap(req.Env),
	})
	if e.err != nil {
		return nil, e.err
	}
	if e.process == nil {
		e.process = &fakeRemoteProcess{}
	}
	return e.process, nil
}

type fakeRemoteProcess struct {
	stdin  captureWriteCloser
	stdout []byte
	stderr []byte
	result remoteexec.Result
	err    error
	killed bool
}

func (p *fakeRemoteProcess) Stdin() io.WriteCloser {
	return &p.stdin
}

func (p *fakeRemoteProcess) Stdout() io.ReadCloser {
	return io.NopCloser(bytes.NewReader(p.stdout))
}

func (p *fakeRemoteProcess) Stderr() io.ReadCloser {
	return io.NopCloser(bytes.NewReader(p.stderr))
}

func (p *fakeRemoteProcess) Wait() (remoteexec.Result, error) {
	return p.result, p.err
}

func (p *fakeRemoteProcess) Signal(os.Signal) error {
	return nil
}

func (p *fakeRemoteProcess) Kill() error {
	p.killed = true
	return nil
}

type captureWriteCloser struct {
	bytes.Buffer
	closed bool
}

func (w *captureWriteCloser) Close() error {
	w.closed = true
	return nil
}

type remoteEventOptions struct {
	EventID      string
	MessageID    string
	RootID       string
	ChatID       string
	SenderOpenID string
	SelfOpenID   string
	Text         string
}

func remoteEventJSON(t *testing.T, opts remoteEventOptions) []byte {
	t.Helper()
	content, err := lark.TextContent(opts.Text)
	if err != nil {
		t.Fatalf("TextContent returned error: %v", err)
	}
	raw, err := json.Marshal(map[string]any{
		"schema": "2.0",
		"header": map[string]any{
			"event_id":   opts.EventID,
			"event_type": lark.MessageReceiveEventType,
		},
		"event": map[string]any{
			"sender": map[string]any{
				"sender_type": "app",
				"sender_id": map[string]any{
					"open_id": opts.SenderOpenID,
				},
			},
			"message": map[string]any{
				"message_id":   opts.MessageID,
				"root_id":      opts.RootID,
				"chat_id":      opts.ChatID,
				"message_type": lark.MessageTypeText,
				"content":      content,
				"mentions": []map[string]any{
					{
						"key":  "@_user_1",
						"name": "self",
						"id": map[string]any{
							"open_id": opts.SelfOpenID,
						},
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}
	return raw
}

func encodeRemoteFrames(t *testing.T, frames ...protocol.Frame) string {
	t.Helper()
	text, err := protocol.EncodeFrames(frames)
	if err != nil {
		t.Fatalf("EncodeFrames returned error: %v", err)
	}
	return text
}

func remoteReplyFrames(t *testing.T, sender *fakeLarkClient) []protocol.Frame {
	t.Helper()
	var out []protocol.Frame
	for _, reply := range sender.replies {
		out = append(out, decodeFrames(t, reply.text)...)
	}
	return out
}

type retryRemoteSender struct {
	mu      sync.Mutex
	errs    []error
	replies []sentMessage
	calls   chan error
}

func (s *retryRemoteSender) ReplyRootMessage(_ context.Context, chatID, rootMessageID, mentionOpenID, text string) (string, error) {
	s.mu.Lock()
	s.replies = append(s.replies, sentMessage{
		chatID:        chatID,
		rootMessageID: rootMessageID,
		mentionOpenID: mentionOpenID,
		text:          text,
	})
	var err error
	if len(s.errs) > 0 {
		err = s.errs[0]
		s.errs = s.errs[1:]
	}
	s.mu.Unlock()

	if s.calls != nil {
		s.calls <- err
	}
	if err != nil {
		return "", err
	}
	return "om_reply", nil
}

func (s *retryRemoteSender) setErrors(errs ...error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.errs = append([]error(nil), errs...)
}

func waitRetrySenderCall(c <-chan error) error {
	select {
	case err := <-c:
		return err
	case <-time.After(2 * time.Second):
		return errors.New("timed out waiting for reply send")
	}
}

func waitQueuePendingLen(t *testing.T, queue *outbound.Queue, want int) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()

	for {
		if got := queue.PendingLen(); got == want {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("PendingLen did not become %d, got %d", want, queue.PendingLen())
		case <-ticker.C:
		}
	}
}

func remoteFrameTypes(frames []protocol.Frame) []protocol.FrameType {
	out := make([]protocol.FrameType, 0, len(frames))
	for _, frame := range frames {
		out = append(out, frame.Type)
	}
	return out
}
