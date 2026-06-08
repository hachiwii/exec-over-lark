package daemon

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/hachiwii/exec-over-lark/internal/config"
	"github.com/hachiwii/exec-over-lark/internal/lark"
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
	waitReplyFramesLen(t, fakeLark, 4)
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

func TestRemoteRunFiltersMentionsAndChatAllowlistBeforeProtocolParsing(t *testing.T) {
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
		ExecEnabled:    true,
		DefaultShell:   "/bin/zsh",
		AllowedChatIDs: []string{"oc_allowed"},
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

func TestRemoteRunDoesNotFilterSenderOpenID(t *testing.T) {
	start := protocol.StartPayload{Cmd: "printf ok", Heartbeat: protocol.HeartbeatConfig{Timeout: "30s"}}
	event := remoteEventJSON(t, remoteEventOptions{
		EventID:      "evt_other_sender",
		MessageID:    "om_root",
		ChatID:       "oc_allowed",
		SenderOpenID: "ou_any_sender",
		SelfOpenID:   "ou_self_bot",
		Text:         lark.BuildMentionedText("ou_self_bot", encodeRemoteFrames(t, jsonFrame(t, 1, protocol.TypeStart, start))),
	})

	fakeLark := &fakeLarkClient{}
	executor := &fakeRemoteExecutor{
		process: &fakeRemoteProcess{
			result: remoteexec.Result{ExitCode: 0},
		},
	}
	remote := newTestRemote(t, strings.NewReader(string(event)+"\n"), fakeLark, executor, RemoteConfig{
		ExecEnabled:    true,
		DefaultShell:   "/bin/zsh",
		AllowedChatIDs: []string{"oc_allowed"},
	})

	if err := remote.Run(context.Background()); err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if len(executor.requests) != 1 {
		t.Fatalf("executor requests = %d, want 1", len(executor.requests))
	}
	waitReplyFramesLen(t, fakeLark, 1)
	if len(fakeLark.replies) == 0 {
		t.Fatal("expected replies to arbitrary sender")
	}
	for _, reply := range fakeLark.replies {
		if reply.mentionOpenID != "ou_any_sender" {
			t.Fatalf("reply mentionOpenID = %q, want ou_any_sender", reply.mentionOpenID)
		}
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
	waitReplyFramesLen(t, fakeLark, 2)
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

func TestRemoteClientInterruptCloseSignalsProcess(t *testing.T) {
	proc := &fakeRemoteProcess{
		waitCh:   make(chan remoteexec.Result, 1),
		signalCh: make(chan os.Signal, 1),
	}
	fakeLark := &fakeLarkClient{}
	executor := &fakeRemoteExecutor{process: proc, startCh: make(chan struct{}, 1)}
	remote := newTestRemote(t, strings.NewReader(""), fakeLark, executor, RemoteConfig{
		ExecEnabled:  true,
		DefaultShell: "/bin/zsh",
	})

	start := protocol.StartPayload{Cmd: "sleep 60", Heartbeat: protocol.HeartbeatConfig{Timeout: "30s"}}
	if err := remote.HandleMessageEvent(context.Background(), lark.MessageEvent{
		MessageID:     "om_root",
		RootMessageID: "om_root",
		ChatID:        "oc_chat",
		SenderOpenID:  "ou_client",
		Mentions:      []lark.Mention{{OpenID: "ou_self_bot"}},
		Frames:        []protocol.Frame{jsonFrame(t, 1, protocol.TypeStart, start)},
	}); err != nil {
		t.Fatalf("HandleMessageEvent start returned error: %v", err)
	}
	receiveStarted(t, executor.startCh)

	if err := remote.HandleMessageEvent(context.Background(), lark.MessageEvent{
		MessageID:     "om_close",
		RootMessageID: "om_root",
		ChatID:        "oc_chat",
		SenderOpenID:  "ou_client",
		Mentions:      []lark.Mention{{OpenID: "ou_self_bot"}},
		Frames: []protocol.Frame{
			jsonFrame(t, 2, protocol.TypeClose, protocol.ClosePayload{Reason: protocol.CloseReasonClientInterrupt}),
		},
	}); err != nil {
		t.Fatalf("HandleMessageEvent close returned error: %v", err)
	}

	if got := receiveSignal(t, proc.signalCh); got != syscall.SIGINT {
		t.Fatalf("signal = %v, want SIGINT", got)
	}
	if proc.killed {
		t.Fatal("process was killed immediately; want graceful SIGINT for client interrupt")
	}
	proc.waitCh <- remoteexec.Result{ExitCode: 130}
	waitRemoteTasks(t, remote)
}

func newTestRemote(t *testing.T, stream io.Reader, sender *fakeLarkClient, executor *fakeRemoteExecutor, cfg RemoteConfig) *RemoteDaemon {
	t.Helper()
	if cfg.LarkTextRequestLimitBytes == 0 {
		cfg.LarkTextRequestLimitBytes = config.DefaultLarkTextRequestLimitBytes
	}
	remote, err := NewRemote(RemoteOptions{
		Config:        cfg,
		EventStream:   stream,
		SelfBotOpenID: "ou_self_bot",
		Sender:        sender,
		Executor:      executor,
		Outbound:      newTestOutboundManager(t, sender),
	})
	if err != nil {
		t.Fatalf("NewRemote returned error: %v", err)
	}
	return remote
}

type fakeRemoteExecutor struct {
	requests []remoteexec.Request
	process  *fakeRemoteProcess
	err      error
	startCh  chan struct{}
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
	if e.startCh != nil {
		e.startCh <- struct{}{}
	}
	return e.process, nil
}

type fakeRemoteProcess struct {
	stdin    captureWriteCloser
	stdout   []byte
	stderr   []byte
	result   remoteexec.Result
	err      error
	killed   bool
	waitCh   chan remoteexec.Result
	signalCh chan os.Signal
	signals  []os.Signal
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
	if p.waitCh != nil {
		return <-p.waitCh, p.err
	}
	return p.result, p.err
}

func (p *fakeRemoteProcess) Signal(sig os.Signal) error {
	p.signals = append(p.signals, sig)
	if p.signalCh != nil {
		p.signalCh <- sig
	}
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

func remoteFrameTypes(frames []protocol.Frame) []protocol.FrameType {
	out := make([]protocol.FrameType, 0, len(frames))
	for _, frame := range frames {
		out = append(out, frame.Type)
	}
	return out
}

func receiveSignal(t *testing.T, ch <-chan os.Signal) os.Signal {
	t.Helper()
	select {
	case sig := <-ch:
		return sig
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for signal")
		return nil
	}
}

func waitRemoteTasks(t *testing.T, remote *RemoteDaemon) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		remote.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for remote task")
	}
}

func receiveStarted(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for remote process start")
	}
}
