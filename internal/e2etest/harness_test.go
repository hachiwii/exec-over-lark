package e2etest

import (
	"bytes"
	"context"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/hachiwii/exec-over-lark/internal/config"
	"github.com/hachiwii/exec-over-lark/internal/daemon"
	"github.com/hachiwii/exec-over-lark/internal/protocol"
	"github.com/hachiwii/exec-over-lark/internal/remoteexec"
)

func TestHarnessRunsCLIThroughFakeLarkEchoHello(t *testing.T) {
	h := newHarness(t, Options{})
	defer closeHarness(t, h)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := h.Start(ctx); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}

	result, err := h.RunCLI(ctx, RunRequest{Command: "printf hello"})
	if err != nil {
		t.Fatalf("RunCLI returned error: %v", err)
	}
	if result.ExitCode != 0 {
		t.Fatalf("exit code = %d, stderr = %q", result.ExitCode, result.Stderr)
	}
	if string(result.Stdout) != "hello" {
		t.Fatalf("stdout = %q, want hello", result.Stdout)
	}
}

func TestHarnessReportsNonZeroExit(t *testing.T) {
	h := newStartedHarness(t, Options{})
	defer closeHarness(t, h)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result, err := h.RunIPC(ctx, RunRequest{Command: "exit 7"})
	if err != nil {
		t.Fatalf("RunIPC returned error: %v", err)
	}
	if result.ExitCode != 7 {
		t.Fatalf("exit code = %d, want 7; stderr = %q error = %q", result.ExitCode, result.Stderr, result.ErrorMessage)
	}
}

func TestHarnessForwardsStdinToCatLikeCommand(t *testing.T) {
	h := newStartedHarness(t, Options{})
	defer closeHarness(t, h)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result, err := h.RunIPC(ctx, RunRequest{
		Command: "dd bs=6 count=1 2>/dev/null",
		Stdin:   []byte("hello\n"),
	})
	if err != nil {
		t.Fatalf("RunIPC returned error: %v", err)
	}
	if result.ExitCode != 0 {
		t.Fatalf("exit code = %d, stderr = %q error = %q", result.ExitCode, result.Stderr, result.ErrorMessage)
	}
	if string(result.Stdout) != "hello\n" {
		t.Fatalf("stdout = %q, want hello newline", result.Stdout)
	}
}

func TestHarnessHandlesLargeOutputChunks(t *testing.T) {
	h := newStartedHarness(t, Options{
		RemoteConfig: daemon.RemoteConfig{
			StreamChunkBytes: 1024,
		},
		RemoteExecutor: staticExecutor{
			stdout: bytes.Repeat([]byte("x"), 50000),
		},
	})
	defer closeHarness(t, h)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result, err := h.RunIPC(ctx, RunRequest{Command: "large-output"})
	if err != nil {
		t.Fatalf("RunIPC returned error: %v", err)
	}
	if result.ExitCode != 0 {
		t.Fatalf("exit code = %d, stderr = %q error = %q", result.ExitCode, result.Stderr, result.ErrorMessage)
	}
	if len(result.Stdout) != 50000 {
		t.Fatalf("stdout bytes = %d, want 50000", len(result.Stdout))
	}

	stdoutFrames := 0
	for _, msg := range h.Messages() {
		if msg.SenderOpenID != DefaultServerBotOpenID {
			continue
		}
		for _, frame := range msg.Frames {
			if frame.Type == protocol.TypeStdout {
				stdoutFrames++
			}
		}
	}
	if stdoutFrames < 2 {
		t.Fatalf("server stdout frames = %d, want output split across multiple frames", stdoutFrames)
	}
}

func TestHarnessSurfacesHeartbeatTimeout(t *testing.T) {
	h := newHarness(t, Options{
		LocalTickInterval:  5 * time.Millisecond,
		LocalFlushInterval: time.Millisecond,
	})
	defer closeHarness(t, h)
	h.localCfg.Connection.HeartbeatInterval = config.Duration(10 * time.Millisecond)
	h.localCfg.Connection.HeartbeatTimeout = config.Duration(30 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := h.StartLocal(ctx); err != nil {
		t.Fatalf("StartLocal returned error: %v", err)
	}

	result, err := h.RunIPC(ctx, RunRequest{Command: "printf never"})
	if err != nil {
		t.Fatalf("RunIPC returned error: %v", err)
	}
	if !strings.Contains(result.ErrorMessage, "peer heartbeat timeout") {
		t.Fatalf("error message = %q, detail = %q; want heartbeat timeout", result.ErrorMessage, result.ErrorDetail)
	}
}

func TestHarnessBootstrapMessage(t *testing.T) {
	h := newHarness(t, Options{})
	defer closeHarness(t, h)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := h.BootstrapServer(ctx, ""); err != nil {
		t.Fatalf("BootstrapServer returned error: %v", err)
	}
	messages := h.BootstrapMessages()
	if len(messages) != 1 {
		t.Fatalf("bootstrap messages = %d, want 1", len(messages))
	}
	msg := messages[0]
	if msg.ChatID != DefaultChatID || msg.SenderOpenID != DefaultServerBotOpenID {
		t.Fatalf("bootstrap target = %#v, want default chat/server bot", msg)
	}
	for _, want := range []string{
		"exec-over-lark server ready",
		"chat_id: " + DefaultChatID,
		"bot_openid: " + DefaultServerBotOpenID,
	} {
		if !strings.Contains(msg.Text, want) {
			t.Fatalf("bootstrap text = %q, missing %q", msg.Text, want)
		}
	}
}

func newStartedHarness(t *testing.T, opts Options) *Harness {
	t.Helper()
	h := newHarness(t, opts)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := h.Start(ctx); err != nil {
		closeHarness(t, h)
		t.Fatalf("Start returned error: %v", err)
	}
	return h
}

func newHarness(t *testing.T, opts Options) *Harness {
	t.Helper()
	h, err := NewHarness(opts)
	if err != nil {
		t.Fatalf("NewHarness returned error: %v", err)
	}
	return h
}

func closeHarness(t *testing.T, h *Harness) {
	t.Helper()
	if err := h.Close(); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}
}

type staticExecutor struct {
	stdout   []byte
	stderr   []byte
	exitCode int
}

func (e staticExecutor) Start(context.Context, remoteexec.Request) (daemon.RemoteProcess, error) {
	return &staticProcess{
		stdout:   append([]byte(nil), e.stdout...),
		stderr:   append([]byte(nil), e.stderr...),
		exitCode: e.exitCode,
	}, nil
}

type staticProcess struct {
	stdin    bytes.Buffer
	stdout   []byte
	stderr   []byte
	exitCode int
}

func (p *staticProcess) Stdin() io.WriteCloser {
	return nopWriteCloser{Writer: &p.stdin}
}

func (p *staticProcess) Stdout() io.ReadCloser {
	return io.NopCloser(bytes.NewReader(p.stdout))
}

func (p *staticProcess) Stderr() io.ReadCloser {
	return io.NopCloser(bytes.NewReader(p.stderr))
}

func (p *staticProcess) Wait() (remoteexec.Result, error) {
	return remoteexec.Result{ExitCode: p.exitCode}, nil
}

func (p *staticProcess) Signal(os.Signal) error {
	return nil
}

func (p *staticProcess) Kill() error {
	return nil
}

type nopWriteCloser struct {
	io.Writer
}

func (w nopWriteCloser) Close() error {
	return nil
}
