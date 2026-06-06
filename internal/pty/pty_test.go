package pty

import (
	"context"
	"errors"
	"io"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	creackpty "github.com/creack/pty"
	"golang.org/x/term"
)

func TestStartEchoesInputAndMergesOutput(t *testing.T) {
	requirePTY(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	session, err := New("/bin/sh").Start(ctx, Request{
		Command: `IFS= read -r line; printf 'got:%s\n' "$line"; printf 'err:%s\n' "$line" >&2`,
		Rows:    25,
		Cols:    100,
	})
	if err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	t.Cleanup(func() { _ = session.Close() })

	outCh := readAllAsync(session.Output())
	if _, err := io.WriteString(session.Stdin(), "hello\n"); err != nil {
		t.Fatalf("write stdin: %v", err)
	}

	result, err := session.Wait()
	if err != nil {
		t.Fatalf("Wait returned error: %v", err)
	}
	if result.ExitCode != 0 {
		t.Fatalf("ExitCode = %d, want 0", result.ExitCode)
	}

	output := normalizeTerminalOutput(receiveRead(t, outCh, session))
	if !strings.Contains(output, "hello\n") {
		t.Fatalf("output = %q, want terminal echo", output)
	}
	if !strings.Contains(output, "got:hello\n") {
		t.Fatalf("output = %q, want stdout content", output)
	}
	if !strings.Contains(output, "err:hello\n") {
		t.Fatalf("output = %q, want stderr merged into PTY output", output)
	}
}

func TestResizeUpdatesPTYSize(t *testing.T) {
	requirePTY(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	session, err := New("/bin/sh").Start(ctx, Request{
		Command: "sleep 30",
		Rows:    31,
		Cols:    91,
	})
	if err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	t.Cleanup(func() { _ = session.Close() })

	size, err := session.Size()
	if err != nil {
		t.Fatalf("Size returned error: %v", err)
	}
	if size.Rows != 31 || size.Cols != 91 {
		t.Fatalf("initial size = %#v, want 31x91", size)
	}

	if err := session.Resize(42, 132); err != nil {
		t.Fatalf("Resize returned error: %v", err)
	}
	size, err = session.Size()
	if err != nil {
		t.Fatalf("Size after resize returned error: %v", err)
	}
	if size.Rows != 42 || size.Cols != 132 {
		t.Fatalf("resized size = %#v, want 42x132", size)
	}

	if err := session.Resize(0, 132); !errors.Is(err, ErrInvalidSize) {
		t.Fatalf("Resize invalid error = %v, want ErrInvalidSize", err)
	}
}

func TestSignalNameInterruptsProcessGroup(t *testing.T) {
	requirePTY(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	session, err := New("/bin/sh").Start(ctx, Request{
		Command: `trap 'printf interrupted; exit 130' INT; while :; do sleep 1; done`,
	})
	if err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	t.Cleanup(func() { _ = session.Close() })

	outCh := readAllAsync(session.Output())
	time.Sleep(100 * time.Millisecond)
	if err := session.SignalName("INT"); err != nil {
		t.Fatalf("SignalName returned error: %v", err)
	}

	result, err := session.Wait()
	if err != nil {
		t.Fatalf("Wait returned error: %v", err)
	}
	if result.ExitCode != 130 {
		t.Fatalf("ExitCode = %d, want 130", result.ExitCode)
	}

	output := normalizeTerminalOutput(receiveRead(t, outCh, session))
	if !strings.Contains(output, "interrupted") {
		t.Fatalf("output = %q, want trap output", output)
	}
}

func TestMakeRawRestore(t *testing.T) {
	master, tty := openTestPTY(t)
	defer master.Close()
	defer tty.Close()

	fd := int(tty.Fd())
	before, err := term.GetState(fd)
	if err != nil {
		t.Fatalf("GetState before raw: %v", err)
	}

	state, err := MakeRaw(fd)
	if err != nil {
		t.Fatalf("MakeRaw returned error: %v", err)
	}
	during, err := term.GetState(fd)
	if err != nil {
		t.Fatalf("GetState during raw: %v", err)
	}
	if reflect.DeepEqual(before, during) {
		t.Fatal("terminal state did not change after MakeRaw")
	}

	if err := state.Restore(); err != nil {
		t.Fatalf("Restore returned error: %v", err)
	}
	after, err := term.GetState(fd)
	if err != nil {
		t.Fatalf("GetState after restore: %v", err)
	}
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("terminal state was not restored\nbefore=%#v\nafter=%#v", before, after)
	}
	if err := state.Restore(); err != nil {
		t.Fatalf("second Restore returned error: %v", err)
	}
}

func TestRunReturnsStartErrorsWithDesignedExitCodes(t *testing.T) {
	requirePTY(t)

	missingShell := "/tmp/elark-missing-shell-for-pty-test"
	result, err := New(missingShell).Run(context.Background(), Request{Command: "true"}, nil, nil)
	if err == nil {
		t.Fatal("Run returned nil error, want StartError")
	}
	var startErr *StartError
	if !errors.As(err, &startErr) {
		t.Fatalf("error = %T, want *StartError", err)
	}
	if result.ExitCode != ExitCodeCommandStartError || startErr.ExitCode != ExitCodeCommandStartError {
		t.Fatalf("exit codes result/startErr = %d/%d, want %d", result.ExitCode, startErr.ExitCode, ExitCodeCommandStartError)
	}
}

type readResult struct {
	data []byte
	err  error
}

func readAllAsync(r io.Reader) <-chan readResult {
	ch := make(chan readResult, 1)
	go func() {
		data, err := io.ReadAll(r)
		ch <- readResult{data: data, err: err}
	}()
	return ch
}

func receiveRead(t *testing.T, ch <-chan readResult, session *Session) string {
	t.Helper()

	select {
	case result := <-ch:
		if result.err != nil && !IsReadEOF(result.err) {
			t.Fatalf("read PTY output: %v", result.err)
		}
		return string(result.data)
	case <-time.After(2 * time.Second):
		_ = session.master.Close()
		t.Fatal("timed out reading PTY output")
		return ""
	}
}

func normalizeTerminalOutput(s string) string {
	s = strings.ReplaceAll(s, "\r\n", "\n")
	s = strings.ReplaceAll(s, "\r", "\n")
	return s
}

func requirePTY(t *testing.T) {
	t.Helper()
	master, tty := openTestPTY(t)
	_ = master.Close()
	_ = tty.Close()
}

func openTestPTY(t *testing.T) (*os.File, *os.File) {
	t.Helper()
	master, tty, err := creackpty.Open()
	if err != nil {
		if errors.Is(err, creackpty.ErrUnsupported) {
			t.Skipf("PTY unavailable: %v", err)
		}
		t.Skipf("open PTY: %v", err)
	}
	return master, tty
}
