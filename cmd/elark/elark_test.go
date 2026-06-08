package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/hachiwii/exec-over-lark/internal/ipc"
)

func TestHostCommandUsesNonPTYByDefault(t *testing.T) {
	fake := &fakeClient{
		messages: []ipc.Message{
			ipc.ExitMessage("", 0),
		},
	}
	app, stdout, stderr := newTestApp(fake, nil)
	configPath := testCLIConfigPath(t)

	code := app.run([]string{
		"--config", configPath,
		"--socket", "/tmp/elarkd.sock",
		"--cwd", "/srv/app",
		"macmini",
		"uname",
		"-a",
	})
	if code != 0 {
		t.Fatalf("exit code = %d, stderr = %q", code, stderr.String())
	}
	if stdout.String() != "" {
		t.Fatalf("stdout = %q, want empty", stdout.String())
	}
	if len(fake.starts) != 1 {
		t.Fatalf("starts = %d, want 1", len(fake.starts))
	}
	start := fake.starts[0]
	if start.Host != "macmini" || start.Cmd != "uname -a" || start.Pty {
		t.Fatalf("start request = %#v, want non-PTY uname command", start)
	}
	if start.Cwd != "/srv/app" {
		t.Fatalf("cwd = %q, want /srv/app", start.Cwd)
	}
	if start.Env["TERM"] != "" {
		t.Fatalf("TERM env for non-PTY = %q, want empty", start.Env["TERM"])
	}
}

func TestHostWithoutCommandAllocatesPTYByDefault(t *testing.T) {
	fake := &fakeClient{messages: []ipc.Message{ipc.ExitMessage("", 0)}}
	app, _, stderr := newTestApp(fake, nil)
	configPath := testCLIConfigPath(t)

	code := app.run([]string{"--config", configPath, "--socket", "/tmp/elarkd.sock", "macmini"})
	if code != 0 {
		t.Fatalf("exit code = %d, stderr = %q", code, stderr.String())
	}
	if len(fake.starts) != 1 {
		t.Fatalf("starts = %d, want 1", len(fake.starts))
	}
	start := fake.starts[0]
	if start.Host != "macmini" || start.Cmd != "" || !start.Pty {
		t.Fatalf("start request = %#v, want default PTY login shell", start)
	}
	if start.Env["TERM"] != "xterm-256color" {
		t.Fatalf("TERM env = %q, want xterm-256color fallback", start.Env["TERM"])
	}
}

func TestTTYFlagsOverrideDefaults(t *testing.T) {
	t.Run("force command PTY", func(t *testing.T) {
		fake := &fakeClient{messages: []ipc.Message{ipc.ExitMessage("", 0)}}
		app, _, stderr := newTestApp(fake, nil)
		configPath := testCLIConfigPath(t)

		code := app.run([]string{"--config", configPath, "--socket", "/tmp/elarkd.sock", "-t", "macmini", "vim", "file"})
		if code != 0 {
			t.Fatalf("exit code = %d, stderr = %q", code, stderr.String())
		}
		if len(fake.starts) != 1 || !fake.starts[0].Pty || fake.starts[0].Cmd != "vim file" {
			t.Fatalf("start request = %#v, want forced PTY command", fake.starts)
		}
	})

	t.Run("disable default PTY", func(t *testing.T) {
		fake := &fakeClient{messages: []ipc.Message{ipc.ExitMessage("", 0)}}
		app, _, stderr := newTestApp(fake, nil)
		configPath := testCLIConfigPath(t)

		code := app.run([]string{"--config", configPath, "--socket", "/tmp/elarkd.sock", "-T", "macmini"})
		if code != 0 {
			t.Fatalf("exit code = %d, stderr = %q", code, stderr.String())
		}
		if len(fake.starts) != 1 || fake.starts[0].Pty {
			t.Fatalf("start request = %#v, want PTY disabled", fake.starts)
		}
	})
}

func TestTerminalEnvUsesLocalTermWithFallback(t *testing.T) {
	if got := terminalEnv(func(string) string { return "xterm-ghostty" })["TERM"]; got != "xterm-ghostty" {
		t.Fatalf("TERM = %q, want local term", got)
	}
	for _, value := range []string{"", "dumb"} {
		got := terminalEnv(func(string) string { return value })["TERM"]
		if got != "xterm-256color" {
			t.Fatalf("TERM fallback for %q = %q, want xterm-256color", value, got)
		}
	}
}

func TestConfigSocketIsUsedWithoutPrintingSecret(t *testing.T) {
	dir := t.TempDir()
	writeCLIConfig(t, dir, "super-secret-value")
	socketPath := filepath.Join(dir, "elarkd.sock")

	fake := &fakeClient{messages: []ipc.Message{ipc.ExitMessage("", 0)}}
	app, stdout, stderr := newTestApp(fake, nil)
	dialer := &recordingDialer{client: fake}
	app.dial = dialer.dial

	code := app.run([]string{"--config", filepath.Join(dir, "config.toml"), "macmini", "date"})
	if code != 0 {
		t.Fatalf("exit code = %d, stderr = %q", code, stderr.String())
	}
	if got := dialer.socketPath; got != socketPath {
		t.Fatalf("socket path = %q, want %q", got, socketPath)
	}
	if len(fake.starts) != 1 {
		t.Fatalf("starts = %d, want 1", len(fake.starts))
	}
	start := fake.starts[0]
	if start.HostConfig.ChatID != "oc_dev" || start.HostConfig.PeerBotOpenID != "ou_server" || start.HostConfig.DefaultCWD != "/srv/app" {
		t.Fatalf("host config = %#v, want config file host content", start.HostConfig)
	}
	if start.Cwd != "/srv/app" {
		t.Fatalf("cwd = %q, want host default cwd", start.Cwd)
	}
	combined := stdout.String() + stderr.String()
	if strings.Contains(combined, "super-secret-value") || strings.Contains(combined, "app_secret") {
		t.Fatalf("CLI output leaked secret material: %q", combined)
	}
}

func TestStreamsOutputAndReturnsRemoteExitCode(t *testing.T) {
	fake := &fakeClient{
		messages: []ipc.Message{
			ipc.StartAckMessage(""),
			ipc.StdoutMessage("", []byte("hello\n")),
			ipc.StderrMessage("", []byte("warn\n")),
			ipc.ExitMessage("", 7),
		},
	}
	app, stdout, stderr := newTestApp(fake, nil)
	configPath := testCLIConfigPath(t)

	code := app.run([]string{"--config", configPath, "--socket", "/tmp/elarkd.sock", "macmini", "job"})
	if code != 7 {
		t.Fatalf("exit code = %d, want 7", code)
	}
	if stdout.String() != "hello\n" {
		t.Fatalf("stdout = %q, want hello", stdout.String())
	}
	if stderr.String() != "warn\n" {
		t.Fatalf("stderr = %q, want warn", stderr.String())
	}
}

func TestSendsPipedStdin(t *testing.T) {
	fake := &fakeClient{messages: []ipc.Message{ipc.ExitMessage("", 0)}}
	app, _, stderr := newTestApp(fake, strings.NewReader("hello\n"))
	configPath := testCLIConfigPath(t)

	code := app.run([]string{"--config", configPath, "--socket", "/tmp/elarkd.sock", "macmini", "cat"})
	if code != 0 {
		t.Fatalf("exit code = %d, stderr = %q", code, stderr.String())
	}
	if len(fake.stdin) != 1 || string(fake.stdin[0]) != "hello\n" {
		t.Fatalf("stdin writes = %q, want hello", fake.stdin)
	}
}

func TestInteractivePTYStreamsTerminalStdin(t *testing.T) {
	stdinReader, stdinWriter := io.Pipe()
	defer stdinWriter.Close()
	fake := &fakeClient{
		receiveCh: make(chan ipc.Message, 2),
		startCh:   make(chan ipc.StartSessionRequest, 1),
		stdinCh:   make(chan []byte, 1),
	}
	app, _, stderr := newTestApp(fake, stdinReader)
	app.stdinIsTerminal = func(io.Reader) bool { return true }
	rawState := &fakeTerminalState{restored: make(chan struct{}, 1)}
	rawCh := make(chan struct{}, 1)
	app.makeRawTerminal = func(io.Reader) (terminalRestorer, error) {
		rawCh <- struct{}{}
		return rawState, nil
	}
	configPath := testCLIConfigPath(t)

	codeCh := make(chan int, 1)
	go func() {
		codeCh <- app.run([]string{"--config", configPath, "--socket", "/tmp/elarkd.sock", "macmini"})
	}()

	start := receiveStart(t, fake.startCh)
	if !start.Pty || start.Cmd != "" {
		t.Fatalf("start request = %#v, want interactive PTY", start)
	}
	fake.receiveCh <- ipc.StartAckMessage("")
	receiveRawMode(t, rawCh)

	if _, err := stdinWriter.Write([]byte("pwd\n")); err != nil {
		t.Fatalf("write test stdin: %v", err)
	}
	if got := receiveBytes(t, fake.stdinCh); string(got) != "pwd\n" {
		t.Fatalf("forwarded stdin = %q, want pwd newline", got)
	}

	_ = stdinWriter.Close()
	fake.receiveCh <- ipc.ExitMessage("", 0)
	if code := receiveExitCode(t, codeCh); code != 0 {
		t.Fatalf("exit code = %d, stderr = %q", code, stderr.String())
	}
	receiveClosed(t, rawState.restored)
}

func TestInterruptClosesNonPTYSession(t *testing.T) {
	fake := &fakeClient{
		closeSessionCh: make(chan string, 1),
		clientClosedCh: make(chan struct{}, 1),
	}
	app, _, _ := newTestApp(fake, nil)
	signals := installFakeSignalNotify(app)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	forwarder := app.forwardSignals(ctx, cancel, fake, "req-1", false, make(chan struct{}))
	defer forwarder.stop()

	signals.send(os.Interrupt)
	if got := receiveInterruptCode(t, forwarder.interrupted); got != clientInterruptExitCode {
		t.Fatalf("interrupt exit code = %d, want %d", got, clientInterruptExitCode)
	}
	if got := receiveString(t, fake.closeSessionCh); got != "req-1" {
		t.Fatalf("closed request = %q, want req-1", got)
	}
	receiveClosed(t, fake.clientClosedCh)
	if len(fake.signals) != 0 {
		t.Fatalf("signals = %v, want none", fake.signals)
	}
}

func TestInterruptClosesPTYBeforeStartAck(t *testing.T) {
	fake := &fakeClient{
		closeSessionCh: make(chan string, 1),
		clientClosedCh: make(chan struct{}, 1),
	}
	app, _, _ := newTestApp(fake, nil)
	signals := installFakeSignalNotify(app)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	forwarder := app.forwardSignals(ctx, cancel, fake, "req-1", true, make(chan struct{}))
	defer forwarder.stop()

	signals.send(os.Interrupt)
	if got := receiveInterruptCode(t, forwarder.interrupted); got != clientInterruptExitCode {
		t.Fatalf("interrupt exit code = %d, want %d", got, clientInterruptExitCode)
	}
	if got := receiveString(t, fake.closeSessionCh); got != "req-1" {
		t.Fatalf("closed request = %q, want req-1", got)
	}
	receiveClosed(t, fake.clientClosedCh)
	if len(fake.signals) != 0 {
		t.Fatalf("signals = %v, want none", fake.signals)
	}
}

func TestInterruptSignalsPTYAfterStartAck(t *testing.T) {
	fake := &fakeClient{
		signalCh: make(chan string, 1),
	}
	app, _, _ := newTestApp(fake, nil)
	signals := installFakeSignalNotify(app)
	startAcked := make(chan struct{})
	close(startAcked)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	forwarder := app.forwardSignals(ctx, cancel, fake, "req-1", true, startAcked)
	defer forwarder.stop()

	signals.send(os.Interrupt)
	if got := receiveString(t, fake.signalCh); got != "INT" {
		t.Fatalf("signal = %q, want INT", got)
	}
	select {
	case code := <-forwarder.interrupted:
		t.Fatalf("unexpected interrupt exit code %d", code)
	default:
	}
	if len(fake.closes) != 0 {
		t.Fatalf("closes = %v, want none", fake.closes)
	}
}

func TestTermSignalStillForwardsToRemote(t *testing.T) {
	fake := &fakeClient{
		signalCh: make(chan string, 1),
	}
	app, _, _ := newTestApp(fake, nil)
	signals := installFakeSignalNotify(app)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	forwarder := app.forwardSignals(ctx, cancel, fake, "req-1", false, make(chan struct{}))
	defer forwarder.stop()

	signals.send(syscall.SIGTERM)
	if got := receiveString(t, fake.signalCh); got != "TERM" {
		t.Fatalf("signal = %q, want TERM", got)
	}
	if len(fake.closes) != 0 {
		t.Fatalf("closes = %v, want none", fake.closes)
	}
}

func TestDaemonUnavailablePrompt(t *testing.T) {
	app, _, stderr := newTestApp(nil, nil)
	app.dial = func(context.Context, string) (daemonClient, error) {
		return nil, errors.New("connect: no such file")
	}
	configPath := testCLIConfigPath(t)

	code := app.run([]string{"--config", configPath, "--socket", "/tmp/missing.sock", "macmini", "date"})
	if code != 1 {
		t.Fatalf("exit code = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "elarkd start") {
		t.Fatalf("stderr = %q, want elarkd start hint", stderr.String())
	}
}

func TestDaemonStatusUsesFakeDial(t *testing.T) {
	fake := &fakeClient{}
	app, stdout, stderr := newTestApp(fake, nil)

	code := app.run([]string{"--socket", "/tmp/elarkd.sock", "daemon", "status"})
	if code != 0 {
		t.Fatalf("exit code = %d, stderr = %q", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "running") {
		t.Fatalf("stdout = %q, want running status", stdout.String())
	}
	if !fake.closed {
		t.Fatal("daemon status should close the client after probing")
	}
}

func TestHostsAndDoctorDoNotPrintSecrets(t *testing.T) {
	dir := shortTempDir(t)
	writeCLIConfig(t, dir, "never-print-me")
	configPath := filepath.Join(dir, "config.toml")

	t.Run("hosts", func(t *testing.T) {
		app, stdout, stderr := newTestApp(&fakeClient{}, nil)
		code := app.run([]string{"--config", configPath, "hosts"})
		if code != 0 {
			t.Fatalf("exit code = %d, stderr = %q", code, stderr.String())
		}
		combined := stdout.String() + stderr.String()
		if !strings.Contains(combined, "macmini") {
			t.Fatalf("output = %q, want host name", combined)
		}
		if strings.Contains(combined, "never-print-me") || strings.Contains(combined, "app_secret") {
			t.Fatalf("hosts leaked secret material: %q", combined)
		}
	})

	t.Run("doctor", func(t *testing.T) {
		fake := &fakeClient{
			status: ipc.DaemonStatus{
				Running:    true,
				SocketPath: filepath.Join(dir, "elarkd.sock"),
				Event:      ipc.EventConnectionStatus{Checked: true, Connected: true},
				Outbound:   ipc.OutboundQueueStatus{Checked: true},
			},
		}
		app, stdout, stderr := newTestApp(fake, nil)
		code := app.run([]string{"--config", configPath, "doctor"})
		if code != 0 {
			t.Fatalf("exit code = %d, stderr = %q", code, stderr.String())
		}
		combined := stdout.String() + stderr.String()
		for _, want := range []string{
			"exec-over-lark doctor",
			"[ok] config_load",
			"[ok] daemon_status",
			"[ok] daemon_socket",
			"[ok] event_connection",
			"[ok] outbound_queue",
		} {
			if !strings.Contains(combined, want) {
				t.Fatalf("output = %q, want %q", combined, want)
			}
		}
		if len(fake.statusRequests) != 1 {
			t.Fatalf("doctor status requests = %d, want 1", len(fake.statusRequests))
		}
		for _, forbidden := range []string{"host_config", "token_refresh", "bot_open_id", "chat", "peer_bot", "bootstrap", "ping_root_message"} {
			if strings.Contains(combined, forbidden) {
				t.Fatalf("doctor still reports %s: %q", forbidden, combined)
			}
		}
		if strings.Contains(combined, "never-print-me") || strings.Contains(combined, "app_secret") {
			t.Fatalf("doctor leaked secret material: %q", combined)
		}
	})
}

func TestKillSendsCloseRequest(t *testing.T) {
	fake := &fakeClient{}
	app, stdout, stderr := newTestApp(fake, nil)

	code := app.run([]string{"--socket", "/tmp/elarkd.sock", "kill", "om_conn"})
	if code != 0 {
		t.Fatalf("exit code = %d, stderr = %q", code, stderr.String())
	}
	if len(fake.closes) != 1 || fake.closes[0] != "om_conn" {
		t.Fatalf("close requests = %#v, want om_conn", fake.closes)
	}
	if !strings.Contains(stdout.String(), "om_conn") {
		t.Fatalf("stdout = %q, want conn id", stdout.String())
	}
}

func TestUnsupportedControlSurfacesAreWired(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{name: "sessions", args: []string{"sessions"}, want: "sessions RPC"},
		{name: "attach", args: []string{"attach", "om_conn"}, want: "attach RPC"},
		{name: "daemon stop", args: []string{"--socket", "/tmp/elarkd.sock", "daemon", "stop"}, want: "control RPC"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			app, _, stderr := newTestApp(&fakeClient{}, nil)
			code := app.run(tc.args)
			if code != 1 {
				t.Fatalf("exit code = %d, want 1", code)
			}
			if !strings.Contains(stderr.String(), tc.want) {
				t.Fatalf("stderr = %q, want %q", stderr.String(), tc.want)
			}
		})
	}
}

func newTestApp(client *fakeClient, stdin io.Reader) (*app, *bytes.Buffer, *bytes.Buffer) {
	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	if stdin == nil {
		stdin = strings.NewReader("")
	}
	dialer := &recordingDialer{client: client}
	return &app{
		stdin:       stdin,
		stdout:      stdout,
		stderr:      stderr,
		dial:        dialer.dial,
		startDaemon: func(context.Context, string, io.Writer, io.Writer) error { return nil },
		getenv: func(key string) string {
			switch key {
			case "LINES":
				return "24"
			case "COLUMNS":
				return "80"
			case "TERM":
				return ""
			default:
				return ""
			}
		},
		notifySignals:    func(chan<- os.Signal, ...os.Signal) {},
		stopSignalNotify: func(chan<- os.Signal) {},
		stdinIsTerminal:  stdinIsTerminal,
		makeRawTerminal:  makeRawTerminal,
	}, stdout, stderr
}

type fakeTerminalState struct {
	restored chan struct{}
}

func (s *fakeTerminalState) Restore() error {
	if s.restored != nil {
		s.restored <- struct{}{}
	}
	return nil
}

type fakeSignalNotify struct {
	ch chan<- os.Signal
}

func installFakeSignalNotify(app *app) *fakeSignalNotify {
	notify := &fakeSignalNotify{}
	app.notifySignals = func(ch chan<- os.Signal, _ ...os.Signal) {
		notify.ch = ch
	}
	app.stopSignalNotify = func(chan<- os.Signal) {}
	return notify
}

func (n *fakeSignalNotify) send(sig os.Signal) {
	n.ch <- sig
}

type recordingDialer struct {
	client     *fakeClient
	socketPath string
}

func (d *recordingDialer) dial(ctx context.Context, socketPath string) (daemonClient, error) {
	d.socketPath = socketPath
	if d.client == nil {
		return nil, fmt.Errorf("no fake client for %s", socketPath)
	}
	return d.client, nil
}

type fakeClient struct {
	starts         []ipc.StartSessionRequest
	stdin          [][]byte
	resizes        [][2]int
	signals        []string
	closes         []string
	messages       []ipc.Message
	receiveCh      chan ipc.Message
	status         ipc.DaemonStatus
	statusRequests []ipc.StatusRequest
	closed         bool

	startCh        chan ipc.StartSessionRequest
	stdinCh        chan []byte
	closeSessionCh chan string
	signalCh       chan string
	clientClosedCh chan struct{}
}

func (f *fakeClient) StartSession(ctx context.Context, req ipc.StartSessionRequest) error {
	f.starts = append(f.starts, req)
	if f.startCh != nil {
		f.startCh <- req
	}
	return nil
}

func (f *fakeClient) SendStdin(ctx context.Context, requestID string, data []byte) error {
	f.stdin = append(f.stdin, append([]byte(nil), data...))
	if f.stdinCh != nil {
		f.stdinCh <- append([]byte(nil), data...)
	}
	return nil
}

func (f *fakeClient) Resize(ctx context.Context, requestID string, rows, cols int) error {
	f.resizes = append(f.resizes, [2]int{rows, cols})
	return nil
}

func (f *fakeClient) Signal(ctx context.Context, requestID, name string) error {
	f.signals = append(f.signals, name)
	if f.signalCh != nil {
		f.signalCh <- name
	}
	return nil
}

func (f *fakeClient) CloseSession(ctx context.Context, requestID, reason string) error {
	f.closes = append(f.closes, requestID)
	if f.closeSessionCh != nil {
		f.closeSessionCh <- requestID
	}
	return nil
}

func (f *fakeClient) Status(ctx context.Context, req ipc.StatusRequest) (ipc.DaemonStatus, error) {
	f.statusRequests = append(f.statusRequests, req)
	if !f.status.Running && !f.status.Event.Checked && !f.status.Outbound.Checked {
		return ipc.DaemonStatus{Running: true}, nil
	}
	return f.status, nil
}

func (f *fakeClient) Receive(ctx context.Context) (ipc.Message, error) {
	if f.receiveCh != nil {
		select {
		case <-ctx.Done():
			return ipc.Message{}, ctx.Err()
		case msg := <-f.receiveCh:
			if msg.RequestID == "" && len(f.starts) > 0 {
				msg.RequestID = f.starts[len(f.starts)-1].RequestID
			}
			return msg, nil
		}
	}
	if len(f.messages) == 0 {
		return ipc.Message{}, errors.New("no fake message")
	}
	msg := f.messages[0]
	f.messages = f.messages[1:]
	if msg.RequestID == "" && len(f.starts) > 0 {
		msg.RequestID = f.starts[len(f.starts)-1].RequestID
	}
	return msg, nil
}

func (f *fakeClient) Close() error {
	f.closed = true
	if f.clientClosedCh != nil {
		f.clientClosedCh <- struct{}{}
	}
	return nil
}

func receiveInterruptCode(t *testing.T, ch <-chan int) int {
	t.Helper()
	select {
	case code := <-ch:
		return code
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for interrupt")
		return 0
	}
}

func receiveString(t *testing.T, ch <-chan string) string {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for string")
		return ""
	}
}

func receiveBytes(t *testing.T, ch <-chan []byte) []byte {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for bytes")
		return nil
	}
}

func receiveStart(t *testing.T, ch <-chan ipc.StartSessionRequest) ipc.StartSessionRequest {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for start")
		return ipc.StartSessionRequest{}
	}
}

func receiveExitCode(t *testing.T, ch <-chan int) int {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for exit code")
		return 0
	}
}

func receiveRawMode(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for raw mode")
	}
}

func receiveClosed(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for close")
	}
}

func startTestUnixSocket(t *testing.T, path string) {
	t.Helper()
	if err := os.RemoveAll(path); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		_ = listener.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = listener.Close()
		_ = os.Remove(path)
	})
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			_ = conn.Close()
		}
	}()
}

func shortTempDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "elark-command-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = os.RemoveAll(dir)
	})
	return dir
}

func writeCLIConfig(t *testing.T, dir, secret string) {
	t.Helper()
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	socketPath := filepath.Join(dir, "elarkd.sock")
	content := `node_name = "local"
default_host = "macmini"

[ipc]
enabled = true
socket_path = "` + filepath.ToSlash(socketPath) + `"

[lark]
app_id = "cli_client_xxx"
app_secret = "` + secret + `"

[exec]
enabled = false

[hosts.macmini]
chat_id = "oc_dev"
peer_bot_open_id = "ou_server"
default_cwd = "/srv/app"
`
	path := filepath.Join(dir, "config.toml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func testCLIConfigPath(t *testing.T) string {
	t.Helper()
	dir := shortTempDir(t)
	writeCLIConfig(t, dir, "test-secret")
	return filepath.Join(dir, "config.toml")
}
