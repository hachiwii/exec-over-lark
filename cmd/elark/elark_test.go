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
	"testing"

	"github.com/hachiwii/exec-over-lark/internal/config"
	"github.com/hachiwii/exec-over-lark/internal/doctor"
	"github.com/hachiwii/exec-over-lark/internal/ipc"
	"github.com/hachiwii/exec-over-lark/internal/lark"
)

func TestHostCommandUsesNonPTYByDefault(t *testing.T) {
	fake := &fakeClient{
		messages: []ipc.Message{
			ipc.ExitMessage("", 0),
		},
	}
	app, stdout, stderr := newTestApp(fake, nil)

	code := app.run([]string{
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
}

func TestHostWithoutCommandAllocatesPTYByDefault(t *testing.T) {
	fake := &fakeClient{messages: []ipc.Message{ipc.ExitMessage("", 0)}}
	app, _, stderr := newTestApp(fake, nil)

	code := app.run([]string{"--socket", "/tmp/elarkd.sock", "macmini"})
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
}

func TestTTYFlagsOverrideDefaults(t *testing.T) {
	t.Run("force command PTY", func(t *testing.T) {
		fake := &fakeClient{messages: []ipc.Message{ipc.ExitMessage("", 0)}}
		app, _, stderr := newTestApp(fake, nil)

		code := app.run([]string{"--socket", "/tmp/elarkd.sock", "-t", "macmini", "vim", "file"})
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

		code := app.run([]string{"--socket", "/tmp/elarkd.sock", "-T", "macmini"})
		if code != 0 {
			t.Fatalf("exit code = %d, stderr = %q", code, stderr.String())
		}
		if len(fake.starts) != 1 || fake.starts[0].Pty {
			t.Fatalf("start request = %#v, want PTY disabled", fake.starts)
		}
	})
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
	combined := stdout.String() + stderr.String()
	if strings.Contains(combined, "super-secret-value") || strings.Contains(combined, "app_secret") {
		t.Fatalf("CLI output leaked secret material: %q", combined)
	}
}

func TestStreamsOutputAndReturnsRemoteExitCode(t *testing.T) {
	fake := &fakeClient{
		messages: []ipc.Message{
			ipc.StdoutMessage("", []byte("hello\n")),
			ipc.StderrMessage("", []byte("warn\n")),
			ipc.ExitMessage("", 7),
		},
	}
	app, stdout, stderr := newTestApp(fake, nil)

	code := app.run([]string{"--socket", "/tmp/elarkd.sock", "macmini", "job"})
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

	code := app.run([]string{"--socket", "/tmp/elarkd.sock", "macmini", "cat"})
	if code != 0 {
		t.Fatalf("exit code = %d, stderr = %q", code, stderr.String())
	}
	if len(fake.stdin) != 1 || string(fake.stdin[0]) != "hello\n" {
		t.Fatalf("stdin writes = %q, want hello", fake.stdin)
	}
}

func TestDaemonUnavailablePrompt(t *testing.T) {
	app, _, stderr := newTestApp(nil, nil)
	app.dial = func(context.Context, string) (daemonClient, error) {
		return nil, errors.New("connect: no such file")
	}

	code := app.run([]string{"--socket", "/tmp/missing.sock", "macmini", "date"})
	if code != 1 {
		t.Fatalf("exit code = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "elark daemon start") {
		t.Fatalf("stderr = %q, want daemon start hint", stderr.String())
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
		startTestUnixSocket(t, filepath.Join(dir, "elarkd.sock"))
		fakeLark := &fakeDoctorLark{
			token:         "tenant-token-never-print-me",
			openID:        "ou_self_bot",
			chatAvailable: true,
			rootMessageID: "om_doctor_ping",
		}
		app, stdout, stderr := newTestApp(&fakeClient{}, nil)
		app.newLarkClient = func(*config.Config) (doctor.LarkClient, error) {
			return fakeLark, nil
		}
		code := app.run([]string{"--config", configPath, "doctor", "macmini"})
		if code != 0 {
			t.Fatalf("exit code = %d, stderr = %q", code, stderr.String())
		}
		combined := stdout.String() + stderr.String()
		for _, want := range []string{
			"exec-over-lark doctor macmini",
			"[ok] config_load",
			"[ok] host_config",
			"[ok] daemon_socket",
			"[ok] token_refresh",
			"[ok] bot_open_id",
			"[ok] chat",
			"[ok] ping_root_message",
			"[skipped] peer_bot",
			"[skipped] bootstrap",
		} {
			if !strings.Contains(combined, want) {
				t.Fatalf("output = %q, want %q", combined, want)
			}
		}
		if fakeLark.tokenCalls != 1 || fakeLark.openIDCalls != 1 || fakeLark.chatCalls != 1 || fakeLark.rootCalls != 1 {
			t.Fatalf("doctor lark calls token/openID/chat/root = %d/%d/%d/%d, want 1/1/1/1",
				fakeLark.tokenCalls, fakeLark.openIDCalls, fakeLark.chatCalls, fakeLark.rootCalls)
		}
		if strings.Contains(combined, "never-print-me") || strings.Contains(combined, "app_secret") || strings.Contains(combined, "tenant-token-never-print-me") {
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
		newLarkClient: func(*config.Config) (doctor.LarkClient, error) {
			return &fakeDoctorLark{
				token:         "tenant-token",
				openID:        "ou_self_bot",
				chatAvailable: true,
				rootMessageID: "om_doctor_ping",
			}, nil
		},
		getenv: func(key string) string {
			switch key {
			case "LINES":
				return "24"
			case "COLUMNS":
				return "80"
			default:
				return ""
			}
		},
	}, stdout, stderr
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
	starts   []ipc.StartSessionRequest
	stdin    [][]byte
	resizes  [][2]int
	signals  []string
	closes   []string
	messages []ipc.Message
	closed   bool
}

func (f *fakeClient) StartSession(ctx context.Context, req ipc.StartSessionRequest) error {
	f.starts = append(f.starts, req)
	return nil
}

func (f *fakeClient) SendStdin(ctx context.Context, requestID string, data []byte) error {
	f.stdin = append(f.stdin, append([]byte(nil), data...))
	return nil
}

func (f *fakeClient) Resize(ctx context.Context, requestID string, rows, cols int) error {
	f.resizes = append(f.resizes, [2]int{rows, cols})
	return nil
}

func (f *fakeClient) Signal(ctx context.Context, requestID, name string) error {
	f.signals = append(f.signals, name)
	return nil
}

func (f *fakeClient) CloseSession(ctx context.Context, requestID, reason string) error {
	f.closes = append(f.closes, requestID)
	return nil
}

func (f *fakeClient) Receive(ctx context.Context) (ipc.Message, error) {
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
	return nil
}

type fakeDoctorLark struct {
	token         string
	openID        string
	chatAvailable bool
	rootMessageID string

	tokenCalls  int
	openIDCalls int
	chatCalls   int
	rootCalls   int
}

func (f *fakeDoctorLark) TenantAccessToken(context.Context) (string, error) {
	f.tokenCalls++
	return f.token, nil
}

func (f *fakeDoctorLark) BotOpenID(context.Context) (string, error) {
	f.openIDCalls++
	return f.openID, nil
}

func (f *fakeDoctorLark) ChatAvailable(context.Context, string) (bool, error) {
	f.chatCalls++
	return f.chatAvailable, nil
}

func (f *fakeDoctorLark) SendRootMessage(context.Context, string, string, string) (lark.RootMessage, error) {
	f.rootCalls++
	return lark.RootMessage{MessageID: f.rootMessageID}, nil
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
	dir, err := os.MkdirTemp("/tmp", "elark-cli-*")
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
