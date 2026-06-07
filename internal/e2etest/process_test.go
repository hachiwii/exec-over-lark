package e2etest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pelletier/go-toml/v2"
)

func TestProcessE2ELocalElarkdAndCLI(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	repoRoot, err := findRepoRoot()
	if err != nil {
		t.Fatalf("findRepoRoot returned error: %v", err)
	}
	binDir := t.TempDir()
	elarkdPath := buildCommandBinary(t, ctx, repoRoot, binDir, "elarkd", "./cmd/elarkd")
	elarkPath := buildCommandBinary(t, ctx, repoRoot, binDir, "elark", "./cmd/elark")

	runtimeDir, err := os.MkdirTemp("/tmp", "elark-process-e2e-*")
	if err != nil {
		t.Fatalf("create runtime dir: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(runtimeDir) })
	if err := os.Chmod(runtimeDir, 0o700); err != nil {
		t.Fatalf("chmod runtime dir: %v", err)
	}
	socketPath := filepath.Join(runtimeDir, "elarkd.sock")

	cwd := filepath.Join(t.TempDir(), "cwd")
	if err := os.MkdirAll(cwd, 0o700); err != nil {
		t.Fatalf("create cwd: %v", err)
	}
	if err := os.WriteFile(filepath.Join(cwd, "sample.txt"), []byte("sample\n"), 0o600); err != nil {
		t.Fatalf("write cwd sample: %v", err)
	}

	configPath := writeProcessE2EConfig(t, runtimeDir, socketPath, cwd)
	daemonCmd, daemonOut, daemonErr := startLoopbackElarkd(t, ctx, elarkdPath, configPath, socketPath)
	defer stopProcess(t, daemonCmd, daemonOut, daemonErr)

	t.Run("common non-interactive commands", func(t *testing.T) {
		result := runElarkCommand(t, ctx, elarkPath, configPath, nil, "loopback", "printf 'hello\\n'")
		assertCLIResult(t, result, 0, "hello\n", "")

		result = runElarkCommand(t, ctx, elarkPath, configPath, nil, "--cwd", cwd, "loopback", "pwd")
		if result.exitCode != 0 {
			t.Fatalf("pwd exit = %d, stderr = %q", result.exitCode, result.stderr)
		}
		gotCWD := canonicalPath(t, strings.TrimSpace(result.stdout))
		wantCWD := canonicalPath(t, cwd)
		if gotCWD != wantCWD {
			t.Fatalf("pwd = %q, want %q; stderr = %q", gotCWD, wantCWD, result.stderr)
		}

		result = runElarkCommand(t, ctx, elarkPath, configPath, nil, "loopback", "printf out; printf err >&2; exit 7")
		assertCLIResult(t, result, 7, "out", "err")
	})

	t.Run("non-interactive stdin piping", func(t *testing.T) {
		result := runElarkCommand(t, ctx, elarkPath, configPath, []byte("hello\n"), "loopback", "dd bs=6 count=1 2>/dev/null")
		assertCLIResult(t, result, 0, "hello\n", "")
	})

	t.Run("forced PTY command", func(t *testing.T) {
		result := runElarkCommand(t, ctx, elarkPath, configPath, nil, "-t", "loopback", "if [ -t 1 ]; then printf tty-yes; else printf tty-no; fi")
		if result.exitCode != 0 {
			t.Fatalf("forced PTY exit = %d, stdout = %q, stderr = %q", result.exitCode, result.stdout, result.stderr)
		}
		if !strings.Contains(normalizeTerminalOutput(result.stdout), "tty-yes") {
			t.Fatalf("forced PTY stdout = %q, want tty-yes; stderr = %q", result.stdout, result.stderr)
		}
	})

	t.Run("default interactive login shell", func(t *testing.T) {
		result := runElarkCommand(t, ctx, elarkPath, configPath, []byte("printf 'interactive-ok\\n'; exit\n"), "loopback")
		if result.exitCode != 0 {
			t.Fatalf("interactive shell exit = %d, stdout = %q, stderr = %q", result.exitCode, result.stdout, result.stderr)
		}
		if !strings.Contains(normalizeTerminalOutput(result.stdout), "interactive-ok") {
			t.Fatalf("interactive shell stdout = %q, want interactive-ok; stderr = %q", result.stdout, result.stderr)
		}
	})
}

func TestRealLarkProcessE2ELocalElarkdAndCLIVisibleMessages(t *testing.T) {
	fixturePath := strings.TrimSpace(os.Getenv("ELARK_REAL_LARK_FIXTURE"))
	if fixturePath == "" {
		t.Skip("set ELARK_REAL_LARK_FIXTURE to run real Lark-visible process e2e")
	}
	fixture := loadProcessE2ERealFixture(t, fixturePath)

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	repoRoot, err := findRepoRoot()
	if err != nil {
		t.Fatalf("findRepoRoot returned error: %v", err)
	}
	binDir := t.TempDir()
	elarkdPath := buildCommandBinary(t, ctx, repoRoot, binDir, "elarkd", "./cmd/elarkd")
	elarkPath := buildCommandBinary(t, ctx, repoRoot, binDir, "elark", "./cmd/elark")

	runtimeDir, err := os.MkdirTemp("/tmp", "elark-real-process-e2e-*")
	if err != nil {
		t.Fatalf("create runtime dir: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(runtimeDir) })
	if err := os.Chmod(runtimeDir, 0o700); err != nil {
		t.Fatalf("chmod runtime dir: %v", err)
	}
	socketPath := filepath.Join(runtimeDir, "elarkd.sock")
	configPath := writeProcessE2EConfigWithIDs(t, runtimeDir, socketPath, t.TempDir(), fixture.ChatID, fixture.Client.BotOpenID, fixture.Server.BotOpenID)
	daemonCmd, daemonOut, daemonErr := startLoopbackElarkdWithEnv(t, ctx, elarkdPath, configPath, socketPath, []string{
		"ELARKD_E2E_LARK_MIRROR_FIXTURE=" + fixturePath,
	})
	defer stopProcess(t, daemonCmd, daemonOut, daemonErr)

	marker := "elark-real-process-e2e-" + time.Now().UTC().Format("20060102T150405")
	result := runElarkCommand(t, ctx, elarkPath, configPath, nil, "loopback", "printf "+shellQuote(marker))
	assertCLIResult(t, result, 0, marker, "")
}

type cliRunResult struct {
	stdout   string
	stderr   string
	exitCode int
}

func buildCommandBinary(t *testing.T, ctx context.Context, repoRoot, binDir, name, pkg string) string {
	t.Helper()
	path := filepath.Join(binDir, name)
	cmd := exec.CommandContext(ctx, "go", "build", "-o", path, pkg)
	cmd.Dir = repoRoot
	cmd.Env = append(os.Environ(), "GOTOOLCHAIN=local")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("build %s: %v: %s", pkg, err, strings.TrimSpace(string(out)))
	}
	return path
}

func writeProcessE2EConfig(t *testing.T, dir, socketPath, cwd string) string {
	t.Helper()
	return writeProcessE2EConfigWithIDs(t, dir, socketPath, cwd, "oc_loopback", "ou_loopback_client", "ou_loopback_server")
}

func writeProcessE2EConfigWithIDs(t *testing.T, dir, socketPath, cwd, chatID, clientOpenID, serverOpenID string) string {
	t.Helper()
	configPath := filepath.Join(dir, "config.toml")
	body := fmt.Sprintf(`node_name = "loopback"
default_host = "loopback"

[ipc]
enabled = true
socket_path = %q

[lark]
app_id = "cli_loopback"
app_secret = "loopback_secret"
send_cooldown = "10ms"
lark_text_request_limit_bytes = 153600

[connection]
heartbeat_interval = "1s"
heartbeat_timeout = "10s"
sequence_gap_timeout = "5s"

[exec]
enabled = true
default_shell = "/bin/sh"
max_sessions = 8
stream_chunk_bytes = 512
allowed_chat_ids = [%q]

[hosts.loopback]
chat_id = %q
peer_bot_open_id = %q
shell = "/bin/sh"
stream_chunk_bytes = 512
default_cwd = %q
`, socketPath, chatID, chatID, serverOpenID, cwd)
	if err := os.WriteFile(configPath, []byte(body), 0o600); err != nil {
		t.Fatalf("write process e2e config: %v", err)
	}
	if err := os.Chmod(configPath, 0o600); err != nil {
		t.Fatalf("chmod process e2e config: %v", err)
	}
	return configPath
}

func startLoopbackElarkd(t *testing.T, ctx context.Context, elarkdPath, configPath, socketPath string) (*exec.Cmd, *bytes.Buffer, *bytes.Buffer) {
	t.Helper()
	return startLoopbackElarkdWithEnv(t, ctx, elarkdPath, configPath, socketPath, nil)
}

func startLoopbackElarkdWithEnv(t *testing.T, ctx context.Context, elarkdPath, configPath, socketPath string, extraEnv []string) (*exec.Cmd, *bytes.Buffer, *bytes.Buffer) {
	t.Helper()
	cmd := exec.CommandContext(ctx, elarkdPath, "--config", configPath)
	cmd.Env = append(os.Environ(), "ELARKD_E2E_LOOPBACK=1")
	cmd.Env = append(cmd.Env, extraEnv...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start loopback elarkd: %v", err)
	}
	waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := waitForSocket(waitCtx, socketPath); err != nil {
		stopProcess(t, cmd, &stdout, &stderr)
		t.Fatalf("wait for loopback elarkd socket: %v\nstdout:\n%s\nstderr:\n%s", err, stdout.String(), stderr.String())
	}
	return cmd, &stdout, &stderr
}

func runElarkCommand(t *testing.T, ctx context.Context, elarkPath, configPath string, stdin []byte, args ...string) cliRunResult {
	t.Helper()
	fullArgs := []string{"--config", configPath, "--timeout", "12s"}
	fullArgs = append(fullArgs, args...)
	cmd := exec.CommandContext(ctx, elarkPath, fullArgs...)
	if stdin != nil {
		cmd.Stdin = bytes.NewReader(stdin)
	}
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	exitCode := 0
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			exitCode = exitErr.ExitCode()
		} else {
			t.Fatalf("run elark %q: %v\nstdout:\n%s\nstderr:\n%s", strings.Join(fullArgs, " "), err, stdout.String(), stderr.String())
		}
	}
	return cliRunResult{stdout: stdout.String(), stderr: stderr.String(), exitCode: exitCode}
}

func assertCLIResult(t *testing.T, result cliRunResult, wantExit int, wantStdout, wantStderr string) {
	t.Helper()
	if result.exitCode != wantExit || result.stdout != wantStdout || result.stderr != wantStderr {
		t.Fatalf("result = exit %d stdout %q stderr %q, want exit %d stdout %q stderr %q",
			result.exitCode, result.stdout, result.stderr, wantExit, wantStdout, wantStderr)
	}
}

func stopProcess(t *testing.T, cmd *exec.Cmd, stdout, stderr *bytes.Buffer) {
	t.Helper()
	if cmd == nil || cmd.Process == nil {
		return
	}
	_ = cmd.Process.Signal(os.Interrupt)
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	timer := time.NewTimer(3 * time.Second)
	defer timer.Stop()
	select {
	case err := <-done:
		if err != nil {
			var exitErr *exec.ExitError
			if !errors.As(err, &exitErr) {
				t.Fatalf("loopback elarkd exited with error: %v\nstdout:\n%s\nstderr:\n%s", err, stdout.String(), stderr.String())
			}
		}
	case <-timer.C:
		_ = cmd.Process.Kill()
		<-done
		t.Fatalf("loopback elarkd did not stop\nstdout:\n%s\nstderr:\n%s", stdout.String(), stderr.String())
	}
}

func canonicalPath(t *testing.T, path string) string {
	t.Helper()
	resolved, err := filepath.EvalSymlinks(path)
	if err == nil {
		return resolved
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		t.Fatalf("resolve path %q: %v", path, err)
	}
	return abs
}

func normalizeTerminalOutput(text string) string {
	return strings.ReplaceAll(text, "\r\n", "\n")
}

type processE2ERealFixture struct {
	ChatID string               `toml:"chat_id"`
	Client processE2ERealBotApp `toml:"client"`
	Server processE2ERealBotApp `toml:"server"`
}

type processE2ERealBotApp struct {
	BotOpenID string `toml:"bot_open_id"`
}

func loadProcessE2ERealFixture(t *testing.T, path string) processE2ERealFixture {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read real process e2e fixture: %v", err)
	}
	var fixture processE2ERealFixture
	if err := toml.Unmarshal(raw, &fixture); err != nil {
		t.Fatalf("parse real process e2e fixture: %v", err)
	}
	if strings.TrimSpace(fixture.ChatID) == "" {
		t.Fatal("real process e2e fixture is missing chat_id")
	}
	if strings.TrimSpace(fixture.Client.BotOpenID) == "" {
		t.Fatal("real process e2e fixture is missing client bot_open_id")
	}
	if strings.TrimSpace(fixture.Server.BotOpenID) == "" {
		t.Fatal("real process e2e fixture is missing server bot_open_id")
	}
	return fixture
}

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\\''") + "'"
}
