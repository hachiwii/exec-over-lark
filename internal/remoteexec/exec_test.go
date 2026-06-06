package remoteexec

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRunSeparatesStreamsAndPropagatesExitCode(t *testing.T) {
	script := writeScript(t, "emit.sh", `#!/bin/sh
printf 'stdout:%s\n' "$1"
printf 'stderr:%s\n' "$2" >&2
exit 23
`)

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	result, err := New("/bin/sh").Run(context.Background(), Request{
		Command: shellQuote(script) + " alpha beta",
	}, nil, &stdout, &stderr)
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if result.ExitCode != 23 {
		t.Fatalf("ExitCode = %d, want 23", result.ExitCode)
	}
	if got, want := stdout.String(), "stdout:alpha\n"; got != want {
		t.Fatalf("stdout = %q, want %q", got, want)
	}
	if got, want := stderr.String(), "stderr:beta\n"; got != want {
		t.Fatalf("stderr = %q, want %q", got, want)
	}
}

func TestRunUsesDefaultShellAndCwd(t *testing.T) {
	dir := t.TempDir()
	marker := filepath.Join(dir, "default-shell-used")
	wrapper := writeScript(t, "wrapped-shell.sh", fmt.Sprintf(`#!/bin/sh
printf used > %s
exec /bin/sh "$@"
`, shellQuote(marker)))

	workdir := filepath.Join(dir, "work")
	if err := os.Mkdir(workdir, 0o755); err != nil {
		t.Fatalf("mkdir workdir: %v", err)
	}

	var stdout bytes.Buffer
	result, err := New(wrapper).Run(context.Background(), Request{
		Command: `printf '%s\n' "$(pwd)"`,
		Cwd:     workdir,
	}, nil, &stdout, nil)
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if result.ExitCode != 0 {
		t.Fatalf("ExitCode = %d, want 0", result.ExitCode)
	}
	wantCwd, err := filepath.EvalSymlinks(workdir)
	if err != nil {
		t.Fatalf("resolve workdir: %v", err)
	}
	if got, want := strings.TrimSpace(stdout.String()), wantCwd; got != want {
		t.Fatalf("pwd = %q, want %q", got, want)
	}
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("default shell wrapper was not used: %v", err)
	}
}

func TestStartExposesStdinPipe(t *testing.T) {
	script := writeScript(t, "upper.sh", `#!/bin/sh
tr '[:lower:]' '[:upper:]'
`)

	session, err := New("/bin/sh").Start(context.Background(), Request{
		Command: shellQuote(script),
	})
	if err != nil {
		t.Fatalf("Start returned error: %v", err)
	}

	if _, err := io.WriteString(session.Stdin(), "hello\n"); err != nil {
		t.Fatalf("write stdin: %v", err)
	}
	if err := session.Stdin().Close(); err != nil {
		t.Fatalf("close stdin: %v", err)
	}

	stdout, err := io.ReadAll(session.Stdout())
	if err != nil {
		t.Fatalf("read stdout: %v", err)
	}
	stderr, err := io.ReadAll(session.Stderr())
	if err != nil {
		t.Fatalf("read stderr: %v", err)
	}
	result, err := session.Wait()
	if err != nil {
		t.Fatalf("Wait returned error: %v", err)
	}
	if result.ExitCode != 0 {
		t.Fatalf("ExitCode = %d, want 0", result.ExitCode)
	}
	if got, want := string(stdout), "HELLO\n"; got != want {
		t.Fatalf("stdout = %q, want %q", got, want)
	}
	if len(stderr) != 0 {
		t.Fatalf("stderr = %q, want empty", string(stderr))
	}
}

func TestRunCopiesStdinReader(t *testing.T) {
	script := writeScript(t, "count.sh", `#!/bin/sh
wc -c
`)

	var stdout bytes.Buffer
	result, err := New("/bin/sh").Run(context.Background(), Request{
		Command: shellQuote(script),
	}, strings.NewReader("abcdef"), &stdout, nil)
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if result.ExitCode != 0 {
		t.Fatalf("ExitCode = %d, want 0", result.ExitCode)
	}
	if got, want := strings.TrimSpace(stdout.String()), "6"; got != want {
		t.Fatalf("wc output = %q, want %q", got, want)
	}
}

func TestRunReturnsExitCodeForMissingCwd(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing")

	result, err := New("/bin/sh").Run(context.Background(), Request{
		Command: "true",
		Cwd:     missing,
	}, nil, nil, nil)
	if err == nil {
		t.Fatal("Run returned nil error, want StartError")
	}

	var startErr *StartError
	if !errors.As(err, &startErr) {
		t.Fatalf("error = %T, want *StartError", err)
	}
	if result.ExitCode != ExitCodeCwdError {
		t.Fatalf("result ExitCode = %d, want %d", result.ExitCode, ExitCodeCwdError)
	}
	if startErr.ExitCode != ExitCodeCwdError {
		t.Fatalf("StartError ExitCode = %d, want %d", startErr.ExitCode, ExitCodeCwdError)
	}
}

func TestRunReturnsExitCodeForMissingShell(t *testing.T) {
	missingShell := filepath.Join(t.TempDir(), "missing-shell")

	result, err := New(missingShell).Run(context.Background(), Request{
		Command: "true",
	}, nil, nil, nil)
	if err == nil {
		t.Fatal("Run returned nil error, want StartError")
	}

	var startErr *StartError
	if !errors.As(err, &startErr) {
		t.Fatalf("error = %T, want *StartError", err)
	}
	if result.ExitCode != ExitCodeCommandStartError {
		t.Fatalf("result ExitCode = %d, want %d", result.ExitCode, ExitCodeCommandStartError)
	}
	if startErr.ExitCode != ExitCodeCommandStartError {
		t.Fatalf("StartError ExitCode = %d, want %d", startErr.ExitCode, ExitCodeCommandStartError)
	}
}

func TestContextCancellationKillsProcess(t *testing.T) {
	script := writeScript(t, "sleep.sh", `#!/bin/sh
sleep 30
`)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	started := time.Now()
	result, err := New("/bin/sh").Run(ctx, Request{
		Command: shellQuote(script),
	}, nil, nil, nil)
	elapsed := time.Since(started)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Run error = %v, want context deadline exceeded", err)
	}
	if !result.Canceled {
		t.Fatalf("Canceled = false, want true")
	}
	if result.ExitCode == 0 {
		t.Fatalf("ExitCode = 0, want non-zero cancellation status")
	}
	if elapsed > 2*time.Second {
		t.Fatalf("cancellation took %s, want under 2s", elapsed)
	}
}

func writeScript(t *testing.T, name, content string) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(content), 0o755); err != nil {
		t.Fatalf("write script %s: %v", name, err)
	}
	return path
}

func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "'\\''") + "'"
}
