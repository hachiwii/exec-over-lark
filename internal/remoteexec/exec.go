package remoteexec

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
)

const (
	// ExitCodeCwdError matches the server-side design for an invalid cwd.
	ExitCodeCwdError = 1
	// ExitCodeCommandStartError matches the server-side design for shell/start failures.
	ExitCodeCommandStartError = 127
)

// Request describes one non-interactive remote command.
type Request struct {
	// Command is passed to the selected shell as: shell -lc <Command>.
	Command string
	// Shell overrides the executor default shell when non-empty.
	Shell string
	// Cwd is the command working directory. Empty means the executing user's home.
	Cwd string
	// Env adds or overrides process environment variables.
	Env map[string]string
}

// Result is the process completion status returned by Wait or Run.
type Result struct {
	ExitCode int
	Canceled bool
}

// StartError is returned when a process cannot be started.
type StartError struct {
	ExitCode int
	Message  string
	Err      error
}

func (e *StartError) Error() string {
	if e.Err == nil {
		return e.Message
	}
	return fmt.Sprintf("%s: %v", e.Message, e.Err)
}

func (e *StartError) Unwrap() error {
	return e.Err
}

// Executor starts non-interactive commands through a login-capable shell.
type Executor struct {
	DefaultShell string
}

// New returns an executor that uses defaultShell when a request does not
// specify Shell. If defaultShell is empty, the current user's default shell is
// used, falling back to /bin/sh.
func New(defaultShell string) *Executor {
	return &Executor{DefaultShell: defaultShell}
}

// Start starts req.Command under shell -lc and returns process pipes.
func (e *Executor) Start(ctx context.Context, req Request) (*Session, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	shell, err := resolveShell(firstNonEmpty(req.Shell, e.DefaultShell, userDefaultShell()))
	if err != nil {
		return nil, &StartError{
			ExitCode: ExitCodeCommandStartError,
			Message:  "shell does not exist",
			Err:      err,
		}
	}

	cwd, err := resolveCwd(req.Cwd)
	if err != nil {
		return nil, &StartError{
			ExitCode: ExitCodeCwdError,
			Message:  "cwd does not exist",
			Err:      err,
		}
	}

	cmd := exec.Command(shell, "-lc", req.Command)
	cmd.Dir = cwd
	cmd.Env = mergeEnv(os.Environ(), req.Env)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, err
	}

	if err := cmd.Start(); err != nil {
		return nil, &StartError{
			ExitCode: ExitCodeCommandStartError,
			Message:  "command start failed",
			Err:      err,
		}
	}

	done := make(chan struct{})
	waitCh := make(chan waitResult, 1)
	session := &Session{
		stdin:  stdin,
		stdout: stdout,
		stderr: stderr,
		cmd:    cmd,
		done:   done,
		waitCh: waitCh,
	}

	go func() {
		select {
		case <-ctx.Done():
			session.cancel(ctx.Err())
		case <-done:
		}
	}()

	go func() {
		err := cmd.Wait()
		close(done)
		waitCh <- classifyWait(ctx, err)
	}()

	return session, nil
}

// Run starts a command, copies stdin/stdout/stderr, and waits for completion.
func (e *Executor) Run(ctx context.Context, req Request, stdin io.Reader, stdout, stderr io.Writer) (Result, error) {
	session, err := e.Start(ctx, req)
	if err != nil {
		var startErr *StartError
		if errors.As(err, &startErr) {
			return Result{ExitCode: startErr.ExitCode}, err
		}
		return Result{}, err
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 3)

	copyPipe := func(dst io.Writer, src io.Reader) {
		defer wg.Done()
		if dst == nil {
			dst = io.Discard
		}
		if _, err := io.Copy(dst, src); err != nil {
			errCh <- err
		}
	}

	wg.Add(2)
	go copyPipe(stdout, session.Stdout())
	go copyPipe(stderr, session.Stderr())

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer session.Stdin().Close()
		if stdin == nil {
			return
		}
		if _, err := io.Copy(session.Stdin(), stdin); err != nil && !isClosedPipe(err) {
			errCh <- err
		}
	}()

	result, waitErr := session.Wait()
	wg.Wait()
	close(errCh)

	if waitErr != nil {
		return result, waitErr
	}
	for copyErr := range errCh {
		if copyErr != nil {
			return result, copyErr
		}
	}
	return result, nil
}

// Session is a running non-interactive command with separated stdio pipes.
type Session struct {
	stdin  io.WriteCloser
	stdout io.ReadCloser
	stderr io.ReadCloser
	cmd    *exec.Cmd

	done   chan struct{}
	waitCh chan waitResult

	waitOnce   sync.Once
	waitResult waitResult

	cancelOnce sync.Once
	cancelErr  error
}

func (s *Session) Stdin() io.WriteCloser {
	return s.stdin
}

func (s *Session) Stdout() io.ReadCloser {
	return s.stdout
}

func (s *Session) Stderr() io.ReadCloser {
	return s.stderr
}

// Signal forwards sig to the process group when possible.
func (s *Session) Signal(sig os.Signal) error {
	if s == nil || s.cmd == nil || s.cmd.Process == nil {
		return os.ErrInvalid
	}
	if signal, ok := sig.(syscall.Signal); ok {
		return syscall.Kill(-s.cmd.Process.Pid, signal)
	}
	return s.cmd.Process.Signal(sig)
}

// Kill terminates the process group.
func (s *Session) Kill() error {
	if s == nil || s.cmd == nil || s.cmd.Process == nil {
		return os.ErrInvalid
	}
	return killProcessGroup(s.cmd.Process.Pid)
}

// Wait waits for process completion. Non-zero command exits are reported in
// Result.ExitCode and do not become Go errors. Cancellation does return the
// context error so callers can distinguish local cancellation from remote exit.
func (s *Session) Wait() (Result, error) {
	s.waitOnce.Do(func() {
		s.waitResult = <-s.waitCh
	})
	return s.waitResult.result, s.waitResult.err
}

func (s *Session) cancel(err error) {
	s.cancelOnce.Do(func() {
		s.cancelErr = err
		_ = s.Kill()
	})
}

type waitResult struct {
	result Result
	err    error
}

func classifyWait(ctx context.Context, err error) waitResult {
	if err == nil {
		return waitResult{result: Result{ExitCode: 0}}
	}

	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		result := Result{ExitCode: exitCode(exitErr)}
		if ctxErr := ctx.Err(); ctxErr != nil {
			result.Canceled = true
			return waitResult{result: result, err: ctxErr}
		}
		if result.ExitCode >= 0 {
			return waitResult{result: result}
		}
		return waitResult{result: result, err: err}
	}

	if ctxErr := ctx.Err(); ctxErr != nil {
		return waitResult{result: Result{ExitCode: 1, Canceled: true}, err: ctxErr}
	}
	return waitResult{result: Result{ExitCode: 1}, err: err}
}

func exitCode(err *exec.ExitError) int {
	if status, ok := err.Sys().(syscall.WaitStatus); ok {
		if status.Exited() {
			return status.ExitStatus()
		}
		if status.Signaled() {
			return 128 + int(status.Signal())
		}
	}
	return err.ExitCode()
}

func killProcessGroup(pid int) error {
	if pid <= 0 {
		return os.ErrInvalid
	}
	if err := syscall.Kill(-pid, syscall.SIGKILL); err != nil && !errors.Is(err, syscall.ESRCH) {
		return err
	}
	return nil
}

func resolveShell(shell string) (string, error) {
	if strings.TrimSpace(shell) == "" {
		shell = "/bin/sh"
	}
	if strings.ContainsRune(shell, filepath.Separator) {
		info, err := os.Stat(shell)
		if err != nil {
			return "", err
		}
		if info.IsDir() {
			return "", fmt.Errorf("%s is a directory", shell)
		}
		return shell, nil
	}
	return exec.LookPath(shell)
}

func resolveCwd(cwd string) (string, error) {
	if cwd == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", err
		}
		cwd = home
	}
	info, err := os.Stat(cwd)
	if err != nil {
		return "", err
	}
	if !info.IsDir() {
		return "", fmt.Errorf("%s is not a directory", cwd)
	}
	return cwd, nil
}

func mergeEnv(base []string, overrides map[string]string) []string {
	if len(overrides) == 0 {
		return base
	}

	env := make(map[string]string, len(base)+len(overrides))
	for _, item := range base {
		key, value, ok := strings.Cut(item, "=")
		if ok {
			env[key] = value
		}
	}
	for key, value := range overrides {
		env[key] = value
	}

	keys := make([]string, 0, len(env))
	for key := range env {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	merged := make([]string, 0, len(keys))
	for _, key := range keys {
		merged = append(merged, key+"="+env[key])
	}
	return merged
}

func userDefaultShell() string {
	if shell := os.Getenv("SHELL"); shell != "" {
		return shell
	}
	return "/bin/sh"
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func isClosedPipe(err error) bool {
	return errors.Is(err, os.ErrClosed) || errors.Is(err, io.ErrClosedPipe) || errors.Is(err, syscall.EPIPE)
}
