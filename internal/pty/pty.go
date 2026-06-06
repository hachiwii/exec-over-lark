package pty

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

	creackpty "github.com/creack/pty"
	"golang.org/x/term"
)

const (
	defaultRows = 24
	defaultCols = 80

	// ExitCodeCwdError matches the server-side design for an invalid cwd.
	ExitCodeCwdError = 1
	// ExitCodeCommandStartError matches the server-side design for shell/start failures.
	ExitCodeCommandStartError = 127
)

var (
	ErrInvalidSize   = errors.New("invalid terminal size")
	ErrUnknownSignal = errors.New("unknown signal")
)

// Size is a terminal window size in rows and columns.
type Size struct {
	Rows int
	Cols int
}

// Request describes one command or login shell started under a PTY.
type Request struct {
	// Command is passed to the selected shell as: shell -lc <Command>.
	// Empty starts a login shell by setting argv[0] to "-<shell name>".
	Command string
	// Shell overrides the executor default shell when non-empty.
	Shell string
	// Cwd is the process working directory. Empty means the executing user's home.
	Cwd string
	// Env adds or overrides process environment variables.
	Env map[string]string
	// Rows and Cols set the initial PTY size. Missing values use 24x80.
	Rows int
	Cols int
}

// Result is the process completion status returned by Wait or Run.
type Result struct {
	ExitCode int
	Canceled bool
}

// StartError is returned when a PTY process cannot be started.
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

// Executor starts commands or login shells under a Unix PTY.
type Executor struct {
	DefaultShell string
}

// New returns an executor that uses defaultShell when a request does not
// specify Shell. If defaultShell is empty, the current user's default shell is
// used, falling back to /bin/sh.
func New(defaultShell string) *Executor {
	return &Executor{DefaultShell: defaultShell}
}

// Start starts req under a PTY. PTY output naturally merges stdout and stderr,
// so callers should send everything read from Output or Stdout as stdout frames.
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

	cmd := commandForRequest(shell, req.Command)
	cmd.Dir = cwd
	cmd.Env = mergeEnv(os.Environ(), req.Env)

	size := normalizeSize(Size{Rows: req.Rows, Cols: req.Cols})
	master, err := creackpty.StartWithSize(cmd, &creackpty.Winsize{
		Rows: uint16(size.Rows),
		Cols: uint16(size.Cols),
	})
	if err != nil {
		return nil, &StartError{
			ExitCode: ExitCodeCommandStartError,
			Message:  "pty start failed",
			Err:      err,
		}
	}

	done := make(chan struct{})
	waitCh := make(chan waitResult, 1)
	session := &Session{
		master: master,
		cmd:    cmd,
		done:   done,
		waitCh: waitCh,
	}
	session.stdin = &inputWriter{session: session}

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

// Run starts a PTY command, copies optional stdin into it, copies merged PTY
// output to stdout, and waits for completion.
func (e *Executor) Run(ctx context.Context, req Request, stdin io.Reader, stdout io.Writer) (Result, error) {
	session, err := e.Start(ctx, req)
	if err != nil {
		var startErr *StartError
		if errors.As(err, &startErr) {
			return Result{ExitCode: startErr.ExitCode}, err
		}
		return Result{}, err
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 2)

	wg.Add(1)
	go func() {
		defer wg.Done()
		if stdout == nil {
			stdout = io.Discard
		}
		if _, err := io.Copy(stdout, session.Output()); err != nil && !IsReadEOF(err) {
			errCh <- err
		}
	}()

	if stdin != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := io.Copy(session.Stdin(), stdin); err != nil && !isClosedPipe(err) {
				errCh <- err
			}
			if err := session.CloseInput(); err != nil && !isClosedPipe(err) {
				errCh <- err
			}
		}()
	}

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

// Session is a running PTY-backed process. The PTY master is both the input and
// merged output stream; there is no separate stderr stream in PTY mode.
type Session struct {
	master *os.File
	stdin  *inputWriter
	cmd    *exec.Cmd

	done   chan struct{}
	waitCh chan waitResult

	waitOnce   sync.Once
	waitResult waitResult

	cancelOnce sync.Once
	cancelErr  error
}

// Stdin returns a writer to the PTY master. Closing it writes EOT instead of
// closing the master, so output readers can continue draining process output.
func (s *Session) Stdin() io.WriteCloser {
	return s.stdin
}

// Output returns the merged PTY output stream.
func (s *Session) Output() io.ReadCloser {
	if s == nil {
		return nil
	}
	return s.master
}

// Stdout is an alias for Output. PTY mode merges stdout and stderr.
func (s *Session) Stdout() io.ReadCloser {
	return s.Output()
}

// Write sends bytes to the PTY master.
func (s *Session) Write(p []byte) (int, error) {
	if s == nil || s.master == nil {
		return 0, os.ErrInvalid
	}
	return s.master.Write(p)
}

// Read reads merged output from the PTY master.
func (s *Session) Read(p []byte) (int, error) {
	if s == nil || s.master == nil {
		return 0, os.ErrInvalid
	}
	return s.master.Read(p)
}

// CloseInput sends an EOT byte to the PTY. This is the terminal equivalent of
// EOF for canonical-mode programs such as cat.
func (s *Session) CloseInput() error {
	if s == nil || s.master == nil {
		return os.ErrInvalid
	}
	_, err := s.master.Write([]byte{0x04})
	return err
}

// Resize changes the PTY window size and lets the kernel deliver SIGWINCH to
// the foreground process group.
func (s *Session) Resize(rows, cols int) error {
	if s == nil || s.master == nil {
		return os.ErrInvalid
	}
	size, err := validateSize(Size{Rows: rows, Cols: cols})
	if err != nil {
		return err
	}
	return creackpty.Setsize(s.master, &creackpty.Winsize{
		Rows: uint16(size.Rows),
		Cols: uint16(size.Cols),
	})
}

// Size returns the PTY master's current window size.
func (s *Session) Size() (Size, error) {
	if s == nil || s.master == nil {
		return Size{}, os.ErrInvalid
	}
	return FileSize(s.master)
}

// Signal forwards sig to the PTY child process group when possible.
func (s *Session) Signal(sig os.Signal) error {
	if s == nil || s.cmd == nil || s.cmd.Process == nil {
		return os.ErrInvalid
	}
	if signal, ok := sig.(syscall.Signal); ok {
		return signalProcessGroup(s.cmd.Process.Pid, signal)
	}
	return s.cmd.Process.Signal(sig)
}

// SignalName forwards a protocol signal name such as "INT" or "SIGTERM".
func (s *Session) SignalName(name string) error {
	sig, err := SignalByName(name)
	if err != nil {
		return err
	}
	return s.Signal(sig)
}

// Interrupt implements the remote side of local Ctrl-C handling.
func (s *Session) Interrupt() error {
	return s.Signal(syscall.SIGINT)
}

// Kill terminates the PTY child process group.
func (s *Session) Kill() error {
	if s == nil || s.cmd == nil || s.cmd.Process == nil {
		return os.ErrInvalid
	}
	return signalProcessGroup(s.cmd.Process.Pid, syscall.SIGKILL)
}

// Close closes the PTY master and kills the child process group.
func (s *Session) Close() error {
	if s == nil {
		return nil
	}
	var killErr error
	select {
	case <-s.done:
	default:
		killErr = s.Kill()
	}
	if s.master == nil {
		return killErr
	}
	closeErr := s.master.Close()
	if killErr != nil && !errors.Is(killErr, syscall.ESRCH) {
		return killErr
	}
	return closeErr
}

// Wait waits for process completion. Non-zero command exits are reported in
// Result.ExitCode and do not become Go errors. Cancellation does return the
// context error so callers can distinguish local cancellation from remote exit.
func (s *Session) Wait() (Result, error) {
	if s == nil {
		return Result{}, os.ErrInvalid
	}
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

type inputWriter struct {
	session *Session
}

func (w *inputWriter) Write(p []byte) (int, error) {
	return w.session.Write(p)
}

func (w *inputWriter) Close() error {
	return w.session.CloseInput()
}

type waitResult struct {
	result Result
	err    error
}

// TerminalState is a restorable raw-mode terminal state.
type TerminalState struct {
	fd    int
	state *term.State

	mu       sync.Mutex
	restored bool
}

// MakeRaw puts fd into raw mode and returns a state that must be restored.
func MakeRaw(fd int) (*TerminalState, error) {
	state, err := term.MakeRaw(fd)
	if err != nil {
		return nil, err
	}
	return &TerminalState{fd: fd, state: state}, nil
}

// Restore restores the terminal to the state captured by MakeRaw. It is safe
// to call more than once.
func (s *TerminalState) Restore() error {
	if s == nil || s.state == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.restored {
		return nil
	}
	s.restored = true
	return term.Restore(s.fd, s.state)
}

// IsTerminal reports whether fd is a terminal.
func IsTerminal(fd int) bool {
	return term.IsTerminal(fd)
}

// TerminalSize returns fd's terminal size.
func TerminalSize(fd int) (Size, error) {
	width, height, err := term.GetSize(fd)
	if err != nil {
		return Size{}, err
	}
	return Size{Rows: height, Cols: width}, nil
}

// FileSize returns a PTY or TTY file's terminal size.
func FileSize(file *os.File) (Size, error) {
	if file == nil {
		return Size{}, os.ErrInvalid
	}
	rows, cols, err := creackpty.Getsize(file)
	if err != nil {
		return Size{}, err
	}
	return Size{Rows: rows, Cols: cols}, nil
}

// InheritSize copies the current size from tty to ptyFile.
func InheritSize(ptyFile, tty *os.File) error {
	if ptyFile == nil || tty == nil {
		return os.ErrInvalid
	}
	return creackpty.InheritSize(ptyFile, tty)
}

// SignalByName maps protocol signal names to Unix signals.
func SignalByName(name string) (os.Signal, error) {
	normalized := strings.ToUpper(strings.TrimSpace(name))
	normalized = strings.TrimPrefix(normalized, "SIG")
	switch normalized {
	case "HUP":
		return syscall.SIGHUP, nil
	case "INT":
		return syscall.SIGINT, nil
	case "QUIT":
		return syscall.SIGQUIT, nil
	case "KILL":
		return syscall.SIGKILL, nil
	case "TERM":
		return syscall.SIGTERM, nil
	case "USR1":
		return syscall.SIGUSR1, nil
	case "USR2":
		return syscall.SIGUSR2, nil
	case "WINCH":
		return syscall.SIGWINCH, nil
	case "CONT":
		return syscall.SIGCONT, nil
	case "STOP":
		return syscall.SIGSTOP, nil
	case "TSTP":
		return syscall.SIGTSTP, nil
	default:
		return nil, fmt.Errorf("%w: %q", ErrUnknownSignal, name)
	}
}

// IsReadEOF reports PTY read errors that should be treated as EOF after the
// slave side has closed. Linux commonly returns EIO in that case.
func IsReadEOF(err error) bool {
	return err == nil || errors.Is(err, io.EOF) || errors.Is(err, syscall.EIO) || isClosedPipe(err)
}

func commandForRequest(shell, command string) *exec.Cmd {
	if command != "" {
		return exec.Command(shell, "-lc", command)
	}

	cmd := exec.Command(shell)
	cmd.Args[0] = "-" + filepath.Base(shell)
	return cmd
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

func signalProcessGroup(pid int, sig syscall.Signal) error {
	if pid <= 0 {
		return os.ErrInvalid
	}
	if err := syscall.Kill(-pid, sig); err != nil && !errors.Is(err, syscall.ESRCH) {
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

func normalizeSize(size Size) Size {
	if size.Rows <= 0 {
		size.Rows = defaultRows
	}
	if size.Cols <= 0 {
		size.Cols = defaultCols
	}
	return size
}

func validateSize(size Size) (Size, error) {
	if size.Rows <= 0 || size.Cols <= 0 {
		return Size{}, ErrInvalidSize
	}
	return size, nil
}

func isClosedPipe(err error) bool {
	return errors.Is(err, os.ErrClosed) || errors.Is(err, io.ErrClosedPipe) || errors.Is(err, syscall.EPIPE)
}
