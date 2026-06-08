package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/hachiwii/exec-over-lark/internal/config"
	"github.com/hachiwii/exec-over-lark/internal/doctor"
	"github.com/hachiwii/exec-over-lark/internal/ipc"
	"github.com/hachiwii/exec-over-lark/internal/outbound"
	"github.com/hachiwii/exec-over-lark/internal/protocol"
	"github.com/hachiwii/exec-over-lark/internal/pty"
	"github.com/hachiwii/exec-over-lark/internal/version"
	"github.com/pelletier/go-toml/v2"
)

const (
	defaultSocketPath           = "~/.local/run/exec-over-lark/elarkd.sock"
	clientInterruptExitCode     = 130
	clientInterruptCloseTimeout = 2 * time.Second
)

type daemonClient interface {
	StartSession(context.Context, ipc.StartSessionRequest) error
	SendStdin(context.Context, string, []byte) error
	Resize(context.Context, string, int, int) error
	Signal(context.Context, string, string) error
	CloseSession(context.Context, string, string) error
	CloseConn(context.Context, string, string) error
	Status(context.Context, ipc.StatusRequest) (ipc.DaemonStatus, error)
	Sessions(context.Context, ipc.SessionsRequest) ([]ipc.SessionInfo, error)
	Receive(context.Context) (ipc.Message, error)
	Close() error
}

type dialFunc func(context.Context, string) (daemonClient, error)
type terminalRestorer interface {
	Restore() error
}
type makeRawFunc func(io.Reader) (terminalRestorer, error)

type app struct {
	stdin  io.Reader
	stdout io.Writer
	stderr io.Writer

	dial   dialFunc
	getenv func(string) string

	stdinIsTerminal  func(io.Reader) bool
	makeRawTerminal  makeRawFunc
	notifySignals    func(chan<- os.Signal, ...os.Signal)
	stopSignalNotify func(chan<- os.Signal)
}

type ttyMode int

const (
	ttyAuto ttyMode = iota
	ttyForce
	ttyNever
)

type globalOptions struct {
	tty        ttyMode
	configPath string
	cwd        string
	timeout    time.Duration
	socketPath string
	debug      bool
}

type commandKind int

const (
	commandHelp commandKind = iota
	commandVersion
	commandRemote
	commandHosts
	commandDoctor
	commandSessions
	commandKill
)

type parsedCommand struct {
	kind    commandKind
	opts    globalOptions
	host    string
	command string
	connID  string
}

type cliConfig struct {
	DefaultHost string                   `toml:"default_host"`
	IPC         cliIPCConfig             `toml:"ipc"`
	Hosts       map[string]cliHostConfig `toml:"hosts"`
}

type cliIPCConfig struct {
	SocketPath string `toml:"socket_path"`
}

type cliHostConfig struct {
	DefaultCWD string `toml:"default_cwd"`
}

func main() {
	os.Exit(newApp().run(os.Args[1:]))
}

func newApp() *app {
	return &app{
		stdin:            os.Stdin,
		stdout:           os.Stdout,
		stderr:           os.Stderr,
		dial:             dialIPC,
		getenv:           os.Getenv,
		stdinIsTerminal:  stdinIsTerminal,
		makeRawTerminal:  makeRawTerminal,
		notifySignals:    signal.Notify,
		stopSignalNotify: signal.Stop,
	}
}

func dialIPC(ctx context.Context, socketPath string) (daemonClient, error) {
	return ipc.Dial(ctx, socketPath)
}

func (a *app) run(args []string) int {
	cmd, err := parseArgs(args)
	if err != nil {
		fmt.Fprintf(a.stderr, "elark: %v\n", err)
		fmt.Fprint(a.stderr, usage())
		return 2
	}

	switch cmd.kind {
	case commandHelp:
		fmt.Fprint(a.stdout, usage())
		return 0
	case commandVersion:
		fmt.Fprintln(a.stdout, version.String())
		return 0
	case commandRemote:
		return a.runRemote(cmd)
	case commandHosts:
		return a.runHosts(cmd)
	case commandDoctor:
		return a.runDoctor(cmd)
	case commandSessions:
		return a.runSessions(cmd)
	case commandKill:
		return a.runKill(cmd)
	default:
		fmt.Fprint(a.stderr, usage())
		return 2
	}
}

func parseArgs(args []string) (parsedCommand, error) {
	var cmd parsedCommand
	cmd.opts.tty = ttyAuto

	positionals := make([]string, 0, len(args))
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "--" {
			positionals = append(positionals, args[i+1:]...)
			break
		}
		if !strings.HasPrefix(arg, "-") || arg == "-" {
			positionals = append(positionals, args[i:]...)
			break
		}

		switch {
		case arg == "-h" || arg == "--help":
			cmd.kind = commandHelp
			return cmd, nil
		case arg == "--version":
			cmd.kind = commandVersion
			return cmd, nil
		case arg == "-t" || arg == "--tty":
			if cmd.opts.tty == ttyNever {
				return cmd, errors.New("-t/--tty conflicts with -T/--no-tty")
			}
			cmd.opts.tty = ttyForce
		case arg == "-T" || arg == "--no-tty":
			if cmd.opts.tty == ttyForce {
				return cmd, errors.New("-T/--no-tty conflicts with -t/--tty")
			}
			cmd.opts.tty = ttyNever
		case arg == "-c" || arg == "--config":
			value, next, err := optionValue(args, i, arg)
			if err != nil {
				return cmd, err
			}
			cmd.opts.configPath = value
			i = next
		case strings.HasPrefix(arg, "--config="):
			cmd.opts.configPath = strings.TrimPrefix(arg, "--config=")
		case arg == "--cwd":
			value, next, err := optionValue(args, i, arg)
			if err != nil {
				return cmd, err
			}
			cmd.opts.cwd = value
			i = next
		case strings.HasPrefix(arg, "--cwd="):
			cmd.opts.cwd = strings.TrimPrefix(arg, "--cwd=")
		case arg == "--timeout":
			value, next, err := optionValue(args, i, arg)
			if err != nil {
				return cmd, err
			}
			timeout, err := config.ParseDuration(value)
			if err != nil {
				return cmd, fmt.Errorf("invalid --timeout: %w", err)
			}
			cmd.opts.timeout = timeout
			i = next
		case strings.HasPrefix(arg, "--timeout="):
			timeout, err := config.ParseDuration(strings.TrimPrefix(arg, "--timeout="))
			if err != nil {
				return cmd, fmt.Errorf("invalid --timeout: %w", err)
			}
			cmd.opts.timeout = timeout
		case arg == "--socket":
			value, next, err := optionValue(args, i, arg)
			if err != nil {
				return cmd, err
			}
			cmd.opts.socketPath = value
			i = next
		case strings.HasPrefix(arg, "--socket="):
			cmd.opts.socketPath = strings.TrimPrefix(arg, "--socket=")
		case arg == "--debug":
			cmd.opts.debug = true
		default:
			return cmd, fmt.Errorf("unknown option %q", arg)
		}
	}

	if len(positionals) == 0 {
		cmd.kind = commandHelp
		return cmd, nil
	}

	switch positionals[0] {
	case "hosts":
		if len(positionals) != 1 {
			return cmd, errors.New("hosts does not take arguments")
		}
		cmd.kind = commandHosts
	case "doctor":
		if len(positionals) != 1 {
			return cmd, errors.New("doctor does not take arguments")
		}
		cmd.kind = commandDoctor
	case "sessions":
		if len(positionals) != 1 {
			return cmd, errors.New("sessions does not take arguments")
		}
		cmd.kind = commandSessions
	case "kill":
		if len(positionals) != 2 {
			return cmd, errors.New("kill requires a conn_id")
		}
		cmd.kind = commandKill
		cmd.connID = positionals[1]
	default:
		cmd.kind = commandRemote
		cmd.host = positionals[0]
		if strings.TrimSpace(cmd.host) == "" {
			return cmd, errors.New("host is required")
		}
		if len(positionals) > 1 {
			cmd.command = strings.Join(positionals[1:], " ")
		}
	}

	return cmd, nil
}

func optionValue(args []string, index int, name string) (string, int, error) {
	if index+1 >= len(args) {
		return "", index, fmt.Errorf("%s requires a value", name)
	}
	value := args[index+1]
	if value == "" {
		return "", index, fmt.Errorf("%s requires a non-empty value", name)
	}
	return value, index + 1, nil
}

func (a *app) runRemote(cmd parsedCommand) int {
	cfg, socketPath, hostCfg, err := loadRemoteStartConfig(cmd.opts, cmd.host)
	if err != nil {
		if !(strings.TrimSpace(cmd.opts.socketPath) != "" && strings.TrimSpace(cmd.opts.configPath) == "" && isMissingConfigError(err)) {
			fmt.Fprintf(a.stderr, "elark: %v\n", err)
			return 1
		}
		socketPath, err = resolveSocketPath(cmd.opts)
		if err != nil {
			fmt.Fprintf(a.stderr, "elark: %v\n", err)
			return 1
		}
	}

	ctx, cancel := commandContext(cmd.opts)
	defer cancel()

	client, err := a.dial(ctx, socketPath)
	if err != nil {
		fmt.Fprintf(a.stderr, "elark: local elarkd is not running at %s; run `elarkd start`\n", socketPath)
		if cmd.opts.debug {
			fmt.Fprintf(a.stderr, "elark: dial error: %v\n", err)
		}
		return 1
	}
	defer client.Close()

	requestID, err := newRequestID()
	if err != nil {
		fmt.Fprintf(a.stderr, "elark: create request id: %v\n", err)
		return 1
	}

	rows, cols := terminalSizeFromEnv(a.getenv)
	start := ipc.StartSessionRequest{
		RequestID:  requestID,
		Host:       cmd.host,
		HostConfig: hostCfg,
		Cmd:        cmd.command,
		Pty:        shouldAllocatePTY(cmd),
		Cwd:        cmd.opts.cwd,
		Rows:       rows,
		Cols:       cols,
	}
	if start.Pty {
		start.Env = terminalEnv(a.getenv)
	}
	if strings.TrimSpace(start.Cwd) == "" && cfg != nil {
		if host, ok := cfg.Hosts[cmd.host]; ok {
			start.Cwd = host.DefaultCWD
		}
	}
	if err := client.StartSession(ctx, start); err != nil {
		fmt.Fprintf(a.stderr, "elark: start session: %v\n", err)
		return 1
	}

	startAcked := make(chan struct{})
	startAckReceived := false
	markStartAck := func() {
		if startAckReceived {
			return
		}
		startAckReceived = true
		close(startAcked)
	}

	signals := a.forwardSignals(ctx, cancel, client, requestID, start.Pty, startAcked)
	defer signals.stop()

	terminalStdin := start.Pty && a.isStdinTerminal()
	var restoreTerminal func()
	defer func() {
		if restoreTerminal != nil {
			restoreTerminal()
		}
	}()
	startRawForwarding := func() error {
		if !terminalStdin || restoreTerminal != nil {
			return nil
		}
		restore, err := a.forwardRawTerminalStdin(ctx, client, requestID)
		if err != nil {
			return err
		}
		restoreTerminal = restore
		return nil
	}

	if !terminalStdin && shouldReadStdin(a.stdin) {
		data, err := io.ReadAll(a.stdin)
		if err != nil {
			fmt.Fprintf(a.stderr, "elark: read stdin: %v\n", err)
			return 1
		}
		if len(data) > 0 {
			if err := client.SendStdin(ctx, requestID, data); err != nil {
				fmt.Fprintf(a.stderr, "elark: send stdin: %v\n", err)
				return 1
			}
		}
	}

	for {
		msg, err := client.Receive(ctx)
		if err != nil {
			select {
			case code := <-signals.interrupted:
				return code
			default:
			}
			fmt.Fprintf(a.stderr, "elark: receive daemon output: %v\n", err)
			return 1
		}
		if msg.RequestID != "" && msg.RequestID != requestID {
			continue
		}

		switch msg.Type {
		case ipc.TypeStartAck:
			markStartAck()
			if err := startRawForwarding(); err != nil {
				closeCtx, closeCancel := context.WithTimeout(context.Background(), clientInterruptCloseTimeout)
				_ = client.CloseSession(closeCtx, requestID, "local terminal setup failed")
				closeCancel()
				fmt.Fprintf(a.stderr, "elark: prepare terminal: %v\n", err)
				return 1
			}
		case ipc.TypeStdout:
			if _, err := a.stdout.Write(msg.Bytes); err != nil {
				fmt.Fprintf(a.stderr, "elark: write stdout: %v\n", err)
				return 1
			}
		case ipc.TypeStderr:
			if _, err := a.stderr.Write(msg.Bytes); err != nil {
				fmt.Fprintf(a.stderr, "elark: write stderr: %v\n", err)
				return 1
			}
		case ipc.TypeExit:
			return normalizeExitCode(msg.Code)
		case ipc.TypeError:
			rpcErr := msg.AsError()
			if rpcErr == nil {
				fmt.Fprintln(a.stderr, "elark: daemon returned an error")
			} else if rpcErr.Detail != "" {
				fmt.Fprintf(a.stderr, "elark: %s: %s\n", rpcErr.Message, rpcErr.Detail)
			} else {
				fmt.Fprintf(a.stderr, "elark: %s\n", rpcErr.Message)
			}
			return 1
		default:
			fmt.Fprintf(a.stderr, "elark: unexpected daemon message type %q\n", msg.Type)
			return 1
		}
	}
}

func shouldAllocatePTY(cmd parsedCommand) bool {
	switch cmd.opts.tty {
	case ttyForce:
		return true
	case ttyNever:
		return false
	default:
		return cmd.command == ""
	}
}

type signalForwarder struct {
	stop        func()
	interrupted <-chan int
}

func (a *app) forwardSignals(ctx context.Context, cancel context.CancelFunc, client daemonClient, requestID string, pty bool, startAcked <-chan struct{}) signalForwarder {
	sigCh := make(chan os.Signal, 4)
	notify := a.notifySignals
	if notify == nil {
		notify = signal.Notify
	}
	stopNotify := a.stopSignalNotify
	if stopNotify == nil {
		stopNotify = signal.Stop
	}
	notify(sigCh, os.Interrupt, syscall.SIGTERM, syscall.SIGWINCH)
	done := make(chan struct{})
	interrupted := make(chan int, 1)
	var closeOnce sync.Once

	closeForInterrupt := func() {
		closeOnce.Do(func() {
			select {
			case interrupted <- clientInterruptExitCode:
			default:
			}
			closeCtx, closeCancel := context.WithTimeout(context.Background(), clientInterruptCloseTimeout)
			_ = client.CloseSession(closeCtx, requestID, protocol.CloseReasonClientInterrupt)
			closeCancel()
			_ = client.Close()
			cancel()
		})
	}

	go func() {
		for {
			select {
			case <-done:
				return
			case sig := <-sigCh:
				switch sig {
				case os.Interrupt:
					if shouldCloseOnInterrupt(pty, startAcked) {
						closeForInterrupt()
						continue
					}
					_ = client.Signal(ctx, requestID, "INT")
				case syscall.SIGTERM:
					_ = client.Signal(ctx, requestID, "TERM")
				case syscall.SIGWINCH:
					rows, cols := terminalSizeFromEnv(a.getenv)
					if rows > 0 && cols > 0 {
						_ = client.Resize(ctx, requestID, rows, cols)
					}
				}
			}
		}
	}()

	return signalForwarder{
		interrupted: interrupted,
		stop: func() {
			stopNotify(sigCh)
			close(done)
		},
	}
}

func (a *app) isStdinTerminal() bool {
	if a.stdinIsTerminal != nil {
		return a.stdinIsTerminal(a.stdin)
	}
	return stdinIsTerminal(a.stdin)
}

func (a *app) forwardRawTerminalStdin(ctx context.Context, client daemonClient, requestID string) (func(), error) {
	makeRaw := a.makeRawTerminal
	if makeRaw == nil {
		makeRaw = makeRawTerminal
	}
	state, err := makeRaw(a.stdin)
	if err != nil {
		return nil, err
	}
	var restoreOnce sync.Once
	restore := func() {
		restoreOnce.Do(func() {
			_ = state.Restore()
		})
	}

	go func() {
		buf := make([]byte, 32*1024)
		for {
			n, err := a.stdin.Read(buf)
			if n > 0 {
				data := append([]byte(nil), buf[:n]...)
				if ctx.Err() != nil {
					return
				}
				if sendErr := client.SendStdin(ctx, requestID, data); sendErr != nil {
					return
				}
			}
			if err != nil {
				return
			}
		}
	}()
	return restore, nil
}

func shouldCloseOnInterrupt(pty bool, startAcked <-chan struct{}) bool {
	if !pty {
		return true
	}
	select {
	case <-startAcked:
		return false
	default:
		return true
	}
}

func (a *app) runHosts(cmd parsedCommand) int {
	cfg, path, err := loadCLIConfig(cmd.opts.configPath, true)
	if err != nil {
		fmt.Fprintf(a.stderr, "elark: %v\n", err)
		return 1
	}
	if len(cfg.Hosts) == 0 {
		fmt.Fprintf(a.stdout, "No hosts configured in %s\n", path)
		return 0
	}

	names := make([]string, 0, len(cfg.Hosts))
	for name := range cfg.Hosts {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		marker := " "
		if name == cfg.DefaultHost {
			marker = "*"
		}
		if cfg.Hosts[name].DefaultCWD != "" {
			fmt.Fprintf(a.stdout, "%s %s\tcwd=%s\n", marker, name, cfg.Hosts[name].DefaultCWD)
		} else {
			fmt.Fprintf(a.stdout, "%s %s\n", marker, name)
		}
	}
	return 0
}

func (a *app) runDoctor(cmd parsedCommand) int {
	ctx, cancel := commandContext(cmd.opts)
	defer cancel()

	opts := doctor.Options{
		ConfigPath: cmd.opts.configPath,
	}

	cfg, err := config.Load(cmd.opts.configPath)
	if err == nil {
		cfg = cloneConfig(cfg)
		if strings.TrimSpace(cmd.opts.socketPath) != "" {
			cfg.IPC.SocketPath = cmd.opts.socketPath
		}
		opts.Config = cfg
		opts.ConfigPath = ""
		socketPath, socketErr := resolveSocketPathFromRuntimeConfig(cfg)
		if socketErr == nil {
			opts.Daemon = ipcDaemonProbe{
				dial:       a.dial,
				socketPath: socketPath,
			}
		}
	}

	report := doctor.Run(ctx, opts)
	fmt.Fprintln(a.stdout, report.Text())
	if report.Failed() {
		return 1
	}
	return 0
}

func cloneConfig(cfg *config.Config) *config.Config {
	if cfg == nil {
		return nil
	}
	out := *cfg
	if cfg.Hosts != nil {
		out.Hosts = make(map[string]config.HostConfig, len(cfg.Hosts))
		for name, host := range cfg.Hosts {
			out.Hosts[name] = host
		}
	}
	out.Exec.AllowedChatIDs = append([]string(nil), cfg.Exec.AllowedChatIDs...)
	return &out
}

func (a *app) runSessions(cmd parsedCommand) int {
	socketPath, err := resolveSocketPath(cmd.opts)
	if err != nil {
		fmt.Fprintf(a.stderr, "elark: %v\n", err)
		return 1
	}
	ctx, cancel := commandContext(cmd.opts)
	defer cancel()

	client, err := a.dial(ctx, socketPath)
	if err != nil {
		fmt.Fprintf(a.stderr, "elark: local elarkd is not running at %s; run `elarkd start`\n", socketPath)
		return 1
	}
	defer client.Close()

	sessions, err := client.Sessions(ctx, ipc.SessionsRequest{})
	if err != nil {
		fmt.Fprintf(a.stderr, "elark: sessions: %v\n", err)
		return 1
	}
	if len(sessions) == 0 {
		fmt.Fprintln(a.stdout, "No active sessions")
		return 0
	}
	renderSessionsTable(a.stdout, sessions)
	return 0
}

func (a *app) runKill(cmd parsedCommand) int {
	socketPath, err := resolveSocketPath(cmd.opts)
	if err != nil {
		fmt.Fprintf(a.stderr, "elark: %v\n", err)
		return 1
	}
	ctx, cancel := commandContext(cmd.opts)
	defer cancel()

	client, err := a.dial(ctx, socketPath)
	if err != nil {
		fmt.Fprintf(a.stderr, "elark: local elarkd is not running at %s; run `elarkd start`\n", socketPath)
		return 1
	}
	defer client.Close()

	if err := client.CloseConn(ctx, cmd.connID, "kill requested by cli"); err != nil {
		fmt.Fprintf(a.stderr, "elark: kill %s: %v\n", cmd.connID, err)
		return 1
	}
	fmt.Fprintf(a.stdout, "kill requested for %s\n", cmd.connID)
	return 0
}

func renderSessionsTable(w io.Writer, sessions []ipc.SessionInfo) {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "CONN_ID\tHOST\tSTATE\tSTARTED\tLAST_PEER")
	for _, sess := range sessions {
		state := strings.TrimSpace(sess.State)
		if state == "" {
			state = "open"
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\n",
			sess.ConnID,
			emptyDash(sess.Host),
			state,
			formatSessionTime(sess.StartedAt),
			formatSessionTime(sess.LastPeerMessageAt),
		)
	}
	_ = tw.Flush()
}

func formatSessionTime(t time.Time) string {
	if t.IsZero() {
		return "-"
	}
	return t.Local().Format("2006-01-02 15:04:05")
}

func emptyDash(value string) string {
	if strings.TrimSpace(value) == "" {
		return "-"
	}
	return value
}

func commandContext(opts globalOptions) (context.Context, context.CancelFunc) {
	if opts.timeout > 0 {
		return context.WithTimeout(context.Background(), opts.timeout)
	}
	return context.WithCancel(context.Background())
}

type ipcDaemonProbe struct {
	dial       dialFunc
	socketPath string
}

func (p ipcDaemonProbe) Status(ctx context.Context, req doctor.DaemonStatusRequest) (doctor.DaemonStatus, error) {
	if p.dial == nil {
		return doctor.DaemonStatus{}, errors.New("ipc dialer is nil")
	}
	client, err := p.dial(ctx, p.socketPath)
	if err != nil {
		return doctor.DaemonStatus{}, err
	}
	defer client.Close()

	status, err := client.Status(ctx, ipc.StatusRequest{
		ConfigPath: req.ConfigPath,
		SocketPath: req.SocketPath,
		NodeName:   req.NodeName,
	})
	if err != nil {
		return doctor.DaemonStatus{}, err
	}
	return doctorStatusFromIPC(status), nil
}

func doctorStatusFromIPC(status ipc.DaemonStatus) doctor.DaemonStatus {
	out := doctor.DaemonStatus{
		Running:       status.Running,
		SocketPath:    status.SocketPath,
		SelfBotOpenID: status.SelfBotOpenID,
		Event: doctor.EventConnectionStatus{
			Checked:         status.Event.Checked,
			Connected:       status.Event.Connected,
			LastConnectedAt: status.Event.LastConnectedAt,
			LastEventAt:     status.Event.LastEventAt,
			Error:           status.Event.Error,
		},
		Outbound: doctor.OutboundQueueStatus{
			Checked:       status.Outbound.Checked,
			PendingFrames: status.Outbound.PendingFrames,
			LastSentAt:    status.Outbound.LastSentAt,
			HasLastSent:   status.Outbound.HasLastSent,
			NextFlushAt:   status.Outbound.NextFlushAt,
			HasNextFlush:  status.Outbound.HasNextFlush,
		},
	}
	for _, target := range status.Outbound.PendingTargets {
		out.Outbound.PendingTargets = append(out.Outbound.PendingTargets, outbound.Target{
			ChatID:        target.ChatID,
			RootMessageID: target.RootMessageID,
			MentionOpenID: target.MentionOpenID,
		})
	}
	return out
}

func loadRemoteStartConfig(opts globalOptions, hostName string) (*config.Config, string, ipc.HostConfig, error) {
	cfg, err := config.Load(opts.configPath)
	if err != nil {
		return nil, "", ipc.HostConfig{}, err
	}
	cfg = cloneConfig(cfg)
	if strings.TrimSpace(opts.socketPath) != "" {
		cfg.IPC.SocketPath = opts.socketPath
	}
	socketPath, err := resolveSocketPathFromRuntimeConfig(cfg)
	if err != nil {
		return nil, "", ipc.HostConfig{}, err
	}
	host, ok := cfg.Hosts[hostName]
	if !ok {
		return nil, "", ipc.HostConfig{}, fmt.Errorf("host %q is not defined in config", hostName)
	}
	return cfg, socketPath, ipcHostConfig(host), nil
}

func ipcHostConfig(host config.HostConfig) ipc.HostConfig {
	return ipc.HostConfig{
		ChatID:           host.ChatID,
		PeerBotOpenID:    host.PeerBotOpenID,
		Shell:            host.Shell,
		StreamChunkBytes: host.StreamChunkBytes,
		DefaultCWD:       host.DefaultCWD,
	}
}

func resolveSocketPath(opts globalOptions) (string, error) {
	if strings.TrimSpace(opts.socketPath) != "" {
		return expandPath(opts.socketPath)
	}

	cfg, _, err := loadCLIConfig(opts.configPath, false)
	if err == nil {
		return resolveSocketPathFromConfig(opts, cfg)
	}
	if strings.TrimSpace(opts.configPath) != "" {
		return "", err
	}
	if !isMissingConfigError(err) {
		return "", err
	}
	return expandPath(defaultSocketPath)
}

func resolveSocketPathFromRuntimeConfig(cfg *config.Config) (string, error) {
	if cfg == nil || strings.TrimSpace(cfg.IPC.SocketPath) == "" {
		return expandPath(defaultSocketPath)
	}
	return expandPath(cfg.IPC.SocketPath)
}

func resolveSocketPathFromConfig(opts globalOptions, cfg cliConfig) (string, error) {
	if strings.TrimSpace(opts.socketPath) != "" {
		return expandPath(opts.socketPath)
	}
	if strings.TrimSpace(cfg.IPC.SocketPath) != "" {
		return expandPath(cfg.IPC.SocketPath)
	}
	return expandPath(defaultSocketPath)
}

func loadCLIConfig(configPath string, required bool) (cliConfig, string, error) {
	path, err := config.ResolvePath(configPath)
	if err != nil {
		return cliConfig{}, "", err
	}
	if err := config.CheckConfigFilePermissions(path); err != nil {
		if !required && strings.TrimSpace(configPath) == "" && isMissingConfigError(err) {
			return cliConfig{}, path, err
		}
		return cliConfig{}, path, err
	}

	file, err := os.Open(path)
	if err != nil {
		return cliConfig{}, path, fmt.Errorf("open config file %s: %w", path, err)
	}
	defer file.Close()

	cfg, err := parseCLIConfig(file)
	if err != nil {
		return cliConfig{}, path, fmt.Errorf("parse config file %s: %w", path, err)
	}
	if cfg.Hosts == nil {
		cfg.Hosts = make(map[string]cliHostConfig)
	}
	return cfg, path, nil
}

func parseCLIConfig(r io.Reader) (cliConfig, error) {
	var cfg cliConfig
	cfg.Hosts = make(map[string]cliHostConfig)

	section := ""
	scanner := bufio.NewScanner(r)
	lineNo := 0
	for scanner.Scan() {
		lineNo++
		line := strings.TrimSpace(stripTomlComment(scanner.Text()))
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, "[") && strings.HasSuffix(line, "]") {
			section = strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(line, "["), "]"))
			continue
		}

		key, rawValue, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		key = strings.TrimSpace(key)
		rawValue = strings.TrimSpace(rawValue)
		if key == "app_secret" {
			continue
		}

		switch {
		case section == "" && key == "default_host":
			value, err := parseTomlStringValue(rawValue)
			if err != nil {
				return cliConfig{}, fmt.Errorf("line %d: default_host: %w", lineNo, err)
			}
			cfg.DefaultHost = value
		case section == "ipc" && key == "socket_path":
			value, err := parseTomlStringValue(rawValue)
			if err != nil {
				return cliConfig{}, fmt.Errorf("line %d: ipc.socket_path: %w", lineNo, err)
			}
			cfg.IPC.SocketPath = value
		case strings.HasPrefix(section, "hosts.") && key == "default_cwd":
			value, err := parseTomlStringValue(rawValue)
			if err != nil {
				return cliConfig{}, fmt.Errorf("line %d: %s.default_cwd: %w", lineNo, section, err)
			}
			hostName := strings.TrimPrefix(section, "hosts.")
			hostName = strings.Trim(hostName, `"`)
			host := cfg.Hosts[hostName]
			host.DefaultCWD = value
			cfg.Hosts[hostName] = host
		case strings.HasPrefix(section, "hosts."):
			hostName := strings.TrimPrefix(section, "hosts.")
			hostName = strings.Trim(hostName, `"`)
			if _, ok := cfg.Hosts[hostName]; !ok {
				cfg.Hosts[hostName] = cliHostConfig{}
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return cliConfig{}, err
	}
	return cfg, nil
}

func stripTomlComment(line string) string {
	inString := false
	escaped := false
	for i, r := range line {
		if escaped {
			escaped = false
			continue
		}
		if r == '\\' && inString {
			escaped = true
			continue
		}
		if r == '"' {
			inString = !inString
			continue
		}
		if r == '#' && !inString {
			return line[:i]
		}
	}
	return line
}

func parseTomlStringValue(raw string) (string, error) {
	var value struct {
		V string `toml:"v"`
	}
	if err := toml.Unmarshal([]byte("v = "+raw), &value); err != nil {
		return "", err
	}
	return value.V, nil
}

func isMissingConfigError(err error) bool {
	return err != nil && strings.Contains(err.Error(), "does not exist")
}

func expandPath(path string) (string, error) {
	expanded, err := config.ExpandUserPath(path)
	if err != nil {
		return "", err
	}
	if filepath.IsAbs(expanded) {
		return filepath.Clean(expanded), nil
	}
	abs, err := filepath.Abs(expanded)
	if err != nil {
		return "", fmt.Errorf("resolve path %q: %w", path, err)
	}
	return abs, nil
}

func newRequestID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return "cli-" + hex.EncodeToString(b[:]), nil
}

func shouldReadStdin(r io.Reader) bool {
	file, ok := r.(*os.File)
	if !ok {
		return true
	}
	info, err := file.Stat()
	if err != nil {
		return false
	}
	return info.Mode()&os.ModeCharDevice == 0
}

func stdinIsTerminal(r io.Reader) bool {
	file, ok := r.(*os.File)
	if !ok {
		return false
	}
	return pty.IsTerminal(int(file.Fd()))
}

func makeRawTerminal(r io.Reader) (terminalRestorer, error) {
	file, ok := r.(*os.File)
	if !ok {
		return nil, errors.New("stdin is not a terminal file")
	}
	return pty.MakeRaw(int(file.Fd()))
}

func terminalEnv(getenv func(string) string) map[string]string {
	term := ""
	if getenv != nil {
		term = strings.TrimSpace(getenv("TERM"))
	}
	if term == "" || term == "dumb" {
		term = "xterm-256color"
	}
	return map[string]string{"TERM": term}
}

func terminalSizeFromEnv(getenv func(string) string) (int, int) {
	if getenv == nil {
		return 0, 0
	}
	rows, _ := strconv.Atoi(getenv("LINES"))
	cols, _ := strconv.Atoi(getenv("COLUMNS"))
	if rows < 0 {
		rows = 0
	}
	if cols < 0 {
		cols = 0
	}
	return rows, cols
}

func normalizeExitCode(code int) int {
	if code < 0 {
		return 1
	}
	if code > 255 {
		return 255
	}
	return code
}

func usage() string {
	return `Usage:
  elark [OPTIONS] HOST [COMMAND]
  elark [OPTIONS] HOST
  elark [OPTIONS] hosts
  elark [OPTIONS] doctor
  elark [OPTIONS] sessions
  elark [OPTIONS] kill <conn_id>

Options:
  -t, --tty          allocate remote PTY
  -T, --no-tty       disable remote PTY
  -c, --config PATH  use config file (default ~/.elark/config.toml)
      --cwd PATH     remote working directory override
      --timeout DURATION
      --socket PATH  daemon Unix socket override
      --debug        include debug detail in local errors
      --version      print elark version
`
}
