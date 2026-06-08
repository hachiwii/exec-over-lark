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
	"os/exec"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/hachiwii/exec-over-lark/internal/config"
	"github.com/hachiwii/exec-over-lark/internal/doctor"
	"github.com/hachiwii/exec-over-lark/internal/ipc"
	"github.com/hachiwii/exec-over-lark/internal/outbound"
	"github.com/hachiwii/exec-over-lark/internal/protocol"
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
	Status(context.Context, ipc.StatusRequest) (ipc.DaemonStatus, error)
	Receive(context.Context) (ipc.Message, error)
	Close() error
}

type dialFunc func(context.Context, string) (daemonClient, error)
type daemonStarter func(context.Context, string, io.Writer, io.Writer) error

type app struct {
	stdin  io.Reader
	stdout io.Writer
	stderr io.Writer

	dial        dialFunc
	startDaemon daemonStarter
	getenv      func(string) string

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
	commandRemote
	commandHosts
	commandDoctor
	commandDaemon
	commandSessions
	commandAttach
	commandKill
)

type parsedCommand struct {
	kind      commandKind
	opts      globalOptions
	host      string
	command   string
	daemonSub string
	connID    string
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
		startDaemon:      startDaemonProcess,
		getenv:           os.Getenv,
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
	case commandRemote:
		return a.runRemote(cmd)
	case commandHosts:
		return a.runHosts(cmd)
	case commandDoctor:
		return a.runDoctor(cmd)
	case commandDaemon:
		return a.runDaemon(cmd)
	case commandSessions:
		return a.runSessions(cmd)
	case commandAttach:
		return a.runAttach(cmd)
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
	case "daemon":
		if len(positionals) != 2 {
			return cmd, errors.New("daemon requires one subcommand: start, status, or stop")
		}
		switch positionals[1] {
		case "start", "status", "stop":
			cmd.kind = commandDaemon
			cmd.daemonSub = positionals[1]
		default:
			return cmd, fmt.Errorf("unknown daemon subcommand %q", positionals[1])
		}
	case "sessions":
		if len(positionals) != 1 {
			return cmd, errors.New("sessions does not take arguments")
		}
		cmd.kind = commandSessions
	case "attach":
		if len(positionals) != 2 {
			return cmd, errors.New("attach requires a conn_id")
		}
		cmd.kind = commandAttach
		cmd.connID = positionals[1]
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

	if shouldReadStdin(a.stdin) {
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

func (a *app) runDaemon(cmd parsedCommand) int {
	socketPath, err := resolveSocketPath(cmd.opts)
	if err != nil {
		fmt.Fprintf(a.stderr, "elark: %v\n", err)
		return 1
	}

	switch cmd.daemonSub {
	case "status":
		if a.daemonReachable(cmd.opts, socketPath) {
			fmt.Fprintf(a.stdout, "elarkd is running at %s\n", socketPath)
			return 0
		}
		fmt.Fprintf(a.stderr, "elarkd is not running at %s\n", socketPath)
		return 1
	case "start":
		if a.daemonReachable(cmd.opts, socketPath) {
			fmt.Fprintf(a.stdout, "elarkd is already running at %s\n", socketPath)
			return 0
		}
		ctx, cancel := commandContext(cmd.opts)
		defer cancel()
		if err := a.startDaemon(ctx, cmd.opts.configPath, a.stdout, a.stderr); err != nil {
			fmt.Fprintf(a.stderr, "elark: start daemon: %v\n", err)
			return 1
		}
		fmt.Fprintln(a.stdout, "elarkd start requested")
		return 0
	case "stop":
		fmt.Fprintln(a.stderr, "elark daemon stop requires daemon control RPC, which is not available in IPC v1")
		return 1
	default:
		fmt.Fprintf(a.stderr, "elark: unknown daemon subcommand %q\n", cmd.daemonSub)
		return 2
	}
}

func (a *app) runSessions(cmd parsedCommand) int {
	fmt.Fprintln(a.stderr, "elark sessions requires daemon sessions RPC, which is not available in IPC v1")
	return 1
}

func (a *app) runAttach(cmd parsedCommand) int {
	fmt.Fprintf(a.stderr, "elark attach %s requires daemon attach RPC, which is not available in IPC v1\n", cmd.connID)
	return 1
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

	if err := client.CloseSession(ctx, cmd.connID, "kill requested by cli"); err != nil {
		fmt.Fprintf(a.stderr, "elark: kill %s: %v\n", cmd.connID, err)
		return 1
	}
	fmt.Fprintf(a.stdout, "kill requested for %s\n", cmd.connID)
	return 0
}

func (a *app) daemonReachable(opts globalOptions, socketPath string) bool {
	ctx, cancel := commandContext(opts)
	defer cancel()

	client, err := a.dial(ctx, socketPath)
	if err != nil {
		return false
	}
	_ = client.Close()
	return true
}

func commandContext(opts globalOptions) (context.Context, context.CancelFunc) {
	if opts.timeout > 0 {
		return context.WithTimeout(context.Background(), opts.timeout)
	}
	return context.WithCancel(context.Background())
}

func startDaemonProcess(ctx context.Context, configPath string, stdout, stderr io.Writer) error {
	args := []string{"run"}
	if strings.TrimSpace(configPath) != "" {
		path, err := config.ResolvePath(configPath)
		if err != nil {
			return err
		}
		args = append(args, "--config", path)
	}

	cmd := exec.Command("elarkd", args...)
	cmd.Stdin = nil
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	if err := cmd.Start(); err != nil {
		return err
	}
	return nil
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
  elark [OPTIONS] attach <conn_id>
  elark [OPTIONS] kill <conn_id>
  elark [OPTIONS] daemon start|status|stop

Options:
  -t, --tty          allocate remote PTY
  -T, --no-tty       disable remote PTY
  -c, --config PATH  use config file (default ~/.elark/config.toml)
      --cwd PATH     remote working directory override
      --timeout DURATION
      --socket PATH  daemon Unix socket override
      --debug        include debug detail in local errors
`
}
