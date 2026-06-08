package doctor

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/hachiwii/exec-over-lark/internal/config"
	"github.com/hachiwii/exec-over-lark/internal/outbound"
	"github.com/hachiwii/exec-over-lark/internal/session"
)

const defaultDialTimeout = 250 * time.Millisecond

type Status string

const (
	StatusOK      Status = "ok"
	StatusWarning Status = "warning"
	StatusFailed  Status = "failed"
	StatusSkipped Status = "skipped"
)

type CheckID string

const (
	CheckConfigPath        CheckID = "config_path"
	CheckConfigPermissions CheckID = "config_permissions"
	CheckConfigLoad        CheckID = "config_load"
	CheckDaemonSocket      CheckID = "daemon_socket"
	CheckDaemonStatus      CheckID = "daemon_status"
	CheckEventConnection   CheckID = "event_connection"
	CheckOutboundQueue     CheckID = "outbound_queue"
)

type Check struct {
	ID      CheckID `json:"id"`
	Status  Status  `json:"status"`
	Message string  `json:"message"`
	Detail  string  `json:"detail,omitempty"`
}

type Report struct {
	GeneratedAt time.Time `json:"generated_at"`
	ConfigPath  string    `json:"config_path,omitempty"`
	NodeName    string    `json:"node_name,omitempty"`
	Checks      []Check   `json:"checks"`
}

func (r Report) Failed() bool {
	for _, check := range r.Checks {
		if check.Status == StatusFailed {
			return true
		}
	}
	return false
}

func (r Report) Check(id CheckID) (Check, bool) {
	for _, check := range r.Checks {
		if check.ID == id {
			return check, true
		}
	}
	return Check{}, false
}

func (r Report) Text() string {
	var lines []string
	header := "exec-over-lark doctor"
	lines = append(lines, header)
	if r.ConfigPath != "" {
		lines = append(lines, "config: "+r.ConfigPath)
	}
	if r.NodeName != "" {
		lines = append(lines, "node: "+r.NodeName)
	}
	for _, check := range r.Checks {
		line := fmt.Sprintf("[%s] %s: %s", check.Status, check.ID, check.Message)
		if check.Detail != "" {
			line += " (" + check.Detail + ")"
		}
		lines = append(lines, line)
	}
	return strings.Join(lines, "\n")
}

func (r Report) String() string {
	return r.Text()
}

type Options struct {
	ConfigPath string
	Config     *config.Config

	LoadConfig func(path string) (*config.Config, error)
	Daemon     Daemon

	Now         func() time.Time
	DialTimeout time.Duration
}

type Daemon interface {
	Status(ctx context.Context, req DaemonStatusRequest) (DaemonStatus, error)
}

type DaemonStatusRequest struct {
	ConfigPath string
	SocketPath string
	NodeName   string
}

type DaemonStatus struct {
	Running       bool
	SocketPath    string
	SelfBotOpenID string

	Event    EventConnectionStatus
	Outbound OutboundQueueStatus

	LocalSessions  []session.Snapshot
	RemoteSessions []session.Snapshot
}

type EventConnectionStatus struct {
	Checked         bool
	Connected       bool
	LastConnectedAt time.Time
	LastEventAt     time.Time
	Error           string
}

type OutboundQueueStatus struct {
	Checked        bool
	PendingFrames  int
	PendingTargets []outbound.Target
	LastSentAt     time.Time
	HasLastSent    bool
	NextFlushAt    time.Time
	HasNextFlush   bool
}

func OutboundStatusFromManager(manager *outbound.Manager) OutboundQueueStatus {
	if manager == nil {
		return OutboundQueueStatus{}
	}
	status := manager.Status()
	return OutboundQueueStatus{
		Checked:        true,
		PendingFrames:  status.PendingFrames,
		PendingTargets: status.PendingTargets,
		LastSentAt:     status.LastAttemptAt,
		HasLastSent:    status.HasLastAttempt,
		NextFlushAt:    status.NextFlushAt,
		HasNextFlush:   status.HasNextFlush,
	}
}

func Run(ctx context.Context, opts Options) Report {
	if ctx == nil {
		ctx = context.Background()
	}
	r := runner{
		opts: opts,
	}
	if r.opts.Now == nil {
		r.opts.Now = time.Now
	}
	if r.opts.LoadConfig == nil {
		r.opts.LoadConfig = config.Load
	}
	if r.opts.DialTimeout <= 0 {
		r.opts.DialTimeout = defaultDialTimeout
	}
	r.report.GeneratedAt = r.opts.Now()
	r.run(ctx)
	return r.report
}

type runner struct {
	opts    Options
	report  Report
	cfg     *config.Config
	secrets []string
}

func (r *runner) run(ctx context.Context) {
	cfg, ok := r.loadConfig()
	if !ok {
		return
	}
	r.cfg = cfg
	r.report.NodeName = cfg.NodeName

	socketPath := r.socketPath(cfg)
	daemonStatus, hasDaemonStatus := r.checkDaemon(ctx, socketPath)
	r.checkEventStatus(daemonStatus, hasDaemonStatus)
	r.checkOutboundStatus(daemonStatus, hasDaemonStatus)
}

func (r *runner) loadConfig() (*config.Config, bool) {
	if r.opts.Config != nil {
		cfg := r.opts.Config
		r.secrets = appendSecret(r.secrets, cfg.Lark.AppSecret)
		if cfg.Path != "" {
			path, err := config.ResolvePath(cfg.Path)
			if err != nil {
				r.add(CheckConfigPath, StatusFailed, "config path could not be resolved", err.Error())
				return nil, false
			}
			cfg.Path = path
			r.report.ConfigPath = path
			r.add(CheckConfigPath, StatusOK, "config path resolved", path)
			if err := config.CheckConfigFilePermissions(path); err != nil {
				r.add(CheckConfigPermissions, StatusFailed, "config file permissions are not secure", err.Error())
			} else {
				r.add(CheckConfigPermissions, StatusOK, "config file permissions are secure", "file <= 0600 and parent <= 0700")
			}
		} else {
			r.add(CheckConfigPath, StatusSkipped, "config was provided in memory", "")
			r.add(CheckConfigPermissions, StatusSkipped, "config file permissions were not checked", "in-memory config has no path")
		}
		config.ApplyDefaults(cfg)
		if err := config.Validate(cfg); err != nil {
			r.add(CheckConfigLoad, StatusFailed, "config validation failed", err.Error())
			return nil, false
		}
		r.add(CheckConfigLoad, StatusOK, "config loaded and validated", "")
		return cfg, true
	}

	path, err := config.ResolvePath(r.opts.ConfigPath)
	if err != nil {
		r.add(CheckConfigPath, StatusFailed, "config path could not be resolved", err.Error())
		return nil, false
	}
	r.report.ConfigPath = path
	r.add(CheckConfigPath, StatusOK, "config path resolved", path)

	if err := config.CheckConfigFilePermissions(path); err != nil {
		r.add(CheckConfigPermissions, StatusFailed, "config file permissions are not secure", err.Error())
		return nil, false
	}
	r.add(CheckConfigPermissions, StatusOK, "config file permissions are secure", "file <= 0600 and parent <= 0700")

	cfg, err := r.opts.LoadConfig(path)
	if err != nil {
		r.add(CheckConfigLoad, StatusFailed, "config load failed", err.Error())
		return nil, false
	}
	r.secrets = appendSecret(r.secrets, cfg.Lark.AppSecret)
	config.ApplyDefaults(cfg)
	if err := config.Validate(cfg); err != nil {
		r.add(CheckConfigLoad, StatusFailed, "config validation failed", err.Error())
		return nil, false
	}
	if cfg.Path == "" {
		cfg.Path = path
	}
	r.add(CheckConfigLoad, StatusOK, "config loaded and validated", "")
	return cfg, true
}

func (r *runner) socketPath(cfg *config.Config) string {
	if cfg == nil || strings.TrimSpace(cfg.IPC.SocketPath) == "" {
		return ""
	}
	path, err := config.ExpandUserPath(cfg.IPC.SocketPath)
	if err != nil {
		return cfg.IPC.SocketPath
	}
	if filepath.IsAbs(path) {
		return filepath.Clean(path)
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return filepath.Clean(path)
	}
	return abs
}

func (r *runner) checkDaemon(ctx context.Context, socketPath string) (DaemonStatus, bool) {
	if r.cfg == nil || !r.cfg.IPC.Enabled {
		r.add(CheckDaemonSocket, StatusFailed, "ipc.enabled must be true for elark doctor", "")
		r.add(CheckDaemonStatus, StatusFailed, "daemon status requires local IPC", "enable ipc.enabled and start elarkd")
		return DaemonStatus{}, false
	}
	if strings.TrimSpace(socketPath) == "" {
		r.add(CheckDaemonSocket, StatusFailed, "ipc socket path is empty", "")
		r.add(CheckDaemonStatus, StatusFailed, "daemon status requires local IPC", "ipc.socket_path is empty")
		return DaemonStatus{}, false
	}

	if r.opts.Daemon != nil {
		status, err := r.opts.Daemon.Status(ctx, DaemonStatusRequest{
			ConfigPath: r.report.ConfigPath,
			SocketPath: socketPath,
			NodeName:   r.report.NodeName,
		})
		if err != nil {
			r.add(CheckDaemonSocket, StatusFailed, "daemon socket is not reachable", err.Error())
			r.add(CheckDaemonStatus, StatusFailed, "daemon status request failed", err.Error())
			return DaemonStatus{}, false
		}
		detail := "daemon reported stopped"
		checkStatus := StatusFailed
		if status.Running {
			checkStatus = StatusOK
			detail = "daemon reported running"
		}
		if status.SocketPath != "" {
			detail += " at " + status.SocketPath
		}
		r.add(CheckDaemonStatus, checkStatus, "daemon status received", detail)
		if status.Running {
			r.add(CheckDaemonSocket, StatusOK, "daemon socket is reachable", firstNonEmpty(status.SocketPath, socketPath))
		} else {
			r.add(CheckDaemonSocket, StatusFailed, "daemon socket is not reachable", firstNonEmpty(status.SocketPath, socketPath))
		}
		return status, true
	}

	if err := checkSocketFile(socketPath); err != nil {
		r.add(CheckDaemonSocket, StatusFailed, "daemon socket is not ready", err.Error())
		r.add(CheckDaemonStatus, StatusSkipped, "daemon status was not requested", "socket check failed")
		return DaemonStatus{}, false
	}
	conn, err := net.DialTimeout("unix", socketPath, r.opts.DialTimeout)
	if err != nil {
		r.add(CheckDaemonSocket, StatusFailed, "daemon socket is not reachable", err.Error())
		r.add(CheckDaemonStatus, StatusSkipped, "daemon status was not requested", "no daemon probe configured")
		return DaemonStatus{}, false
	}
	_ = conn.Close()
	r.add(CheckDaemonSocket, StatusOK, "daemon socket accepts connections", socketPath)
	r.add(CheckDaemonStatus, StatusSkipped, "daemon status was not requested", "no daemon probe configured")
	return DaemonStatus{}, false
}

func (r *runner) checkEventStatus(status DaemonStatus, hasStatus bool) {
	if !hasStatus || !status.Event.Checked {
		r.add(CheckEventConnection, StatusSkipped, "event connection status is not available", "")
		return
	}
	if status.Event.Connected {
		r.add(CheckEventConnection, StatusOK, "lark event connection is connected", eventDetail(status.Event))
		return
	}
	detail := eventDetail(status.Event)
	r.add(CheckEventConnection, StatusFailed, "lark event connection is disconnected", detail)
}

func (r *runner) checkOutboundStatus(status DaemonStatus, hasStatus bool) {
	if !hasStatus || !status.Outbound.Checked {
		r.add(CheckOutboundQueue, StatusSkipped, "outbound queue status is not available", "")
		return
	}
	queue := status.Outbound
	detail := outboundDetail(queue)
	if queue.PendingFrames > 0 {
		r.add(CheckOutboundQueue, StatusWarning, "outbound queue has pending frames", detail)
		return
	}
	r.add(CheckOutboundQueue, StatusOK, "outbound queue is empty", detail)
}

func (r *runner) add(id CheckID, status Status, message, detail string) {
	r.report.Checks = append(r.report.Checks, Check{
		ID:      id,
		Status:  status,
		Message: redactSensitive(message, r.secrets),
		Detail:  redactSensitive(detail, r.secrets),
	})
}

func checkSocketFile(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("daemon socket %s does not exist; run elarkd start", path)
		}
		return fmt.Errorf("stat daemon socket %s: %w", path, err)
	}
	if info.Mode()&os.ModeSocket == 0 {
		return fmt.Errorf("daemon socket %s is not a unix socket", path)
	}
	if info.Mode().Perm()&^0o600 != 0 {
		return fmt.Errorf("daemon socket %s permissions must not be wider than 0600", path)
	}
	return nil
}

func eventDetail(status EventConnectionStatus) string {
	var parts []string
	if !status.LastConnectedAt.IsZero() {
		parts = append(parts, "last_connected_at="+status.LastConnectedAt.Format(time.RFC3339))
	}
	if !status.LastEventAt.IsZero() {
		parts = append(parts, "last_event_at="+status.LastEventAt.Format(time.RFC3339))
	}
	if strings.TrimSpace(status.Error) != "" {
		parts = append(parts, "error="+strings.TrimSpace(status.Error))
	}
	return strings.Join(parts, " ")
}

func outboundDetail(status OutboundQueueStatus) string {
	parts := []string{fmt.Sprintf("pending_frames=%d", status.PendingFrames)}
	if status.HasLastSent {
		parts = append(parts, "last_sent_at="+status.LastSentAt.Format(time.RFC3339))
	}
	if status.HasNextFlush {
		parts = append(parts, "next_flush_at="+status.NextFlushAt.Format(time.RFC3339))
	}
	if len(status.PendingTargets) > 0 {
		parts = append(parts, "queued_targets="+strings.Join(formatTargets(status.PendingTargets), ","))
	}
	return strings.Join(parts, " ")
}

func formatTargets(targets []outbound.Target) []string {
	out := make([]string, 0, len(targets))
	for _, target := range targets {
		if target.RootMessageID == "" {
			out = append(out, target.ChatID+"/root")
		} else {
			out = append(out, target.ChatID+"/"+target.RootMessageID)
		}
	}
	sort.Strings(out)
	return out
}

func appendSecret(secrets []string, secret string) []string {
	secret = strings.TrimSpace(secret)
	if len(secret) < 4 {
		return secrets
	}
	for _, existing := range secrets {
		if existing == secret {
			return secrets
		}
	}
	return append(secrets, secret)
}

var sensitivePatterns = []*regexp.Regexp{
	regexp.MustCompile(`(?i)(app_secret\s*[:=]\s*)[^,\s;]+`),
	regexp.MustCompile(`(?i)(tenant_access_token\s*[:=]\s*)[^,\s;]+`),
	regexp.MustCompile(`(?i)(access_token\s*[:=]\s*)[^,\s;]+`),
	regexp.MustCompile(`(?i)(authorization\s*[:=]\s*)[^,\s;]+`),
	regexp.MustCompile(`(?i)(bearer\s+)[^,\s;]+`),
}

func redactSensitive(text string, secrets []string) string {
	if text == "" {
		return ""
	}
	out := text
	for _, secret := range secrets {
		if strings.TrimSpace(secret) != "" {
			out = strings.ReplaceAll(out, secret, "[redacted]")
		}
	}
	for _, pattern := range sensitivePatterns {
		out = pattern.ReplaceAllString(out, "${1}[redacted]")
	}
	return out
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
