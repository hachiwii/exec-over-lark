package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/pelletier/go-toml/v2"
)

const (
	DefaultConfigDir  = ".elark"
	DefaultConfigFile = "config.toml"

	DefaultSendCooldown              = time.Second
	DefaultLarkTextRequestLimitBytes = 153600
	DefaultHeartbeatInterval         = 10 * time.Second
	DefaultHeartbeatTimeout          = 30 * time.Second
	DefaultSequenceGapTimeout        = 30 * time.Second
	DefaultStreamChunkBytes          = 12000
	DefaultMaxSessions               = 8
)

type Role string

const (
	RoleClient Role = "client"
	RoleServer Role = "server"
)

type Config struct {
	Path        string                `toml:"-"`
	NodeName    string                `toml:"node_name"`
	DefaultHost string                `toml:"default_host"`
	IPC         IPCConfig             `toml:"ipc"`
	Lark        LarkConfig            `toml:"lark"`
	Connection  ConnectionConfig      `toml:"connection"`
	Exec        ExecConfig            `toml:"exec"`
	Hosts       map[string]HostConfig `toml:"hosts"`
}

type IPCConfig struct {
	Enabled    bool   `toml:"enabled"`
	SocketPath string `toml:"socket_path"`
}

type LarkConfig struct {
	AppID                     string   `toml:"app_id"`
	AppSecret                 string   `toml:"app_secret"`
	SendCooldown              Duration `toml:"send_cooldown"`
	LarkTextRequestLimitBytes int      `toml:"lark_text_request_limit_bytes"`
}

type ConnectionConfig struct {
	HeartbeatInterval  Duration `toml:"heartbeat_interval"`
	HeartbeatTimeout   Duration `toml:"heartbeat_timeout"`
	SequenceGapTimeout Duration `toml:"sequence_gap_timeout"`
}

type ExecConfig struct {
	Enabled              bool     `toml:"enabled"`
	DefaultShell         string   `toml:"default_shell"`
	MaxSessions          int      `toml:"max_sessions"`
	StreamChunkBytes     int      `toml:"stream_chunk_bytes"`
	AllowedChatIDs       []string `toml:"allowed_chat_ids"`
	AllowedSenderOpenIDs []string `toml:"allowed_sender_open_ids"`
}

type HostConfig struct {
	ChatID           string `toml:"chat_id"`
	PeerBotOpenID    string `toml:"peer_bot_open_id"`
	Shell            string `toml:"shell"`
	StreamChunkBytes int    `toml:"stream_chunk_bytes"`
	DefaultCWD       string `toml:"default_cwd"`
}

type Duration time.Duration

func (d Duration) Duration() time.Duration {
	return time.Duration(d)
}

func (d Duration) String() string {
	return time.Duration(d).String()
}

func (d *Duration) UnmarshalText(text []byte) error {
	parsed, err := ParseDuration(string(text))
	if err != nil {
		return err
	}
	*d = Duration(parsed)
	return nil
}

func (d Duration) MarshalText() ([]byte, error) {
	return []byte(time.Duration(d).String()), nil
}

var durationTokenRE = regexp.MustCompile(`([0-9]+(?:\.[0-9]+)?)(ms|s|m|h|d)`)

func ParseDuration(value string) (time.Duration, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, errors.New("duration is empty")
	}

	matches := durationTokenRE.FindAllStringSubmatchIndex(value, -1)
	if len(matches) == 0 {
		return 0, fmt.Errorf("invalid duration %q", value)
	}

	var total time.Duration
	pos := 0
	for _, match := range matches {
		if match[0] != pos {
			return 0, fmt.Errorf("invalid duration %q", value)
		}

		number := value[match[2]:match[3]]
		unit := value[match[4]:match[5]]
		amount, err := strconv.ParseFloat(number, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid duration %q", value)
		}

		var unitDuration time.Duration
		switch unit {
		case "ms":
			unitDuration = time.Millisecond
		case "s":
			unitDuration = time.Second
		case "m":
			unitDuration = time.Minute
		case "h":
			unitDuration = time.Hour
		case "d":
			unitDuration = 24 * time.Hour
		default:
			return 0, fmt.Errorf("invalid duration unit %q", unit)
		}

		total += time.Duration(amount * float64(unitDuration))
		pos = match[1]
	}
	if pos != len(value) {
		return 0, fmt.Errorf("invalid duration %q", value)
	}
	return total, nil
}

func DefaultPath() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("resolve home directory: %w", err)
	}
	if home == "" {
		return "", errors.New("resolve home directory: HOME is empty")
	}
	return filepath.Join(home, DefaultConfigDir, DefaultConfigFile), nil
}

func ResolvePath(configPath string) (string, error) {
	if strings.TrimSpace(configPath) == "" {
		return DefaultPath()
	}

	expanded, err := ExpandUserPath(configPath)
	if err != nil {
		return "", err
	}
	if filepath.IsAbs(expanded) {
		return filepath.Clean(expanded), nil
	}
	abs, err := filepath.Abs(expanded)
	if err != nil {
		return "", fmt.Errorf("resolve config path %q: %w", configPath, err)
	}
	return abs, nil
}

func ExpandUserPath(path string) (string, error) {
	if path == "~" || strings.HasPrefix(path, "~/") {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", fmt.Errorf("resolve home directory: %w", err)
		}
		if home == "" {
			return "", errors.New("resolve home directory: HOME is empty")
		}
		if path == "~" {
			return home, nil
		}
		return filepath.Join(home, path[2:]), nil
	}
	return path, nil
}

func Load(configPath string) (*Config, error) {
	path, err := ResolvePath(configPath)
	if err != nil {
		return nil, err
	}
	if err := CheckConfigFilePermissions(path); err != nil {
		return nil, err
	}

	content, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config file %s: %w", path, err)
	}

	var cfg Config
	if err := toml.Unmarshal(content, &cfg); err != nil {
		return nil, fmt.Errorf("parse config file %s: %w", path, err)
	}
	cfg.Path = path
	ApplyDefaults(&cfg)
	if err := Validate(&cfg); err != nil {
		return nil, fmt.Errorf("validate config file %s: %w", path, err)
	}
	return &cfg, nil
}

func ApplyDefaults(cfg *Config) {
	if cfg.Lark.SendCooldown == 0 {
		cfg.Lark.SendCooldown = Duration(DefaultSendCooldown)
	}
	if cfg.Lark.LarkTextRequestLimitBytes == 0 {
		cfg.Lark.LarkTextRequestLimitBytes = DefaultLarkTextRequestLimitBytes
	}
	if cfg.Connection.HeartbeatInterval == 0 {
		cfg.Connection.HeartbeatInterval = Duration(DefaultHeartbeatInterval)
	}
	if cfg.Connection.HeartbeatTimeout == 0 {
		cfg.Connection.HeartbeatTimeout = Duration(DefaultHeartbeatTimeout)
	}
	if cfg.Connection.SequenceGapTimeout == 0 {
		cfg.Connection.SequenceGapTimeout = Duration(DefaultSequenceGapTimeout)
	}
	if cfg.Exec.MaxSessions == 0 {
		cfg.Exec.MaxSessions = DefaultMaxSessions
	}
	if cfg.Exec.StreamChunkBytes == 0 {
		cfg.Exec.StreamChunkBytes = DefaultStreamChunkBytes
	}
	for name, host := range cfg.Hosts {
		if host.StreamChunkBytes == 0 {
			host.StreamChunkBytes = DefaultStreamChunkBytes
			cfg.Hosts[name] = host
		}
	}
}

func Validate(cfg *Config) error {
	if cfg == nil {
		return errors.New("config is nil")
	}
	if strings.TrimSpace(cfg.NodeName) == "" {
		return errors.New("node_name is required")
	}
	if strings.TrimSpace(cfg.Lark.AppID) == "" {
		return errors.New("lark.app_id is required")
	}
	if strings.TrimSpace(cfg.Lark.AppSecret) == "" {
		return errors.New("lark.app_secret is required")
	}
	if cfg.Lark.SendCooldown.Duration() <= 0 {
		return errors.New("lark.send_cooldown must be greater than zero")
	}
	if cfg.Lark.LarkTextRequestLimitBytes != DefaultLarkTextRequestLimitBytes {
		return fmt.Errorf("lark.lark_text_request_limit_bytes must be %d", DefaultLarkTextRequestLimitBytes)
	}

	heartbeatInterval := cfg.Connection.HeartbeatInterval.Duration()
	heartbeatTimeout := cfg.Connection.HeartbeatTimeout.Duration()
	if heartbeatInterval <= 0 {
		return errors.New("connection.heartbeat_interval must be greater than zero")
	}
	if heartbeatTimeout <= 0 {
		return errors.New("connection.heartbeat_timeout must be greater than zero")
	}
	if cfg.Connection.SequenceGapTimeout.Duration() <= 0 {
		return errors.New("connection.sequence_gap_timeout must be greater than zero")
	}
	if heartbeatInterval >= heartbeatTimeout {
		return errors.New("connection.heartbeat_interval must be less than connection.heartbeat_timeout")
	}

	if cfg.IPC.Enabled {
		socketPath, err := ExpandUserPath(cfg.IPC.SocketPath)
		if err != nil {
			return err
		}
		if strings.TrimSpace(socketPath) == "" {
			return errors.New("ipc.socket_path is required when ipc.enabled is true")
		}
		if err := CheckSecureDirectory(filepath.Dir(socketPath), "ipc socket directory"); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}

	if cfg.DefaultHost != "" {
		if cfg.Hosts == nil {
			return fmt.Errorf("default_host %q is not defined in hosts", cfg.DefaultHost)
		}
		if _, ok := cfg.Hosts[cfg.DefaultHost]; !ok {
			return fmt.Errorf("default_host %q is not defined in hosts", cfg.DefaultHost)
		}
	}
	for name, host := range cfg.Hosts {
		if strings.TrimSpace(name) == "" {
			return errors.New("hosts contains an empty host name")
		}
		if err := validateLarkID("hosts."+name+".chat_id", host.ChatID, "oc_"); err != nil {
			return err
		}
		if err := validateLarkID("hosts."+name+".peer_bot_open_id", host.PeerBotOpenID, "ou_"); err != nil {
			return err
		}
		if host.StreamChunkBytes <= 0 {
			return fmt.Errorf("hosts.%s.stream_chunk_bytes must be greater than zero", name)
		}
	}

	if cfg.Exec.Enabled {
		if strings.TrimSpace(cfg.Exec.DefaultShell) == "" {
			return errors.New("exec.default_shell is required when exec.enabled is true")
		}
		if cfg.Exec.MaxSessions <= 0 {
			return errors.New("exec.max_sessions must be greater than zero")
		}
		if cfg.Exec.StreamChunkBytes <= 0 {
			return errors.New("exec.stream_chunk_bytes must be greater than zero")
		}
	}
	if cfg.Exec.AllowedChatIDs != nil {
		if len(cfg.Exec.AllowedChatIDs) == 0 {
			return errors.New("exec.allowed_chat_ids must be omitted or non-empty")
		}
		for i, id := range cfg.Exec.AllowedChatIDs {
			if err := validateLarkID(fmt.Sprintf("exec.allowed_chat_ids[%d]", i), id, "oc_"); err != nil {
				return err
			}
		}
	}
	if cfg.Exec.AllowedSenderOpenIDs != nil {
		if len(cfg.Exec.AllowedSenderOpenIDs) == 0 {
			return errors.New("exec.allowed_sender_open_ids must be omitted or non-empty")
		}
		for i, id := range cfg.Exec.AllowedSenderOpenIDs {
			if err := validateLarkID(fmt.Sprintf("exec.allowed_sender_open_ids[%d]", i), id, "ou_"); err != nil {
				return err
			}
		}
	}

	return nil
}

var larkIDRE = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

func validateLarkID(field, value, prefix string) error {
	value = strings.TrimSpace(value)
	if value == "" {
		return fmt.Errorf("%s is required", field)
	}
	if !strings.HasPrefix(value, prefix) {
		return fmt.Errorf("%s must start with %q", field, prefix)
	}
	if !larkIDRE.MatchString(value) {
		return fmt.Errorf("%s has invalid characters", field)
	}
	return nil
}

func CheckConfigFilePermissions(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("config file %s does not exist; run elarkd init --client or elarkd init --server", path)
		}
		return fmt.Errorf("stat config file %s: %w", path, err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("config file %s must not be a symlink", path)
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("config file %s must be a regular file", path)
	}
	if err := checkCurrentUserOwner(info, "config file "+path); err != nil {
		return err
	}
	if info.Mode().Perm()&^0o600 != 0 {
		return fmt.Errorf("config file %s permissions must not be wider than 0600", path)
	}
	return CheckSecureDirectory(filepath.Dir(path), "config file parent directory")
}

func CheckSecureDirectory(path, label string) error {
	info, err := os.Stat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("%s %s does not exist: %w", label, path, os.ErrNotExist)
		}
		return fmt.Errorf("stat %s %s: %w", label, path, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("%s %s must be a directory", label, path)
	}
	if err := checkCurrentUserOwner(info, label+" "+path); err != nil {
		return err
	}
	if info.Mode().Perm()&^0o700 != 0 {
		return fmt.Errorf("%s %s permissions must not be wider than 0700", label, path)
	}
	return nil
}

func checkCurrentUserOwner(info os.FileInfo, label string) error {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return fmt.Errorf("cannot determine owner for %s", label)
	}
	if int(stat.Uid) != os.Getuid() {
		return fmt.Errorf("%s must be owned by the current user", label)
	}
	return nil
}

func WriteInitTemplate(configPath string, role Role, force bool) (string, error) {
	path, err := ResolvePath(configPath)
	if err != nil {
		return "", err
	}
	body, err := Template(role)
	if err != nil {
		return "", err
	}

	parent := filepath.Dir(path)
	if err := os.MkdirAll(parent, 0o700); err != nil {
		return "", fmt.Errorf("create config directory %s: %w", parent, err)
	}
	if err := CheckSecureDirectory(parent, "config file parent directory"); err != nil {
		return "", err
	}

	flags := os.O_WRONLY | os.O_CREATE
	if force {
		flags |= os.O_TRUNC
	} else {
		flags |= os.O_EXCL
	}
	file, err := os.OpenFile(path, flags, 0o600)
	if err != nil {
		if errors.Is(err, os.ErrExist) {
			return "", fmt.Errorf("config file %s already exists; use --force to overwrite", path)
		}
		return "", fmt.Errorf("create config file %s: %w", path, err)
	}
	defer file.Close()

	if err := file.Chmod(0o600); err != nil {
		return "", fmt.Errorf("set config file permissions %s: %w", path, err)
	}
	if _, err := file.WriteString(body); err != nil {
		return "", fmt.Errorf("write config file %s: %w", path, err)
	}
	return path, nil
}

func Template(role Role) (string, error) {
	switch role {
	case RoleClient:
		return clientTemplate, nil
	case RoleServer:
		return serverTemplate, nil
	default:
		return "", fmt.Errorf("unknown init role %q", role)
	}
}

const clientTemplate = `node_name = "local"
default_host = "example"

[ipc]
enabled = true
socket_path = "~/.local/run/exec-over-lark/elarkd.sock"

[lark]
app_id = "cli_client_xxx"
app_secret = "client_secret_xxx"
send_cooldown = "1000ms"
lark_text_request_limit_bytes = 153600

[connection]
heartbeat_interval = "10s"
heartbeat_timeout = "30s"
sequence_gap_timeout = "30s"

[exec]
enabled = false

[hosts.example]
chat_id = "oc_xxx"
peer_bot_open_id = "ou_server_bot"
shell = "/bin/zsh"
stream_chunk_bytes = 12000
`

const serverTemplate = `node_name = "server"

[ipc]
enabled = false
socket_path = "~/.local/run/exec-over-lark/elarkd.sock"

[lark]
app_id = "cli_server_xxx"
app_secret = "server_secret_xxx"
send_cooldown = "1000ms"
lark_text_request_limit_bytes = 153600

[connection]
heartbeat_interval = "10s"
heartbeat_timeout = "30s"
sequence_gap_timeout = "30s"

[exec]
enabled = true
default_shell = "/bin/zsh"
max_sessions = 8
stream_chunk_bytes = 12000
# Optional allowlists. Omit these fields to allow all chats or senders.
# allowed_chat_ids = ["oc_xxx", "oc_yyy"]
# allowed_sender_open_ids = ["ou_client_bot"]
`
