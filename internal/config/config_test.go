package config

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestDefaultPathUsesHome(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)

	got, err := DefaultPath()
	if err != nil {
		t.Fatalf("DefaultPath returned error: %v", err)
	}
	want := filepath.Join(home, ".elark", "config.toml")
	if got != want {
		t.Fatalf("DefaultPath = %q, want %q", got, want)
	}

	got, err = ResolvePath("~/.custom/elark.toml")
	if err != nil {
		t.Fatalf("ResolvePath returned error: %v", err)
	}
	want = filepath.Join(home, ".custom", "elark.toml")
	if got != want {
		t.Fatalf("ResolvePath = %q, want %q", got, want)
	}
}

func TestWriteInitTemplateClientDefaultPath(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)

	path, err := WriteInitTemplate("", RoleClient, false)
	if err != nil {
		t.Fatalf("WriteInitTemplate returned error: %v", err)
	}
	wantPath := filepath.Join(home, ".elark", "config.toml")
	if path != wantPath {
		t.Fatalf("path = %q, want %q", path, wantPath)
	}

	assertMode(t, filepath.Dir(path), 0o700)
	assertMode(t, path, 0o600)

	content := readFile(t, path)
	for _, required := range []string{
		`[ipc]`,
		`enabled = true`,
		`[exec]`,
		`enabled = false`,
		`[hosts.example]`,
	} {
		if !strings.Contains(content, required) {
			t.Fatalf("client template missing %q:\n%s", required, content)
		}
	}
}

func TestWriteInitTemplateRejectsExistingWithoutForce(t *testing.T) {
	dir := t.TempDir()
	chmod(t, dir, 0o700)
	path := filepath.Join(dir, "config.toml")
	if err := os.WriteFile(path, []byte("existing"), 0o600); err != nil {
		t.Fatal(err)
	}

	_, err := WriteInitTemplate(path, RoleServer, false)
	if err == nil {
		t.Fatal("expected existing file error")
	}
	if got := readFile(t, path); got != "existing" {
		t.Fatalf("file was overwritten without force: %q", got)
	}

	if _, err := WriteInitTemplate(path, RoleServer, true); err != nil {
		t.Fatalf("force WriteInitTemplate returned error: %v", err)
	}
	assertMode(t, path, 0o600)
	if got := readFile(t, path); !strings.Contains(got, "allowed_chat_ids") {
		t.Fatalf("server template was not written:\n%s", got)
	}
}

func TestWriteInitTemplateRejectsWideParent(t *testing.T) {
	root := t.TempDir()
	parent := filepath.Join(root, "wide")
	if err := os.Mkdir(parent, 0o755); err != nil {
		t.Fatal(err)
	}
	defer os.Chmod(parent, 0o700)

	_, err := WriteInitTemplate(filepath.Join(parent, "config.toml"), RoleClient, false)
	if err == nil {
		t.Fatal("expected wide parent permission error")
	}
	if !strings.Contains(err.Error(), "0700") {
		t.Fatalf("error = %v, want 0700 mention", err)
	}
}

func TestLoadValidConfigAppliesDefaults(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.toml")
	writeConfig(t, path, `node_name = "local"
default_host = "dev"

[ipc]
enabled = false

[lark]
app_id = "cli_client_xxx"
app_secret = "redacted"

[exec]
enabled = false

[hosts.dev]
chat_id = "oc_dev"
peer_bot_open_id = "ou_server"
`)

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if cfg.Path != path {
		t.Fatalf("Path = %q, want %q", cfg.Path, path)
	}
	if cfg.Lark.SendCooldown.Duration() != 500*time.Millisecond {
		t.Fatalf("SendCooldown = %s", cfg.Lark.SendCooldown)
	}
	if cfg.Lark.LarkTextRequestLimitBytes != 153600 {
		t.Fatalf("LarkTextRequestLimitBytes = %d", cfg.Lark.LarkTextRequestLimitBytes)
	}
	if cfg.Connection.HeartbeatInterval.Duration() != 10*time.Second {
		t.Fatalf("HeartbeatInterval = %s", cfg.Connection.HeartbeatInterval)
	}
	if cfg.Connection.HeartbeatTimeout.Duration() != 30*time.Second {
		t.Fatalf("HeartbeatTimeout = %s", cfg.Connection.HeartbeatTimeout)
	}
	if cfg.Connection.SequenceGapTimeout.Duration() != 30*time.Second {
		t.Fatalf("SequenceGapTimeout = %s", cfg.Connection.SequenceGapTimeout)
	}
	if cfg.Hosts["dev"].StreamChunkBytes != 12000 {
		t.Fatalf("host stream chunk default = %d", cfg.Hosts["dev"].StreamChunkBytes)
	}
}

func TestLoadRejectsMissingConfigWithInitHint(t *testing.T) {
	dir := t.TempDir()
	_, err := Load(filepath.Join(dir, "missing.toml"))
	if err == nil {
		t.Fatal("expected missing config error")
	}
	for _, want := range []string{"does not exist", "elarkd init --client", "elarkd init --server"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error = %v, want %q", err, want)
		}
	}
}

func TestLoadRejectsWideConfigFilePermissions(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.toml")
	writeConfig(t, path, validServerConfig())
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}

	_, err := Load(path)
	if err == nil {
		t.Fatal("expected wide config file permission error")
	}
	if !strings.Contains(err.Error(), "0600") {
		t.Fatalf("error = %v, want 0600 mention", err)
	}
}

func TestLoadRejectsWideConfigParentPermissions(t *testing.T) {
	root := t.TempDir()
	parent := filepath.Join(root, "wide")
	if err := os.Mkdir(parent, 0o755); err != nil {
		t.Fatal(err)
	}
	defer os.Chmod(parent, 0o700)

	path := filepath.Join(parent, "config.toml")
	writeConfig(t, path, validServerConfig())
	chmod(t, parent, 0o755)
	defer chmod(t, parent, 0o700)

	_, err := Load(path)
	if err == nil {
		t.Fatal("expected wide parent permission error")
	}
	if !strings.Contains(err.Error(), "0700") {
		t.Fatalf("error = %v, want 0700 mention", err)
	}
}

func TestLoadRejectsExplicitEmptyAllowlists(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.toml")
	writeConfig(t, path, `node_name = "server"

[ipc]
enabled = false

[lark]
app_id = "cli_server_xxx"
app_secret = "redacted"

[exec]
enabled = true
default_shell = "/bin/zsh"
allowed_chat_ids = []
`)

	_, err := Load(path)
	if err == nil {
		t.Fatal("expected empty allowlist error")
	}
	if !strings.Contains(err.Error(), "allowed_chat_ids") {
		t.Fatalf("error = %v, want allowed_chat_ids mention", err)
	}
}

func TestLoadRejectsInvalidHeartbeatOrder(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.toml")
	writeConfig(t, path, `node_name = "server"

[ipc]
enabled = false

[lark]
app_id = "cli_server_xxx"
app_secret = "redacted"

[connection]
heartbeat_interval = "30s"
heartbeat_timeout = "10s"

[exec]
enabled = true
default_shell = "/bin/zsh"
`)

	_, err := Load(path)
	if err == nil {
		t.Fatal("expected heartbeat validation error")
	}
	if !strings.Contains(err.Error(), "heartbeat_interval") {
		t.Fatalf("error = %v, want heartbeat_interval mention", err)
	}
}

func TestLoadRejectsInsecureIPCSocketDirectory(t *testing.T) {
	root := t.TempDir()
	socketDir := filepath.Join(root, "socket")
	if err := os.Mkdir(socketDir, 0o755); err != nil {
		t.Fatal(err)
	}
	defer os.Chmod(socketDir, 0o700)

	configDir := filepath.Join(root, "config")
	if err := os.Mkdir(configDir, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(configDir, "config.toml")
	writeConfig(t, path, `node_name = "local"

[ipc]
enabled = true
socket_path = "`+filepath.ToSlash(filepath.Join(socketDir, "elarkd.sock"))+`"

[lark]
app_id = "cli_client_xxx"
app_secret = "redacted"

[exec]
enabled = false
`)

	_, err := Load(path)
	if err == nil {
		t.Fatal("expected insecure socket directory error")
	}
	if !strings.Contains(err.Error(), "ipc socket directory") {
		t.Fatalf("error = %v, want ipc socket directory mention", err)
	}
}

func TestParseDurationSupportsDays(t *testing.T) {
	got, err := ParseDuration("1d2h30m500ms")
	if err != nil {
		t.Fatalf("ParseDuration returned error: %v", err)
	}
	want := 24*time.Hour + 2*time.Hour + 30*time.Minute + 500*time.Millisecond
	if got != want {
		t.Fatalf("duration = %s, want %s", got, want)
	}
}

func TestValidateRejectsInvalidLarkIDs(t *testing.T) {
	err := Validate(&Config{
		NodeName: "local",
		Lark: LarkConfig{
			AppID:                     "cli_client_xxx",
			AppSecret:                 "redacted",
			SendCooldown:              Duration(time.Second),
			LarkTextRequestLimitBytes: DefaultLarkTextRequestLimitBytes,
		},
		Connection: ConnectionConfig{
			HeartbeatInterval:  Duration(10 * time.Second),
			HeartbeatTimeout:   Duration(30 * time.Second),
			SequenceGapTimeout: Duration(30 * time.Second),
		},
		Hosts: map[string]HostConfig{
			"bad": {
				ChatID:        "chat",
				PeerBotOpenID: "ou_peer",
			},
		},
	})
	if err == nil {
		t.Fatal("expected invalid chat ID error")
	}
	if !strings.Contains(err.Error(), "oc_") {
		t.Fatalf("error = %v, want oc_ mention", err)
	}
}

func writeConfig(t *testing.T, path, body string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	chmod(t, filepath.Dir(path), 0o700)
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	chmod(t, path, 0o600)
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(content)
}

func assertMode(t *testing.T, path string, want os.FileMode) {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != want {
		t.Fatalf("%s mode = %#o, want %#o", path, got, want)
	}
}

func chmod(t *testing.T, path string, mode os.FileMode) {
	t.Helper()
	if err := os.Chmod(path, mode); err != nil {
		t.Fatal(err)
	}
}

func validServerConfig() string {
	return `node_name = "server"

[ipc]
enabled = false

[lark]
app_id = "cli_server_xxx"
app_secret = "redacted"
send_cooldown = "500ms"
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
`
}

func TestCheckSecureDirectoryWrapsNotExist(t *testing.T) {
	err := CheckSecureDirectory(filepath.Join(t.TempDir(), "missing"), "test directory")
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("error = %v, want os.ErrNotExist wrapping", err)
	}
}
