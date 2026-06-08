package doctor

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hachiwii/exec-over-lark/internal/config"
	"github.com/hachiwii/exec-over-lark/internal/outbound"
)

func TestRunAllChecksPassWithFakeDaemon(t *testing.T) {
	now := time.Date(2026, 6, 6, 12, 0, 0, 0, time.UTC)
	secret := "super-secret-value"
	path := writeDoctorConfig(t, secret)
	daemon := &fakeDaemon{
		status: DaemonStatus{
			Running:    true,
			SocketPath: filepath.Join(t.TempDir(), "elarkd.sock"),
			Event: EventConnectionStatus{
				Checked:     true,
				Connected:   true,
				LastEventAt: now.Add(-time.Second),
			},
			Outbound: OutboundQueueStatus{
				Checked:       true,
				PendingFrames: 0,
				LastSentAt:    now.Add(-time.Minute),
				HasLastSent:   true,
			},
		},
	}

	report := Run(context.Background(), Options{
		ConfigPath: path,
		Daemon:     daemon,
		Now:        func() time.Time { return now },
	})

	if report.Failed() {
		t.Fatalf("report failed:\n%s", report.Text())
	}
	for _, id := range []CheckID{
		CheckConfigPermissions,
		CheckConfigLoad,
		CheckDaemonStatus,
		CheckDaemonSocket,
		CheckEventConnection,
		CheckOutboundQueue,
	} {
		assertCheckStatus(t, report, id, StatusOK)
	}
	assertCheckMissing(t, report, "host_config")
	assertCheckMissing(t, report, "token_refresh")
	assertCheckMissing(t, report, "bot_open_id")
	assertCheckMissing(t, report, "chat")
	assertCheckMissing(t, report, "peer_bot")
	assertCheckMissing(t, report, "bootstrap")
	assertCheckMissing(t, report, "ping_root_message")
	if got := len(daemon.requests); got != 1 {
		t.Fatalf("daemon requests = %d, want 1", got)
	}

	text := report.Text()
	for _, forbidden := range []string{secret} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("report leaked secret %q:\n%s", forbidden, text)
		}
	}
}

func TestRunReportsFailuresWarningsAndRedactsSecrets(t *testing.T) {
	now := time.Date(2026, 6, 6, 12, 0, 0, 0, time.UTC)
	secret := "super-secret-value"
	path := writeDoctorConfig(t, secret)
	daemon := &fakeDaemon{
		status: DaemonStatus{
			Running: false,
			Event: EventConnectionStatus{
				Checked:   true,
				Connected: false,
				Error:     "websocket closed",
			},
			Outbound: OutboundQueueStatus{
				Checked:       true,
				PendingFrames: 3,
				PendingTargets: []outbound.Target{{
					ChatID:        "oc_macmini",
					RootMessageID: "om_root",
					MentionOpenID: "ou_server_bot",
				}},
				NextFlushAt:  now.Add(time.Second),
				HasNextFlush: true,
			},
		},
	}
	report := Run(context.Background(), Options{
		ConfigPath: path,
		Daemon:     daemon,
		Now:        func() time.Time { return now },
	})

	if !report.Failed() {
		t.Fatalf("report did not fail:\n%s", report.Text())
	}
	assertCheckStatus(t, report, CheckDaemonStatus, StatusFailed)
	assertCheckStatus(t, report, CheckDaemonSocket, StatusFailed)
	assertCheckStatus(t, report, CheckEventConnection, StatusFailed)
	assertCheckStatus(t, report, CheckOutboundQueue, StatusWarning)
	assertCheckMissing(t, report, "token_refresh")
	assertCheckMissing(t, report, "bot_open_id")
	assertCheckMissing(t, report, "chat")
	assertCheckMissing(t, report, "peer_bot")
	assertCheckMissing(t, report, "bootstrap")
	assertCheckMissing(t, report, "ping_root_message")

	text := report.Text()
	for _, forbidden := range []string{secret, "raw-access-token", "raw-token"} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("report leaked secret %q:\n%s", forbidden, text)
		}
	}
}

func TestRunRejectsInsecureConfigPermissions(t *testing.T) {
	path := writeDoctorConfig(t, "super-secret-value")
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}

	report := Run(context.Background(), Options{ConfigPath: path})
	if !report.Failed() {
		t.Fatalf("report did not fail:\n%s", report.Text())
	}
	assertCheckStatus(t, report, CheckConfigPath, StatusOK)
	assertCheckStatus(t, report, CheckConfigPermissions, StatusFailed)
	if _, ok := report.Check("token_refresh"); ok {
		t.Fatalf("token refresh ran after insecure config:\n%s", report.Text())
	}
}

func TestRunAcceptsInMemoryFakeConfig(t *testing.T) {
	socketDir := t.TempDir()
	if err := os.Chmod(socketDir, 0o700); err != nil {
		t.Fatal(err)
	}
	cfg := &config.Config{
		NodeName: "server",
		Lark: config.LarkConfig{
			AppID:     "cli_server_xxx",
			AppSecret: "fake-secret-value",
		},
		IPC: config.IPCConfig{
			Enabled:    true,
			SocketPath: filepath.Join(socketDir, "elarkd.sock"),
		},
		Exec: config.ExecConfig{
			Enabled:      true,
			DefaultShell: "/bin/zsh",
		},
	}

	report := Run(context.Background(), Options{
		Config: cfg,
		Daemon: &fakeDaemon{status: DaemonStatus{
			Running: true,
			Event:   EventConnectionStatus{Checked: true, Connected: true},
			Outbound: OutboundQueueStatus{
				Checked: true,
			},
		}},
	})

	if report.Failed() {
		t.Fatalf("report failed:\n%s", report.Text())
	}
	assertCheckStatus(t, report, CheckConfigPath, StatusSkipped)
	assertCheckStatus(t, report, CheckConfigPermissions, StatusSkipped)
	assertCheckStatus(t, report, CheckConfigLoad, StatusOK)
	assertCheckMissing(t, report, "host_config")
}

func writeDoctorConfig(t *testing.T, secret string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "config.toml")
	socketPath := filepath.Join(dir, "run", "elarkd.sock")
	body := fmt.Sprintf(`node_name = "local"
default_host = "macmini"

[ipc]
enabled = true
socket_path = %q

[lark]
app_id = "cli_client_xxx"
app_secret = %q

[exec]
enabled = false

[hosts.macmini]
chat_id = "oc_macmini"
peer_bot_open_id = "ou_server_bot"
shell = "/bin/zsh"
`, socketPath, secret)
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func assertCheckStatus(t *testing.T, report Report, id CheckID, want Status) {
	t.Helper()
	check, ok := report.Check(id)
	if !ok {
		t.Fatalf("missing check %s in report:\n%s", id, report.Text())
	}
	if check.Status != want {
		t.Fatalf("check %s status = %s, want %s:\n%s", id, check.Status, want, report.Text())
	}
}

func assertCheckMissing(t *testing.T, report Report, id CheckID) {
	t.Helper()
	if _, ok := report.Check(id); ok {
		t.Fatalf("unexpected check %s in report:\n%s", id, report.Text())
	}
}

type fakeDaemon struct {
	status   DaemonStatus
	err      error
	requests []DaemonStatusRequest
}

func (f *fakeDaemon) Status(_ context.Context, req DaemonStatusRequest) (DaemonStatus, error) {
	f.requests = append(f.requests, req)
	if f.err != nil {
		return DaemonStatus{}, f.err
	}
	return f.status, nil
}
