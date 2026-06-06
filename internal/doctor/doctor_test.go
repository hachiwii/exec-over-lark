package doctor

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hachiwii/exec-over-lark/internal/config"
	"github.com/hachiwii/exec-over-lark/internal/lark"
	"github.com/hachiwii/exec-over-lark/internal/outbound"
)

func TestRunAllChecksPassWithFakeDaemonAndLark(t *testing.T) {
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
	larkClient := &fakeLark{
		token:         "tenant-token-secret",
		botOpenID:     "ou_self_bot",
		chatAvailable: true,
		rootMessageID: "om_ping",
		peerInChat:    true,
		bootstrap: BootstrapStatus{
			Found:         true,
			Matches:       true,
			ChatID:        "oc_macmini",
			BotOpenID:     "ou_server_bot",
			LastMessageID: "om_bootstrap",
		},
	}

	report := Run(context.Background(), Options{
		ConfigPath: path,
		Host:       "macmini",
		Daemon:     daemon,
		Lark:       larkClient,
		Now:        func() time.Time { return now },
	})

	if report.Failed() {
		t.Fatalf("report failed:\n%s", report.Text())
	}
	for _, id := range []CheckID{
		CheckConfigPermissions,
		CheckConfigLoad,
		CheckHostConfig,
		CheckDaemonStatus,
		CheckDaemonSocket,
		CheckTokenRefresh,
		CheckBotOpenID,
		CheckEventConnection,
		CheckChat,
		CheckPing,
		CheckPeerBot,
		CheckBootstrap,
		CheckOutboundQueue,
	} {
		assertCheckStatus(t, report, id, StatusOK)
	}
	if got := len(daemon.requests); got != 1 {
		t.Fatalf("daemon requests = %d, want 1", got)
	}
	if daemon.requests[0].Host != "macmini" {
		t.Fatalf("daemon host = %q, want macmini", daemon.requests[0].Host)
	}
	if got := len(larkClient.sentRoots); got != 1 {
		t.Fatalf("root pings = %d, want 1", got)
	}
	if larkClient.sentRoots[0].chatID != "oc_macmini" || larkClient.sentRoots[0].mentionOpenID != "ou_server_bot" {
		t.Fatalf("root ping target = %#v", larkClient.sentRoots[0])
	}

	text := report.Text()
	for _, forbidden := range []string{secret, "tenant-token-secret"} {
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
	larkClient := &fakeLark{
		tokenErr:      fmt.Errorf("auth failed app_secret=%s bearer raw-access-token", secret),
		botOpenID:     "ou_self_bot",
		chatAvailable: false,
		sendErr:       errors.New("send denied tenant_access_token=raw-token"),
		peerInChat:    false,
		bootstrap: BootstrapStatus{
			Found:         true,
			Matches:       false,
			ChatID:        "oc_wrong",
			BotOpenID:     "ou_wrong",
			LastMessageID: "om_bootstrap",
		},
	}

	report := Run(context.Background(), Options{
		ConfigPath: path,
		Host:       "macmini",
		Daemon:     daemon,
		Lark:       larkClient,
		Now:        func() time.Time { return now },
	})

	if !report.Failed() {
		t.Fatalf("report did not fail:\n%s", report.Text())
	}
	assertCheckStatus(t, report, CheckDaemonStatus, StatusFailed)
	assertCheckStatus(t, report, CheckDaemonSocket, StatusFailed)
	assertCheckStatus(t, report, CheckTokenRefresh, StatusFailed)
	assertCheckStatus(t, report, CheckEventConnection, StatusFailed)
	assertCheckStatus(t, report, CheckChat, StatusFailed)
	assertCheckStatus(t, report, CheckPing, StatusFailed)
	assertCheckStatus(t, report, CheckPeerBot, StatusFailed)
	assertCheckStatus(t, report, CheckBootstrap, StatusFailed)
	assertCheckStatus(t, report, CheckOutboundQueue, StatusWarning)

	text := report.Text()
	for _, forbidden := range []string{secret, "raw-access-token", "raw-token"} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("report leaked secret %q:\n%s", forbidden, text)
		}
	}
	if !strings.Contains(text, "[redacted]") {
		t.Fatalf("report did not show redaction marker:\n%s", text)
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
	if _, ok := report.Check(CheckTokenRefresh); ok {
		t.Fatalf("token refresh ran after insecure config:\n%s", report.Text())
	}
}

func TestRunAcceptsInMemoryFakeConfig(t *testing.T) {
	cfg := &config.Config{
		NodeName: "server",
		Lark: config.LarkConfig{
			AppID:     "cli_server_xxx",
			AppSecret: "fake-secret-value",
		},
		Exec: config.ExecConfig{
			Enabled:      true,
			DefaultShell: "/bin/zsh",
		},
	}

	report := Run(context.Background(), Options{
		Config:   cfg,
		SkipPing: true,
	})

	if report.Failed() {
		t.Fatalf("report failed:\n%s", report.Text())
	}
	assertCheckStatus(t, report, CheckConfigPath, StatusSkipped)
	assertCheckStatus(t, report, CheckConfigPermissions, StatusSkipped)
	assertCheckStatus(t, report, CheckConfigLoad, StatusOK)
	assertCheckStatus(t, report, CheckHostConfig, StatusSkipped)
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

type fakeRootSend struct {
	chatID        string
	mentionOpenID string
	text          string
}

type fakeLark struct {
	token     string
	tokenErr  error
	botOpenID string
	botErr    error

	chatAvailable bool
	chatErr       error

	rootMessageID string
	sendErr       error
	sentRoots     []fakeRootSend

	peerInChat bool
	peerErr    error

	bootstrap    BootstrapStatus
	bootstrapErr error
}

func (f *fakeLark) TenantAccessToken(context.Context) (string, error) {
	if f.tokenErr != nil {
		return "", f.tokenErr
	}
	return f.token, nil
}

func (f *fakeLark) BotOpenID(context.Context) (string, error) {
	if f.botErr != nil {
		return "", f.botErr
	}
	return f.botOpenID, nil
}

func (f *fakeLark) ChatAvailable(_ context.Context, chatID string) (bool, error) {
	if f.chatErr != nil {
		return false, f.chatErr
	}
	return f.chatAvailable, nil
}

func (f *fakeLark) SendRootMessage(_ context.Context, chatID, mentionOpenID, text string) (lark.RootMessage, error) {
	f.sentRoots = append(f.sentRoots, fakeRootSend{chatID: chatID, mentionOpenID: mentionOpenID, text: text})
	if f.sendErr != nil {
		return lark.RootMessage{}, f.sendErr
	}
	return lark.RootMessage{MessageID: f.rootMessageID}, nil
}

func (f *fakeLark) PeerBotInChat(_ context.Context, chatID, peerBotOpenID string) (bool, error) {
	if f.peerErr != nil {
		return false, f.peerErr
	}
	return f.peerInChat, nil
}

func (f *fakeLark) BootstrapStatus(_ context.Context, chatID, peerBotOpenID string) (BootstrapStatus, error) {
	if f.bootstrapErr != nil {
		return BootstrapStatus{}, f.bootstrapErr
	}
	return f.bootstrap, nil
}
