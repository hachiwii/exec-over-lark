package main

import (
	"strings"
	"testing"
)

func TestLaunchdPlistRunsElarkdRun(t *testing.T) {
	body := launchdPlist(serviceSpec{
		BinaryPath: "/usr/local/bin/elarkd",
		ConfigPath: "/Users/me/.elark/config.toml",
		LogPath:    "/Users/me/.local/state/exec-over-lark/elarkd.log",
		ErrorPath:  "/Users/me/.local/state/exec-over-lark/elarkd.err.log",
	})
	for _, want := range []string{
		"<string>/usr/local/bin/elarkd</string>",
		"<string>run</string>",
		"<string>-config</string>",
		"<string>/Users/me/.elark/config.toml</string>",
		"<key>KeepAlive</key>",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("launchd plist missing %q:\n%s", want, body)
		}
	}
}

func TestSystemdServiceRunsElarkdRun(t *testing.T) {
	body := systemdService(serviceSpec{
		BinaryPath: "/usr/local/bin/elarkd",
		ConfigPath: "/home/me/.elark/config.toml",
	})
	for _, want := range []string{
		`ExecStart="/usr/local/bin/elarkd" run -config "/home/me/.elark/config.toml"`,
		"Restart=on-failure",
		"WantedBy=default.target",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("systemd service missing %q:\n%s", want, body)
		}
	}
}

func TestSystemdSystemServiceUsesMultiUserTarget(t *testing.T) {
	body := systemdService(serviceSpec{
		System:     true,
		BinaryPath: "/usr/local/bin/elarkd",
		ConfigPath: "/etc/elark/config.toml",
	})
	if !strings.Contains(body, "WantedBy=multi-user.target") {
		t.Fatalf("system service = %q, want multi-user target", body)
	}
}
