package main

import (
	"os"
	"path/filepath"
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

func TestLaunchdSystemPlistRunsAsTargetUser(t *testing.T) {
	body := launchdPlist(serviceSpec{
		System:     true,
		BinaryPath: "/Users/me/.local/bin/elarkd",
		ConfigPath: "/Users/me/.elark/config.toml",
		LogPath:    "/Users/me/.local/state/exec-over-lark/elarkd.log",
		ErrorPath:  "/Users/me/.local/state/exec-over-lark/elarkd.err.log",
		UserName:   "me",
	})
	for _, want := range []string{
		"<key>UserName</key>",
		"<string>me</string>",
		"<string>/Users/me/.local/bin/elarkd</string>",
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

func TestSystemdSystemServiceUsesMultiUserTargetAndTargetUser(t *testing.T) {
	body := systemdService(serviceSpec{
		System:     true,
		BinaryPath: "/usr/local/bin/elarkd",
		ConfigPath: "/etc/elark/config.toml",
		UserName:   "me",
		UserHome:   "/home/me",
	})
	for _, want := range []string{
		"WantedBy=multi-user.target",
		"User=me",
		`Environment="HOME=/home/me"`,
		`WorkingDirectory="/home/me"`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("systemd service missing %q:\n%s", want, body)
		}
	}
}

func TestResolveConfigPathForUserUsesTargetHome(t *testing.T) {
	home := filepath.Join(t.TempDir(), "home")
	got, err := resolveConfigPathForUser("", home)
	if err != nil {
		t.Fatalf("ResolveConfigPath default returned error: %v", err)
	}
	want := filepath.Join(home, ".elark", "config.toml")
	if got != want {
		t.Fatalf("ResolveConfigPath default = %q, want %q", got, want)
	}

	got, err = resolveConfigPathForUser("~/.custom/elark.toml", home)
	if err != nil {
		t.Fatalf("ResolveConfigPath custom returned error: %v", err)
	}
	want = filepath.Join(home, ".custom", "elark.toml")
	if got != want {
		t.Fatalf("ResolveConfigPath custom = %q, want %q", got, want)
	}
}

func TestInstallRecordRoundTrip(t *testing.T) {
	home := t.TempDir()
	spec := serviceSpec{
		OS:         "darwin",
		System:     true,
		FilePath:   "/Library/LaunchDaemons/" + serviceLabel + ".plist",
		BinaryPath: "/Users/me/.local/bin/elarkd",
		ConfigPath: "/Users/me/.elark/config.toml",
		UserName:   "me",
		UserUID:    os.Getuid(),
		UserGID:    os.Getgid(),
		UserHome:   home,
	}
	if err := writeInstallRecord(spec); err != nil {
		t.Fatalf("writeInstallRecord returned error: %v", err)
	}
	record, err := loadInstallRecord(home)
	if err != nil {
		t.Fatalf("loadInstallRecord returned error: %v", err)
	}
	if record == nil {
		t.Fatal("loadInstallRecord returned nil record")
	}
	if !record.System || record.ServiceMode != "system" || record.BinaryPath != spec.BinaryPath || record.ConfigPath != spec.ConfigPath {
		t.Fatalf("install record mismatch: %#v", record)
	}
}

func TestPrintServiceStatus(t *testing.T) {
	var out strings.Builder
	printServiceStatus(&out, serviceStatus{
		Spec: serviceSpec{
			System:     true,
			FilePath:   "/Library/LaunchDaemons/" + serviceLabel + ".plist",
			BinaryPath: "/Users/me/.local/bin/elarkd",
			ConfigPath: "/Users/me/.elark/config.toml",
			UserName:   "me",
		},
		Installed: true,
		Loaded:    true,
		Running:   true,
		State:     "running",
	})
	for _, want := range []string{
		"status: running",
		"installed: yes",
		"running: yes",
		"mode: system",
		"user: me",
	} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("status output missing %q:\n%s", want, out.String())
		}
	}
}
