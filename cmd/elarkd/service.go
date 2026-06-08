package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"

	"github.com/hachiwii/exec-over-lark/internal/config"
)

const (
	serviceLabel = "com.hachiwii.exec-over-lark.elarkd"
	systemdUnit  = "elarkd.service"
)

func runServiceCommand(action string, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("elarkd "+action, flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), "Usage:\n  elarkd %s [--config PATH] [--system]\n", action)
	}
	configPath := fs.String("config", "", "path to config file")
	system := fs.Bool("system", false, "install or control the system daemon")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if fs.NArg() != 0 {
		fmt.Fprintf(stderr, "unexpected %s arguments: %v\n", action, fs.Args())
		return 2
	}

	spec, err := buildServiceSpec(*configPath, *system)
	if err != nil {
		fmt.Fprintf(stderr, "elarkd %s: %v\n", action, err)
		return 1
	}
	if spec.System && os.Geteuid() != 0 {
		fmt.Fprintf(stderr, "elarkd %s: --system requires root; rerun with sudo\n", action)
		return 1
	}

	if err := runServiceAction(action, spec); err != nil {
		fmt.Fprintf(stderr, "elarkd %s: %v\n", action, err)
		return 1
	}
	fmt.Fprintf(stdout, "elarkd %s complete\n", action)
	if action == "install" {
		fmt.Fprintf(stdout, "service file: %s\n", spec.FilePath)
	}
	return 0
}

type serviceSpec struct {
	OS         string
	System     bool
	FilePath   string
	Domain     string
	BinaryPath string
	ConfigPath string
	LogPath    string
	ErrorPath  string
}

func buildServiceSpec(configPath string, system bool) (serviceSpec, error) {
	binaryPath, err := os.Executable()
	if err != nil {
		return serviceSpec{}, fmt.Errorf("resolve elarkd executable: %w", err)
	}
	binaryPath, err = filepath.Abs(binaryPath)
	if err != nil {
		return serviceSpec{}, fmt.Errorf("resolve elarkd executable: %w", err)
	}
	resolvedConfig, err := config.ResolvePath(configPath)
	if err != nil {
		return serviceSpec{}, err
	}

	switch runtime.GOOS {
	case "darwin":
		return buildDarwinServiceSpec(binaryPath, resolvedConfig, system)
	case "linux":
		return buildLinuxServiceSpec(binaryPath, resolvedConfig, system)
	default:
		return serviceSpec{}, fmt.Errorf("unsupported operating system: %s", runtime.GOOS)
	}
}

func buildDarwinServiceSpec(binaryPath, configPath string, system bool) (serviceSpec, error) {
	if system {
		return serviceSpec{
			OS:         "darwin",
			System:     true,
			FilePath:   filepath.Join("/Library/LaunchDaemons", serviceLabel+".plist"),
			Domain:     "system",
			BinaryPath: binaryPath,
			ConfigPath: configPath,
			LogPath:    filepath.Join("/var/log", "elarkd.log"),
			ErrorPath:  filepath.Join("/var/log", "elarkd.err.log"),
		}, nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return serviceSpec{}, err
	}
	return serviceSpec{
		OS:         "darwin",
		FilePath:   filepath.Join(home, "Library", "LaunchAgents", serviceLabel+".plist"),
		Domain:     "gui/" + strconv.Itoa(os.Getuid()),
		BinaryPath: binaryPath,
		ConfigPath: configPath,
		LogPath:    filepath.Join(home, ".local", "state", "exec-over-lark", "elarkd.log"),
		ErrorPath:  filepath.Join(home, ".local", "state", "exec-over-lark", "elarkd.err.log"),
	}, nil
}

func buildLinuxServiceSpec(binaryPath, configPath string, system bool) (serviceSpec, error) {
	if system {
		return serviceSpec{
			OS:         "linux",
			System:     true,
			FilePath:   filepath.Join("/etc/systemd/system", systemdUnit),
			BinaryPath: binaryPath,
			ConfigPath: configPath,
		}, nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return serviceSpec{}, err
	}
	return serviceSpec{
		OS:         "linux",
		FilePath:   filepath.Join(home, ".config", "systemd", "user", systemdUnit),
		BinaryPath: binaryPath,
		ConfigPath: configPath,
	}, nil
}

func runServiceAction(action string, spec serviceSpec) error {
	switch action {
	case "install":
		return installService(spec)
	case "uninstall":
		return uninstallService(spec)
	case "start":
		return startService(spec)
	case "restart":
		if err := stopService(spec); err != nil {
			return err
		}
		return startService(spec)
	case "stop":
		return stopService(spec)
	default:
		return fmt.Errorf("unknown service action %q", action)
	}
}

func installService(spec serviceSpec) error {
	if err := os.MkdirAll(filepath.Dir(spec.FilePath), 0o755); err != nil {
		return err
	}
	if spec.LogPath != "" {
		if err := os.MkdirAll(filepath.Dir(spec.LogPath), 0o755); err != nil {
			return err
		}
	}
	var body string
	switch spec.OS {
	case "darwin":
		body = launchdPlist(spec)
	case "linux":
		body = systemdService(spec)
	default:
		return fmt.Errorf("unsupported operating system: %s", spec.OS)
	}
	if err := os.WriteFile(spec.FilePath, []byte(body), 0o644); err != nil {
		return err
	}
	if spec.OS == "linux" {
		return systemctl(spec, "daemon-reload")
	}
	return nil
}

func uninstallService(spec serviceSpec) error {
	_ = stopService(spec)
	if err := os.Remove(spec.FilePath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if spec.OS == "linux" {
		return systemctl(spec, "daemon-reload")
	}
	return nil
}

func startService(spec serviceSpec) error {
	switch spec.OS {
	case "darwin":
		if _, err := os.Stat(spec.FilePath); err != nil {
			return err
		}
		if err := runCmd("launchctl", "bootstrap", spec.Domain, spec.FilePath); err != nil {
			if !strings.Contains(err.Error(), "Bootstrap failed") && !strings.Contains(err.Error(), "already") {
				return err
			}
		}
		return runCmd("launchctl", "kickstart", "-k", spec.Domain+"/"+serviceLabel)
	case "linux":
		return systemctl(spec, "start", systemdUnit)
	default:
		return fmt.Errorf("unsupported operating system: %s", spec.OS)
	}
}

func stopService(spec serviceSpec) error {
	switch spec.OS {
	case "darwin":
		if _, err := os.Stat(spec.FilePath); errors.Is(err, os.ErrNotExist) {
			return nil
		}
		err := runCmd("launchctl", "bootout", spec.Domain, spec.FilePath)
		if err != nil && !strings.Contains(err.Error(), "Boot-out failed") && !strings.Contains(err.Error(), "No such process") {
			return err
		}
		return nil
	case "linux":
		err := systemctl(spec, "stop", systemdUnit)
		if err != nil && !strings.Contains(err.Error(), "not loaded") {
			return err
		}
		return nil
	default:
		return fmt.Errorf("unsupported operating system: %s", spec.OS)
	}
}

func launchdPlist(spec serviceSpec) string {
	return `<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>` + xmlEscape(serviceLabel) + `</string>
  <key>ProgramArguments</key>
  <array>
    <string>` + xmlEscape(spec.BinaryPath) + `</string>
    <string>run</string>
    <string>-config</string>
    <string>` + xmlEscape(spec.ConfigPath) + `</string>
  </array>
  <key>RunAtLoad</key>
  <true/>
  <key>KeepAlive</key>
  <true/>
  <key>StandardOutPath</key>
  <string>` + xmlEscape(spec.LogPath) + `</string>
  <key>StandardErrorPath</key>
  <string>` + xmlEscape(spec.ErrorPath) + `</string>
</dict>
</plist>
`
}

func systemdService(spec serviceSpec) string {
	wantedBy := "default.target"
	if spec.System {
		wantedBy = "multi-user.target"
	}
	return `[Unit]
Description=exec-over-lark daemon
After=network-online.target

[Service]
Type=simple
ExecStart=` + systemdQuote(spec.BinaryPath) + ` run -config ` + systemdQuote(spec.ConfigPath) + `
Restart=on-failure

[Install]
WantedBy=` + wantedBy + `
`
}

func systemctl(spec serviceSpec, args ...string) error {
	full := []string{}
	if !spec.System {
		full = append(full, "--user")
	}
	full = append(full, args...)
	return runCmd("systemctl", full...)
}

func runCmd(name string, args ...string) error {
	cmd := exec.Command(name, args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		msg := strings.TrimSpace(string(out))
		if msg == "" {
			msg = err.Error()
		}
		return fmt.Errorf("%s %s: %s", name, strings.Join(args, " "), msg)
	}
	return nil
}

func xmlEscape(value string) string {
	replacer := strings.NewReplacer(
		"&", "&amp;",
		"<", "&lt;",
		">", "&gt;",
		`"`, "&quot;",
		"'", "&apos;",
	)
	return replacer.Replace(value)
}

func systemdQuote(value string) string {
	return strconv.Quote(value)
}
