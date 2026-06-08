package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	osuser "os/user"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/hachiwii/exec-over-lark/internal/config"
	"github.com/hachiwii/exec-over-lark/internal/ipc"
	"github.com/hachiwii/exec-over-lark/internal/version"
)

const (
	serviceLabel      = "com.hachiwii.exec-over-lark.elarkd"
	systemdUnit       = "elarkd.service"
	installRecordFile = "install.json"
	runtimeStatusFile = "runtime-status.json"
)

func runServiceCommand(action string, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("elarkd "+action, flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), "Usage:\n  elarkd %s [--config PATH] [--system]\n", action)
	}
	configPath := fs.String("config", "", "path to config file")
	system := fs.Bool("system", false, "install or control the system daemon")
	explicitSystem := hasBoolFlag(args, "system")
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

	target, err := resolveServiceUser()
	if err != nil {
		fmt.Fprintf(stderr, "elarkd %s: %v\n", action, err)
		return 1
	}
	record, err := loadInstallRecord(target.HomeDir)
	if err != nil {
		fmt.Fprintf(stderr, "elarkd %s: %v\n", action, err)
		return 1
	}
	effectiveSystem := *system
	if !explicitSystem && record != nil {
		effectiveSystem = record.System
	}
	effectiveConfig := *configPath
	if strings.TrimSpace(effectiveConfig) == "" && record != nil {
		effectiveConfig = record.ConfigPath
	}

	spec, err := buildServiceSpec(effectiveConfig, effectiveSystem, target)
	if err != nil {
		fmt.Fprintf(stderr, "elarkd %s: %v\n", action, err)
		return 1
	}
	if record != nil && action != "install" && !explicitSystem {
		applyInstallRecord(&spec, *record)
	}
	if spec.System && os.Geteuid() != 0 && action != "status" {
		fmt.Fprintf(stderr, "elarkd %s: system service requires root; rerun with sudo\n", action)
		return 1
	}

	if action == "status" {
		status, err := getServiceStatus(spec)
		if err != nil {
			fmt.Fprintf(stderr, "elarkd status: %v\n", err)
			return 1
		}
		enrichServiceRuntimeStatus(&status)
		printServiceStatus(stdout, status)
		return 0
	}

	if err := runServiceAction(action, spec); err != nil {
		fmt.Fprintf(stderr, "elarkd %s: %v\n", action, err)
		return 1
	}
	fmt.Fprintf(stdout, "elarkd %s complete\n", action)
	if action == "install" {
		fmt.Fprintf(stdout, "service file: %s\n", spec.FilePath)
		fmt.Fprintf(stdout, "install record: %s\n", installRecordPath(spec.UserHome))
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
	UserName   string
	UserUID    int
	UserGID    int
	UserHome   string
}

type serviceUser struct {
	Username string
	UID      int
	GID      int
	HomeDir  string
}

type installRecord struct {
	Version      int    `json:"version"`
	OS           string `json:"os"`
	System       bool   `json:"system"`
	ServiceMode  string `json:"service_mode"`
	ServiceLabel string `json:"service_label"`
	ServiceFile  string `json:"service_file"`
	BinaryPath   string `json:"binary_path"`
	ConfigPath   string `json:"config_path"`
	UserName     string `json:"user_name"`
	UserUID      int    `json:"user_uid"`
	UserHome     string `json:"user_home"`
	InstalledAt  string `json:"installed_at"`
}

func buildServiceSpec(configPath string, system bool, target serviceUser) (serviceSpec, error) {
	binaryPath, err := os.Executable()
	if err != nil {
		return serviceSpec{}, fmt.Errorf("resolve elarkd executable: %w", err)
	}
	binaryPath, err = filepath.Abs(binaryPath)
	if err != nil {
		return serviceSpec{}, fmt.Errorf("resolve elarkd executable: %w", err)
	}
	resolvedConfig, err := resolveConfigPathForUser(configPath, target.HomeDir)
	if err != nil {
		return serviceSpec{}, err
	}

	switch runtime.GOOS {
	case "darwin":
		return buildDarwinServiceSpec(binaryPath, resolvedConfig, system, target)
	case "linux":
		return buildLinuxServiceSpec(binaryPath, resolvedConfig, system, target)
	default:
		return serviceSpec{}, fmt.Errorf("unsupported operating system: %s", runtime.GOOS)
	}
}

func buildDarwinServiceSpec(binaryPath, configPath string, system bool, target serviceUser) (serviceSpec, error) {
	uid := target.UID
	if uid < 0 {
		uid = os.Getuid()
	}
	if system {
		return serviceSpec{
			OS:         "darwin",
			System:     true,
			FilePath:   filepath.Join("/Library/LaunchDaemons", serviceLabel+".plist"),
			Domain:     "system",
			BinaryPath: binaryPath,
			ConfigPath: configPath,
			LogPath:    filepath.Join(target.HomeDir, ".local", "state", "exec-over-lark", "elarkd.log"),
			ErrorPath:  filepath.Join(target.HomeDir, ".local", "state", "exec-over-lark", "elarkd.err.log"),
			UserName:   target.Username,
			UserUID:    uid,
			UserGID:    target.GID,
			UserHome:   target.HomeDir,
		}, nil
	}
	return serviceSpec{
		OS:         "darwin",
		FilePath:   filepath.Join(target.HomeDir, "Library", "LaunchAgents", serviceLabel+".plist"),
		Domain:     "gui/" + strconv.Itoa(uid),
		BinaryPath: binaryPath,
		ConfigPath: configPath,
		LogPath:    filepath.Join(target.HomeDir, ".local", "state", "exec-over-lark", "elarkd.log"),
		ErrorPath:  filepath.Join(target.HomeDir, ".local", "state", "exec-over-lark", "elarkd.err.log"),
		UserName:   target.Username,
		UserUID:    uid,
		UserGID:    target.GID,
		UserHome:   target.HomeDir,
	}, nil
}

func buildLinuxServiceSpec(binaryPath, configPath string, system bool, target serviceUser) (serviceSpec, error) {
	if system {
		return serviceSpec{
			OS:         "linux",
			System:     true,
			FilePath:   filepath.Join("/etc/systemd/system", systemdUnit),
			BinaryPath: binaryPath,
			ConfigPath: configPath,
			UserName:   target.Username,
			UserUID:    target.UID,
			UserGID:    target.GID,
			UserHome:   target.HomeDir,
		}, nil
	}
	return serviceSpec{
		OS:         "linux",
		FilePath:   filepath.Join(target.HomeDir, ".config", "systemd", "user", systemdUnit),
		BinaryPath: binaryPath,
		ConfigPath: configPath,
		UserName:   target.Username,
		UserUID:    target.UID,
		UserGID:    target.GID,
		UserHome:   target.HomeDir,
	}, nil
}

func resolveServiceUser() (serviceUser, error) {
	if os.Geteuid() == 0 {
		if uid := strings.TrimSpace(os.Getenv("SUDO_UID")); uid != "" && uid != "0" {
			if u, err := osuser.LookupId(uid); err == nil {
				return serviceUserFromOSUser(u)
			}
		}
		if name := strings.TrimSpace(os.Getenv("SUDO_USER")); name != "" && name != "root" {
			u, err := osuser.Lookup(name)
			if err != nil {
				return serviceUser{}, fmt.Errorf("resolve sudo user %q: %w", name, err)
			}
			return serviceUserFromOSUser(u)
		}
	}
	u, err := osuser.Current()
	if err != nil {
		return serviceUser{}, fmt.Errorf("resolve current user: %w", err)
	}
	return serviceUserFromOSUser(u)
}

func serviceUserFromOSUser(u *osuser.User) (serviceUser, error) {
	uid, err := strconv.Atoi(u.Uid)
	if err != nil {
		uid = -1
	}
	gid, err := strconv.Atoi(u.Gid)
	if err != nil {
		gid = -1
	}
	home := strings.TrimSpace(u.HomeDir)
	if home == "" {
		if currentHome, err := os.UserHomeDir(); err == nil {
			home = currentHome
		}
	}
	if home == "" {
		return serviceUser{}, errors.New("resolve home directory: user home is empty")
	}
	name := strings.TrimSpace(u.Username)
	if name == "" {
		name = u.Uid
	}
	return serviceUser{Username: name, UID: uid, GID: gid, HomeDir: home}, nil
}

func resolveConfigPathForUser(configPath, home string) (string, error) {
	if strings.TrimSpace(configPath) == "" {
		if home == "" {
			return "", errors.New("resolve home directory: user home is empty")
		}
		return filepath.Join(home, config.DefaultConfigDir, config.DefaultConfigFile), nil
	}
	expanded := configPath
	if expanded == "~" || strings.HasPrefix(expanded, "~/") {
		if home == "" {
			return "", errors.New("resolve home directory: user home is empty")
		}
		if expanded == "~" {
			expanded = home
		} else {
			expanded = filepath.Join(home, expanded[2:])
		}
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

func hasBoolFlag(args []string, name string) bool {
	short := "-" + name
	long := "--" + name
	for _, arg := range args {
		if arg == "--" {
			return false
		}
		if arg == short || arg == long || strings.HasPrefix(arg, short+"=") || strings.HasPrefix(arg, long+"=") {
			return true
		}
	}
	return false
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
	if !spec.System {
		if err := chownUserPath(filepath.Dir(spec.FilePath), spec); err != nil {
			return err
		}
	}
	if spec.LogPath != "" {
		if err := os.MkdirAll(filepath.Dir(spec.LogPath), 0o755); err != nil {
			return err
		}
		if err := chownUserPath(filepath.Dir(spec.LogPath), spec); err != nil {
			return err
		}
		if err := ensureLogFile(spec.LogPath, spec); err != nil {
			return err
		}
	}
	if spec.ErrorPath != "" {
		if err := os.MkdirAll(filepath.Dir(spec.ErrorPath), 0o755); err != nil {
			return err
		}
		if err := chownUserPath(filepath.Dir(spec.ErrorPath), spec); err != nil {
			return err
		}
		if err := ensureLogFile(spec.ErrorPath, spec); err != nil {
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
	if !spec.System {
		if err := chownForServiceUser(spec.FilePath, spec); err != nil {
			return err
		}
	}
	if spec.OS == "linux" {
		if err := systemctl(spec, "daemon-reload"); err != nil {
			return err
		}
	}
	return writeInstallRecord(spec)
}

func uninstallService(spec serviceSpec) error {
	_ = stopService(spec)
	if err := os.Remove(spec.FilePath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if spec.OS == "linux" {
		if err := systemctl(spec, "daemon-reload"); err != nil {
			return err
		}
	}
	return removeInstallRecord(spec)
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

type serviceStatus struct {
	Spec      serviceSpec
	Installed bool
	Loaded    bool
	Running   bool
	State     string
	Detail    string
	Version   string
}

func getServiceStatus(spec serviceSpec) (serviceStatus, error) {
	status := serviceStatus{Spec: spec, State: "not installed"}
	if _, err := os.Stat(spec.FilePath); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return status, nil
		}
		return serviceStatus{}, err
	}
	status.Installed = true
	status.State = "stopped"

	switch spec.OS {
	case "darwin":
		output, err := runCmdOutput("launchctl", "print", spec.Domain+"/"+serviceLabel)
		if err != nil {
			status.Detail = strings.TrimSpace(err.Error())
			return status, nil
		}
		status.Loaded = true
		if strings.Contains(output, "state = running") || strings.Contains(output, "\npid = ") {
			status.Running = true
			status.State = "running"
		} else {
			status.State = "loaded"
		}
		return status, nil
	case "linux":
		args := systemctlArgs(spec, "is-active", systemdUnit)
		output, err := runCmdOutput("systemctl", args...)
		state := firstLine(strings.TrimSpace(output))
		if state == "" {
			state = "unknown"
		}
		status.State = state
		status.Loaded = state != "inactive" && state != "unknown"
		status.Running = state == "active"
		if err != nil && strings.TrimSpace(output) == "" {
			status.Detail = strings.TrimSpace(err.Error())
		}
		return status, nil
	default:
		return serviceStatus{}, fmt.Errorf("unsupported operating system: %s", spec.OS)
	}
}

func enrichServiceRuntimeStatus(status *serviceStatus) {
	if status == nil || !status.Running {
		return
	}
	version, err := queryRunningDaemonVersion(status.Spec)
	if err != nil || strings.TrimSpace(version) == "" {
		version, err = readRuntimeStatusVersion(status.Spec.UserHome)
	}
	if err != nil || strings.TrimSpace(version) == "" {
		status.Version = "unknown"
		return
	}
	status.Version = version
}

func queryRunningDaemonVersion(spec serviceSpec) (string, error) {
	cfg, err := config.Load(spec.ConfigPath)
	if err != nil {
		return "", err
	}
	if !cfg.IPC.Enabled {
		return "", errors.New("ipc is disabled")
	}
	socketPath, err := expandServiceUserPath(cfg.IPC.SocketPath, spec.UserHome)
	if err != nil {
		return "", err
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	client, err := ipc.Dial(ctx, socketPath)
	if err != nil {
		return "", err
	}
	defer client.Close()
	status, err := client.Status(ctx, ipc.StatusRequest{RequestID: "elarkd-status-version"})
	if err != nil {
		return "", err
	}
	return status.Version, nil
}

type runtimeStatusRecord struct {
	Version    string `json:"version"`
	PID        int    `json:"pid"`
	BinaryPath string `json:"binary_path,omitempty"`
	ConfigPath string `json:"config_path,omitempty"`
	StartedAt  string `json:"started_at"`
}

func writeRuntimeStatus(configPath string) func() {
	home, err := os.UserHomeDir()
	if err != nil || strings.TrimSpace(home) == "" {
		return func() {}
	}
	path := runtimeStatusPath(home)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return func() {}
	}
	binaryPath, _ := os.Executable()
	if binaryPath != "" {
		if abs, err := filepath.Abs(binaryPath); err == nil {
			binaryPath = abs
		}
	}
	resolvedConfig, _ := config.ResolvePath(configPath)
	record := runtimeStatusRecord{
		Version:    version.String(),
		PID:        os.Getpid(),
		BinaryPath: binaryPath,
		ConfigPath: resolvedConfig,
		StartedAt:  time.Now().UTC().Format(time.RFC3339),
	}
	raw, err := json.MarshalIndent(record, "", "  ")
	if err != nil {
		return func() {}
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, append(raw, '\n'), 0o600); err != nil {
		return func() {}
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return func() {}
	}
	return func() {
		_ = os.Remove(path)
	}
}

func readRuntimeStatusVersion(home string) (string, error) {
	path := runtimeStatusPath(home)
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	var record runtimeStatusRecord
	if err := json.Unmarshal(raw, &record); err != nil {
		return "", err
	}
	return record.Version, nil
}

func runtimeStatusPath(home string) string {
	return filepath.Join(home, ".local", "state", "exec-over-lark", runtimeStatusFile)
}

func expandServiceUserPath(path, home string) (string, error) {
	if path == "~" || strings.HasPrefix(path, "~/") {
		home = strings.TrimSpace(home)
		if home == "" {
			return "", errors.New("service user home is empty")
		}
		if path == "~" {
			return home, nil
		}
		return filepath.Join(home, path[2:]), nil
	}
	return config.ExpandUserPath(path)
}

func printServiceStatus(w io.Writer, status serviceStatus) {
	spec := status.Spec
	fmt.Fprintf(w, "status: %s\n", status.State)
	fmt.Fprintf(w, "installed: %s\n", yesNo(status.Installed))
	fmt.Fprintf(w, "loaded: %s\n", yesNo(status.Loaded))
	fmt.Fprintf(w, "running: %s\n", yesNo(status.Running))
	fmt.Fprintf(w, "mode: %s\n", serviceMode(spec.System))
	if spec.UserName != "" {
		fmt.Fprintf(w, "user: %s\n", spec.UserName)
	}
	if status.Version != "" {
		fmt.Fprintf(w, "version: %s\n", status.Version)
	}
	fmt.Fprintf(w, "service file: %s\n", spec.FilePath)
	fmt.Fprintf(w, "binary: %s\n", spec.BinaryPath)
	fmt.Fprintf(w, "config: %s\n", spec.ConfigPath)
	if status.Detail != "" {
		fmt.Fprintf(w, "detail: %s\n", firstLine(status.Detail))
	}
}

func yesNo(value bool) string {
	if value {
		return "yes"
	}
	return "no"
}

func firstLine(value string) string {
	value = strings.TrimSpace(value)
	if i := strings.IndexByte(value, '\n'); i >= 0 {
		return strings.TrimSpace(value[:i])
	}
	return value
}

func serviceMode(system bool) string {
	if system {
		return "system"
	}
	return "user"
}

func installRecordPath(home string) string {
	return filepath.Join(home, config.DefaultConfigDir, installRecordFile)
}

func loadInstallRecord(home string) (*installRecord, error) {
	path := installRecordPath(home)
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("read install record %s: %w", path, err)
	}
	var record installRecord
	if err := json.Unmarshal(data, &record); err != nil {
		return nil, fmt.Errorf("parse install record %s: %w", path, err)
	}
	return &record, nil
}

func applyInstallRecord(spec *serviceSpec, record installRecord) {
	if record.BinaryPath != "" {
		spec.BinaryPath = record.BinaryPath
	}
	if record.ConfigPath != "" {
		spec.ConfigPath = record.ConfigPath
	}
	if record.ServiceFile != "" {
		spec.FilePath = record.ServiceFile
	}
	if record.UserName != "" {
		spec.UserName = record.UserName
	}
	if record.UserUID >= 0 {
		spec.UserUID = record.UserUID
	}
	if record.UserHome != "" {
		spec.UserHome = record.UserHome
	}
}

func writeInstallRecord(spec serviceSpec) error {
	dir := filepath.Join(spec.UserHome, config.DefaultConfigDir)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		return err
	}
	if err := chownUserPath(dir, spec); err != nil {
		return err
	}

	record := installRecord{
		Version:      1,
		OS:           spec.OS,
		System:       spec.System,
		ServiceMode:  serviceMode(spec.System),
		ServiceLabel: serviceLabel,
		ServiceFile:  spec.FilePath,
		BinaryPath:   spec.BinaryPath,
		ConfigPath:   spec.ConfigPath,
		UserName:     spec.UserName,
		UserUID:      spec.UserUID,
		UserHome:     spec.UserHome,
		InstalledAt:  time.Now().UTC().Format(time.RFC3339),
	}
	data, err := json.MarshalIndent(record, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')

	path := installRecordPath(spec.UserHome)
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	if err := chownForServiceUser(tmp, spec); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	if err := chownForServiceUser(path, spec); err != nil {
		return err
	}
	return nil
}

func removeInstallRecord(spec serviceSpec) error {
	err := os.Remove(installRecordPath(spec.UserHome))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func chownForServiceUser(path string, spec serviceSpec) error {
	if os.Geteuid() != 0 || spec.UserUID < 0 {
		return nil
	}
	return os.Chown(path, spec.UserUID, spec.UserGID)
}

func chownUserPath(path string, spec serviceSpec) error {
	if os.Geteuid() != 0 || spec.UserUID < 0 {
		return nil
	}
	home := filepath.Clean(spec.UserHome)
	cleanPath := filepath.Clean(path)
	rel, err := filepath.Rel(home, cleanPath)
	if err != nil || rel == "." || strings.HasPrefix(rel, "..") {
		return chownForServiceUser(cleanPath, spec)
	}
	current := home
	for _, part := range strings.Split(rel, string(os.PathSeparator)) {
		if part == "" || part == "." {
			continue
		}
		current = filepath.Join(current, part)
		if err := chownForServiceUser(current, spec); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return nil
}

func ensureLogFile(path string, spec serviceSpec) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	return chownForServiceUser(path, spec)
}

func launchdPlist(spec serviceSpec) string {
	userName := ""
	if spec.System && strings.TrimSpace(spec.UserName) != "" {
		userName = `  <key>UserName</key>
  <string>` + xmlEscape(spec.UserName) + `</string>
`
	}
	return `<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>` + xmlEscape(serviceLabel) + `</string>
` + userName + `  <key>ProgramArguments</key>
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
	runAs := ""
	if spec.System && strings.TrimSpace(spec.UserName) != "" {
		runAs = `User=` + spec.UserName + `
Environment=` + systemdQuote("HOME="+spec.UserHome) + `
WorkingDirectory=` + systemdQuote(spec.UserHome) + `
`
	}
	return `[Unit]
Description=exec-over-lark daemon
After=network-online.target

[Service]
Type=simple
` + runAs + `ExecStart=` + systemdQuote(spec.BinaryPath) + ` run -config ` + systemdQuote(spec.ConfigPath) + `
Restart=on-failure

[Install]
WantedBy=` + wantedBy + `
`
}

func systemctl(spec serviceSpec, args ...string) error {
	return runCmd("systemctl", systemctlArgs(spec, args...)...)
}

func systemctlArgs(spec serviceSpec, args ...string) []string {
	full := []string{}
	if !spec.System {
		full = append(full, "--user")
	}
	full = append(full, args...)
	return full
}

func runCmd(name string, args ...string) error {
	_, err := runCmdOutput(name, args...)
	return err
}

func runCmdOutput(name string, args ...string) (string, error) {
	cmd := exec.Command(name, args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		msg := strings.TrimSpace(string(out))
		if msg == "" {
			msg = err.Error()
		}
		return string(out), fmt.Errorf("%s %s: %s", name, strings.Join(args, " "), msg)
	}
	return string(out), nil
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
