package main

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/hachiwii/exec-over-lark/internal/daemon"
	"github.com/hachiwii/exec-over-lark/internal/lark"
	"github.com/hachiwii/exec-over-lark/internal/version"
)

func TestHelpIncludesAllCommandsAndGlobalOptions(t *testing.T) {
	var stdout, stderr bytes.Buffer

	code := run([]string{"--help"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("exit code = %d, want 0; stderr = %q", code, stderr.String())
	}
	output := stdout.String() + stderr.String()
	for _, want := range []string{
		"elarkd [--config PATH]",
		"elarkd [--help|--version]",
		"elarkd run [--config PATH]",
		"elarkd init (--client|--server)",
		"elarkd install [--config PATH] [--system]",
		"elarkd uninstall [--config PATH] [--system]",
		"elarkd start [--config PATH] [--system]",
		"elarkd restart [--config PATH] [--system]",
		"elarkd stop [--config PATH] [--system]",
		"elarkd status [--config PATH] [--system]",
		"elarkd doctor [--config PATH]",
		"elarkd help",
		"Commands:",
		"run        run elarkd in the foreground",
		"init       write a client or server config template",
		"install    install the background daemon service",
		"uninstall  remove the installed daemon service",
		"start      start the installed daemon service",
		"restart    restart the installed daemon service",
		"stop       stop the installed daemon service",
		"status     show the installed daemon service status",
		"doctor     check config validity and Lark token refresh",
		"help       show this help",
		"--version",
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("help output = %q, want %q", output, want)
		}
	}
}

func TestVersionOptionPrintsVersion(t *testing.T) {
	oldVersion := version.Version
	version.Version = "v9.8.7-test"
	t.Cleanup(func() {
		version.Version = oldVersion
	})

	var stdout, stderr bytes.Buffer
	code := run([]string{"--version"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("exit code = %d, want 0; stderr = %q", code, stderr.String())
	}
	if strings.TrimSpace(stdout.String()) != "v9.8.7-test" {
		t.Fatalf("stdout = %q, want version", stdout.String())
	}
	if stderr.String() != "" {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

func TestInitHelp(t *testing.T) {
	var stdout, stderr bytes.Buffer

	code := run([]string{"init", "--help"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("exit code = %d, want 0; stderr = %q", code, stderr.String())
	}
	output := stdout.String() + stderr.String()
	for _, want := range []string{
		"elarkd init (--client|--server)",
		"-client",
		"-server",
		"-force",
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("init help output = %q, want %q", output, want)
		}
	}
}

func TestDoctorHelp(t *testing.T) {
	var stdout, stderr bytes.Buffer

	code := run([]string{"doctor", "--help"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("exit code = %d, want 0; stderr = %q", code, stderr.String())
	}
	output := stdout.String() + stderr.String()
	for _, want := range []string{
		"elarkd doctor [--config PATH]",
		"-config",
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("doctor help output = %q, want %q", output, want)
		}
	}
}

func TestCombinedEventHandlerDispatchesToAllEnabledModes(t *testing.T) {
	event := lark.MessageEvent{EventID: "evt_1", ChatID: "oc_chat", RootMessageID: "om_root", MessageID: "om_reply"}
	var calls []string
	handler := combinedEventHandler(&bytes.Buffer{},
		namedEventHandler{name: "local", handler: func(context.Context, lark.MessageEvent) error {
			calls = append(calls, "local")
			return nil
		}},
		namedEventHandler{name: "remote", handler: func(context.Context, lark.MessageEvent) error {
			calls = append(calls, "remote")
			return lark.ErrIgnoredEvent
		}},
	)

	if err := handler(context.Background(), event); err != nil {
		t.Fatalf("handler returned error: %v", err)
	}
	if strings.Join(calls, ",") != "local,remote" {
		t.Fatalf("calls = %v, want local,remote", calls)
	}
}

func TestCombinedEventHandlerIsolatesModeHandlerError(t *testing.T) {
	wantErr := errors.New("session failed")
	var stderr bytes.Buffer
	handler := combinedEventHandler(&stderr,
		namedEventHandler{name: "local", handler: func(context.Context, lark.MessageEvent) error {
			return daemon.ErrIgnoredEvent
		}},
		namedEventHandler{name: "remote", handler: func(context.Context, lark.MessageEvent) error {
			return wantErr
		}},
	)

	err := handler(context.Background(), lark.MessageEvent{EventID: "evt_2", ChatID: "oc_chat", RootMessageID: "om_root", MessageID: "om_root"})
	if err != nil {
		t.Fatalf("handler returned error: %v", err)
	}
	if !strings.Contains(stderr.String(), "ignored remote event handler error") || !strings.Contains(stderr.String(), wantErr.Error()) {
		t.Fatalf("stderr = %q, want remote handler error log", stderr.String())
	}
}

func TestCombinedEventHandlerReturnsIgnoredWhenNoModeHandlesEvent(t *testing.T) {
	handler := combinedEventHandler(&bytes.Buffer{},
		namedEventHandler{name: "local", handler: func(context.Context, lark.MessageEvent) error {
			return daemon.ErrIgnoredEvent
		}},
		namedEventHandler{name: "remote", handler: func(context.Context, lark.MessageEvent) error {
			return lark.ErrIgnoredEvent
		}},
	)

	err := handler(context.Background(), lark.MessageEvent{EventID: "evt_3"})
	if !errors.Is(err, daemon.ErrIgnoredEvent) {
		t.Fatalf("handler error = %v, want ErrIgnoredEvent", err)
	}
}
