package main

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/hachiwii/exec-over-lark/internal/daemon"
	"github.com/hachiwii/exec-over-lark/internal/lark"
)

func TestHelpIncludesInitCommand(t *testing.T) {
	var stdout, stderr bytes.Buffer

	code := run([]string{"--help"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("exit code = %d, want 0; stderr = %q", code, stderr.String())
	}
	output := stdout.String() + stderr.String()
	for _, want := range []string{
		"elarkd run [--config PATH]",
		"elarkd init (--client|--server)",
		"elarkd install [--config PATH] [--system]",
		"elarkd doctor [--config PATH]",
		"Commands:",
		"run        run elarkd in the foreground",
		"init       write a client or server config template",
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("help output = %q, want %q", output, want)
		}
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
