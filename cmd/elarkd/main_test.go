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
