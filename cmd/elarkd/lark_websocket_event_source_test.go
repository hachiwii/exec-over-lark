package main

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/hachiwii/exec-over-lark/internal/bootstrap"
	"github.com/hachiwii/exec-over-lark/internal/lark"
	"github.com/hachiwii/exec-over-lark/internal/protocol"
)

func TestLarkWebSocketEventSourceHandlePayloadDispatchesMessageEvent(t *testing.T) {
	sender := &fakeBootstrapSender{}
	source := &larkWebSocketEventSource{bootstrapSender: sender}

	var events []lark.MessageEvent
	err := source.handlePayload(
		context.Background(),
		websocketMessageEventJSON(t, "evt_msg", "oc_chat", "om_root", "ou_client_bot", "ou_server_bot"),
		"ou_server_bot",
		func(_ context.Context, event lark.MessageEvent) error {
			events = append(events, event)
			return nil
		},
	)
	if err != nil {
		t.Fatalf("handlePayload returned error: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("message events = %d, want 1", len(events))
	}
	if events[0].EventID != "evt_msg" || events[0].ChatID != "oc_chat" || events[0].RootMessageID != "om_root" {
		t.Fatalf("message event = %#v", events[0])
	}
	if len(sender.messages) != 0 {
		t.Fatalf("bootstrap messages = %d, want 0", len(sender.messages))
	}
}

func TestLarkWebSocketEventSourceHandlePayloadSendsBootstrapOnBotAdded(t *testing.T) {
	sender := &fakeBootstrapSender{}
	source := &larkWebSocketEventSource{bootstrapSender: sender}
	handlerCalled := false

	err := source.handlePayload(
		context.Background(),
		websocketBotAddedEventJSON(t, "evt_boot", "oc_boot", "ou_server_bot"),
		"ou_server_bot",
		func(context.Context, lark.MessageEvent) error {
			handlerCalled = true
			return nil
		},
	)
	if err != nil {
		t.Fatalf("handlePayload returned error: %v", err)
	}
	if handlerCalled {
		t.Fatal("message handler was called for bootstrap event")
	}
	if len(sender.messages) != 1 {
		t.Fatalf("bootstrap messages = %d, want 1", len(sender.messages))
	}
	msg := sender.messages[0]
	if msg.chatID != "oc_boot" {
		t.Fatalf("bootstrap chatID = %q, want oc_boot", msg.chatID)
	}
	for _, want := range []string{"exec-over-lark server ready", "chat_id: oc_boot", "bot_openid: ou_server_bot"} {
		if !strings.Contains(msg.text, want) {
			t.Fatalf("bootstrap text = %q, missing %q", msg.text, want)
		}
	}
}

func TestLarkWebSocketEventSourceHandlePayloadIgnoresDisabledAndNonTargetBootstrap(t *testing.T) {
	tests := []struct {
		name   string
		source *larkWebSocketEventSource
		raw    []byte
	}{
		{
			name:   "bootstrap disabled",
			source: &larkWebSocketEventSource{},
			raw:    websocketBotAddedEventJSON(t, "evt_disabled", "oc_boot", "ou_server_bot"),
		},
		{
			name:   "different bot",
			source: &larkWebSocketEventSource{bootstrapSender: &fakeBootstrapSender{}},
			raw:    websocketBotAddedEventJSON(t, "evt_other_bot", "oc_boot", "ou_other_bot"),
		},
		{
			name:   "different event",
			source: &larkWebSocketEventSource{bootstrapSender: &fakeBootstrapSender{}},
			raw:    websocketOtherEventJSON(t, "evt_other_event"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handlerCalled := false
			err := tt.source.handlePayload(
				context.Background(),
				tt.raw,
				"ou_server_bot",
				func(context.Context, lark.MessageEvent) error {
					handlerCalled = true
					return nil
				},
			)
			if err != nil {
				t.Fatalf("handlePayload returned error: %v", err)
			}
			if handlerCalled {
				t.Fatal("message handler was called for ignored bootstrap event")
			}
			if sender, ok := tt.source.bootstrapSender.(*fakeBootstrapSender); ok && len(sender.messages) != 0 {
				t.Fatalf("bootstrap messages = %d, want 0", len(sender.messages))
			}
		})
	}
}

func websocketMessageEventJSON(t *testing.T, eventID, chatID, messageID, senderOpenID, selfOpenID string) []byte {
	t.Helper()

	frames, err := protocol.EncodeFrames([]protocol.Frame{
		{Seq: 1, Type: protocol.TypeStart, Payload: []byte(`{}`)},
	})
	if err != nil {
		t.Fatalf("EncodeFrames returned error: %v", err)
	}
	content, err := lark.TextContent(lark.BuildMentionedText(selfOpenID, frames))
	if err != nil {
		t.Fatalf("TextContent returned error: %v", err)
	}

	raw, err := json.Marshal(map[string]any{
		"schema": "2.0",
		"header": map[string]any{
			"event_id":   eventID,
			"event_type": lark.MessageReceiveEventType,
		},
		"event": map[string]any{
			"sender": map[string]any{
				"sender_type": "app",
				"sender_id": map[string]any{
					"open_id": senderOpenID,
				},
			},
			"message": map[string]any{
				"message_id":   messageID,
				"chat_id":      chatID,
				"message_type": lark.MessageTypeText,
				"content":      content,
				"mentions": []map[string]any{
					{
						"key": "@_user_1",
						"id": map[string]any{
							"open_id": selfOpenID,
						},
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("marshal message event: %v", err)
	}
	return raw
}

func websocketBotAddedEventJSON(t *testing.T, eventID, chatID, addedBotOpenID string) []byte {
	t.Helper()

	raw, err := json.Marshal(map[string]any{
		"schema": "2.0",
		"header": map[string]any{
			"event_id":   eventID,
			"event_type": bootstrap.BotAddedEventType,
		},
		"event": map[string]any{
			"chat_id": chatID,
			"bot_id": map[string]any{
				"open_id": addedBotOpenID,
			},
		},
	})
	if err != nil {
		t.Fatalf("marshal bot added event: %v", err)
	}
	return raw
}

func websocketOtherEventJSON(t *testing.T, eventID string) []byte {
	t.Helper()

	raw, err := json.Marshal(map[string]any{
		"schema": "2.0",
		"header": map[string]any{
			"event_id":   eventID,
			"event_type": "im.chat.member.user.added_v1",
		},
		"event": map[string]any{
			"chat_id": "oc_boot",
		},
	})
	if err != nil {
		t.Fatalf("marshal other event: %v", err)
	}
	return raw
}

type fakeBootstrapMessage struct {
	chatID string
	text   string
}

type fakeBootstrapSender struct {
	messages []fakeBootstrapMessage
}

func (f *fakeBootstrapSender) SendTextMessage(_ context.Context, chatID, text string) error {
	f.messages = append(f.messages, fakeBootstrapMessage{chatID: chatID, text: text})
	return nil
}
