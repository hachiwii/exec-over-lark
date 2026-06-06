package bootstrap

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func TestHandleEventJSONSendsSimpleBootstrapMessage(t *testing.T) {
	sender := &fakeSender{}
	err := HandleEventJSON(
		context.Background(),
		sender,
		addedToChatEventJSON(t, "evt_boot", "oc_test", "ou_server_bot"),
		"ou_server_bot",
	)
	if err != nil {
		t.Fatalf("HandleEventJSON returned error: %v", err)
	}

	if got := len(sender.messages); got != 1 {
		t.Fatalf("sent messages = %d, want 1", got)
	}
	msg := sender.messages[0]
	if msg.chatID != "oc_test" {
		t.Fatalf("chatID = %q, want oc_test", msg.chatID)
	}

	want := "exec-over-lark server ready\nchat_id: oc_test\nbot_openid: ou_server_bot"
	if msg.text != want {
		t.Fatalf("text = %q, want %q", msg.text, want)
	}
	if got := strings.Count(msg.text, "\n"); got != 2 {
		t.Fatalf("message line breaks = %d, want 2", got)
	}

	for _, forbidden := range []string{"secret", "config", "template", "toml", "app_id", "app_secret", "hosts.", "peer_bot_open_id"} {
		if strings.Contains(msg.text, forbidden) {
			t.Fatalf("bootstrap message contains forbidden text %q: %q", forbidden, msg.text)
		}
	}
}

func TestHandleEventJSONIgnoresDifferentAddedBot(t *testing.T) {
	sender := &fakeSender{}
	err := HandleEventJSON(
		context.Background(),
		sender,
		addedToChatEventJSON(t, "evt_other", "oc_test", "ou_other_bot"),
		"ou_server_bot",
	)
	if !errors.Is(err, ErrIgnoredEvent) {
		t.Fatalf("HandleEventJSON error = %v, want ErrIgnoredEvent", err)
	}
	if got := len(sender.messages); got != 0 {
		t.Fatalf("sent messages = %d, want 0", got)
	}
}

func TestHandleEventJSONAcceptsBotAddedEventWithoutBotID(t *testing.T) {
	sender := &fakeSender{}
	err := HandleEventJSON(
		context.Background(),
		sender,
		addedToChatEventJSON(t, "evt_no_bot_id", "oc_test", ""),
		"ou_server_bot",
	)
	if err != nil {
		t.Fatalf("HandleEventJSON returned error: %v", err)
	}
	if got := len(sender.messages); got != 1 {
		t.Fatalf("sent messages = %d, want 1", got)
	}
}

func TestHandleAddedToChatEventWrapsSendErrorWithChatID(t *testing.T) {
	wantErr := errors.New("send failed")
	sender := &fakeSender{err: wantErr}

	err := HandleAddedToChatEvent(
		context.Background(),
		sender,
		AddedToChatEvent{ChatID: "oc_failure"},
		"ou_server_bot",
	)
	if !errors.Is(err, wantErr) {
		t.Fatalf("HandleAddedToChatEvent error = %v, want wrapped send error", err)
	}
	if !strings.Contains(err.Error(), "oc_failure") {
		t.Fatalf("error = %q, want chat ID for diagnostics", err.Error())
	}
}

func TestMessageRequiresChatIDAndBotOpenID(t *testing.T) {
	if _, err := Message("", "ou_server_bot"); err == nil {
		t.Fatal("Message with empty chatID returned nil error")
	}
	if _, err := Message("oc_test", ""); err == nil {
		t.Fatal("Message with empty botOpenID returned nil error")
	}
}

type sentMessage struct {
	chatID string
	text   string
}

type fakeSender struct {
	err      error
	messages []sentMessage
}

func (f *fakeSender) SendTextMessage(ctx context.Context, chatID, text string) error {
	if f.err != nil {
		return f.err
	}
	f.messages = append(f.messages, sentMessage{chatID: chatID, text: text})
	return nil
}

func addedToChatEventJSON(t *testing.T, eventID, chatID, addedBotOpenID string) []byte {
	t.Helper()

	event := map[string]any{
		"chat_id": chatID,
	}
	if addedBotOpenID != "" {
		event["bot_id"] = map[string]any{"open_id": addedBotOpenID}
	}

	raw, err := json.Marshal(map[string]any{
		"schema": "2.0",
		"header": map[string]any{
			"event_id":   eventID,
			"event_type": BotAddedEventType,
		},
		"event": event,
	})
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}
	return raw
}
