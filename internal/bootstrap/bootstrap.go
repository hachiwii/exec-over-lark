package bootstrap

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

const BotAddedEventType = "im.chat.member.bot.added_v1"

var (
	ErrIgnoredEvent = errors.New("ignored bootstrap event")
	ErrNoSender     = errors.New("bootstrap sender is nil")
)

type Sender interface {
	SendTextMessage(ctx context.Context, chatID, text string) error
}

type AddedToChatEvent struct {
	EventID        string
	EventType      string
	ChatID         string
	AddedBotOpenID string
}

func HandleAddedToChatEvent(ctx context.Context, sender Sender, event AddedToChatEvent, botOpenID string) error {
	if sender == nil {
		return ErrNoSender
	}
	event.ChatID = strings.TrimSpace(event.ChatID)
	if event.ChatID == "" {
		return errors.New("bootstrap chat_id is required")
	}

	text, err := Message(event.ChatID, botOpenID)
	if err != nil {
		return err
	}
	if err := sender.SendTextMessage(ctx, event.ChatID, text); err != nil {
		return fmt.Errorf("send bootstrap message to chat %s: %w", event.ChatID, err)
	}
	return nil
}

func HandleEventJSON(ctx context.Context, sender Sender, data []byte, botOpenID string) error {
	event, err := ParseAddedToChatEvent(data, botOpenID)
	if err != nil {
		return err
	}
	return HandleAddedToChatEvent(ctx, sender, event, botOpenID)
}

func Message(chatID, botOpenID string) (string, error) {
	chatID = strings.TrimSpace(chatID)
	if chatID == "" {
		return "", errors.New("bootstrap chat_id is required")
	}
	botOpenID = strings.TrimSpace(botOpenID)
	if botOpenID == "" {
		return "", errors.New("bootstrap bot_openid is required")
	}

	return fmt.Sprintf("exec-over-lark server ready\nchat_id: %s\nbot_openid: %s", chatID, botOpenID), nil
}

func ParseAddedToChatEvent(data []byte, selfBotOpenID string) (AddedToChatEvent, error) {
	var envelope rawEnvelope
	if err := json.Unmarshal(data, &envelope); err != nil {
		return AddedToChatEvent{}, fmt.Errorf("decode bootstrap event: %w", err)
	}
	if envelope.Type == "url_verification" || envelope.Type == "challenge" {
		return AddedToChatEvent{}, ErrIgnoredEvent
	}
	if envelope.Header.EventType != BotAddedEventType {
		return AddedToChatEvent{}, ErrIgnoredEvent
	}

	chatID := strings.TrimSpace(envelope.Event.ChatID)
	if chatID == "" {
		return AddedToChatEvent{}, errors.New("bootstrap event missing chat_id")
	}

	addedBotOpenID := firstNonEmpty(
		envelope.Event.BotID.OpenID,
		envelope.Event.MemberID.OpenID,
		envelope.Event.OpenID,
	)
	if self := strings.TrimSpace(selfBotOpenID); self != "" && addedBotOpenID != "" && addedBotOpenID != self {
		return AddedToChatEvent{}, ErrIgnoredEvent
	}

	return AddedToChatEvent{
		EventID:        envelope.Header.EventID,
		EventType:      envelope.Header.EventType,
		ChatID:         chatID,
		AddedBotOpenID: addedBotOpenID,
	}, nil
}

type rawEnvelope struct {
	Type   string    `json:"type"`
	Header rawHeader `json:"header"`
	Event  rawEvent  `json:"event"`
}

type rawHeader struct {
	EventID   string `json:"event_id"`
	EventType string `json:"event_type"`
}

type rawEvent struct {
	ChatID   string        `json:"chat_id"`
	BotID    rawIdentifier `json:"bot_id"`
	MemberID rawIdentifier `json:"member_id"`
	OpenID   string        `json:"open_id"`
}

type rawIdentifier struct {
	OpenID string
}

func (id *rawIdentifier) UnmarshalJSON(data []byte) error {
	var value string
	if err := json.Unmarshal(data, &value); err == nil {
		id.OpenID = value
		return nil
	}

	var object struct {
		OpenID string `json:"open_id"`
	}
	if err := json.Unmarshal(data, &object); err != nil {
		return err
	}
	id.OpenID = object.OpenID
	return nil
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
