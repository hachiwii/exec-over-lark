package outbound

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/hachiwii/exec-over-lark/internal/protocol"
)

const (
	DefaultSendCooldown              = 500 * time.Millisecond
	DefaultLarkTextRequestLimitBytes = 153600
)

var (
	ErrFrameTooLarge = errors.New("outbound frame exceeds lark text request limit")
	ErrInvalidTarget = errors.New("invalid outbound target")
	ErrNoSender      = errors.New("outbound sender is nil")
)

type RootMessage struct {
	MessageID string
}

type Clock interface {
	Now() time.Time
}

type Target struct {
	ChatID        string
	RootMessageID string
	MentionOpenID string
}

type RequestSizer func(Target, string) (int, error)

type realClock struct{}

func (realClock) Now() time.Time {
	return time.Now()
}

func (t Target) validate() error {
	if strings.TrimSpace(t.ChatID) == "" {
		return fmt.Errorf("%w: chat id is required", ErrInvalidTarget)
	}
	if strings.TrimSpace(t.MentionOpenID) == "" {
		return fmt.Errorf("%w: mention open id is required", ErrInvalidTarget)
	}
	return nil
}

func defaultRequestSizer(target Target, text string) (int, error) {
	content, err := json.Marshal(map[string]string{"text": text})
	if err != nil {
		return 0, err
	}
	body := struct {
		ChatID        string `json:"chat_id,omitempty"`
		RootMessageID string `json:"root_message_id,omitempty"`
		MentionOpenID string `json:"mention_open_id,omitempty"`
		MsgType       string `json:"msg_type"`
		Content       string `json:"content"`
	}{
		ChatID:        target.ChatID,
		RootMessageID: target.RootMessageID,
		MentionOpenID: target.MentionOpenID,
		MsgType:       "text",
		Content:       string(content),
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return 0, err
	}
	return len(raw), nil
}

func cloneFrames(frames []protocol.Frame) []protocol.Frame {
	if len(frames) == 0 {
		return nil
	}
	cloned := make([]protocol.Frame, len(frames))
	for i, frame := range frames {
		cloned[i] = protocol.Frame{
			Seq:     frame.Seq,
			Type:    frame.Type,
			Payload: cloneBytes(frame.Payload),
		}
	}
	return cloned
}

func cloneBytes(in []byte) []byte {
	if len(in) == 0 {
		return nil
	}
	return append([]byte(nil), in...)
}
