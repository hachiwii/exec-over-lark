package main

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/hachiwii/exec-over-lark/internal/bootstrap"
	"github.com/hachiwii/exec-over-lark/internal/lark"
	"github.com/hachiwii/exec-over-lark/internal/protocol"
)

func FuzzLarkWebSocketEventSourceHandlePayloadNeverPanics(f *testing.F) {
	for _, seed := range []string{
		string(fuzzWebsocketMessageEventJSON("evt_valid", "om_valid", "oc_chat", "ou_sender", "ou_self_bot", lark.BuildMentionedText("ou_self_bot", fuzzWebsocketEncodedFrames(protocol.Frame{Seq: 1, Type: protocol.TypeStart, Payload: []byte(`{}`)})))),
		string(fuzzWebsocketMessageEventJSON("evt_bad_protocol", "om_bad_protocol", "oc_chat", "ou_sender", "ou_self_bot", lark.BuildMentionedText("ou_self_bot", "EOL1 1 start !!!"))),
		string(fuzzWebsocketBotAddedEventJSON("evt_boot", "oc_chat", "ou_self_bot")),
		string(fuzzWebsocketOtherEventJSON("evt_other")),
		`{"schema":"2.0","header":`,
		`EOL1 1 start !!!`,
		``,
	} {
		f.Add([]byte(seed))
	}

	f.Fuzz(func(t *testing.T, payload []byte) {
		sender := &fakeBootstrapSender{}
		source := &larkWebSocketEventSource{bootstrapSender: sender}
		_ = source.handlePayload(context.Background(), payload, "ou_self_bot", func(context.Context, lark.MessageEvent) error {
			return nil
		})
	})
}

func fuzzWebsocketMessageEventJSON(eventID, messageID, chatID, senderOpenID, selfOpenID, text string) []byte {
	content, err := lark.TextContent(text)
	if err != nil {
		panic(err)
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
				"root_id":      messageID,
				"chat_id":      chatID,
				"message_type": lark.MessageTypeText,
				"content":      content,
				"mentions": []map[string]any{
					{
						"key":  "@_user_1",
						"name": "self",
						"id": map[string]any{
							"open_id": selfOpenID,
						},
					},
				},
			},
		},
	})
	if err != nil {
		panic(err)
	}
	return raw
}

func fuzzWebsocketBotAddedEventJSON(eventID, chatID, addedBotOpenID string) []byte {
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
		panic(err)
	}
	return raw
}

func fuzzWebsocketOtherEventJSON(eventID string) []byte {
	raw, err := json.Marshal(map[string]any{
		"schema": "2.0",
		"header": map[string]any{
			"event_id":   eventID,
			"event_type": "im.chat.member.user.added_v1",
		},
		"event": map[string]any{
			"chat_id": "oc_chat",
		},
	})
	if err != nil {
		panic(err)
	}
	return raw
}

func fuzzWebsocketEncodedFrames(frames ...protocol.Frame) string {
	text, err := protocol.EncodeFrames(frames)
	if err != nil {
		panic(err)
	}
	return text
}
