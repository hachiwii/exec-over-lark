package lark

import (
	"encoding/json"
	"testing"

	"github.com/hachiwii/exec-over-lark/internal/protocol"
)

func FuzzParseMessageReceiveEventNeverPanics(f *testing.F) {
	for _, seed := range []string{
		string(fuzzMessageEventJSON("evt_valid", "om_valid", "oc_chat", "ou_sender", "ou_self_bot", BuildMentionedText("ou_self_bot", fuzzEncodedFrames(protocol.Frame{Seq: 1, Type: protocol.TypeStart, Payload: []byte(`{}`)})), MessageTypeText)),
		string(fuzzMessageEventJSON("evt_bad_protocol", "om_bad_protocol", "oc_chat", "ou_sender", "ou_self_bot", BuildMentionedText("ou_self_bot", "EOL1 1 start !!!"), MessageTypeText)),
		string(fuzzMessageEventJSON("evt_unmentioned", "om_unmentioned", "oc_chat", "ou_sender", "ou_other_bot", BuildMentionedText("ou_other_bot", "EOL1 1 start e30="), MessageTypeText)),
		string(fuzzMessageEventJSON("evt_non_text", "om_non_text", "oc_chat", "ou_sender", "ou_self_bot", BuildMentionedText("ou_self_bot", "EOL1 1 start e30="), "image")),
		`{"schema":"2.0","header":{"event_id":"evt_other","event_type":"im.chat.member.user.added_v1"},"event":{"chat_id":"oc_chat"}}`,
		`{"schema":"2.0","header":{"event_id":"evt_boot","event_type":"im.chat.member.bot.added_v1"},"event":{"chat_id":"oc_chat","bot_id":{"open_id":"ou_self_bot"}}}`,
		`{"type":"url_verification","challenge":"abc"}`,
		`{"schema":"2.0","header":`,
		`EOL1 1 start !!!`,
		``,
	} {
		f.Add([]byte(seed))
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = ParseMessageReceiveEvent(data, "ou_self_bot")
	})
}

func fuzzMessageEventJSON(eventID, messageID, chatID, senderOpenID, selfOpenID, text, messageType string) []byte {
	if messageType == "" {
		messageType = MessageTypeText
	}
	content, err := TextContent(text)
	if err != nil {
		panic(err)
	}

	raw, err := json.Marshal(map[string]any{
		"schema": "2.0",
		"header": map[string]any{
			"event_id":   eventID,
			"event_type": MessageReceiveEventType,
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
				"message_type": messageType,
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

func fuzzEncodedFrames(frames ...protocol.Frame) string {
	text, err := protocol.EncodeFrames(frames)
	if err != nil {
		panic(err)
	}
	return text
}
