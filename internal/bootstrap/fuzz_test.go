package bootstrap

import (
	"encoding/json"
	"testing"
)

func FuzzParseAddedToChatEventNeverPanics(f *testing.F) {
	for _, seed := range []string{
		string(fuzzAddedToChatEventJSON("evt_boot", "oc_chat", "ou_self_bot")),
		string(fuzzAddedToChatEventJSON("evt_other_bot", "oc_chat", "ou_other_bot")),
		`{"schema":"2.0","header":{"event_id":"evt_message","event_type":"im.message.receive_v1"},"event":{"message":{"message_type":"text"}}}`,
		`{"type":"url_verification","challenge":"abc"}`,
		`{"schema":"2.0","header":{"event_id":"evt_bad","event_type":"im.chat.member.bot.added_v1"},"event":{"chat_id":123,"bot_id":[]}}`,
		`{"schema":"2.0","header":`,
		`EOL1 1 start !!!`,
		``,
	} {
		f.Add([]byte(seed))
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = ParseAddedToChatEvent(data, "ou_self_bot")
	})
}

func fuzzAddedToChatEventJSON(eventID, chatID, addedBotOpenID string) []byte {
	raw, err := json.Marshal(map[string]any{
		"schema": "2.0",
		"header": map[string]any{
			"event_id":   eventID,
			"event_type": BotAddedEventType,
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
