package lark

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/hachiwii/exec-over-lark/internal/protocol"
)

func TestTenantTokenAndBotOpenIDAreCached(t *testing.T) {
	var tokenRequests int
	var botRequests int

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case tenantTokenPath:
			tokenRequests++
			if r.Method != http.MethodPost {
				t.Fatalf("token method = %s, want POST", r.Method)
			}
			var req tenantTokenRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Fatalf("decode token request: %v", err)
			}
			if req.AppID != "cli_test" || req.AppSecret != "secret_test" {
				t.Fatal("token request did not contain expected app credentials")
			}
			writeJSON(t, w, map[string]any{
				"code":                0,
				"msg":                 "ok",
				"tenant_access_token": "token-a",
				"expire":              3600,
			})
		case botInfoPath:
			botRequests++
			if got := r.Header.Get("Authorization"); got != "Bearer token-a" {
				t.Fatalf("bot Authorization = %q, want Bearer token-a", got)
			}
			writeJSON(t, w, map[string]any{
				"code": 0,
				"msg":  "ok",
				"bot": map[string]any{
					"open_id": "ou_self_bot",
				},
			})
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	defer server.Close()

	client := newTestClient(t, server.URL)
	ctx := context.Background()

	for i := 0; i < 2; i++ {
		token, err := client.TenantAccessToken(ctx)
		if err != nil {
			t.Fatalf("TenantAccessToken returned error: %v", err)
		}
		if token != "token-a" {
			t.Fatalf("token = %q, want token-a", token)
		}
	}
	for i := 0; i < 2; i++ {
		openID, err := client.BotOpenID(ctx)
		if err != nil {
			t.Fatalf("BotOpenID returned error: %v", err)
		}
		if openID != "ou_self_bot" {
			t.Fatalf("openID = %q, want ou_self_bot", openID)
		}
	}

	if tokenRequests != 1 {
		t.Fatalf("tokenRequests = %d, want 1", tokenRequests)
	}
	if botRequests != 1 {
		t.Fatalf("botRequests = %d, want 1", botRequests)
	}
}

func TestSendRootMessageRetriesOnceAfterExpiredToken(t *testing.T) {
	var tokenRequests int
	var sendRequests int

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case tenantTokenPath:
			tokenRequests++
			token := "stale-token"
			if tokenRequests == 2 {
				token = "fresh-token"
			}
			writeJSON(t, w, map[string]any{
				"code":                0,
				"tenant_access_token": token,
				"expire":              3600,
			})
		case sendMessagePath:
			sendRequests++
			if sendRequests == 1 {
				if got := r.Header.Get("Authorization"); got != "Bearer stale-token" {
					t.Fatalf("first send Authorization = %q, want stale token", got)
				}
				w.WriteHeader(http.StatusUnauthorized)
				writeJSON(t, w, map[string]any{"code": 99991663, "msg": "token expired"})
				return
			}
			if got := r.Header.Get("Authorization"); got != "Bearer fresh-token" {
				t.Fatalf("second send Authorization = %q, want fresh token", got)
			}
			assertRootSendRequest(t, r, "oc_test", "ou_peer_bot", "EOL1 1 start e30=")
			writeJSON(t, w, map[string]any{
				"code": 0,
				"data": map[string]any{
					"message_id": "om_root",
				},
			})
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	defer server.Close()

	client := newTestClient(t, server.URL)
	root, err := client.SendRootMessage(context.Background(), "oc_test", "ou_peer_bot", "EOL1 1 start e30=")
	if err != nil {
		t.Fatalf("SendRootMessage returned error: %v", err)
	}
	if root.MessageID != "om_root" {
		t.Fatalf("root.MessageID = %q, want om_root", root.MessageID)
	}
	if tokenRequests != 2 {
		t.Fatalf("tokenRequests = %d, want 2", tokenRequests)
	}
	if sendRequests != 2 {
		t.Fatalf("sendRequests = %d, want 2", sendRequests)
	}
}

func TestSendRootAndReplyMessageJSON(t *testing.T) {
	var tokenRequests int
	var rootRequests int
	var replyRequests int

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case tenantTokenPath:
			tokenRequests++
			writeJSON(t, w, map[string]any{
				"code":                0,
				"tenant_access_token": "token-a",
				"expire":              3600,
			})
		case sendMessagePath:
			rootRequests++
			if got := r.URL.Query().Get("receive_id_type"); got != "chat_id" {
				t.Fatalf("receive_id_type = %q, want chat_id", got)
			}
			if got := r.Header.Get("Authorization"); got != "Bearer token-a" {
				t.Fatalf("root Authorization = %q, want Bearer token-a", got)
			}
			assertRootSendRequest(t, r, "oc_json", "ou_server", "EOL1 1 start e30=")
			writeJSON(t, w, map[string]any{
				"code": 0,
				"data": map[string]any{"message_id": "om_root"},
			})
		case "/open-apis/im/v1/messages/om_root/reply":
			replyRequests++
			if got := r.Header.Get("Authorization"); got != "Bearer token-a" {
				t.Fatalf("reply Authorization = %q, want Bearer token-a", got)
			}
			assertReplyRequest(t, r, "ou_client", "EOL1 1 stdout b2sK")
			writeJSON(t, w, map[string]any{
				"code": 0,
				"data": map[string]any{"message_id": "om_reply"},
			})
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	defer server.Close()

	client := newTestClient(t, server.URL)
	ctx := context.Background()

	root, err := client.SendRootMessage(ctx, "oc_json", "ou_server", "EOL1 1 start e30=")
	if err != nil {
		t.Fatalf("SendRootMessage returned error: %v", err)
	}
	if root.MessageID != "om_root" {
		t.Fatalf("root.MessageID = %q, want om_root", root.MessageID)
	}
	replyID, err := client.ReplyRootMessage(ctx, "oc_json", root.MessageID, "ou_client", "EOL1 1 stdout b2sK")
	if err != nil {
		t.Fatalf("ReplyRootMessage returned error: %v", err)
	}
	if replyID != "om_reply" {
		t.Fatalf("replyID = %q, want om_reply", replyID)
	}
	if tokenRequests != 1 {
		t.Fatalf("tokenRequests = %d, want 1", tokenRequests)
	}
	if rootRequests != 1 || replyRequests != 1 {
		t.Fatalf("rootRequests/replyRequests = %d/%d, want 1/1", rootRequests, replyRequests)
	}
}

func TestSendRootMessageIncludesLarkErrorBodyForNon2xx(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case tenantTokenPath:
			writeJSON(t, w, map[string]any{
				"code":                0,
				"tenant_access_token": "token-a",
				"expire":              3600,
			})
		case sendMessagePath:
			w.WriteHeader(http.StatusBadRequest)
			writeJSON(t, w, map[string]any{
				"code": 230001,
				"msg":  "bad receive_id",
			})
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	defer server.Close()

	client := newTestClient(t, server.URL)
	_, err := client.SendRootMessage(context.Background(), "oc_json", "ou_server", "EOL1 1 start e30=")
	if err == nil {
		t.Fatal("SendRootMessage returned nil error")
	}
	if got := err.Error(); !strings.Contains(got, "HTTP status 400, code 230001: bad receive_id") {
		t.Fatalf("error = %q, want status, code, and message", got)
	}
}

func TestPersistentConnectionEndpoint(t *testing.T) {
	var endpointRequests int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case wsEndpointPath:
			endpointRequests++
			if r.Method != http.MethodPost {
				t.Fatalf("method = %s, want POST", r.Method)
			}
			var req wsEndpointRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Fatalf("decode endpoint request: %v", err)
			}
			if req.AppID != "cli_test" || req.AppSecret != "secret_test" {
				t.Fatalf("endpoint request = %#v", req)
			}
			writeJSON(t, w, map[string]any{
				"code": 0,
				"data": map[string]any{
					"URL": "wss://msg-frontier.feishu.cn/ws/v2?service_id=123",
					"ClientConfig": map[string]any{
						"ReconnectCount":    7,
						"ReconnectInterval": 11,
						"ReconnectNonce":    13,
						"PingInterval":      17,
					},
				},
			})
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	defer server.Close()

	client := newTestClient(t, server.URL)
	endpoint, err := client.PersistentConnectionEndpoint(context.Background())
	if err != nil {
		t.Fatalf("PersistentConnectionEndpoint returned error: %v", err)
	}
	if endpoint.URL != "wss://msg-frontier.feishu.cn/ws/v2?service_id=123" {
		t.Fatalf("URL = %q", endpoint.URL)
	}
	if endpoint.ClientConfig.ReconnectCount != 7 ||
		endpoint.ClientConfig.ReconnectInterval != 11*time.Second ||
		endpoint.ClientConfig.ReconnectNonce != 13*time.Second ||
		endpoint.ClientConfig.PingInterval != 17*time.Second {
		t.Fatalf("ClientConfig = %#v", endpoint.ClientConfig)
	}
	if endpointRequests != 1 {
		t.Fatalf("endpointRequests = %d, want 1", endpointRequests)
	}
}

func TestParseMessageReceiveEvent(t *testing.T) {
	payload := []byte(`{"pty":true,"heartbeat":{"interval":"10s","timeout":"30s"}}`)
	framesText, err := protocol.EncodeFrames([]protocol.Frame{
		{Seq: 1, Type: protocol.TypeStart, Payload: payload},
		{Seq: 2, Type: protocol.TypeHeartbeat, Payload: []byte(`{}`)},
	})
	if err != nil {
		t.Fatalf("EncodeFrames returned error: %v", err)
	}

	eventJSON := messageEventJSON(t, eventOptions{
		EventID:       "evt_1",
		MessageID:     "om_reply",
		RootMessageID: "om_root",
		ChatID:        "oc_test",
		SenderOpenID:  "ou_client_bot",
		SelfOpenID:    "ou_self_bot",
		Text:          BuildMentionedText("ou_self_bot", framesText),
	})

	event, err := ParseMessageReceiveEvent(eventJSON, "ou_self_bot")
	if err != nil {
		t.Fatalf("ParseMessageReceiveEvent returned error: %v", err)
	}
	if event.EventID != "evt_1" || event.EventType != MessageReceiveEventType {
		t.Fatalf("event header = %#v", event)
	}
	if event.MessageID != "om_reply" || event.RootMessageID != "om_root" {
		t.Fatalf("message IDs = %q/%q, want om_reply/om_root", event.MessageID, event.RootMessageID)
	}
	if event.ChatID != "oc_test" || event.SenderOpenID != "ou_client_bot" {
		t.Fatalf("chat/sender = %q/%q", event.ChatID, event.SenderOpenID)
	}
	if len(event.Mentions) != 1 || event.Mentions[0].OpenID != "ou_self_bot" {
		t.Fatalf("mentions = %#v", event.Mentions)
	}
	if len(event.Frames) != 2 || event.Frames[0].Seq != 1 || event.Frames[0].Type != protocol.TypeStart {
		t.Fatalf("frames = %#v", event.Frames)
	}
	if string(event.Frames[0].Payload) != string(payload) {
		t.Fatalf("payload = %s, want %s", event.Frames[0].Payload, payload)
	}
}

func TestParseMessageReceiveEventUsesMessageIDAsRootForRootMessages(t *testing.T) {
	eventJSON := messageEventJSON(t, eventOptions{
		EventID:      "evt_root",
		MessageID:    "om_root",
		ChatID:       "oc_test",
		SenderOpenID: "ou_client_bot",
		SelfOpenID:   "ou_self_bot",
		Text:         BuildMentionedText("ou_self_bot", "EOL1 1 start e30="),
	})

	event, err := ParseMessageReceiveEvent(eventJSON, "ou_self_bot")
	if err != nil {
		t.Fatalf("ParseMessageReceiveEvent returned error: %v", err)
	}
	if event.RootMessageID != "om_root" {
		t.Fatalf("RootMessageID = %q, want om_root", event.RootMessageID)
	}
}

func TestParseMessageReceiveEventIgnoresUnmentionedAndNonText(t *testing.T) {
	unmentioned := messageEventJSON(t, eventOptions{
		EventID:      "evt_unmentioned",
		MessageID:    "om_root",
		ChatID:       "oc_test",
		SenderOpenID: "ou_client_bot",
		SelfOpenID:   "ou_other_bot",
		Text:         BuildMentionedText("ou_other_bot", "EOL1 1 start e30="),
	})
	if _, err := ParseMessageReceiveEvent(unmentioned, "ou_self_bot"); !errors.Is(err, ErrIgnoredEvent) {
		t.Fatalf("unmentioned error = %v, want ErrIgnoredEvent", err)
	}

	nonText := messageEventJSON(t, eventOptions{
		EventID:      "evt_image",
		MessageID:    "om_root",
		ChatID:       "oc_test",
		SenderOpenID: "ou_client_bot",
		SelfOpenID:   "ou_self_bot",
		MessageType:  "image",
		Text:         BuildMentionedText("ou_self_bot", "EOL1 1 start e30="),
	})
	if _, err := ParseMessageReceiveEvent(nonText, "ou_self_bot"); !errors.Is(err, ErrIgnoredEvent) {
		t.Fatalf("non-text error = %v, want ErrIgnoredEvent", err)
	}
}

func TestParseMessageEventStreamSkipsIgnoredEvents(t *testing.T) {
	valid := messageEventJSON(t, eventOptions{
		EventID:      "evt_valid",
		MessageID:    "om_root",
		ChatID:       "oc_test",
		SenderOpenID: "ou_client_bot",
		SelfOpenID:   "ou_self_bot",
		Text:         BuildMentionedText("ou_self_bot", "EOL1 1 start e30="),
	})
	ignored := messageEventJSON(t, eventOptions{
		EventID:      "evt_ignored",
		MessageID:    "om_other",
		ChatID:       "oc_test",
		SenderOpenID: "ou_client_bot",
		SelfOpenID:   "ou_other_bot",
		Text:         BuildMentionedText("ou_other_bot", "EOL1 1 start e30="),
	})

	events, err := ParseMessageEventStream(strings.NewReader(string(ignored)+"\n\n"+string(valid)+"\n"), "ou_self_bot")
	if err != nil {
		t.Fatalf("ParseMessageEventStream returned error: %v", err)
	}
	if len(events) != 1 || events[0].EventID != "evt_valid" {
		t.Fatalf("events = %#v, want only evt_valid", events)
	}
}

func newTestClient(t *testing.T, baseURL string) *Client {
	t.Helper()
	client, err := NewClient(ClientConfig{
		AppID:     "cli_test",
		AppSecret: "secret_test",
		BaseURL:   baseURL,
		Now: func() time.Time {
			return time.Date(2026, 6, 6, 12, 0, 0, 0, time.UTC)
		},
	})
	if err != nil {
		t.Fatalf("NewClient returned error: %v", err)
	}
	return client
}

func assertRootSendRequest(t *testing.T, r *http.Request, wantChatID, wantMentionOpenID, wantText string) {
	t.Helper()
	if r.Method != http.MethodPost {
		t.Fatalf("method = %s, want POST", r.Method)
	}
	var req messageRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		t.Fatalf("decode send request: %v", err)
	}
	if req.ReceiveID != wantChatID {
		t.Fatalf("receive_id = %q, want %q", req.ReceiveID, wantChatID)
	}
	if req.MsgType != MessageTypeText {
		t.Fatalf("msg_type = %q, want text", req.MsgType)
	}
	assertTextContent(t, req.Content, wantMentionOpenID, wantText)
}

func assertReplyRequest(t *testing.T, r *http.Request, wantMentionOpenID, wantText string) {
	t.Helper()
	if r.Method != http.MethodPost {
		t.Fatalf("method = %s, want POST", r.Method)
	}
	var raw map[string]json.RawMessage
	if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
		t.Fatalf("decode reply request: %v", err)
	}
	if _, ok := raw["receive_id"]; ok {
		t.Fatalf("reply request unexpectedly contains receive_id: %s", raw["receive_id"])
	}
	var req messageRequest
	remarshal, err := json.Marshal(raw)
	if err != nil {
		t.Fatalf("remarshal reply request: %v", err)
	}
	if err := json.Unmarshal(remarshal, &req); err != nil {
		t.Fatalf("decode reply request into messageRequest: %v", err)
	}
	if req.MsgType != MessageTypeText {
		t.Fatalf("msg_type = %q, want text", req.MsgType)
	}
	assertTextContent(t, req.Content, wantMentionOpenID, wantText)
}

func assertTextContent(t *testing.T, rawContent, wantMentionOpenID, wantText string) {
	t.Helper()
	var content struct {
		Text string `json:"text"`
	}
	if err := json.Unmarshal([]byte(rawContent), &content); err != nil {
		t.Fatalf("decode text content: %v", err)
	}
	want := BuildAtMention(wantMentionOpenID) + "\n" + wantText
	if content.Text != want {
		t.Fatalf("content text mismatch\nwant: %q\n got: %q", want, content.Text)
	}
}

type eventOptions struct {
	EventID       string
	MessageID     string
	RootMessageID string
	ChatID        string
	SenderOpenID  string
	SelfOpenID    string
	MessageType   string
	Text          string
}

func messageEventJSON(t *testing.T, opts eventOptions) []byte {
	t.Helper()
	messageType := opts.MessageType
	if messageType == "" {
		messageType = MessageTypeText
	}
	content, err := TextContent(opts.Text)
	if err != nil {
		t.Fatalf("TextContent returned error: %v", err)
	}
	raw, err := json.Marshal(map[string]any{
		"schema": "2.0",
		"header": map[string]any{
			"event_id":   opts.EventID,
			"event_type": MessageReceiveEventType,
		},
		"event": map[string]any{
			"sender": map[string]any{
				"sender_type": "app",
				"sender_id": map[string]any{
					"open_id": opts.SenderOpenID,
				},
			},
			"message": map[string]any{
				"message_id":   opts.MessageID,
				"root_id":      opts.RootMessageID,
				"chat_id":      opts.ChatID,
				"message_type": messageType,
				"content":      content,
				"mentions": []map[string]any{
					{
						"key":  "@_user_1",
						"name": "self",
						"id": map[string]any{
							"open_id": opts.SelfOpenID,
						},
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}
	return raw
}

func writeJSON(t *testing.T, w http.ResponseWriter, value any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(value); err != nil {
		t.Fatalf("write JSON: %v", err)
	}
}
