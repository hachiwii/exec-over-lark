package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	gorillaws "github.com/gorilla/websocket"
	"github.com/hachiwii/exec-over-lark/internal/bootstrap"
	"github.com/hachiwii/exec-over-lark/internal/lark"
	"github.com/hachiwii/exec-over-lark/internal/protocol"
	sdkws "github.com/larksuite/oapi-sdk-go/v3/ws"
)

func TestLarkWebSocketEventSourceHandlePayloadDispatchesMessageEvent(t *testing.T) {
	sender := &fakeBootstrapSender{}
	source := &larkWebSocketEventSource{bootstrapSender: sender}

	var events []lark.MessageEvent
	err := source.handlePayload(
		context.Background(),
		websocketProtocolMessageEventJSON(t, "evt_msg", "oc_chat", "om_root", "ou_client_bot", "ou_server_bot"),
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

func TestLarkWebSocketEventSourceHandleFrameIgnoresMalformedProtocolEvent(t *testing.T) {
	var stderr bytes.Buffer
	source := &larkWebSocketEventSource{stderr: &stderr}
	payload := websocketMessageEventJSON(t, websocketEventOptions{
		EventID:      "evt_bad_protocol",
		MessageID:    "om_bad_protocol",
		ChatID:       "oc_test",
		SenderOpenID: "ou_client",
		SelfOpenID:   "ou_self_bot",
		Text:         lark.BuildMentionedText("ou_self_bot", "EOL1 1 start !!!"),
	})

	handlerCalled := false
	status, err := callHandleFrame(t, source, payload, func(context.Context, lark.MessageEvent) error {
		handlerCalled = true
		return nil
	})
	if err != nil {
		t.Fatalf("handleFrame returned error: %v", err)
	}
	if status != http.StatusOK {
		t.Fatalf("ack status = %d, want %d", status, http.StatusOK)
	}
	if handlerCalled {
		t.Fatal("handler was called for malformed protocol event")
	}
	if !strings.Contains(stderr.String(), "ignored malformed protocol event") {
		t.Fatalf("stderr = %q, want malformed protocol log", stderr.String())
	}
}

func TestLarkWebSocketEventSourceHandleFrameIsolatesHandlerError(t *testing.T) {
	var stderr bytes.Buffer
	source := &larkWebSocketEventSource{stderr: &stderr}
	payload := websocketMessageEventJSON(t, websocketEventOptions{
		EventID:      "evt_handler_error",
		MessageID:    "om_handler_error",
		ChatID:       "oc_test",
		SenderOpenID: "ou_client",
		SelfOpenID:   "ou_self_bot",
		Text:         lark.BuildMentionedText("ou_self_bot", encodeWebsocketFrames(t, jsonFrame(t, 1, protocol.TypeStart, protocol.StartPayload{}))),
	})
	wantErr := errors.New("handler failed")
	var gotEvent lark.MessageEvent

	status, err := callHandleFrame(t, source, payload, func(_ context.Context, event lark.MessageEvent) error {
		gotEvent = event
		return wantErr
	})
	if err != nil {
		t.Fatalf("handleFrame returned error: %v", err)
	}
	if status != http.StatusOK {
		t.Fatalf("ack status = %d, want %d", status, http.StatusOK)
	}
	if gotEvent.EventID != "evt_handler_error" || gotEvent.MessageID != "om_handler_error" {
		t.Fatalf("event = %#v", gotEvent)
	}
	if !strings.Contains(stderr.String(), "ignored daemon handler error") || !strings.Contains(stderr.String(), wantErr.Error()) {
		t.Fatalf("stderr = %q, want handler error log", stderr.String())
	}
}

func callHandleFrame(t *testing.T, source *larkWebSocketEventSource, payload []byte, handler func(context.Context, lark.MessageEvent) error) (int, error) {
	t.Helper()
	serverConn, clientConn := websocketPair(t)
	frame := websocketEventFrame(payload)
	errCh := make(chan error, 1)
	go func() {
		errCh <- source.handleFrame(context.Background(), serverConn, &sync.Mutex{}, map[string]wsChunkSet{}, frame, "ou_self_bot", handler)
	}()

	status := readAckStatus(t, clientConn)
	select {
	case err := <-errCh:
		return status, err
	case <-time.After(time.Second):
		t.Fatal("handleFrame did not return")
		return 0, nil
	}
}

func websocketPair(t *testing.T) (*gorillaws.Conn, *gorillaws.Conn) {
	t.Helper()
	upgrader := gorillaws.Upgrader{}
	serverConns := make(chan *gorillaws.Conn, 1)
	upgradeErrs := make(chan error, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			upgradeErrs <- err
			return
		}
		serverConns <- conn
	}))
	t.Cleanup(server.Close)

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	clientConn, _, err := gorillaws.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		select {
		case upgradeErr := <-upgradeErrs:
			t.Fatalf("upgrade websocket: %v", upgradeErr)
		default:
			t.Fatalf("dial websocket: %v", err)
		}
	}

	var serverConn *gorillaws.Conn
	select {
	case serverConn = <-serverConns:
	case err := <-upgradeErrs:
		t.Fatalf("upgrade websocket: %v", err)
	case <-time.After(time.Second):
		t.Fatal("server websocket was not accepted")
	}
	t.Cleanup(func() {
		_ = clientConn.Close()
		_ = serverConn.Close()
	})
	return serverConn, clientConn
}

func websocketEventFrame(payload []byte) *sdkws.Frame {
	headers := sdkws.Headers{}
	headers.Add(sdkws.HeaderType, string(sdkws.MessageTypeEvent))
	return &sdkws.Frame{
		Method:  int32(sdkws.FrameTypeData),
		Headers: headers,
		Payload: payload,
	}
}

func readAckStatus(t *testing.T, conn *gorillaws.Conn) int {
	t.Helper()
	messageType, raw, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read websocket ack: %v", err)
	}
	if messageType != gorillaws.BinaryMessage {
		t.Fatalf("ack message type = %d, want binary", messageType)
	}
	var frame sdkws.Frame
	if err := frame.Unmarshal(raw); err != nil {
		t.Fatalf("decode websocket ack: %v", err)
	}
	var response sdkws.Response
	if err := json.Unmarshal(frame.Payload, &response); err != nil {
		t.Fatalf("decode ack payload: %v", err)
	}
	return response.StatusCode
}

type websocketEventOptions struct {
	EventID      string
	MessageID    string
	RootID       string
	ChatID       string
	SenderOpenID string
	SelfOpenID   string
	Text         string
}

func websocketProtocolMessageEventJSON(t *testing.T, eventID, chatID, messageID, senderOpenID, selfOpenID string) []byte {
	t.Helper()
	return websocketMessageEventJSON(t, websocketEventOptions{
		EventID:      eventID,
		MessageID:    messageID,
		ChatID:       chatID,
		SenderOpenID: senderOpenID,
		SelfOpenID:   selfOpenID,
		Text:         lark.BuildMentionedText(selfOpenID, encodeWebsocketFrames(t, protocol.Frame{Seq: 1, Type: protocol.TypeStart, Payload: []byte(`{}`)})),
	})
}

func websocketMessageEventJSON(t *testing.T, opts websocketEventOptions) []byte {
	t.Helper()
	content, err := lark.TextContent(opts.Text)
	if err != nil {
		t.Fatalf("TextContent returned error: %v", err)
	}

	raw, err := json.Marshal(map[string]any{
		"schema": "2.0",
		"header": map[string]any{
			"event_id":   opts.EventID,
			"event_type": lark.MessageReceiveEventType,
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
				"root_id":      opts.RootID,
				"chat_id":      opts.ChatID,
				"message_type": lark.MessageTypeText,
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

func encodeWebsocketFrames(t *testing.T, frames ...protocol.Frame) string {
	t.Helper()
	text, err := protocol.EncodeFrames(frames)
	if err != nil {
		t.Fatalf("EncodeFrames returned error: %v", err)
	}
	return text
}

func jsonFrame(t *testing.T, seq uint64, typ protocol.FrameType, payload any) protocol.Frame {
	t.Helper()
	frame, err := protocol.NewJSONFrame(seq, typ, payload)
	if err != nil {
		t.Fatalf("NewJSONFrame returned error: %v", err)
	}
	return frame
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
