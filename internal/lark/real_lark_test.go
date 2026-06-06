package lark

import (
	"context"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/hachiwii/exec-over-lark/internal/protocol"
	"github.com/pelletier/go-toml/v2"
)

const realLarkFixtureEnv = "ELARK_REAL_LARK_FIXTURE"

type realLarkFixture struct {
	ChatID string         `toml:"chat_id"`
	Client realLarkBotApp `toml:"client"`
	Server realLarkBotApp `toml:"server"`
}

type realLarkBotApp struct {
	AppID     string `toml:"app_id"`
	AppSecret string `toml:"app_secret"`
	BotOpenID string `toml:"bot_open_id"`
}

func TestRealLarkBotMessaging(t *testing.T) {
	fixturePath := strings.TrimSpace(os.Getenv(realLarkFixtureEnv))
	if fixturePath == "" {
		t.Skipf("set %s to run real Lark-backed messaging test", realLarkFixtureEnv)
	}

	fixture := loadRealLarkFixture(t, fixturePath)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	clientBot := newRealLarkTestClient(t, fixture.Client)
	serverBot := newRealLarkTestClient(t, fixture.Server)

	clientOpenID := mustResolveFixtureBotOpenID(t, ctx, "client", clientBot, fixture.Client.BotOpenID)
	serverOpenID := mustResolveFixtureBotOpenID(t, ctx, "server", serverBot, fixture.Server.BotOpenID)

	startFrame, err := protocol.NewJSONFrame(1, protocol.TypeStart, protocol.StartPayload{
		Cmd: "printf exec-over-lark-real-lark-test",
		Heartbeat: protocol.HeartbeatConfig{
			Interval: "10s",
			Timeout:  "30s",
		},
	})
	if err != nil {
		t.Fatalf("build start frame: %v", err)
	}
	startText, err := protocol.EncodeFrames([]protocol.Frame{startFrame})
	if err != nil {
		t.Fatalf("encode start frame: %v", err)
	}

	root, err := clientBot.SendRootMessage(ctx, fixture.ChatID, serverOpenID, startText)
	if err != nil {
		t.Fatalf("send client root message: %v", err)
	}
	if strings.TrimSpace(root.MessageID) == "" {
		t.Fatal("send client root message returned an empty message id")
	}

	ackFrame, err := protocol.NewJSONFrame(1, protocol.TypeStartAck, protocol.StartAckPayload{
		Heartbeat: protocol.HeartbeatConfig{
			Interval: "10s",
			Timeout:  "30s",
		},
	})
	if err != nil {
		t.Fatalf("build start_ack frame: %v", err)
	}
	ackText, err := protocol.EncodeFrames([]protocol.Frame{ackFrame})
	if err != nil {
		t.Fatalf("encode start_ack frame: %v", err)
	}

	replyID, err := serverBot.ReplyRootMessage(ctx, fixture.ChatID, root.MessageID, clientOpenID, ackText)
	if err != nil {
		t.Fatalf("send server reply message: %v", err)
	}
	if strings.TrimSpace(replyID) == "" {
		t.Fatal("send server reply message returned an empty message id")
	}
}

func loadRealLarkFixture(t *testing.T, path string) realLarkFixture {
	t.Helper()

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read real Lark fixture: %v", err)
	}
	var fixture realLarkFixture
	if err := toml.Unmarshal(raw, &fixture); err != nil {
		t.Fatalf("parse real Lark fixture: %v", err)
	}
	if strings.TrimSpace(fixture.ChatID) == "" {
		t.Fatal("real Lark fixture is missing chat_id")
	}
	requireRealLarkBotApp(t, "client", fixture.Client)
	requireRealLarkBotApp(t, "server", fixture.Server)
	return fixture
}

func requireRealLarkBotApp(t *testing.T, label string, app realLarkBotApp) {
	t.Helper()
	if strings.TrimSpace(app.AppID) == "" {
		t.Fatalf("real Lark fixture is missing %s app_id", label)
	}
	if strings.TrimSpace(app.AppSecret) == "" {
		t.Fatalf("real Lark fixture is missing %s app_secret", label)
	}
	if strings.TrimSpace(app.BotOpenID) == "" {
		t.Fatalf("real Lark fixture is missing %s bot_open_id", label)
	}
}

func newRealLarkTestClient(t *testing.T, app realLarkBotApp) *Client {
	t.Helper()
	client, err := NewClient(ClientConfig{
		AppID:     app.AppID,
		AppSecret: app.AppSecret,
		HTTPClient: &http.Client{
			Timeout: 15 * time.Second,
		},
	})
	if err != nil {
		t.Fatalf("create real Lark client: %v", err)
	}
	return client
}

func mustResolveFixtureBotOpenID(t *testing.T, ctx context.Context, label string, client *Client, wantOpenID string) string {
	t.Helper()
	openID, err := client.BotOpenID(ctx)
	if err != nil {
		t.Fatalf("resolve %s bot open_id: %v", label, err)
	}
	if strings.TrimSpace(openID) == "" {
		t.Fatalf("resolved %s bot open_id is empty", label)
	}
	if want := strings.TrimSpace(wantOpenID); want != "" && openID != want {
		t.Logf("resolved %s bot open_id differs from fixture; using fixture open_id for chat mention", label)
		return want
	}
	return openID
}
