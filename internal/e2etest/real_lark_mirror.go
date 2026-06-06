package e2etest

import (
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/hachiwii/exec-over-lark/internal/lark"
	"github.com/pelletier/go-toml/v2"
)

type realLarkMirrorFixture struct {
	ChatID string            `toml:"chat_id"`
	Client realLarkMirrorApp `toml:"client"`
	Server realLarkMirrorApp `toml:"server"`
}

type realLarkMirrorApp struct {
	AppID     string `toml:"app_id"`
	AppSecret string `toml:"app_secret"`
	BotOpenID string `toml:"bot_open_id"`
}

func RealLarkMirrorFromFixture(path string) (MirrorOptions, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return MirrorOptions{}, fmt.Errorf("read real Lark mirror fixture: %w", err)
	}
	var fixture realLarkMirrorFixture
	if err := toml.Unmarshal(raw, &fixture); err != nil {
		return MirrorOptions{}, fmt.Errorf("parse real Lark mirror fixture: %w", err)
	}
	if strings.TrimSpace(fixture.ChatID) == "" {
		return MirrorOptions{}, fmt.Errorf("real Lark mirror fixture is missing chat_id")
	}
	client, err := newRealLarkMirrorClient("client", fixture.Client)
	if err != nil {
		return MirrorOptions{}, err
	}
	server, err := newRealLarkMirrorClient("server", fixture.Server)
	if err != nil {
		return MirrorOptions{}, err
	}
	return MirrorOptions{Client: client, Server: server}, nil
}

func newRealLarkMirrorClient(label string, app realLarkMirrorApp) (*lark.Client, error) {
	if strings.TrimSpace(app.AppID) == "" {
		return nil, fmt.Errorf("real Lark mirror fixture is missing %s app_id", label)
	}
	if strings.TrimSpace(app.AppSecret) == "" {
		return nil, fmt.Errorf("real Lark mirror fixture is missing %s app_secret", label)
	}
	return lark.NewClient(lark.ClientConfig{
		AppID:     app.AppID,
		AppSecret: app.AppSecret,
		HTTPClient: &http.Client{
			Timeout: 15 * time.Second,
		},
	})
}
