package lark

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/hachiwii/exec-over-lark/internal/protocol"
)

const (
	DefaultBaseURL = "https://open.feishu.cn"

	tenantTokenPath = "/open-apis/auth/v3/tenant_access_token/internal"
	botInfoPath     = "/open-apis/bot/v3/info"
	sendMessagePath = "/open-apis/im/v1/messages"

	MessageReceiveEventType = "im.message.receive_v1"
	MessageTypeText         = "text"
)

var (
	ErrIgnoredEvent = errors.New("ignored lark event")
	ErrNoOpenID     = errors.New("lark bot open_id not found in response")
)

type ClientConfig struct {
	AppID            string
	AppSecret        string
	BaseURL          string
	HTTPClient       *http.Client
	Now              func() time.Time
	TokenRefreshSkew time.Duration
}

type Client struct {
	appID            string
	appSecret        string
	baseURL          *url.URL
	httpClient       *http.Client
	now              func() time.Time
	tokenRefreshSkew time.Duration

	mu             sync.Mutex
	tenantToken    string
	tokenExpiresAt time.Time
	tokenRefreshAt time.Time
	botOpenID      string
}

type RootMessage struct {
	MessageID string
}

func NewClient(cfg ClientConfig) (*Client, error) {
	appID := strings.TrimSpace(cfg.AppID)
	if appID == "" {
		return nil, errors.New("lark app_id is required")
	}
	appSecret := strings.TrimSpace(cfg.AppSecret)
	if appSecret == "" {
		return nil, errors.New("lark app_secret is required")
	}

	base := strings.TrimSpace(cfg.BaseURL)
	if base == "" {
		base = DefaultBaseURL
	}
	parsedBase, err := url.Parse(base)
	if err != nil {
		return nil, fmt.Errorf("parse lark base URL: %w", err)
	}
	if parsedBase.Scheme == "" || parsedBase.Host == "" {
		return nil, fmt.Errorf("parse lark base URL: %q is not absolute", base)
	}

	httpClient := cfg.HTTPClient
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	skew := cfg.TokenRefreshSkew
	if skew == 0 {
		skew = 5 * time.Minute
	}
	if skew < 0 {
		return nil, errors.New("token refresh skew must not be negative")
	}

	return &Client{
		appID:            appID,
		appSecret:        appSecret,
		baseURL:          parsedBase,
		httpClient:       httpClient,
		now:              now,
		tokenRefreshSkew: skew,
	}, nil
}

func (c *Client) TenantAccessToken(ctx context.Context) (string, error) {
	return c.tenantAccessToken(ctx, false)
}

func (c *Client) BotOpenID(ctx context.Context) (string, error) {
	c.mu.Lock()
	if c.botOpenID != "" {
		openID := c.botOpenID
		c.mu.Unlock()
		return openID, nil
	}
	c.mu.Unlock()

	var out botInfoResponse
	if err := c.doAuthorizedJSON(ctx, http.MethodGet, botInfoPath, nil, nil, &out); err != nil {
		return "", err
	}

	openID := firstNonEmpty(out.OpenID, out.Bot.OpenID, out.Data.OpenID, out.Data.Bot.OpenID)
	if openID == "" {
		return "", ErrNoOpenID
	}

	c.mu.Lock()
	c.botOpenID = openID
	c.mu.Unlock()
	return openID, nil
}

func (c *Client) SendRootMessage(ctx context.Context, chatID, mentionOpenID, text string) (RootMessage, error) {
	chatID = strings.TrimSpace(chatID)
	if chatID == "" {
		return RootMessage{}, errors.New("chatID is required")
	}
	mentionOpenID = strings.TrimSpace(mentionOpenID)
	if mentionOpenID == "" {
		return RootMessage{}, errors.New("mentionOpenID is required")
	}

	content, err := TextContent(BuildMentionedText(mentionOpenID, text))
	if err != nil {
		return RootMessage{}, err
	}

	query := url.Values{"receive_id_type": []string{"chat_id"}}
	req := messageRequest{
		ReceiveID: chatID,
		MsgType:   MessageTypeText,
		Content:   content,
	}
	var out messageResponse
	if err := c.doAuthorizedJSON(ctx, http.MethodPost, sendMessagePath, query, req, &out); err != nil {
		return RootMessage{}, err
	}

	messageID := firstNonEmpty(out.MessageID, out.RootID)
	if messageID == "" {
		return RootMessage{}, errors.New("lark send root message response missing message_id")
	}
	return RootMessage{MessageID: messageID}, nil
}

func (c *Client) ReplyRootMessage(ctx context.Context, chatID, rootMessageID, mentionOpenID, text string) (string, error) {
	if strings.TrimSpace(chatID) == "" {
		return "", errors.New("chatID is required")
	}
	rootMessageID = strings.TrimSpace(rootMessageID)
	if rootMessageID == "" {
		return "", errors.New("rootMessageID is required")
	}
	mentionOpenID = strings.TrimSpace(mentionOpenID)
	if mentionOpenID == "" {
		return "", errors.New("mentionOpenID is required")
	}

	content, err := TextContent(BuildMentionedText(mentionOpenID, text))
	if err != nil {
		return "", err
	}

	req := messageRequest{
		MsgType: MessageTypeText,
		Content: content,
	}
	replyPath := sendMessagePath + "/" + url.PathEscape(rootMessageID) + "/reply"
	var out messageResponse
	if err := c.doAuthorizedJSON(ctx, http.MethodPost, replyPath, nil, req, &out); err != nil {
		return "", err
	}

	messageID := firstNonEmpty(out.MessageID, out.RootID)
	if messageID == "" {
		return "", errors.New("lark reply response missing message_id")
	}
	return messageID, nil
}

func BuildAtMention(openID string) string {
	return fmt.Sprintf(`<at user_id="%s"></at>`, html.EscapeString(strings.TrimSpace(openID)))
}

func BuildMentionedText(mentionOpenID, text string) string {
	mentionOpenID = strings.TrimSpace(mentionOpenID)
	if mentionOpenID == "" {
		return text
	}
	if strings.TrimSpace(text) == "" {
		return BuildAtMention(mentionOpenID)
	}
	return BuildAtMention(mentionOpenID) + "\n" + text
}

func TextContent(text string) (string, error) {
	raw, err := json.Marshal(struct {
		Text string `json:"text"`
	}{Text: text})
	if err != nil {
		return "", fmt.Errorf("marshal lark text content: %w", err)
	}
	return string(raw), nil
}

func (c *Client) tenantAccessToken(ctx context.Context, force bool) (string, error) {
	c.mu.Lock()
	if !force && c.tenantToken != "" && c.now().Before(c.tokenRefreshAt) {
		token := c.tenantToken
		c.mu.Unlock()
		return token, nil
	}
	c.mu.Unlock()

	token, expire, err := c.fetchTenantAccessToken(ctx)
	if err != nil {
		return "", err
	}

	now := c.now()
	expireDuration := time.Duration(expire) * time.Second
	expiresAt := now.Add(expireDuration)
	refreshAt := expiresAt.Add(-c.tokenRefreshSkew)
	if !refreshAt.After(now) {
		refreshAt = expiresAt
	}

	c.mu.Lock()
	c.tenantToken = token
	c.tokenExpiresAt = expiresAt
	c.tokenRefreshAt = refreshAt
	c.mu.Unlock()
	return token, nil
}

func (c *Client) fetchTenantAccessToken(ctx context.Context) (string, int, error) {
	reqBody := tenantTokenRequest{
		AppID:     c.appID,
		AppSecret: c.appSecret,
	}
	respBody, status, err := c.doRawJSON(ctx, http.MethodPost, tenantTokenPath, nil, reqBody, "")
	if err != nil {
		return "", 0, err
	}
	if status < 200 || status >= 300 {
		return "", 0, &apiError{Status: status, Path: tenantTokenPath}
	}

	var out tenantTokenResponse
	if err := json.Unmarshal(respBody, &out); err != nil {
		return "", 0, fmt.Errorf("decode lark tenant token response: %w", err)
	}
	if out.Code != 0 {
		return "", 0, &apiError{Code: out.Code, Msg: out.Msg, Path: tenantTokenPath}
	}
	token := firstNonEmpty(out.TenantAccessToken, out.Data.TenantAccessToken)
	if token == "" {
		return "", 0, errors.New("lark tenant token response missing tenant_access_token")
	}
	expire := firstPositive(out.Expire, out.Data.Expire, 7200)
	return token, expire, nil
}

func (c *Client) doAuthorizedJSON(ctx context.Context, method, endpoint string, query url.Values, body any, out any) error {
	var lastErr error
	for attempt := 0; attempt < 2; attempt++ {
		token, err := c.tenantAccessToken(ctx, attempt > 0)
		if err != nil {
			return err
		}
		err = c.doJSONWithToken(ctx, method, endpoint, query, body, token, out)
		if err == nil {
			return nil
		}
		lastErr = err
		var apiErr *apiError
		if attempt == 0 && errors.As(err, &apiErr) && apiErr.TokenExpired() {
			c.invalidateTenantToken(token)
			continue
		}
		return err
	}
	return lastErr
}

func (c *Client) doJSONWithToken(ctx context.Context, method, endpoint string, query url.Values, body any, token string, out any) error {
	respBody, status, err := c.doRawJSON(ctx, method, endpoint, query, body, token)
	if err != nil {
		return err
	}
	if status == http.StatusUnauthorized {
		return apiErrorFromResponse(endpoint, status, respBody)
	}
	if status < 200 || status >= 300 {
		return apiErrorFromResponse(endpoint, status, respBody)
	}

	var envelope larkEnvelope
	if err := json.Unmarshal(respBody, &envelope); err != nil {
		return fmt.Errorf("decode lark API response for %s: %w", endpoint, err)
	}
	if envelope.Code != 0 {
		return &apiError{Code: envelope.Code, Msg: envelope.Msg, Path: endpoint}
	}
	if out == nil {
		return nil
	}
	if len(envelope.Data) > 0 && string(envelope.Data) != "null" {
		if err := json.Unmarshal(envelope.Data, out); err != nil {
			return fmt.Errorf("decode lark API data for %s: %w", endpoint, err)
		}
		return nil
	}
	if err := json.Unmarshal(respBody, out); err != nil {
		return fmt.Errorf("decode lark API body for %s: %w", endpoint, err)
	}
	return nil
}

func (c *Client) doRawJSON(ctx context.Context, method, endpoint string, query url.Values, body any, token string) ([]byte, int, error) {
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return nil, 0, fmt.Errorf("marshal lark request for %s: %w", endpoint, err)
		}
		reader = bytes.NewReader(raw)
	}

	req, err := http.NewRequestWithContext(ctx, method, c.endpointURL(endpoint, query), reader)
	if err != nil {
		return nil, 0, fmt.Errorf("build lark request for %s: %w", endpoint, err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json; charset=utf-8")
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("send lark request for %s: %w", endpoint, err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, resp.StatusCode, fmt.Errorf("read lark response for %s: %w", endpoint, err)
	}
	return respBody, resp.StatusCode, nil
}

func apiErrorFromResponse(endpoint string, status int, respBody []byte) error {
	err := &apiError{Status: status, Path: endpoint}
	var envelope larkEnvelope
	if json.Unmarshal(respBody, &envelope) == nil {
		err.Code = envelope.Code
		err.Msg = envelope.Msg
	}
	return err
}

func (c *Client) endpointURL(endpoint string, query url.Values) string {
	u := *c.baseURL
	basePath := strings.TrimRight(u.Path, "/")
	endpointPath := "/" + strings.TrimLeft(endpoint, "/")
	u.Path = basePath + endpointPath
	if query != nil {
		u.RawQuery = query.Encode()
	}
	return u.String()
}

func (c *Client) invalidateTenantToken(token string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.tenantToken == token {
		c.tenantToken = ""
		c.tokenExpiresAt = time.Time{}
		c.tokenRefreshAt = time.Time{}
	}
}

type tenantTokenRequest struct {
	AppID     string `json:"app_id"`
	AppSecret string `json:"app_secret"`
}

type tenantTokenResponse struct {
	Code              int    `json:"code"`
	Msg               string `json:"msg"`
	TenantAccessToken string `json:"tenant_access_token"`
	Expire            int    `json:"expire"`
	Data              struct {
		TenantAccessToken string `json:"tenant_access_token"`
		Expire            int    `json:"expire"`
	} `json:"data"`
}

type larkEnvelope struct {
	Code int             `json:"code"`
	Msg  string          `json:"msg"`
	Data json.RawMessage `json:"data"`
}

type apiError struct {
	Status int
	Code   int
	Msg    string
	Path   string
}

func (e *apiError) Error() string {
	if e.Status != 0 {
		if e.Msg != "" {
			return fmt.Sprintf("lark API %s returned HTTP status %d, code %d: %s", e.Path, e.Status, e.Code, e.Msg)
		}
		if e.Code != 0 {
			return fmt.Sprintf("lark API %s returned HTTP status %d, code %d", e.Path, e.Status, e.Code)
		}
		return fmt.Sprintf("lark API %s returned HTTP status %d", e.Path, e.Status)
	}
	if e.Msg != "" {
		return fmt.Sprintf("lark API %s returned code %d: %s", e.Path, e.Code, e.Msg)
	}
	return fmt.Sprintf("lark API %s returned code %d", e.Path, e.Code)
}

func (e *apiError) TokenExpired() bool {
	if e.Status == http.StatusUnauthorized {
		return true
	}
	switch e.Code {
	case 99991661, 99991663, 99991664, 99991665, 99991668:
		return true
	}
	msg := strings.ToLower(e.Msg)
	return strings.Contains(msg, "token") &&
		(strings.Contains(msg, "expire") || strings.Contains(msg, "invalid"))
}

type messageRequest struct {
	ReceiveID string `json:"receive_id,omitempty"`
	MsgType   string `json:"msg_type"`
	Content   string `json:"content"`
	RootID    string `json:"root_id,omitempty"`
	ParentID  string `json:"parent_id,omitempty"`
}

type messageResponse struct {
	MessageID string `json:"message_id"`
	RootID    string `json:"root_id"`
}

type botInfoResponse struct {
	OpenID string `json:"open_id"`
	Bot    struct {
		OpenID string `json:"open_id"`
	} `json:"bot"`
	Data struct {
		OpenID string `json:"open_id"`
		Bot    struct {
			OpenID string `json:"open_id"`
		} `json:"bot"`
	} `json:"data"`
}

type MessageEvent struct {
	EventID       string
	EventType     string
	MessageID     string
	RootMessageID string
	ChatID        string
	SenderOpenID  string
	SenderType    string
	MessageType   string
	Text          string
	Mentions      []Mention
	Frames        []protocol.Frame
}

type Mention struct {
	Key     string
	Name    string
	OpenID  string
	UserID  string
	UnionID string
}

func ParseEvent(data []byte, selfBotOpenID string) (MessageEvent, error) {
	return ParseMessageReceiveEvent(data, selfBotOpenID)
}

func ParseMessageReceiveEvent(data []byte, selfBotOpenID string) (MessageEvent, error) {
	var envelope eventEnvelope
	if err := json.Unmarshal(data, &envelope); err != nil {
		return MessageEvent{}, fmt.Errorf("decode lark event: %w", err)
	}
	if envelope.Type == "url_verification" || envelope.Type == "challenge" {
		return MessageEvent{}, ErrIgnoredEvent
	}
	if envelope.Header.EventType != "" && envelope.Header.EventType != MessageReceiveEventType {
		return MessageEvent{}, ErrIgnoredEvent
	}
	if envelope.Event.Message.MessageType != MessageTypeText {
		return MessageEvent{}, ErrIgnoredEvent
	}

	mentions := normalizeMentions(envelope.Event.Message.Mentions)
	if strings.TrimSpace(selfBotOpenID) != "" && !mentionsContainOpenID(mentions, selfBotOpenID) {
		return MessageEvent{}, ErrIgnoredEvent
	}

	text, err := textFromRawContent(envelope.Event.Message.Content)
	if err != nil {
		return MessageEvent{}, err
	}
	protocolText := ExtractProtocolText(text)
	if protocolText == "" {
		return MessageEvent{}, ErrIgnoredEvent
	}

	frames, err := protocol.DecodeFrames(protocolText)
	if err != nil {
		return MessageEvent{}, err
	}

	msg := envelope.Event.Message
	sender := envelope.Event.Sender
	return MessageEvent{
		EventID:       envelope.Header.EventID,
		EventType:     firstNonEmpty(envelope.Header.EventType, MessageReceiveEventType),
		MessageID:     msg.MessageID,
		RootMessageID: firstNonEmpty(msg.RootID, msg.ParentID, msg.MessageID),
		ChatID:        msg.ChatID,
		SenderOpenID:  sender.SenderID.OpenID,
		SenderType:    sender.SenderType,
		MessageType:   msg.MessageType,
		Text:          text,
		Mentions:      mentions,
		Frames:        frames,
	}, nil
}

func ParseMessageEventStream(r io.Reader, selfBotOpenID string) ([]MessageEvent, error) {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	var events []MessageEvent
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		event, err := ParseMessageReceiveEvent([]byte(line), selfBotOpenID)
		if errors.Is(err, ErrIgnoredEvent) {
			continue
		}
		if err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read lark event stream: %w", err)
	}
	return events, nil
}

func ExtractProtocolText(text string) string {
	text = stripLeadingAtMentions(text)
	lines := strings.Split(text, "\n")
	start := -1
	for i, line := range lines {
		if strings.HasPrefix(strings.TrimSpace(line), "EOL1 ") {
			start = i
			break
		}
	}
	if start == -1 {
		return ""
	}
	return strings.Join(lines[start:], "\n")
}

func stripLeadingAtMentions(text string) string {
	trimmed := strings.TrimLeftFunc(text, unicode.IsSpace)
	for strings.HasPrefix(trimmed, "<at ") {
		end := strings.Index(trimmed, "</at>")
		if end < 0 {
			return trimmed
		}
		trimmed = strings.TrimLeftFunc(trimmed[end+len("</at>"):], unicode.IsSpace)
	}
	return trimmed
}

func textFromRawContent(raw json.RawMessage) (string, error) {
	if len(raw) == 0 {
		return "", nil
	}

	var encoded string
	if err := json.Unmarshal(raw, &encoded); err == nil {
		return textFromContentBytes([]byte(encoded))
	}
	return textFromContentBytes(raw)
}

func textFromContentBytes(raw []byte) (string, error) {
	var content struct {
		Text string `json:"text"`
	}
	if err := json.Unmarshal(raw, &content); err == nil {
		return content.Text, nil
	}
	return string(raw), nil
}

type eventEnvelope struct {
	Type   string      `json:"type"`
	Header eventHeader `json:"header"`
	Event  eventBody   `json:"event"`
}

type eventHeader struct {
	EventID   string `json:"event_id"`
	EventType string `json:"event_type"`
}

type eventBody struct {
	Sender  eventSender  `json:"sender"`
	Message eventMessage `json:"message"`
}

type eventSender struct {
	SenderID   Identifier `json:"sender_id"`
	SenderType string     `json:"sender_type"`
}

type eventMessage struct {
	MessageID   string          `json:"message_id"`
	RootID      string          `json:"root_id"`
	ParentID    string          `json:"parent_id"`
	ChatID      string          `json:"chat_id"`
	MessageType string          `json:"message_type"`
	Content     json.RawMessage `json:"content"`
	Mentions    []rawMention    `json:"mentions"`
}

type rawMention struct {
	Key     string     `json:"key"`
	Name    string     `json:"name"`
	ID      Identifier `json:"id"`
	OpenID  string     `json:"open_id"`
	UserID  string     `json:"user_id"`
	UnionID string     `json:"union_id"`
}

type Identifier struct {
	OpenID  string
	UserID  string
	UnionID string
}

func (id *Identifier) UnmarshalJSON(data []byte) error {
	var value string
	if err := json.Unmarshal(data, &value); err == nil {
		id.OpenID = value
		return nil
	}
	var obj struct {
		OpenID  string `json:"open_id"`
		UserID  string `json:"user_id"`
		UnionID string `json:"union_id"`
	}
	if err := json.Unmarshal(data, &obj); err != nil {
		return err
	}
	id.OpenID = obj.OpenID
	id.UserID = obj.UserID
	id.UnionID = obj.UnionID
	return nil
}

func normalizeMentions(raw []rawMention) []Mention {
	mentions := make([]Mention, 0, len(raw))
	for _, mention := range raw {
		mentions = append(mentions, Mention{
			Key:     mention.Key,
			Name:    mention.Name,
			OpenID:  firstNonEmpty(mention.ID.OpenID, mention.OpenID),
			UserID:  firstNonEmpty(mention.ID.UserID, mention.UserID),
			UnionID: firstNonEmpty(mention.ID.UnionID, mention.UnionID),
		})
	}
	return mentions
}

func mentionsContainOpenID(mentions []Mention, openID string) bool {
	openID = strings.TrimSpace(openID)
	for _, mention := range mentions {
		if mention.OpenID == openID {
			return true
		}
	}
	return false
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func firstPositive(values ...int) int {
	for _, value := range values {
		if value > 0 {
			return value
		}
	}
	return 0
}
