package session

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hachiwii/exec-over-lark/internal/outbound"
	"github.com/hachiwii/exec-over-lark/internal/protocol"
)

const (
	DefaultHeartbeatInterval  = 10 * time.Second
	DefaultHeartbeatTimeout   = 30 * time.Second
	DefaultSequenceGapTimeout = 30 * time.Second
	DefaultMaxRemoteSessions  = 8
)

var (
	ErrInvalidSession     = errors.New("invalid session")
	ErrSessionNotFound    = errors.New("session not found")
	ErrSessionClosed      = errors.New("session closed")
	ErrUnauthorizedPeer   = errors.New("unauthorized session peer")
	ErrUnexpectedFrame    = errors.New("unexpected frame for session direction")
	ErrNoOutboundManager  = errors.New("session outbound manager is nil")
	ErrRemoteSessionLimit = errors.New("remote session limit reached")
)

var durationTokenRE = regexp.MustCompile(`([0-9]+(?:\.[0-9]+)?)(ms|s|m|h|d)`)

type Clock interface {
	Now() time.Time
}

type Outbound interface {
	RegisterConnection(outbound.RegisterConnectionRequest) error
	Enqueue(ctx context.Context, connID string, typ protocol.FrameType, payload []byte) error
	EnqueueJSON(ctx context.Context, connID string, typ protocol.FrameType, payload any) error
	MarkCloseAfterDrained(connID string)
	DropConnection(connID string)
}

type Option func(*Manager)

type Manager struct {
	mu sync.Mutex

	clock    Clock
	outbound Outbound

	heartbeatInterval  time.Duration
	heartbeatTimeout   time.Duration
	sequenceGapTimeout time.Duration
	maxRemoteSessions  int

	localByConn    map[string]*localSession
	localByRequest map[string]string
	remoteByConn   map[string]*remoteSession
}

type realClock struct{}

func (realClock) Now() time.Time {
	return time.Now()
}

func New(opts ...Option) *Manager {
	m := &Manager{
		clock:              realClock{},
		heartbeatInterval:  DefaultHeartbeatInterval,
		heartbeatTimeout:   DefaultHeartbeatTimeout,
		sequenceGapTimeout: DefaultSequenceGapTimeout,
		maxRemoteSessions:  DefaultMaxRemoteSessions,
		localByConn:        make(map[string]*localSession),
		localByRequest:     make(map[string]string),
		remoteByConn:       make(map[string]*remoteSession),
	}
	for _, opt := range opts {
		opt(m)
	}
	if m.clock == nil {
		m.clock = realClock{}
	}
	if m.heartbeatInterval <= 0 {
		m.heartbeatInterval = DefaultHeartbeatInterval
	}
	if m.heartbeatTimeout <= 0 {
		m.heartbeatTimeout = DefaultHeartbeatTimeout
	}
	if m.sequenceGapTimeout <= 0 {
		m.sequenceGapTimeout = DefaultSequenceGapTimeout
	}
	if m.maxRemoteSessions <= 0 {
		m.maxRemoteSessions = DefaultMaxRemoteSessions
	}
	if m.localByConn == nil {
		m.localByConn = make(map[string]*localSession)
	}
	if m.localByRequest == nil {
		m.localByRequest = make(map[string]string)
	}
	if m.remoteByConn == nil {
		m.remoteByConn = make(map[string]*remoteSession)
	}
	return m
}

func WithClock(clock Clock) Option {
	return func(m *Manager) {
		if clock != nil {
			m.clock = clock
		}
	}
}

func WithOutbound(out Outbound) Option {
	return func(m *Manager) {
		m.outbound = out
	}
}

func WithHeartbeatInterval(interval time.Duration) Option {
	return func(m *Manager) {
		m.heartbeatInterval = interval
	}
}

func WithHeartbeatTimeout(timeout time.Duration) Option {
	return func(m *Manager) {
		m.heartbeatTimeout = timeout
	}
}

func WithSequenceGapTimeout(timeout time.Duration) Option {
	return func(m *Manager) {
		m.sequenceGapTimeout = timeout
	}
}

func WithMaxRemoteSessions(max int) Option {
	return func(m *Manager) {
		m.maxRemoteSessions = max
	}
}

type LocalStart struct {
	RequestID        string
	Host             string
	ConnID           string
	RootMessageID    string
	RootMessageURL   string
	ChatID           string
	PeerBotOpenID    string
	NextSendSeq      uint64
	HeartbeatTimeout time.Duration
}

type RemoteStart struct {
	ConnID       string
	ChatID       string
	SenderOpenID string
	Start        protocol.StartPayload
	Receiver     RemoteReceiver
}

type InboundMessage struct {
	ConnID        string
	RootMessageID string
	MessageID     string
	ChatID        string
	SenderOpenID  string
	IsRoot        bool
	Frames        []protocol.Frame
}

type LocalEventType string

const (
	LocalEventStartAck LocalEventType = "start_ack"
	LocalEventStdout   LocalEventType = "stdout"
	LocalEventStderr   LocalEventType = "stderr"
	LocalEventExit     LocalEventType = "exit"
	LocalEventError    LocalEventType = "error"
	LocalEventClose    LocalEventType = "close"
)

type LocalEvent struct {
	Type      LocalEventType
	ConnID    string
	RequestID string
	Bytes     []byte
	Code      int
	Message   string
	Detail    string
	Err       error
}

type Subscriber interface {
	Deliver(ctx context.Context, event LocalEvent) error
}

type RemoteEventType string

const (
	RemoteEventStart              RemoteEventType = "start"
	RemoteEventStdin              RemoteEventType = "stdin"
	RemoteEventResize             RemoteEventType = "resize"
	RemoteEventSignal             RemoteEventType = "signal"
	RemoteEventClose              RemoteEventType = "close"
	RemoteEventError              RemoteEventType = "error"
	RemoteEventSequenceGapTimeout RemoteEventType = "sequence_gap_timeout"
	RemoteEventPeerTimeout        RemoteEventType = "peer_timeout"
)

type RemoteEvent struct {
	Type    RemoteEventType
	ConnID  string
	Start   protocol.StartPayload
	Bytes   []byte
	Rows    int
	Cols    int
	Name    string
	Reason  string
	Message string
	Detail  string
	Err     error
}

type RemoteReceiver interface {
	Deliver(ctx context.Context, event RemoteEvent) error
}

type Snapshot struct {
	Role                   string
	ConnID                 string
	RootMessageID          string
	RootMessageURL         string
	Host                   string
	RequestIDs             []string
	ChatID                 string
	PeerOpenID             string
	StartedAt              time.Time
	LastLocalSendAt        time.Time
	LastPeerMessageAt      time.Time
	LocalHeartbeatInterval time.Duration
	LocalHeartbeatTimeout  time.Duration
	PeerHeartbeatTimeout   time.Duration
	RXNextExpectedSeq      uint64
	RXPendingSeqs          []uint64
	RXGapStartedAt         time.Time
	Closed                 bool
	ClosedAt               time.Time
}

type localSession struct {
	base sessionState

	host           string
	rootMessageURL string
	subscribers    map[string]Subscriber
}

type remoteSession struct {
	base     sessionState
	start    protocol.StartPayload
	receiver RemoteReceiver
}

type sessionState struct {
	connID        string
	rootMessageID string
	chatID        string
	peerOpenID    string
	startedAt     time.Time
	closedAt      time.Time
	closed        bool

	lastLocalSendAt   time.Time
	lastPeerMessageAt time.Time

	localHeartbeatInterval time.Duration
	localHeartbeatTimeout  time.Duration
	peerHeartbeatTimeout   time.Duration

	rx *protocol.RecvWindow
}

func ConnID(rootMessageID, messageID string) string {
	if strings.TrimSpace(rootMessageID) != "" {
		return rootMessageID
	}
	return messageID
}

func (m *Manager) HeartbeatConfig() protocol.HeartbeatConfig {
	return protocol.HeartbeatConfig{
		Interval: m.heartbeatInterval.String(),
		Timeout:  m.heartbeatTimeout.String(),
	}
}

func (m *Manager) RegisterLocal(start LocalStart, subscriber Subscriber) error {
	now := m.clock.Now()
	connID := firstNonEmpty(start.ConnID, start.RootMessageID)
	if strings.TrimSpace(connID) == "" {
		return fmt.Errorf("%w: conn_id/root_message_id is required", ErrInvalidSession)
	}
	if strings.TrimSpace(start.RequestID) == "" {
		return fmt.Errorf("%w: request_id is required", ErrInvalidSession)
	}
	if strings.TrimSpace(start.ChatID) == "" {
		return fmt.Errorf("%w: chat_id is required", ErrInvalidSession)
	}
	if strings.TrimSpace(start.PeerBotOpenID) == "" {
		return fmt.Errorf("%w: peer_bot_open_id is required", ErrInvalidSession)
	}

	nextSeq := start.NextSendSeq
	if nextSeq == 0 {
		nextSeq = 2
	}
	peerTimeout := start.HeartbeatTimeout
	if peerTimeout <= 0 {
		peerTimeout = m.heartbeatTimeout
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if existingConn, ok := m.localByRequest[start.RequestID]; ok && existingConn != connID {
		return fmt.Errorf("%w: request_id %q is already mapped to conn_id %q", ErrInvalidSession, start.RequestID, existingConn)
	}

	sess, ok := m.localByConn[connID]
	if !ok {
		if m.outbound == nil {
			return ErrNoOutboundManager
		}
		sess = &localSession{
			base: sessionState{
				connID:                 connID,
				rootMessageID:          connID,
				chatID:                 start.ChatID,
				peerOpenID:             start.PeerBotOpenID,
				startedAt:              now,
				lastLocalSendAt:        now,
				lastPeerMessageAt:      now,
				localHeartbeatInterval: m.heartbeatInterval,
				localHeartbeatTimeout:  m.heartbeatTimeout,
				peerHeartbeatTimeout:   peerTimeout,
				rx:                     protocol.NewRecvWindow(m.sequenceGapTimeout, protocol.WithClock(m.clock)),
			},
			host:           start.Host,
			rootMessageURL: start.RootMessageURL,
			subscribers:    make(map[string]Subscriber),
		}
		if err := m.outbound.RegisterConnection(outbound.RegisterConnectionRequest{
			ConnID:            connID,
			Role:              outbound.RoleLocal,
			Target:            outbound.Target{ChatID: start.ChatID, RootMessageID: connID, MentionOpenID: start.PeerBotOpenID},
			NextSeq:           nextSeq,
			HeartbeatInterval: m.heartbeatInterval,
			OnDrop: func(ctx context.Context, reason outbound.DropReason) {
				m.handleLocalOutboundDrop(ctx, connID, reason)
			},
			OnDrained: func(ctx context.Context) {
				m.CloseLocalConn(connID)
			},
		}); err != nil {
			return err
		}
		m.localByConn[connID] = sess
	} else if err := sess.base.ensurePeer(start.ChatID, start.PeerBotOpenID); err != nil {
		return err
	}

	if sess.base.closed {
		return fmt.Errorf("%w: %s", ErrSessionClosed, connID)
	}
	if subscriber != nil {
		sess.subscribers[start.RequestID] = subscriber
	}
	m.localByRequest[start.RequestID] = connID
	return nil
}

func (m *Manager) SubscribeLocal(connID, requestID string, subscriber Subscriber) error {
	if strings.TrimSpace(connID) == "" || strings.TrimSpace(requestID) == "" {
		return fmt.Errorf("%w: conn_id and request_id are required", ErrInvalidSession)
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	sess, ok := m.localByConn[connID]
	if !ok || sess.base.closed {
		return fmt.Errorf("%w: %s", ErrSessionNotFound, connID)
	}
	if subscriber != nil {
		sess.subscribers[requestID] = subscriber
	}
	m.localByRequest[requestID] = connID
	return nil
}

func (m *Manager) UnsubscribeLocal(requestID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.unsubscribeLocalLocked(requestID)
}

func (m *Manager) SendLocalStdin(ctx context.Context, requestID string, data []byte) error {
	return m.sendLocalFrame(ctx, requestID, protocol.TypeStdin, append([]byte(nil), data...), false)
}

func (m *Manager) SendLocalResize(ctx context.Context, requestID string, rows, cols int) error {
	return m.sendLocalJSONFrame(ctx, requestID, protocol.TypeResize, protocol.ResizePayload{Rows: rows, Cols: cols}, false)
}

func (m *Manager) SendLocalSignal(ctx context.Context, requestID, name string) error {
	return m.sendLocalJSONFrame(ctx, requestID, protocol.TypeSignal, protocol.SignalPayload{Name: name}, false)
}

func (m *Manager) CloseLocal(ctx context.Context, requestID, reason string) error {
	err := m.sendLocalJSONFrame(ctx, requestID, protocol.TypeClose, protocol.ClosePayload{Reason: reason}, true)
	return err
}

func (m *Manager) ReceiveLocal(ctx context.Context, msg InboundMessage) error {
	connID := normalizeConnID(msg)
	if strings.TrimSpace(connID) == "" {
		return fmt.Errorf("%w: conn_id is required", ErrInvalidSession)
	}
	now := m.clock.Now()

	m.mu.Lock()
	sess, ok := m.localByConn[connID]
	if !ok || sess.base.closed {
		m.mu.Unlock()
		return fmt.Errorf("%w: %s", ErrSessionNotFound, connID)
	}
	if err := sess.base.ensurePeer(msg.ChatID, ""); err != nil {
		m.mu.Unlock()
		return err
	}
	sess.base.lastPeerMessageAt = now

	for _, frame := range msg.Frames {
		result, err := sess.base.rx.OfferAt(frame, now)
		if err != nil {
			m.handleLocalSequenceErrorLocked(ctx, sess, err)
			m.mu.Unlock()
			return err
		}
		for _, delivered := range result.Delivered {
			if err := m.handleLocalFrameLocked(ctx, sess, delivered); err != nil {
				m.mu.Unlock()
				return err
			}
			if sess.base.closed {
				m.mu.Unlock()
				return nil
			}
		}
	}
	m.mu.Unlock()
	return nil
}

func (m *Manager) AcceptRemoteStart(ctx context.Context, msg InboundMessage, receiver RemoteReceiver) (*Snapshot, error) {
	connID := normalizeConnID(msg)
	if strings.TrimSpace(connID) == "" {
		return nil, fmt.Errorf("%w: conn_id is required", ErrInvalidSession)
	}
	if strings.TrimSpace(msg.ChatID) == "" {
		return nil, fmt.Errorf("%w: chat_id is required", ErrInvalidSession)
	}
	if strings.TrimSpace(msg.SenderOpenID) == "" {
		return nil, fmt.Errorf("%w: sender_open_id is required", ErrInvalidSession)
	}
	if len(msg.Frames) == 0 {
		return nil, fmt.Errorf("%w: start message has no frames", ErrInvalidSession)
	}

	now := m.clock.Now()

	m.mu.Lock()
	if _, ok := m.remoteByConn[connID]; ok {
		m.mu.Unlock()
		return nil, fmt.Errorf("%w: %s", ErrInvalidSession, connID)
	}
	if len(m.remoteByConn) >= m.maxRemoteSessions {
		err := m.replyRemoteError(ctx, connID, msg.ChatID, msg.SenderOpenID, "remote session limit reached", "")
		m.mu.Unlock()
		if err != nil {
			return nil, err
		}
		return nil, ErrRemoteSessionLimit
	}

	rx := protocol.NewRecvWindow(m.sequenceGapTimeout, protocol.WithClock(m.clock))
	var startPayload protocol.StartPayload
	var gotStart bool
	for _, frame := range msg.Frames {
		result, err := rx.OfferAt(frame, now)
		if err != nil {
			m.mu.Unlock()
			return nil, err
		}
		for _, delivered := range result.Delivered {
			if delivered.Type != protocol.TypeStart || gotStart {
				m.mu.Unlock()
				return nil, fmt.Errorf("%w: %s", ErrUnexpectedFrame, delivered.Type)
			}
			payload, err := protocol.DecodeJSONPayload[protocol.StartPayload](delivered)
			if err != nil {
				m.mu.Unlock()
				return nil, err
			}
			startPayload = payload
			gotStart = true
		}
	}
	if !gotStart {
		m.mu.Unlock()
		return nil, fmt.Errorf("%w: start frame was not deliverable", ErrUnexpectedFrame)
	}

	peerTimeout := parseHeartbeatTimeout(startPayload.Heartbeat, m.heartbeatTimeout)
	sess := &remoteSession{
		base: sessionState{
			connID:                 connID,
			rootMessageID:          connID,
			chatID:                 msg.ChatID,
			peerOpenID:             msg.SenderOpenID,
			startedAt:              now,
			lastLocalSendAt:        now,
			lastPeerMessageAt:      now,
			localHeartbeatInterval: m.heartbeatInterval,
			localHeartbeatTimeout:  m.heartbeatTimeout,
			peerHeartbeatTimeout:   peerTimeout,
			rx:                     rx,
		},
		start:    startPayload,
		receiver: receiver,
	}
	if m.outbound == nil {
		m.mu.Unlock()
		return nil, ErrNoOutboundManager
	}
	if err := m.outbound.RegisterConnection(outbound.RegisterConnectionRequest{
		ConnID:            connID,
		Role:              outbound.RoleRemote,
		Target:            outbound.Target{ChatID: msg.ChatID, RootMessageID: connID, MentionOpenID: msg.SenderOpenID},
		NextSeq:           1,
		HeartbeatInterval: m.heartbeatInterval,
		OnDrop: func(ctx context.Context, reason outbound.DropReason) {
			m.handleRemoteOutboundDrop(ctx, connID, reason)
		},
		OnDrained: func(ctx context.Context) {
			m.CloseRemoteConn(connID)
		},
	}); err != nil {
		m.mu.Unlock()
		return nil, err
	}
	if err := m.outbound.EnqueueJSON(ctx, connID, protocol.TypeStartAck, protocol.StartAckPayload{Heartbeat: m.HeartbeatConfig()}); err != nil {
		m.outbound.DropConnection(connID)
		m.mu.Unlock()
		return nil, err
	}

	m.remoteByConn[connID] = sess
	if receiver != nil {
		_ = receiver.Deliver(ctx, RemoteEvent{
			Type:   RemoteEventStart,
			ConnID: connID,
			Start:  cloneStartPayload(startPayload),
		})
	}
	snapshot := snapshotRemote(sess)
	m.mu.Unlock()
	return &snapshot, nil
}

func (m *Manager) RegisterRemote(ctx context.Context, start RemoteStart) error {
	if strings.TrimSpace(start.ConnID) == "" {
		return fmt.Errorf("%w: conn_id is required", ErrInvalidSession)
	}
	frame, err := protocol.NewJSONFrame(1, protocol.TypeStart, start.Start)
	if err != nil {
		return err
	}
	_, err = m.AcceptRemoteStart(ctx, InboundMessage{
		ConnID:        start.ConnID,
		RootMessageID: start.ConnID,
		ChatID:        start.ChatID,
		SenderOpenID:  start.SenderOpenID,
		IsRoot:        true,
		Frames:        []protocol.Frame{frame},
	}, start.Receiver)
	return err
}

func (m *Manager) ReceiveRemote(ctx context.Context, msg InboundMessage) error {
	connID := normalizeConnID(msg)
	if strings.TrimSpace(connID) == "" {
		return fmt.Errorf("%w: conn_id is required", ErrInvalidSession)
	}
	now := m.clock.Now()

	m.mu.Lock()
	sess, ok := m.remoteByConn[connID]
	if !ok || sess.base.closed {
		m.mu.Unlock()
		return fmt.Errorf("%w: %s", ErrSessionNotFound, connID)
	}
	if err := sess.base.ensurePeer(msg.ChatID, msg.SenderOpenID); err != nil {
		m.mu.Unlock()
		return err
	}
	sess.base.lastPeerMessageAt = now

	for _, frame := range msg.Frames {
		result, err := sess.base.rx.OfferAt(frame, now)
		if err != nil {
			m.handleRemoteSequenceErrorLocked(ctx, sess, err)
			m.mu.Unlock()
			return err
		}
		for _, delivered := range result.Delivered {
			if err := m.handleRemoteFrameLocked(ctx, sess, delivered); err != nil {
				m.mu.Unlock()
				return err
			}
			if sess.base.closed {
				m.mu.Unlock()
				return nil
			}
		}
	}
	m.mu.Unlock()
	return nil
}

func (m *Manager) SendRemoteStdout(ctx context.Context, connID string, data []byte) error {
	return m.sendRemoteFrame(ctx, connID, protocol.TypeStdout, append([]byte(nil), data...), false)
}

func (m *Manager) SendRemoteStderr(ctx context.Context, connID string, data []byte) error {
	return m.sendRemoteFrame(ctx, connID, protocol.TypeStderr, append([]byte(nil), data...), false)
}

func (m *Manager) SendRemoteExit(ctx context.Context, connID string, code int) error {
	return m.sendRemoteJSONFrame(ctx, connID, protocol.TypeExit, protocol.ExitPayload{Code: code}, true)
}

func (m *Manager) SendRemoteError(ctx context.Context, connID, message, detail string) error {
	return m.sendRemoteJSONFrame(ctx, connID, protocol.TypeError, protocol.ErrorPayload{Message: message, Detail: detail}, true)
}

func (m *Manager) CloseRemote(ctx context.Context, connID, reason string) error {
	return m.sendRemoteJSONFrame(ctx, connID, protocol.TypeClose, protocol.ClosePayload{Reason: reason}, true)
}

func (m *Manager) Tick(ctx context.Context) error {
	now := m.clock.Now()
	var errOut error

	m.mu.Lock()
	for _, sess := range sortedLocalSessions(m.localByConn) {
		if sess.base.closed {
			continue
		}
		if err := sess.base.rx.CheckGapTimeoutAt(now); err != nil {
			m.handleLocalSequenceErrorLocked(ctx, sess, err)
			errOut = firstErr(errOut, err)
			continue
		}
		if sess.base.peerTimedOut(now) {
			err := fmt.Errorf("peer heartbeat timeout after %s", sess.base.peerHeartbeatTimeout)
			m.deliverLocalErrorLocked(ctx, sess, err, "peer heartbeat timeout", "")
			m.closeLocalLocked(sess)
			errOut = firstErr(errOut, err)
			continue
		}
	}
	for _, sess := range sortedRemoteSessions(m.remoteByConn) {
		if sess.base.closed {
			continue
		}
		if err := sess.base.rx.CheckGapTimeoutAt(now); err != nil {
			m.handleRemoteSequenceErrorLocked(ctx, sess, err)
			errOut = firstErr(errOut, err)
			continue
		}
		if sess.base.peerTimedOut(now) {
			event := RemoteEvent{
				Type:    RemoteEventPeerTimeout,
				ConnID:  sess.base.connID,
				Message: "peer heartbeat timeout",
				Err:     fmt.Errorf("peer heartbeat timeout after %s", sess.base.peerHeartbeatTimeout),
			}
			m.deliverRemoteLocked(ctx, sess, event)
			m.closeRemoteLocked(sess)
			errOut = firstErr(errOut, event.Err)
			continue
		}
	}
	m.mu.Unlock()
	return errOut
}

func (m *Manager) LocalSnapshot(connID string) (Snapshot, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	sess, ok := m.localByConn[connID]
	if !ok {
		return Snapshot{}, false
	}
	return snapshotLocal(sess), true
}

func (m *Manager) RemoteSnapshot(connID string) (Snapshot, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	sess, ok := m.remoteByConn[connID]
	if !ok {
		return Snapshot{}, false
	}
	return snapshotRemote(sess), true
}

func (m *Manager) LocalSessions() []Snapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]Snapshot, 0, len(m.localByConn))
	for _, sess := range sortedLocalSessions(m.localByConn) {
		out = append(out, snapshotLocal(sess))
	}
	return out
}

func (m *Manager) RemoteSessions() []Snapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]Snapshot, 0, len(m.remoteByConn))
	for _, sess := range sortedRemoteSessions(m.remoteByConn) {
		out = append(out, snapshotRemote(sess))
	}
	return out
}

func (m *Manager) ConnIDForRequest(requestID string) (string, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	connID, ok := m.localByRequest[requestID]
	return connID, ok
}

func (m *Manager) CloseLocalConn(connID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if sess, ok := m.localByConn[connID]; ok {
		m.closeLocalLocked(sess)
	}
}

func (m *Manager) CloseRemoteConn(connID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if sess, ok := m.remoteByConn[connID]; ok {
		m.closeRemoteLocked(sess)
	}
}

func (m *Manager) handleLocalOutboundDrop(ctx context.Context, connID string, reason outbound.DropReason) {
	m.mu.Lock()
	defer m.mu.Unlock()
	sess, ok := m.localByConn[connID]
	if !ok || sess.base.closed {
		return
	}
	detail := ""
	if reason.Err != nil {
		detail = reason.Err.Error()
	}
	m.deliverLocalErrorLocked(ctx, sess, reason.Err, "outbound send failed", detail)
	m.closeLocalLocked(sess)
}

func (m *Manager) handleRemoteOutboundDrop(ctx context.Context, connID string, reason outbound.DropReason) {
	m.mu.Lock()
	defer m.mu.Unlock()
	sess, ok := m.remoteByConn[connID]
	if !ok || sess.base.closed {
		return
	}
	detail := ""
	if reason.Err != nil {
		detail = reason.Err.Error()
	}
	m.deliverRemoteLocked(ctx, sess, RemoteEvent{
		Type:    RemoteEventError,
		ConnID:  connID,
		Message: "outbound send failed",
		Detail:  detail,
		Err:     reason.Err,
	})
	m.closeRemoteLocked(sess)
}

func (m *Manager) sendLocalJSONFrame(ctx context.Context, requestID string, typ protocol.FrameType, payload any, closeAfter bool) error {
	raw, err := protocol.MarshalJSONPayload(payload)
	if err != nil {
		return err
	}
	return m.sendLocalFrame(ctx, requestID, typ, raw, closeAfter)
}

func (m *Manager) sendLocalFrame(ctx context.Context, requestID string, typ protocol.FrameType, payload []byte, closeAfter bool) error {
	m.mu.Lock()
	connID, ok := m.localByRequest[requestID]
	if !ok {
		m.mu.Unlock()
		return fmt.Errorf("%w: request_id %q", ErrSessionNotFound, requestID)
	}
	sess, ok := m.localByConn[connID]
	if !ok || sess.base.closed {
		m.mu.Unlock()
		return fmt.Errorf("%w: %s", ErrSessionClosed, connID)
	}
	if m.outbound == nil {
		m.mu.Unlock()
		return ErrNoOutboundManager
	}
	m.mu.Unlock()
	if err := m.outbound.Enqueue(ctx, connID, typ, payload); err != nil {
		return err
	}
	if closeAfter {
		m.outbound.MarkCloseAfterDrained(connID)
	}
	return nil
}

func (m *Manager) sendRemoteJSONFrame(ctx context.Context, connID string, typ protocol.FrameType, payload any, closeAfter bool) error {
	raw, err := protocol.MarshalJSONPayload(payload)
	if err != nil {
		return err
	}
	return m.sendRemoteFrame(ctx, connID, typ, raw, closeAfter)
}

func (m *Manager) sendRemoteFrame(ctx context.Context, connID string, typ protocol.FrameType, payload []byte, closeAfter bool) error {
	m.mu.Lock()
	sess, ok := m.remoteByConn[connID]
	if !ok || sess.base.closed {
		m.mu.Unlock()
		return fmt.Errorf("%w: %s", ErrSessionNotFound, connID)
	}
	if m.outbound == nil {
		m.mu.Unlock()
		return ErrNoOutboundManager
	}
	m.mu.Unlock()
	if err := m.outbound.Enqueue(ctx, connID, typ, payload); err != nil {
		return err
	}
	if closeAfter {
		m.outbound.MarkCloseAfterDrained(connID)
	}
	return nil
}

func (m *Manager) handleLocalFrameLocked(ctx context.Context, sess *localSession, frame protocol.Frame) error {
	switch frame.Type {
	case protocol.TypeStartAck:
		payload, err := protocol.DecodeJSONPayload[protocol.StartAckPayload](frame)
		if err != nil {
			m.deliverLocalErrorLocked(ctx, sess, err, "invalid start_ack payload", err.Error())
			m.closeLocalLocked(sess)
			return err
		}
		sess.base.peerHeartbeatTimeout = parseHeartbeatTimeout(payload.Heartbeat, m.heartbeatTimeout)
		return m.deliverLocalLocked(ctx, sess, LocalEvent{Type: LocalEventStartAck, ConnID: sess.base.connID})
	case protocol.TypeHeartbeat:
		return nil
	case protocol.TypeStdout:
		return m.deliverLocalLocked(ctx, sess, LocalEvent{Type: LocalEventStdout, ConnID: sess.base.connID, Bytes: cloneBytes(frame.Payload)})
	case protocol.TypeStderr:
		return m.deliverLocalLocked(ctx, sess, LocalEvent{Type: LocalEventStderr, ConnID: sess.base.connID, Bytes: cloneBytes(frame.Payload)})
	case protocol.TypeExit:
		payload, err := protocol.DecodeJSONPayload[protocol.ExitPayload](frame)
		if err != nil {
			m.deliverLocalErrorLocked(ctx, sess, err, "invalid exit payload", err.Error())
			m.closeLocalLocked(sess)
			return err
		}
		err = m.deliverLocalLocked(ctx, sess, LocalEvent{Type: LocalEventExit, ConnID: sess.base.connID, Code: payload.Code})
		m.closeLocalLocked(sess)
		return err
	case protocol.TypeError:
		payload, err := protocol.DecodeJSONPayload[protocol.ErrorPayload](frame)
		if err != nil {
			m.deliverLocalErrorLocked(ctx, sess, err, "invalid error payload", err.Error())
			m.closeLocalLocked(sess)
			return err
		}
		err = m.deliverLocalLocked(ctx, sess, LocalEvent{Type: LocalEventError, ConnID: sess.base.connID, Message: payload.Message, Detail: payload.Detail})
		m.closeLocalLocked(sess)
		return err
	case protocol.TypeClose:
		payload, err := protocol.DecodeJSONPayload[protocol.ClosePayload](frame)
		if err != nil {
			m.deliverLocalErrorLocked(ctx, sess, err, "invalid close payload", err.Error())
			m.closeLocalLocked(sess)
			return err
		}
		err = m.deliverLocalLocked(ctx, sess, LocalEvent{Type: LocalEventClose, ConnID: sess.base.connID, Message: payload.Reason})
		m.closeLocalLocked(sess)
		return err
	default:
		err := fmt.Errorf("%w: %s", ErrUnexpectedFrame, frame.Type)
		m.deliverLocalErrorLocked(ctx, sess, err, "unexpected frame", string(frame.Type))
		m.closeLocalLocked(sess)
		return err
	}
	return nil
}

func (m *Manager) handleRemoteFrameLocked(ctx context.Context, sess *remoteSession, frame protocol.Frame) error {
	switch frame.Type {
	case protocol.TypeHeartbeat:
		return nil
	case protocol.TypeStdin:
		return m.deliverRemoteLocked(ctx, sess, RemoteEvent{Type: RemoteEventStdin, ConnID: sess.base.connID, Bytes: cloneBytes(frame.Payload)})
	case protocol.TypeResize:
		payload, err := protocol.DecodeJSONPayload[protocol.ResizePayload](frame)
		if err != nil {
			return err
		}
		return m.deliverRemoteLocked(ctx, sess, RemoteEvent{Type: RemoteEventResize, ConnID: sess.base.connID, Rows: payload.Rows, Cols: payload.Cols})
	case protocol.TypeSignal:
		payload, err := protocol.DecodeJSONPayload[protocol.SignalPayload](frame)
		if err != nil {
			return err
		}
		return m.deliverRemoteLocked(ctx, sess, RemoteEvent{Type: RemoteEventSignal, ConnID: sess.base.connID, Name: payload.Name})
	case protocol.TypeClose:
		payload, err := protocol.DecodeJSONPayload[protocol.ClosePayload](frame)
		if err != nil {
			return err
		}
		err = m.deliverRemoteLocked(ctx, sess, RemoteEvent{Type: RemoteEventClose, ConnID: sess.base.connID, Reason: payload.Reason})
		m.closeRemoteLocked(sess)
		return err
	case protocol.TypeError:
		payload, err := protocol.DecodeJSONPayload[protocol.ErrorPayload](frame)
		if err != nil {
			return err
		}
		err = m.deliverRemoteLocked(ctx, sess, RemoteEvent{Type: RemoteEventError, ConnID: sess.base.connID, Message: payload.Message, Detail: payload.Detail})
		m.closeRemoteLocked(sess)
		return err
	default:
		err := fmt.Errorf("%w: %s", ErrUnexpectedFrame, frame.Type)
		m.deliverRemoteLocked(ctx, sess, RemoteEvent{Type: RemoteEventError, ConnID: sess.base.connID, Message: "unexpected frame", Detail: string(frame.Type), Err: err})
		m.closeRemoteLocked(sess)
		return err
	}
}

func (m *Manager) handleLocalSequenceErrorLocked(ctx context.Context, sess *localSession, err error) {
	m.deliverLocalErrorLocked(ctx, sess, err, "sequence gap timeout", err.Error())
	m.closeLocalLocked(sess)
}

func (m *Manager) handleRemoteSequenceErrorLocked(ctx context.Context, sess *remoteSession, err error) {
	m.deliverRemoteLocked(ctx, sess, RemoteEvent{
		Type:    RemoteEventSequenceGapTimeout,
		ConnID:  sess.base.connID,
		Message: "sequence gap timeout",
		Detail:  err.Error(),
		Err:     err,
	})
	if m.outbound != nil {
		_ = m.outbound.EnqueueJSON(ctx, sess.base.connID, protocol.TypeError, protocol.ErrorPayload{Message: "sequence gap timeout", Detail: err.Error()})
		m.outbound.MarkCloseAfterDrained(sess.base.connID)
		return
	}
	m.closeRemoteLocked(sess)
}

func (m *Manager) deliverLocalErrorLocked(ctx context.Context, sess *localSession, err error, message, detail string) {
	_ = m.deliverLocalLocked(ctx, sess, LocalEvent{
		Type:    LocalEventError,
		ConnID:  sess.base.connID,
		Message: message,
		Detail:  detail,
		Err:     err,
	})
}

func (m *Manager) deliverLocalLocked(ctx context.Context, sess *localSession, event LocalEvent) error {
	var errOut error
	for _, subscriber := range sortedSubscribers(sess.subscribers) {
		if subscriber.sub == nil {
			continue
		}
		next := event
		next.RequestID = subscriber.requestID
		next.Bytes = cloneBytes(event.Bytes)
		if err := subscriber.sub.Deliver(ctx, next); err != nil {
			errOut = firstErr(errOut, err)
		}
	}
	return errOut
}

func (m *Manager) deliverRemoteLocked(ctx context.Context, sess *remoteSession, event RemoteEvent) error {
	if sess.receiver == nil {
		return nil
	}
	event.Bytes = cloneBytes(event.Bytes)
	return sess.receiver.Deliver(ctx, event)
}

func (m *Manager) replyRemoteError(ctx context.Context, connID, chatID, senderOpenID, message, detail string) error {
	if m.outbound == nil {
		return ErrNoOutboundManager
	}
	target := outbound.Target{
		ChatID:        chatID,
		RootMessageID: connID,
		MentionOpenID: senderOpenID,
	}
	if err := m.outbound.RegisterConnection(outbound.RegisterConnectionRequest{
		ConnID:            connID,
		Role:              outbound.RoleRemote,
		Target:            target,
		NextSeq:           1,
		HeartbeatInterval: 0,
	}); err != nil {
		return err
	}
	if err := m.outbound.EnqueueJSON(ctx, connID, protocol.TypeError, protocol.ErrorPayload{Message: message, Detail: detail}); err != nil {
		m.outbound.DropConnection(connID)
		return err
	}
	m.outbound.MarkCloseAfterDrained(connID)
	return nil
}

func (m *Manager) closeLocalLocked(sess *localSession) {
	if sess.base.closed {
		return
	}
	sess.base.closed = true
	sess.base.closedAt = m.clock.Now()
	for requestID, sub := range sess.subscribers {
		if closer, ok := sub.(interface{ Close() error }); ok {
			_ = closer.Close()
		}
		delete(m.localByRequest, requestID)
	}
	delete(m.localByConn, sess.base.connID)
	if m.outbound != nil {
		m.outbound.DropConnection(sess.base.connID)
	}
}

func (m *Manager) closeRemoteLocked(sess *remoteSession) {
	if sess.base.closed {
		return
	}
	sess.base.closed = true
	sess.base.closedAt = m.clock.Now()
	if closer, ok := sess.receiver.(interface{ Close() error }); ok {
		_ = closer.Close()
	}
	delete(m.remoteByConn, sess.base.connID)
	if m.outbound != nil {
		m.outbound.DropConnection(sess.base.connID)
	}
}

func (m *Manager) unsubscribeLocalLocked(requestID string) {
	connID, ok := m.localByRequest[requestID]
	if !ok {
		return
	}
	delete(m.localByRequest, requestID)
	if sess, ok := m.localByConn[connID]; ok {
		if sub := sess.subscribers[requestID]; sub != nil {
			if closer, ok := sub.(interface{ Close() error }); ok {
				_ = closer.Close()
			}
		}
		delete(sess.subscribers, requestID)
	}
}

func (s *sessionState) ensurePeer(chatID, peerOpenID string) error {
	if chatID != "" && chatID != s.chatID {
		return fmt.Errorf("%w: chat_id %q does not match session chat_id %q", ErrUnauthorizedPeer, chatID, s.chatID)
	}
	if peerOpenID != "" && peerOpenID != s.peerOpenID {
		return fmt.Errorf("%w: sender_open_id %q does not match peer_open_id %q", ErrUnauthorizedPeer, peerOpenID, s.peerOpenID)
	}
	return nil
}

func (s *sessionState) peerTimedOut(now time.Time) bool {
	if s.closed || s.peerHeartbeatTimeout <= 0 {
		return false
	}
	return now.After(s.lastPeerMessageAt.Add(s.peerHeartbeatTimeout))
}

func snapshotLocal(sess *localSession) Snapshot {
	base := snapshotBase("local", &sess.base)
	base.Host = sess.host
	base.RootMessageURL = sess.rootMessageURL
	base.RequestIDs = sortedRequestIDs(sess.subscribers)
	return base
}

func snapshotRemote(sess *remoteSession) Snapshot {
	return snapshotBase("remote", &sess.base)
}

func snapshotBase(role string, state *sessionState) Snapshot {
	return Snapshot{
		Role:                   role,
		ConnID:                 state.connID,
		RootMessageID:          state.rootMessageID,
		ChatID:                 state.chatID,
		PeerOpenID:             state.peerOpenID,
		StartedAt:              state.startedAt,
		LastLocalSendAt:        state.lastLocalSendAt,
		LastPeerMessageAt:      state.lastPeerMessageAt,
		LocalHeartbeatInterval: state.localHeartbeatInterval,
		LocalHeartbeatTimeout:  state.localHeartbeatTimeout,
		PeerHeartbeatTimeout:   state.peerHeartbeatTimeout,
		RXNextExpectedSeq:      state.rx.NextExpectedSeq,
		RXPendingSeqs:          state.rx.PendingSeqs(),
		RXGapStartedAt:         state.rx.GapStartedAt,
		Closed:                 state.closed,
		ClosedAt:               state.closedAt,
	}
}

func sortedLocalSessions(sessions map[string]*localSession) []*localSession {
	keys := make([]string, 0, len(sessions))
	for key := range sessions {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]*localSession, 0, len(keys))
	for _, key := range keys {
		out = append(out, sessions[key])
	}
	return out
}

func sortedRemoteSessions(sessions map[string]*remoteSession) []*remoteSession {
	keys := make([]string, 0, len(sessions))
	for key := range sessions {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]*remoteSession, 0, len(keys))
	for _, key := range keys {
		out = append(out, sessions[key])
	}
	return out
}

func sortedRequestIDs(subscribers map[string]Subscriber) []string {
	keys := make([]string, 0, len(subscribers))
	for key := range subscribers {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func sortedSubscribers(subscribers map[string]Subscriber) []struct {
	requestID string
	sub       Subscriber
} {
	keys := sortedRequestIDs(subscribers)
	out := make([]struct {
		requestID string
		sub       Subscriber
	}, 0, len(keys))
	for _, key := range keys {
		out = append(out, struct {
			requestID string
			sub       Subscriber
		}{requestID: key, sub: subscribers[key]})
	}
	return out
}

func parseHeartbeatTimeout(h protocol.HeartbeatConfig, fallback time.Duration) time.Duration {
	if strings.TrimSpace(h.Timeout) == "" {
		return fallback
	}
	parsed, err := parseDuration(h.Timeout)
	if err != nil || parsed <= 0 {
		return fallback
	}
	return parsed
}

func parseDuration(value string) (time.Duration, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, errors.New("duration is empty")
	}

	matches := durationTokenRE.FindAllStringSubmatchIndex(value, -1)
	if len(matches) == 0 {
		return 0, fmt.Errorf("invalid duration %q", value)
	}

	var total time.Duration
	pos := 0
	for _, match := range matches {
		if match[0] != pos {
			return 0, fmt.Errorf("invalid duration %q", value)
		}
		number := value[match[2]:match[3]]
		unit := value[match[4]:match[5]]
		amount, err := strconv.ParseFloat(number, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid duration %q", value)
		}

		var unitDuration time.Duration
		switch unit {
		case "ms":
			unitDuration = time.Millisecond
		case "s":
			unitDuration = time.Second
		case "m":
			unitDuration = time.Minute
		case "h":
			unitDuration = time.Hour
		case "d":
			unitDuration = 24 * time.Hour
		default:
			return 0, fmt.Errorf("invalid duration unit %q", unit)
		}
		total += time.Duration(amount * float64(unitDuration))
		pos = match[1]
	}
	if pos != len(value) {
		return 0, fmt.Errorf("invalid duration %q", value)
	}
	return total, nil
}

func normalizeConnID(msg InboundMessage) string {
	return firstNonEmpty(msg.ConnID, msg.RootMessageID, ConnID(msg.RootMessageID, msg.MessageID))
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func firstErr(existing, next error) error {
	if existing != nil {
		return existing
	}
	return next
}

func cloneBytes(in []byte) []byte {
	if len(in) == 0 {
		return nil
	}
	return append([]byte(nil), in...)
}

func cloneStartPayload(in protocol.StartPayload) protocol.StartPayload {
	out := in
	if in.Env != nil {
		out.Env = make(map[string]string, len(in.Env))
		for key, value := range in.Env {
			out.Env[key] = value
		}
	}
	return out
}
