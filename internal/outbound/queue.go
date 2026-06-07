package outbound

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/hachiwii/exec-over-lark/internal/protocol"
)

const (
	DefaultSendCooldown              = time.Second
	DefaultLarkTextRequestLimitBytes = 153600
	DefaultMaxSendAttempts           = 1
)

var (
	ErrFrameTooLarge = errors.New("outbound frame exceeds lark text request limit")
	ErrInvalidTarget = errors.New("invalid outbound target")
	ErrNoSender      = errors.New("outbound sender is nil")
)

type RootMessage struct {
	MessageID string
}

type Sender interface {
	SendRootMessage(ctx context.Context, chatID, mentionOpenID, text string) (RootMessage, error)
	ReplyRootMessage(ctx context.Context, chatID, rootMessageID, mentionOpenID, text string) (messageID string, err error)
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

type Option func(*Queue)

type Queue struct {
	mu sync.Mutex

	sender   Sender
	clock    Clock
	sizer    RequestSizer
	cooldown time.Duration
	limit    int
	attempts int

	lastSentAt  time.Time
	hasLastSent bool
	sending     bool

	order       []string
	pending     map[string]*pendingTarget
	nextRootKey uint64
}

type pendingTarget struct {
	key    string
	target Target
	frames []protocol.Frame
}

type realClock struct{}

func (realClock) Now() time.Time {
	return time.Now()
}

func New(sender Sender, opts ...Option) *Queue {
	q := &Queue{
		sender:   sender,
		clock:    realClock{},
		sizer:    defaultRequestSizer,
		cooldown: DefaultSendCooldown,
		limit:    DefaultLarkTextRequestLimitBytes,
		attempts: DefaultMaxSendAttempts,
		pending:  make(map[string]*pendingTarget),
	}
	for _, opt := range opts {
		opt(q)
	}
	if q.clock == nil {
		q.clock = realClock{}
	}
	if q.sizer == nil {
		q.sizer = defaultRequestSizer
	}
	if q.limit <= 0 {
		q.limit = DefaultLarkTextRequestLimitBytes
	}
	if q.attempts <= 0 {
		q.attempts = DefaultMaxSendAttempts
	}
	if q.pending == nil {
		q.pending = make(map[string]*pendingTarget)
	}
	return q
}

func WithClock(clock Clock) Option {
	return func(q *Queue) {
		if clock != nil {
			q.clock = clock
		}
	}
}

func WithSendCooldown(cooldown time.Duration) Option {
	return func(q *Queue) {
		q.cooldown = cooldown
	}
}

func WithRequestLimitBytes(limit int) Option {
	return func(q *Queue) {
		q.limit = limit
	}
}

func WithRequestSizer(sizer RequestSizer) Option {
	return func(q *Queue) {
		if sizer != nil {
			q.sizer = sizer
		}
	}
}

func WithMaxSendAttempts(attempts int) Option {
	return func(q *Queue) {
		q.attempts = attempts
	}
}

func (q *Queue) Enqueue(ctx context.Context, target Target, frames ...protocol.Frame) error {
	if len(frames) == 0 {
		return nil
	}
	if err := target.validate(); err != nil {
		return err
	}

	q.mu.Lock()
	key := q.targetKeyLocked(target)
	if q.sending || len(q.order) > 0 || !q.canSendLocked(q.clock.Now()) {
		q.enqueueBackLocked(key, target, frames)
		q.mu.Unlock()
		return nil
	}
	q.sending = true
	q.mu.Unlock()

	remaining, err := q.sendOneRequest(ctx, target, frames)
	q.mu.Lock()
	q.sending = false
	if err != nil {
		q.mu.Unlock()
		return err
	}

	q.markSentLocked(q.clock.Now())
	if len(remaining) > 0 {
		q.enqueueFrontLocked(key, target, remaining)
	}
	q.mu.Unlock()
	return nil
}

func (q *Queue) FlushReady(ctx context.Context) (bool, error) {
	q.mu.Lock()
	if q.sending || len(q.order) == 0 || !q.canSendLocked(q.clock.Now()) {
		q.mu.Unlock()
		return false, nil
	}
	item := q.frontLocked()
	frames := cloneFrames(item.frames)
	q.sending = true
	q.mu.Unlock()

	remaining, err := q.sendOneRequest(ctx, item.target, frames)
	q.mu.Lock()
	q.sending = false
	if err != nil {
		q.mu.Unlock()
		return true, err
	}

	q.markSentLocked(q.clock.Now())
	q.dropFrontFramesLocked(item.key, len(frames), remaining)
	q.mu.Unlock()
	return true, nil
}

func (q *Queue) NextFlushAt() (time.Time, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.order) == 0 {
		return time.Time{}, false
	}
	if !q.hasLastSent || q.cooldown <= 0 {
		return q.clock.Now(), true
	}
	return q.lastSentAt.Add(q.cooldown), true
}

func (q *Queue) PendingLen() int {
	q.mu.Lock()
	defer q.mu.Unlock()

	total := 0
	for _, item := range q.pending {
		total += len(item.frames)
	}
	return total
}

func (q *Queue) PendingTargets() []Target {
	q.mu.Lock()
	defer q.mu.Unlock()

	targets := make([]Target, 0, len(q.order))
	for _, key := range q.order {
		targets = append(targets, q.pending[key].target)
	}
	return targets
}

func (q *Queue) LastSentAt() (time.Time, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.lastSentAt, q.hasLastSent
}

func (q *Queue) sendOneRequest(ctx context.Context, target Target, frames []protocol.Frame) ([]protocol.Frame, error) {
	if q.sender == nil {
		return nil, ErrNoSender
	}

	chunk, remaining, err := q.nextChunk(target, frames)
	if err != nil {
		return nil, err
	}
	text, err := protocol.EncodeFrames(chunk)
	if err != nil {
		return nil, err
	}

	var sendErr error
	for attempt := 0; attempt < q.attempts; attempt++ {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if target.RootMessageID == "" {
			_, sendErr = q.sender.SendRootMessage(ctx, target.ChatID, target.MentionOpenID, text)
		} else {
			_, sendErr = q.sender.ReplyRootMessage(ctx, target.ChatID, target.RootMessageID, target.MentionOpenID, text)
		}
		if sendErr == nil {
			return remaining, nil
		}
	}
	return nil, sendErr
}

func (q *Queue) nextChunk(target Target, frames []protocol.Frame) ([]protocol.Frame, []protocol.Frame, error) {
	if len(frames) == 0 {
		return nil, nil, nil
	}

	chunk := make([]protocol.Frame, 0, len(frames))
	for i, frame := range frames {
		candidate := append(append([]protocol.Frame(nil), chunk...), frame)
		fits, err := q.fits(target, candidate)
		if err != nil {
			return nil, nil, err
		}
		if fits {
			chunk = candidate
			continue
		}

		if len(chunk) > 0 {
			return chunk, cloneFrames(frames[i:]), nil
		}

		replacements, err := q.splitFrameUntilFits(target, frame)
		if err != nil {
			return nil, nil, err
		}
		rest, err := shiftSeqs(frames[i+1:], uint64(len(replacements)-1))
		if err != nil {
			return nil, nil, err
		}
		remaining := make([]protocol.Frame, 0, len(replacements)-1+len(rest))
		remaining = append(remaining, replacements[1:]...)
		remaining = append(remaining, rest...)
		return []protocol.Frame{replacements[0]}, remaining, nil
	}

	return chunk, nil, nil
}

func (q *Queue) splitFrameUntilFits(target Target, frame protocol.Frame) ([]protocol.Frame, error) {
	fits, err := q.fits(target, []protocol.Frame{frame})
	if err != nil {
		return nil, err
	}
	if fits {
		return []protocol.Frame{frame}, nil
	}
	if !isStreamingFrame(frame.Type) {
		return nil, fmt.Errorf("%w: seq %d type %s", ErrFrameTooLarge, frame.Seq, frame.Type)
	}
	return q.splitStreamingPayload(target, frame.Seq, frame.Type, frame.Payload)
}

func (q *Queue) splitStreamingPayload(target Target, seq uint64, typ protocol.FrameType, payload []byte) ([]protocol.Frame, error) {
	frame := protocol.Frame{Seq: seq, Type: typ, Payload: cloneBytes(payload)}
	fits, err := q.fits(target, []protocol.Frame{frame})
	if err != nil {
		return nil, err
	}
	if fits {
		return []protocol.Frame{frame}, nil
	}
	if len(payload) <= 1 {
		return nil, fmt.Errorf("%w: seq %d type %s", ErrFrameTooLarge, seq, typ)
	}

	mid := len(payload) / 2
	left, err := q.splitStreamingPayload(target, seq, typ, payload[:mid])
	if err != nil {
		return nil, err
	}
	nextSeq, err := addSeq(seq, uint64(len(left)))
	if err != nil {
		return nil, err
	}
	right, err := q.splitStreamingPayload(target, nextSeq, typ, payload[mid:])
	if err != nil {
		return nil, err
	}
	return append(left, right...), nil
}

func (q *Queue) fits(target Target, frames []protocol.Frame) (bool, error) {
	text, err := protocol.EncodeFrames(frames)
	if err != nil {
		return false, err
	}
	size, err := q.sizer(target, text)
	if err != nil {
		return false, err
	}
	return size <= q.limit, nil
}

func (q *Queue) canSendLocked(now time.Time) bool {
	if !q.hasLastSent || q.cooldown <= 0 {
		return true
	}
	return !now.Before(q.lastSentAt.Add(q.cooldown))
}

func (q *Queue) markSentLocked(now time.Time) {
	q.lastSentAt = now
	q.hasLastSent = true
}

func (q *Queue) targetKeyLocked(target Target) string {
	if target.RootMessageID != "" {
		return "reply\x00" + target.ChatID + "\x00" + target.RootMessageID + "\x00" + target.MentionOpenID
	}
	q.nextRootKey++
	return fmt.Sprintf("root\x00%s\x00%s\x00%d", target.ChatID, target.MentionOpenID, q.nextRootKey)
}

func (q *Queue) enqueueBackLocked(key string, target Target, frames []protocol.Frame) {
	if item, ok := q.pending[key]; ok {
		item.frames = append(item.frames, cloneFrames(frames)...)
		return
	}
	q.pending[key] = &pendingTarget{
		key:    key,
		target: target,
		frames: cloneFrames(frames),
	}
	q.order = append(q.order, key)
}

func (q *Queue) enqueueFrontLocked(key string, target Target, frames []protocol.Frame) {
	if len(frames) == 0 {
		return
	}
	if item, ok := q.pending[key]; ok {
		item.frames = append(cloneFrames(frames), item.frames...)
		q.moveKeyToFrontLocked(key)
		return
	}
	q.pending[key] = &pendingTarget{
		key:    key,
		target: target,
		frames: cloneFrames(frames),
	}
	q.order = append([]string{key}, q.order...)
}

func (q *Queue) frontLocked() *pendingTarget {
	return q.pending[q.order[0]]
}

func (q *Queue) popFrontLocked() *pendingTarget {
	key := q.order[0]
	q.order = q.order[1:]
	item := q.pending[key]
	delete(q.pending, key)
	return item
}

func (q *Queue) dropFrontFramesLocked(key string, sentLen int, remaining []protocol.Frame) {
	item, ok := q.pending[key]
	if !ok {
		return
	}
	if sentLen > len(item.frames) {
		sentLen = len(item.frames)
	}
	tail := cloneFrames(item.frames[sentLen:])
	next := make([]protocol.Frame, 0, len(remaining)+len(tail))
	next = append(next, cloneFrames(remaining)...)
	next = append(next, tail...)
	if len(next) == 0 {
		q.popFrontLocked()
		return
	}
	item.frames = next
	q.moveKeyToFrontLocked(key)
}

func (q *Queue) moveKeyToFrontLocked(key string) {
	if len(q.order) <= 1 || q.order[0] == key {
		return
	}
	next := q.order[:0]
	next = append(next, key)
	for _, existing := range q.order {
		if existing != key {
			next = append(next, existing)
		}
	}
	q.order = next
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

func isStreamingFrame(typ protocol.FrameType) bool {
	return typ == protocol.TypeStdin || typ == protocol.TypeStdout || typ == protocol.TypeStderr
}

func shiftSeqs(frames []protocol.Frame, delta uint64) ([]protocol.Frame, error) {
	if len(frames) == 0 {
		return nil, nil
	}
	shifted := cloneFrames(frames)
	if delta == 0 {
		return shifted, nil
	}
	for i := range shifted {
		seq, err := addSeq(shifted[i].Seq, delta)
		if err != nil {
			return nil, err
		}
		shifted[i].Seq = seq
	}
	return shifted, nil
}

func addSeq(seq uint64, delta uint64) (uint64, error) {
	if seq == 0 {
		return 0, fmt.Errorf("%w: sequence must be positive", protocol.ErrInvalidFrame)
	}
	if delta > math.MaxUint64-seq {
		return 0, protocol.ErrSequencerExhausted
	}
	return seq + delta, nil
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
