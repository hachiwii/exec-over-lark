package outbound

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/hachiwii/exec-over-lark/internal/protocol"
)

func TestQueueSendsFirstIdleMessageImmediately(t *testing.T) {
	clock := newFakeClock()
	sender := &fakeSender{}
	queue := New(sender, WithClock(clock), WithSendCooldown(time.Second))

	target := testTarget("om_root")
	frame := protocol.Frame{Seq: 1, Type: protocol.TypeStdout, Payload: []byte("hello\n")}
	if err := queue.Enqueue(context.Background(), target, frame); err != nil {
		t.Fatalf("Enqueue returned error: %v", err)
	}

	if got := len(sender.messages); got != 1 {
		t.Fatalf("sent messages = %d, want 1", got)
	}
	if sender.messages[0].root {
		t.Fatal("sent root message, want reply")
	}
	assertMessageFrames(t, sender.messages[0], frame)
	if got := queue.PendingLen(); got != 0 {
		t.Fatalf("PendingLen = %d, want 0", got)
	}
	if got, ok := queue.LastSentAt(); !ok || !got.Equal(clock.Now()) {
		t.Fatalf("LastSentAt = %s/%v, want %s/true", got, ok, clock.Now())
	}
}

func TestQueueAggregatesSameTargetDuringCooldown(t *testing.T) {
	clock := newFakeClock()
	sender := &fakeSender{}
	queue := New(sender, WithClock(clock), WithSendCooldown(time.Second))
	target := testTarget("om_root")

	if err := queue.Enqueue(context.Background(), target, protocol.Frame{Seq: 1, Type: protocol.TypeStdout, Payload: []byte("first")}); err != nil {
		t.Fatalf("first Enqueue returned error: %v", err)
	}
	if err := queue.Enqueue(context.Background(), target, protocol.Frame{Seq: 2, Type: protocol.TypeStdout, Payload: []byte("second")}); err != nil {
		t.Fatalf("second Enqueue returned error: %v", err)
	}
	if err := queue.Enqueue(context.Background(), target, protocol.Frame{Seq: 3, Type: protocol.TypeStderr, Payload: []byte("third")}); err != nil {
		t.Fatalf("third Enqueue returned error: %v", err)
	}
	if got := len(sender.messages); got != 1 {
		t.Fatalf("sent messages before cooldown = %d, want 1", got)
	}

	clock.Advance(time.Second - time.Nanosecond)
	flushed, err := queue.FlushReady(context.Background())
	if err != nil {
		t.Fatalf("FlushReady before deadline returned error: %v", err)
	}
	if flushed {
		t.Fatal("FlushReady before deadline flushed, want false")
	}

	clock.Advance(time.Nanosecond)
	flushed, err = queue.FlushReady(context.Background())
	if err != nil {
		t.Fatalf("FlushReady returned error: %v", err)
	}
	if !flushed {
		t.Fatal("FlushReady at deadline did not flush")
	}
	if got := len(sender.messages); got != 2 {
		t.Fatalf("sent messages after cooldown = %d, want 2", got)
	}
	assertMessageFrameSeqs(t, sender.messages[1], 2, 3)
}

func TestQueueKeepsDifferentTargetsSeparate(t *testing.T) {
	clock := newFakeClock()
	sender := &fakeSender{}
	queue := New(sender, WithClock(clock), WithSendCooldown(time.Second))
	targetA := testTarget("om_a")
	targetB := testTarget("om_b")

	if err := queue.Enqueue(context.Background(), targetA, protocol.Frame{Seq: 1, Type: protocol.TypeStdout, Payload: []byte("first")}); err != nil {
		t.Fatalf("initial Enqueue returned error: %v", err)
	}
	if err := queue.Enqueue(context.Background(), targetA, protocol.Frame{Seq: 2, Type: protocol.TypeStdout, Payload: []byte("a")}); err != nil {
		t.Fatalf("target A Enqueue returned error: %v", err)
	}
	if err := queue.Enqueue(context.Background(), targetB, protocol.Frame{Seq: 1, Type: protocol.TypeStdout, Payload: []byte("b")}); err != nil {
		t.Fatalf("target B Enqueue returned error: %v", err)
	}

	clock.Advance(time.Second)
	flushed, err := queue.FlushReady(context.Background())
	if err != nil {
		t.Fatalf("first FlushReady returned error: %v", err)
	}
	if !flushed {
		t.Fatal("first FlushReady did not flush")
	}
	if got := sender.messages[1].rootMessageID; got != "om_a" {
		t.Fatalf("first queued target = %q, want om_a", got)
	}
	assertMessageFrameSeqs(t, sender.messages[1], 2)

	clock.Advance(time.Second)
	flushed, err = queue.FlushReady(context.Background())
	if err != nil {
		t.Fatalf("second FlushReady returned error: %v", err)
	}
	if !flushed {
		t.Fatal("second FlushReady did not flush")
	}
	if got := sender.messages[2].rootMessageID; got != "om_b" {
		t.Fatalf("second queued target = %q, want om_b", got)
	}
	assertMessageFrameSeqs(t, sender.messages[2], 1)
}

func TestQueueSendsRootMessagesThroughRootSender(t *testing.T) {
	clock := newFakeClock()
	sender := &fakeSender{}
	queue := New(sender, WithClock(clock), WithSendCooldown(time.Second))
	target := testTarget("")

	if err := queue.Enqueue(context.Background(), target, protocol.Frame{Seq: 1, Type: protocol.TypeStart, Payload: []byte("{}")}); err != nil {
		t.Fatalf("first root Enqueue returned error: %v", err)
	}
	if err := queue.Enqueue(context.Background(), target, protocol.Frame{Seq: 1, Type: protocol.TypeStart, Payload: []byte("{}")}); err != nil {
		t.Fatalf("second root Enqueue returned error: %v", err)
	}
	if got := len(sender.messages); got != 1 {
		t.Fatalf("sent messages before cooldown = %d, want 1", got)
	}
	if !sender.messages[0].root {
		t.Fatal("first message used reply sender, want root sender")
	}

	clock.Advance(time.Second)
	flushed, err := queue.FlushReady(context.Background())
	if err != nil {
		t.Fatalf("FlushReady returned error: %v", err)
	}
	if !flushed {
		t.Fatal("FlushReady did not flush second root message")
	}
	if got := len(sender.messages); got != 2 {
		t.Fatalf("sent messages after cooldown = %d, want 2", got)
	}
	if !sender.messages[1].root {
		t.Fatal("second message used reply sender, want root sender")
	}
	assertMessageFrameSeqs(t, sender.messages[1], 1)
}

func TestQueueSplitsRequestAtFrameBoundary(t *testing.T) {
	clock := newFakeClock()
	sender := &fakeSender{}
	queue := New(
		sender,
		WithClock(clock),
		WithSendCooldown(time.Second),
		WithRequestSizer(textSize),
	)
	target := testTarget("om_root")

	prime := protocol.Frame{Seq: 1, Type: protocol.TypeHeartbeat, Payload: []byte("{}")}
	if err := queue.Enqueue(context.Background(), target, prime); err != nil {
		t.Fatalf("prime Enqueue returned error: %v", err)
	}
	frame2 := protocol.Frame{Seq: 2, Type: protocol.TypeStdout, Payload: []byte("second")}
	frame3 := protocol.Frame{Seq: 3, Type: protocol.TypeStdout, Payload: []byte("third")}
	line2 := encodedLineLen(t, frame2)
	queue.limit = line2

	if err := queue.Enqueue(context.Background(), target, frame2, frame3); err != nil {
		t.Fatalf("queued Enqueue returned error: %v", err)
	}
	clock.Advance(time.Second)
	flushed, err := queue.FlushReady(context.Background())
	if err != nil {
		t.Fatalf("first FlushReady returned error: %v", err)
	}
	if !flushed {
		t.Fatal("first FlushReady did not flush")
	}
	assertMessageFrameSeqs(t, sender.messages[1], 2)
	if got := len(sender.messages[1].text); got > queue.limit {
		t.Fatalf("first split text length = %d, limit %d", got, queue.limit)
	}

	clock.Advance(time.Second)
	flushed, err = queue.FlushReady(context.Background())
	if err != nil {
		t.Fatalf("second FlushReady returned error: %v", err)
	}
	if !flushed {
		t.Fatal("second FlushReady did not flush")
	}
	assertMessageFrameSeqs(t, sender.messages[2], 3)
	if got := len(sender.messages[2].text); got > queue.limit {
		t.Fatalf("second split text length = %d, limit %d", got, queue.limit)
	}
}

func TestQueueSplitsOversizedStreamingFramePayload(t *testing.T) {
	clock := newFakeClock()
	sender := &fakeSender{}
	queue := New(
		sender,
		WithClock(clock),
		WithSendCooldown(0),
		WithRequestSizer(textSize),
	)
	target := testTarget("om_root")

	payload := bytes.Repeat([]byte("x"), 96)
	oneByte := protocol.Frame{Seq: 1, Type: protocol.TypeStdout, Payload: []byte("x")}
	queue.limit = encodedLineLen(t, oneByte) + 8
	if err := queue.Enqueue(context.Background(), target, protocol.Frame{Seq: 1, Type: protocol.TypeStdout, Payload: payload}); err != nil {
		t.Fatalf("Enqueue returned error: %v", err)
	}
	for queue.PendingLen() > 0 {
		flushed, err := queue.FlushReady(context.Background())
		if err != nil {
			t.Fatalf("FlushReady returned error: %v", err)
		}
		if !flushed {
			t.Fatal("FlushReady returned false with pending frames and zero cooldown")
		}
	}

	if got := len(sender.messages); got <= 1 {
		t.Fatalf("sent messages = %d, want payload split into multiple messages", got)
	}
	var assembled []byte
	var seqs []uint64
	for _, msg := range sender.messages {
		if got := len(msg.text); got > queue.limit {
			t.Fatalf("sent text length = %d, limit %d", got, queue.limit)
		}
		frames := decodeMessageFrames(t, msg)
		if len(frames) != 1 {
			t.Fatalf("message frames = %d, want exactly one split frame", len(frames))
		}
		seqs = append(seqs, frames[0].Seq)
		assembled = append(assembled, frames[0].Payload...)
	}
	if !bytes.Equal(assembled, payload) {
		t.Fatalf("assembled payload length/content mismatch: got %d bytes, want %d", len(assembled), len(payload))
	}
	wantSeqs := make([]uint64, len(seqs))
	for i := range wantSeqs {
		wantSeqs[i] = uint64(i + 1)
	}
	if !reflect.DeepEqual(seqs, wantSeqs) {
		t.Fatalf("split seqs = %v, want %v", seqs, wantSeqs)
	}
}

func TestQueueRejectsOversizedControlFrame(t *testing.T) {
	clock := newFakeClock()
	sender := &fakeSender{}
	queue := New(sender, WithClock(clock), WithRequestSizer(textSize))
	target := testTarget("om_root")

	frame := protocol.Frame{Seq: 1, Type: protocol.TypeError, Payload: bytes.Repeat([]byte("e"), 128)}
	queue.limit = encodedLineLen(t, protocol.Frame{Seq: 1, Type: protocol.TypeError, Payload: []byte("e")})
	err := queue.Enqueue(context.Background(), target, frame)
	if !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("Enqueue error = %v, want ErrFrameTooLarge", err)
	}
	if got := len(sender.messages); got != 0 {
		t.Fatalf("sent messages = %d, want 0", got)
	}
}

func TestQueueReturnsSenderFailure(t *testing.T) {
	clock := newFakeClock()
	wantErr := errors.New("lark unavailable")
	sender := &fakeSender{err: wantErr}
	queue := New(sender, WithClock(clock))
	target := testTarget("om_root")

	err := queue.Enqueue(context.Background(), target, protocol.Frame{Seq: 1, Type: protocol.TypeStdout, Payload: []byte("hello")})
	if !errors.Is(err, wantErr) {
		t.Fatalf("Enqueue error = %v, want %v", err, wantErr)
	}
	if got := len(sender.messages); got != 1 {
		t.Fatalf("send attempts = %d, want 1", got)
	}
	if _, ok := queue.LastSentAt(); ok {
		t.Fatal("LastSentAt set after failed send, want unset")
	}
}

func TestQueueKeepsPendingFramesWhenFlushSendFails(t *testing.T) {
	clock := newFakeClock()
	sender := &fakeSender{}
	queue := New(sender, WithClock(clock), WithSendCooldown(time.Second))
	target := testTarget("om_root")

	if err := queue.Enqueue(context.Background(), target, protocol.Frame{Seq: 1, Type: protocol.TypeStdout, Payload: []byte("first")}); err != nil {
		t.Fatalf("prime Enqueue returned error: %v", err)
	}
	if err := queue.Enqueue(context.Background(), target, protocol.Frame{Seq: 2, Type: protocol.TypeStdout, Payload: []byte("retry me")}); err != nil {
		t.Fatalf("queued Enqueue returned error: %v", err)
	}
	clock.Advance(time.Second)

	wantErr := errors.New("lark unavailable")
	sender.err = wantErr
	flushed, err := queue.FlushReady(context.Background())
	if !flushed {
		t.Fatal("FlushReady did not attempt pending frame")
	}
	if !errors.Is(err, wantErr) {
		t.Fatalf("FlushReady error = %v, want %v", err, wantErr)
	}
	if got := queue.PendingLen(); got != 1 {
		t.Fatalf("PendingLen after failed flush = %d, want 1", got)
	}

	sender.err = nil
	flushed, err = queue.FlushReady(context.Background())
	if err != nil {
		t.Fatalf("retry FlushReady returned error: %v", err)
	}
	if !flushed {
		t.Fatal("retry FlushReady did not flush")
	}
	if got := queue.PendingLen(); got != 0 {
		t.Fatalf("PendingLen after retry = %d, want 0", got)
	}
	assertMessageFrameSeqs(t, sender.messages[len(sender.messages)-1], 2)
}

func TestQueueCapsSendAttempts(t *testing.T) {
	clock := newFakeClock()
	wantErr := errors.New("still down")
	sender := &fakeSender{err: wantErr}
	queue := New(sender, WithClock(clock), WithMaxSendAttempts(2))
	target := testTarget("om_root")

	err := queue.Enqueue(context.Background(), target, protocol.Frame{Seq: 1, Type: protocol.TypeStdout, Payload: []byte("hello")})
	if !errors.Is(err, wantErr) {
		t.Fatalf("Enqueue error = %v, want %v", err, wantErr)
	}
	if got := len(sender.messages); got != 2 {
		t.Fatalf("send attempts = %d, want 2", got)
	}
}

func testTarget(rootMessageID string) Target {
	return Target{
		ChatID:        "oc_chat",
		RootMessageID: rootMessageID,
		MentionOpenID: "ou_peer",
	}
}

func textSize(_ Target, text string) (int, error) {
	return len(text), nil
}

func encodedLineLen(t *testing.T, frame protocol.Frame) int {
	t.Helper()
	line, err := protocol.EncodeFrameLine(frame)
	if err != nil {
		t.Fatalf("EncodeFrameLine returned error: %v", err)
	}
	return len(line)
}

func assertMessageFrames(t *testing.T, msg sentMessage, want ...protocol.Frame) {
	t.Helper()
	got := decodeMessageFrames(t, msg)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("message frames mismatch\nwant: %#v\n got: %#v", want, got)
	}
}

func assertMessageFrameSeqs(t *testing.T, msg sentMessage, want ...uint64) {
	t.Helper()
	frames := decodeMessageFrames(t, msg)
	got := make([]uint64, 0, len(frames))
	for _, frame := range frames {
		got = append(got, frame.Seq)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("message seqs = %v, want %v", got, want)
	}
}

func decodeMessageFrames(t *testing.T, msg sentMessage) []protocol.Frame {
	t.Helper()
	frames, err := protocol.DecodeFrames(msg.text)
	if err != nil {
		t.Fatalf("DecodeFrames(%q) returned error: %v", msg.text, err)
	}
	return frames
}

type sentMessage struct {
	root          bool
	chatID        string
	rootMessageID string
	mentionOpenID string
	text          string
}

type fakeSender struct {
	err      error
	messages []sentMessage
}

func (f *fakeSender) SendRootMessage(ctx context.Context, chatID, mentionOpenID, text string) (RootMessage, error) {
	if err := ctx.Err(); err != nil {
		return RootMessage{}, err
	}
	f.messages = append(f.messages, sentMessage{
		root:          true,
		chatID:        chatID,
		mentionOpenID: mentionOpenID,
		text:          text,
	})
	if f.err != nil {
		return RootMessage{}, f.err
	}
	return RootMessage{MessageID: "om_created"}, nil
}

func (f *fakeSender) ReplyRootMessage(ctx context.Context, chatID, rootMessageID, mentionOpenID, text string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	f.messages = append(f.messages, sentMessage{
		chatID:        chatID,
		rootMessageID: rootMessageID,
		mentionOpenID: mentionOpenID,
		text:          text,
	})
	if f.err != nil {
		return "", f.err
	}
	return "om_reply", nil
}

type fakeClock struct {
	now time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Date(2026, 6, 6, 12, 0, 0, 0, time.UTC)}
}

func (f *fakeClock) Now() time.Time {
	return f.now
}

func (f *fakeClock) Advance(d time.Duration) {
	f.now = f.now.Add(d)
}
