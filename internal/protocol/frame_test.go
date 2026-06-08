package protocol

import (
	"errors"
	"math"
	"reflect"
	"testing"
	"time"
)

func TestFrameGoldenEncodeDecode(t *testing.T) {
	frame := Frame{
		Seq:     1,
		Type:    TypeStart,
		Payload: []byte(`{"cmd":"uname -a","pty":false}`),
	}

	line, err := EncodeFrame(frame)
	if err != nil {
		t.Fatalf("EncodeFrame returned error: %v", err)
	}
	const want = "EOL1 1 start eyJjbWQiOiJ1bmFtZSAtYSIsInB0eSI6ZmFsc2V9"
	if line != want {
		t.Fatalf("encoded frame mismatch\nwant: %s\n got: %s", want, line)
	}

	decoded, err := DecodeFrame(line)
	if err != nil {
		t.Fatalf("DecodeFrame returned error: %v", err)
	}
	if decoded.Seq != frame.Seq || decoded.Type != frame.Type || string(decoded.Payload) != string(frame.Payload) {
		t.Fatalf("decoded frame mismatch: %#v", decoded)
	}
}

func TestMultiFrameEncodeDecode(t *testing.T) {
	frames := []Frame{
		{Seq: 1, Type: TypeStartAck, Payload: []byte(`{"heartbeat":{"interval":"10s","timeout":"30s"}}`)},
		{Seq: 2, Type: TypeStdout, Payload: []byte("Linux macmini ...\n")},
		{Seq: 3, Type: TypeExit, Payload: []byte(`{"code":0}`)},
	}

	text, err := EncodeFrames(frames)
	if err != nil {
		t.Fatalf("EncodeFrames returned error: %v", err)
	}
	const want = "EOL1 1 start_ack eyJoZWFydGJlYXQiOnsiaW50ZXJ2YWwiOiIxMHMiLCJ0aW1lb3V0IjoiMzBzIn19\n" +
		"EOL1 2 stdout TGludXggbWFjbWluaSAuLi4K\n" +
		"EOL1 3 exit eyJjb2RlIjowfQ=="
	if text != want {
		t.Fatalf("encoded frames mismatch\nwant: %s\n got: %s", want, text)
	}

	decoded, err := DecodeFrames("\n" + text + "\n")
	if err != nil {
		t.Fatalf("DecodeFrames returned error: %v", err)
	}
	if !reflect.DeepEqual(decoded, frames) {
		t.Fatalf("decoded frames mismatch\nwant: %#v\n got: %#v", frames, decoded)
	}
}

func TestDecodeFrameRejectsInvalidLines(t *testing.T) {
	tests := []struct {
		name string
		line string
		err  error
	}{
		{name: "empty", line: "", err: ErrInvalidFrame},
		{name: "wrong version", line: "EOL2 1 start e30=", err: ErrInvalidFrame},
		{name: "zero sequence", line: "EOL1 0 start e30=", err: ErrInvalidFrame},
		{name: "non decimal sequence", line: "EOL1 abc start e30=", err: ErrInvalidFrame},
		{name: "unknown type", line: "EOL1 1 mystery e30=", err: ErrInvalidFrameType},
		{name: "bad base64", line: "EOL1 1 start !!!", err: ErrInvalidFrame},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := DecodeFrame(tt.line)
			if !errors.Is(err, tt.err) {
				t.Fatalf("DecodeFrame error = %v, want %v", err, tt.err)
			}
		})
	}
}

func TestJSONPayloadHelpers(t *testing.T) {
	frame, err := NewJSONFrame(1, TypeStart, StartPayload{
		Cmd: "docker compose ps",
		Pty: false,
		Heartbeat: HeartbeatConfig{
			Interval: "10s",
			Timeout:  "30s",
		},
	})
	if err != nil {
		t.Fatalf("NewJSONFrame returned error: %v", err)
	}

	const wantPayload = `{"cmd":"docker compose ps","pty":false,"heartbeat":{"interval":"10s","timeout":"30s"}}`
	if string(frame.Payload) != wantPayload {
		t.Fatalf("payload mismatch\nwant: %s\n got: %s", wantPayload, string(frame.Payload))
	}

	payload, err := DecodeJSONPayload[StartPayload](frame)
	if err != nil {
		t.Fatalf("DecodeJSONPayload returned error: %v", err)
	}
	if payload.Cmd != "docker compose ps" || payload.Heartbeat.Timeout != "30s" {
		t.Fatalf("decoded payload mismatch: %#v", payload)
	}
}

func TestSequencerStartsAtOneAndBuildsFrames(t *testing.T) {
	seq := NewSequencer()
	if got := seq.Peek(); got != 1 {
		t.Fatalf("Peek() = %d, want 1", got)
	}
	if got := seq.Next(); got != 1 {
		t.Fatalf("first Next() = %d, want 1", got)
	}

	frame, err := seq.Frame(TypeStdout, []byte("ok"))
	if err != nil {
		t.Fatalf("Frame returned error: %v", err)
	}
	if frame.Seq != 2 || frame.Type != TypeStdout || string(frame.Payload) != "ok" {
		t.Fatalf("unexpected sequenced frame: %#v", frame)
	}
	if got := seq.Next(); got != 3 {
		t.Fatalf("third sequence = %d, want 3", got)
	}
}

func TestSequencerAllowsMaxUint64BeforeExhaustion(t *testing.T) {
	seq, err := NewSequencerFrom(math.MaxUint64)
	if err != nil {
		t.Fatalf("NewSequencerFrom returned error: %v", err)
	}
	got, err := seq.NextSeq()
	if err != nil {
		t.Fatalf("NextSeq max returned error: %v", err)
	}
	if got != math.MaxUint64 {
		t.Fatalf("NextSeq max = %d, want %d", got, uint64(math.MaxUint64))
	}
	if _, err := seq.NextSeq(); !errors.Is(err, ErrSequencerExhausted) {
		t.Fatalf("NextSeq after max error = %v, want ErrSequencerExhausted", err)
	}
}

func TestRecvWindowDeliversOrderedAndDropsProcessedDuplicates(t *testing.T) {
	window := NewRecvWindow(30 * time.Second)

	delivered, err := window.Receive(Frame{Seq: 1, Type: TypeStdout, Payload: []byte("a")})
	if err != nil {
		t.Fatalf("Receive returned error: %v", err)
	}
	assertSeqs(t, delivered, 1)
	if window.NextExpectedSeq != 2 {
		t.Fatalf("NextExpectedSeq = %d, want 2", window.NextExpectedSeq)
	}

	result, err := window.Offer(Frame{Seq: 1, Type: TypeStdout, Payload: []byte("a duplicate")})
	if err != nil {
		t.Fatalf("duplicate Offer returned error: %v", err)
	}
	if !result.Duplicate || len(result.Delivered) != 0 {
		t.Fatalf("duplicate result = %#v, want duplicate with no delivery", result)
	}
}

func TestRecvWindowBuffersOutOfOrderAndDrainsWhenGapFills(t *testing.T) {
	clock := newFakeClock()
	window := NewRecvWindow(30*time.Second, WithClock(clock))

	result, err := window.Offer(Frame{Seq: 3, Type: TypeStdout, Payload: []byte("c")})
	if err != nil {
		t.Fatalf("Offer seq 3 returned error: %v", err)
	}
	if !result.Buffered || !window.GapOpen {
		t.Fatalf("seq 3 result/window = %#v/%#v, want buffered open gap", result, window)
	}
	if !window.GapStartedAt.Equal(clock.Now()) {
		t.Fatalf("GapStartedAt = %s, want %s", window.GapStartedAt, clock.Now())
	}

	clock.Advance(time.Second)
	result, err = window.Offer(Frame{Seq: 3, Type: TypeStdout, Payload: []byte("c duplicate")})
	if err != nil {
		t.Fatalf("duplicate pending Offer returned error: %v", err)
	}
	if !result.Duplicate || result.Buffered {
		t.Fatalf("duplicate pending result = %#v, want duplicate", result)
	}
	if !window.GapStartedAt.Equal(clock.Now().Add(-time.Second)) {
		t.Fatalf("duplicate pending reset gap timer to %s", window.GapStartedAt)
	}

	delivered, err := window.Receive(Frame{Seq: 1, Type: TypeStdout, Payload: []byte("a")})
	if err != nil {
		t.Fatalf("Receive seq 1 returned error: %v", err)
	}
	assertSeqs(t, delivered, 1)
	if !window.GapOpen {
		t.Fatal("gap should remain open because seq 2 is still missing")
	}
	if !window.GapStartedAt.Equal(clock.Now()) {
		t.Fatalf("GapStartedAt after window move = %s, want %s", window.GapStartedAt, clock.Now())
	}
	if got := window.PendingSeqs(); !reflect.DeepEqual(got, []uint64{3}) {
		t.Fatalf("PendingSeqs = %v, want [3]", got)
	}

	delivered, err = window.Receive(Frame{Seq: 2, Type: TypeStdout, Payload: []byte("b")})
	if err != nil {
		t.Fatalf("Receive seq 2 returned error: %v", err)
	}
	assertSeqs(t, delivered, 2, 3)
	if window.GapOpen || len(window.PendingFrames) != 0 || window.NextExpectedSeq != 4 {
		t.Fatalf("window after drain = %#v, want closed gap and next 4", window)
	}
}

func TestRecvWindowResetsGapTimerWhenWindowMoves(t *testing.T) {
	clock := newFakeClock()
	window := NewRecvWindow(5*time.Second, WithClock(clock))

	if _, err := window.Receive(Frame{Seq: 3, Type: TypeStdout, Payload: []byte("c")}); err != nil {
		t.Fatalf("Receive seq 3 returned error: %v", err)
	}
	firstGapStartedAt := window.GapStartedAt
	clock.Advance(4 * time.Second)
	if _, err := window.Receive(Frame{Seq: 1, Type: TypeStdout, Payload: []byte("a")}); err != nil {
		t.Fatalf("Receive seq 1 returned error: %v", err)
	}
	if !window.GapStartedAt.After(firstGapStartedAt) || !window.GapStartedAt.Equal(clock.Now()) {
		t.Fatalf("GapStartedAt = %s, want reset to %s", window.GapStartedAt, clock.Now())
	}

	clock.Advance(4 * time.Second)
	if err := window.CheckGapTimeout(); err != nil {
		t.Fatalf("CheckGapTimeout before reset deadline returned error: %v", err)
	}
	clock.Advance(time.Second)
	if err := window.CheckGapTimeout(); !errors.Is(err, ErrSequenceGapTimeout) {
		t.Fatalf("CheckGapTimeout at reset deadline error = %v, want ErrSequenceGapTimeout", err)
	}
}

func TestRecvWindowGapTimeoutUsesFakeClock(t *testing.T) {
	clock := newFakeClock()
	window := NewRecvWindow(5*time.Second, WithClock(clock))

	if _, err := window.Receive(Frame{Seq: 2, Type: TypeStdout, Payload: []byte("b")}); err != nil {
		t.Fatalf("Receive seq 2 returned error: %v", err)
	}
	clock.Advance(5*time.Second - time.Nanosecond)
	if err := window.CheckGapTimeout(); err != nil {
		t.Fatalf("CheckGapTimeout before deadline returned error: %v", err)
	}

	clock.Advance(time.Nanosecond)
	err := window.CheckGapTimeout()
	if !errors.Is(err, ErrSequenceGapTimeout) {
		t.Fatalf("CheckGapTimeout at deadline error = %v, want ErrSequenceGapTimeout", err)
	}
	if window.GapOpen || len(window.PendingFrames) != 0 {
		t.Fatalf("window after timeout = %#v, want closed gap and no pending frames", window)
	}
}

func TestRecvWindowRejectsLateGapFillAfterTimeout(t *testing.T) {
	start := time.Date(2026, 6, 6, 12, 0, 0, 0, time.UTC)
	window := NewRecvWindow(5 * time.Second)

	if _, err := window.ReceiveAt(Frame{Seq: 2, Type: TypeStdout, Payload: []byte("b")}, start); err != nil {
		t.Fatalf("ReceiveAt seq 2 returned error: %v", err)
	}
	delivered, err := window.ReceiveAt(Frame{Seq: 1, Type: TypeStdout, Payload: []byte("a")}, start.Add(5*time.Second))
	if !errors.Is(err, ErrSequenceGapTimeout) {
		t.Fatalf("ReceiveAt late gap fill error = %v, want ErrSequenceGapTimeout", err)
	}
	if len(delivered) != 0 {
		t.Fatalf("late gap fill delivered %#v, want none", delivered)
	}
}

func assertSeqs(t *testing.T, frames []Frame, want ...uint64) {
	t.Helper()
	got := make([]uint64, 0, len(frames))
	for _, frame := range frames {
		got = append(got, frame.Seq)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("delivered seqs = %v, want %v", got, want)
	}
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
