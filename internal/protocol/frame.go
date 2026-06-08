package protocol

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"
)

const version = "EOL1"

var (
	ErrInvalidFrame       = errors.New("invalid EOL1 frame")
	ErrInvalidFrameType   = errors.New("invalid EOL1 frame type")
	ErrSequenceGapTimeout = errors.New("sequence gap timeout")
	ErrSequencerExhausted = errors.New("sequence numbers exhausted")
)

type FrameType string

const (
	TypeStart     FrameType = "start"
	TypeStartAck  FrameType = "start_ack"
	TypeStdin     FrameType = "stdin"
	TypeStdout    FrameType = "stdout"
	TypeStderr    FrameType = "stderr"
	TypeResize    FrameType = "resize"
	TypeSignal    FrameType = "signal"
	TypeHeartbeat FrameType = "heartbeat"
	TypeExit      FrameType = "exit"
	TypeClose     FrameType = "close"
	TypeError     FrameType = "error"

	FrameStart     = TypeStart
	FrameStartAck  = TypeStartAck
	FrameStdin     = TypeStdin
	FrameStdout    = TypeStdout
	FrameStderr    = TypeStderr
	FrameResize    = TypeResize
	FrameSignal    = TypeSignal
	FrameHeartbeat = TypeHeartbeat
	FrameExit      = TypeExit
	FrameClose     = TypeClose
	FrameError     = TypeError
)

type Frame struct {
	Seq     uint64
	Type    FrameType
	Payload []byte
}

type HeartbeatConfig struct {
	Interval string `json:"interval"`
	Timeout  string `json:"timeout"`
}

type StartPayload struct {
	Cmd       string            `json:"cmd,omitempty"`
	Pty       bool              `json:"pty"`
	Cwd       string            `json:"cwd,omitempty"`
	Env       map[string]string `json:"env,omitempty"`
	Shell     string            `json:"shell,omitempty"`
	Rows      int               `json:"rows,omitempty"`
	Cols      int               `json:"cols,omitempty"`
	Heartbeat HeartbeatConfig   `json:"heartbeat"`
}

type StartAckPayload struct {
	Heartbeat HeartbeatConfig `json:"heartbeat"`
}

type ResizePayload struct {
	Rows int `json:"rows"`
	Cols int `json:"cols"`
}

type SignalPayload struct {
	Name string `json:"name"`
}

type HeartbeatPayload struct{}

type ExitPayload struct {
	Code int `json:"code"`
}

type ClosePayload struct {
	Reason string `json:"reason"`
}

const CloseReasonClientInterrupt = "client_interrupt"

type ErrorPayload struct {
	Message string `json:"message"`
	Detail  string `json:"detail,omitempty"`
}

func ValidFrameType(typ FrameType) bool {
	switch typ {
	case TypeStart, TypeStartAck, TypeStdin, TypeStdout, TypeStderr, TypeResize,
		TypeSignal, TypeHeartbeat, TypeExit, TypeClose, TypeError:
		return true
	default:
		return false
	}
}

func EncodeFrame(frame Frame) (string, error) {
	return EncodeFrameLine(frame)
}

func EncodeFrameLine(frame Frame) (string, error) {
	if err := validateFrame(frame); err != nil {
		return "", err
	}

	encodedPayload := base64.StdEncoding.EncodeToString(frame.Payload)
	return fmt.Sprintf("%s %d %s %s", version, frame.Seq, frame.Type, encodedPayload), nil
}

func DecodeFrame(line string) (Frame, error) {
	return DecodeFrameLine(line)
}

func DecodeFrameLine(line string) (Frame, error) {
	line = strings.TrimSuffix(line, "\r")
	parts := strings.SplitN(line, " ", 4)
	if len(parts) != 4 {
		return Frame{}, fmt.Errorf("%w: expected four space-separated fields", ErrInvalidFrame)
	}
	if parts[0] != version {
		return Frame{}, fmt.Errorf("%w: unsupported version %q", ErrInvalidFrame, parts[0])
	}
	if parts[1] == "" {
		return Frame{}, fmt.Errorf("%w: empty sequence", ErrInvalidFrame)
	}
	seq, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil || seq == 0 {
		return Frame{}, fmt.Errorf("%w: sequence must be a positive decimal integer", ErrInvalidFrame)
	}

	typ := FrameType(parts[2])
	if !ValidFrameType(typ) {
		return Frame{}, fmt.Errorf("%w: %w: %q", ErrInvalidFrame, ErrInvalidFrameType, parts[2])
	}

	payload, err := decodeBase64Payload(parts[3])
	if err != nil {
		return Frame{}, fmt.Errorf("%w: payload is not valid base64: %v", ErrInvalidFrame, err)
	}

	return Frame{
		Seq:     seq,
		Type:    typ,
		Payload: payload,
	}, nil
}

func EncodeFrames(frames []Frame) (string, error) {
	lines := make([]string, 0, len(frames))
	for _, frame := range frames {
		line, err := EncodeFrameLine(frame)
		if err != nil {
			return "", err
		}
		lines = append(lines, line)
	}
	return strings.Join(lines, "\n"), nil
}

func DecodeFrames(text string) ([]Frame, error) {
	lines := strings.Split(text, "\n")
	frames := make([]Frame, 0, len(lines))
	for i, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}
		frame, err := DecodeFrameLine(line)
		if err != nil {
			return nil, fmt.Errorf("line %d: %w", i+1, err)
		}
		frames = append(frames, frame)
	}
	if len(frames) == 0 {
		return nil, fmt.Errorf("%w: no frames found", ErrInvalidFrame)
	}
	return frames, nil
}

func NewFrame(seq uint64, typ FrameType, payload []byte) (Frame, error) {
	frame := Frame{
		Seq:     seq,
		Type:    typ,
		Payload: append([]byte(nil), payload...),
	}
	if err := validateFrame(frame); err != nil {
		return Frame{}, err
	}
	return frame, nil
}

func NewJSONFrame(seq uint64, typ FrameType, payload any) (Frame, error) {
	raw, err := MarshalJSONPayload(payload)
	if err != nil {
		return Frame{}, err
	}
	return NewFrame(seq, typ, raw)
}

func MarshalJSONPayload(payload any) ([]byte, error) {
	if payload == nil {
		return []byte("{}"), nil
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal payload: %w", err)
	}
	return raw, nil
}

func UnmarshalJSONPayload[T any](payload []byte) (T, error) {
	var out T
	if err := json.Unmarshal(payload, &out); err != nil {
		return out, fmt.Errorf("unmarshal payload: %w", err)
	}
	return out, nil
}

func DecodeJSONPayload[T any](frame Frame) (T, error) {
	return UnmarshalJSONPayload[T](frame.Payload)
}

type Sequencer struct {
	next      uint64
	exhausted bool
}

func NewSequencer() *Sequencer {
	return &Sequencer{next: 1}
}

func NewSequencerFrom(next uint64) (*Sequencer, error) {
	if next == 0 {
		return nil, fmt.Errorf("%w: next sequence must be positive", ErrInvalidFrame)
	}
	return &Sequencer{next: next}, nil
}

func (s *Sequencer) Peek() uint64 {
	s.ensureInitialized()
	if s.exhausted {
		return 0
	}
	return s.next
}

func (s *Sequencer) Next() uint64 {
	seq, err := s.NextSeq()
	if err != nil {
		panic(err)
	}
	return seq
}

func (s *Sequencer) NextSeq() (uint64, error) {
	s.ensureInitialized()
	if s.exhausted {
		return 0, ErrSequencerExhausted
	}
	seq := s.next
	if s.next == math.MaxUint64 {
		s.exhausted = true
	} else {
		s.next++
	}
	return seq, nil
}

func (s *Sequencer) Frame(typ FrameType, payload []byte) (Frame, error) {
	seq, err := s.NextSeq()
	if err != nil {
		return Frame{}, err
	}
	return NewFrame(seq, typ, payload)
}

func (s *Sequencer) JSONFrame(typ FrameType, payload any) (Frame, error) {
	seq, err := s.NextSeq()
	if err != nil {
		return Frame{}, err
	}
	return NewJSONFrame(seq, typ, payload)
}

type Clock interface {
	Now() time.Time
}

type RecvWindowOption func(*RecvWindow)

func WithClock(clock Clock) RecvWindowOption {
	return func(w *RecvWindow) {
		if clock != nil {
			w.now = clock.Now
		}
	}
}

func WithNowFunc(now func() time.Time) RecvWindowOption {
	return func(w *RecvWindow) {
		if now != nil {
			w.now = now
		}
	}
}

type RecvWindow struct {
	NextExpectedSeq uint64
	PendingFrames   map[uint64]Frame
	GapStartedAt    time.Time
	GapOpen         bool

	gapTimeout time.Duration
	now        func() time.Time
}

type RecvResult struct {
	Delivered   []Frame
	Duplicate   bool
	Buffered    bool
	GapTimedOut bool
}

func NewRecvWindow(gapTimeout time.Duration, opts ...RecvWindowOption) *RecvWindow {
	w := &RecvWindow{
		NextExpectedSeq: 1,
		PendingFrames:   make(map[uint64]Frame),
		gapTimeout:      gapTimeout,
		now:             time.Now,
	}
	for _, opt := range opts {
		opt(w)
	}
	return w
}

func (w *RecvWindow) Receive(frame Frame) ([]Frame, error) {
	result, err := w.Offer(frame)
	return result.Delivered, err
}

func (w *RecvWindow) ReceiveAt(frame Frame, now time.Time) ([]Frame, error) {
	result, err := w.OfferAt(frame, now)
	return result.Delivered, err
}

func (w *RecvWindow) Offer(frame Frame) (RecvResult, error) {
	w.init()
	return w.OfferAt(frame, w.now())
}

func (w *RecvWindow) OfferAt(frame Frame, now time.Time) (RecvResult, error) {
	w.init()
	if frame.Seq == 0 {
		return RecvResult{}, fmt.Errorf("%w: sequence must be positive", ErrInvalidFrame)
	}
	if err := w.CheckGapTimeoutAt(now); err != nil {
		return RecvResult{GapTimedOut: true}, err
	}

	if frame.Seq < w.NextExpectedSeq {
		return RecvResult{Duplicate: true}, nil
	}

	if frame.Seq > w.NextExpectedSeq {
		if _, ok := w.PendingFrames[frame.Seq]; ok {
			return RecvResult{Duplicate: true}, nil
		}
		w.PendingFrames[frame.Seq] = frame
		if !w.GapOpen {
			w.GapOpen = true
			w.GapStartedAt = now
		}
		return RecvResult{Buffered: true}, nil
	}

	delivered := []Frame{frame}
	w.NextExpectedSeq++
	for {
		pending, ok := w.PendingFrames[w.NextExpectedSeq]
		if !ok {
			break
		}
		delete(w.PendingFrames, w.NextExpectedSeq)
		delivered = append(delivered, pending)
		w.NextExpectedSeq++
	}

	if len(w.PendingFrames) == 0 {
		w.closeGap()
	} else if !w.GapOpen {
		w.GapOpen = true
		w.GapStartedAt = now
	}

	return RecvResult{Delivered: delivered}, nil
}

func (w *RecvWindow) CheckGapTimeout() error {
	w.init()
	return w.CheckGapTimeoutAt(w.now())
}

func (w *RecvWindow) CheckGapTimeoutAt(now time.Time) error {
	w.init()
	if !w.GapOpen {
		return nil
	}
	if len(w.PendingFrames) == 0 {
		w.closeGap()
		return nil
	}
	if w.gapTimeout <= 0 {
		return nil
	}
	if now.Before(w.GapStartedAt.Add(w.gapTimeout)) {
		return nil
	}

	pending := w.PendingSeqs()
	w.PendingFrames = make(map[uint64]Frame)
	w.closeGap()
	return fmt.Errorf("%w: expected seq %d, pending seqs %v", ErrSequenceGapTimeout, w.NextExpectedSeq, pending)
}

func (w *RecvWindow) PendingSeqs() []uint64 {
	w.init()
	seqs := make([]uint64, 0, len(w.PendingFrames))
	for seq := range w.PendingFrames {
		seqs = append(seqs, seq)
	}
	sort.Slice(seqs, func(i, j int) bool {
		return seqs[i] < seqs[j]
	})
	return seqs
}

func (w *RecvWindow) Reset() {
	w.NextExpectedSeq = 1
	w.PendingFrames = make(map[uint64]Frame)
	w.closeGap()
}

func (s *Sequencer) ensureInitialized() {
	if s.next == 0 {
		s.next = 1
	}
}

func (w *RecvWindow) init() {
	if w.NextExpectedSeq == 0 {
		w.NextExpectedSeq = 1
	}
	if w.PendingFrames == nil {
		w.PendingFrames = make(map[uint64]Frame)
	}
	if w.now == nil {
		w.now = time.Now
	}
}

func (w *RecvWindow) closeGap() {
	w.GapOpen = false
	w.GapStartedAt = time.Time{}
}

func validateFrame(frame Frame) error {
	if frame.Seq == 0 {
		return fmt.Errorf("%w: sequence must be positive", ErrInvalidFrame)
	}
	if !ValidFrameType(frame.Type) {
		return fmt.Errorf("%w: %w: %q", ErrInvalidFrame, ErrInvalidFrameType, frame.Type)
	}
	if strings.ContainsAny(string(frame.Type), " \t\r\n") {
		return fmt.Errorf("%w: %w: type must not contain whitespace", ErrInvalidFrame, ErrInvalidFrameType)
	}
	return nil
}

func decodeBase64Payload(payload string) ([]byte, error) {
	decoded, err := base64.StdEncoding.Strict().DecodeString(payload)
	if err == nil {
		return decoded, nil
	}
	if payload == "" {
		return []byte{}, nil
	}
	rawDecoded, rawErr := base64.RawStdEncoding.Strict().DecodeString(payload)
	if rawErr == nil {
		return rawDecoded, nil
	}
	return nil, err
}
