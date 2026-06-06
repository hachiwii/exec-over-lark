package session

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/hachiwii/exec-over-lark/internal/outbound"
	"github.com/hachiwii/exec-over-lark/internal/protocol"
)

func TestLocalRoutesOutputAndCleansUpOnExit(t *testing.T) {
	clock := newFakeClock()
	queue := &fakeQueue{}
	manager := New(WithClock(clock), WithOutboundQueue(queue))
	sub := &fakeSubscriber{}

	if err := manager.RegisterLocal(LocalStart{
		RequestID:     "req-1",
		Host:          "macmini",
		RootMessageID: "om_root",
		ChatID:        "oc_chat",
		PeerBotOpenID: "ou_server",
	}, sub); err != nil {
		t.Fatalf("RegisterLocal returned error: %v", err)
	}

	frames := []protocol.Frame{
		jsonFrame(t, 1, protocol.TypeStartAck, protocol.StartAckPayload{Heartbeat: protocol.HeartbeatConfig{Interval: "10s", Timeout: "45s"}}),
		{Seq: 2, Type: protocol.TypeStdout, Payload: []byte("out")},
		{Seq: 3, Type: protocol.TypeStderr, Payload: []byte("err")},
		jsonFrame(t, 4, protocol.TypeExit, protocol.ExitPayload{Code: 7}),
	}
	if err := manager.ReceiveLocal(context.Background(), InboundMessage{
		RootMessageID: "om_root",
		ChatID:        "oc_chat",
		SenderOpenID:  "ou_server",
		Frames:        frames,
	}); err != nil {
		t.Fatalf("ReceiveLocal returned error: %v", err)
	}

	gotTypes := localEventTypes(sub.events)
	wantTypes := []LocalEventType{LocalEventStdout, LocalEventStderr, LocalEventExit}
	if !reflect.DeepEqual(gotTypes, wantTypes) {
		t.Fatalf("events = %v, want %v", gotTypes, wantTypes)
	}
	if string(sub.events[0].Bytes) != "out" || string(sub.events[1].Bytes) != "err" || sub.events[2].Code != 7 {
		t.Fatalf("unexpected routed events: %#v", sub.events)
	}
	if _, ok := manager.ConnIDForRequest("req-1"); ok {
		t.Fatal("request mapping still exists after exit")
	}
	if len(manager.LocalSessions()) != 0 {
		t.Fatalf("LocalSessions after exit = %#v, want none", manager.LocalSessions())
	}
	if !sub.closed {
		t.Fatal("subscriber was not closed during session cleanup")
	}
}

func TestLocalHeartbeatAndPeerTimeoutUseFakeClock(t *testing.T) {
	clock := newFakeClock()
	queue := &fakeQueue{}
	manager := New(
		WithClock(clock),
		WithOutboundQueue(queue),
		WithHeartbeatInterval(10*time.Second),
		WithHeartbeatTimeout(30*time.Second),
	)
	sub := &fakeSubscriber{}

	if err := manager.RegisterLocal(LocalStart{
		RequestID:     "req-1",
		RootMessageID: "om_root",
		ChatID:        "oc_chat",
		PeerBotOpenID: "ou_server",
	}, sub); err != nil {
		t.Fatalf("RegisterLocal returned error: %v", err)
	}

	clock.Advance(10 * time.Second)
	if err := manager.Tick(context.Background()); err != nil {
		t.Fatalf("Tick at heartbeat deadline returned error: %v", err)
	}
	if len(queue.items) != 1 {
		t.Fatalf("queued frames = %d, want 1 heartbeat", len(queue.items))
	}
	if queue.items[0].target.RootMessageID != "om_root" || queue.items[0].target.MentionOpenID != "ou_server" {
		t.Fatalf("heartbeat target = %#v", queue.items[0].target)
	}
	assertFrame(t, queue.items[0].frames[0], 2, protocol.TypeHeartbeat)

	clock.Advance(20*time.Second + time.Nanosecond)
	err := manager.Tick(context.Background())
	if err == nil || !containsLocalEvent(sub.events, LocalEventError) {
		t.Fatalf("Tick after peer timeout error/events = %v/%#v, want timeout error event", err, sub.events)
	}
	if len(manager.LocalSessions()) != 0 {
		t.Fatalf("LocalSessions after peer timeout = %#v, want none", manager.LocalSessions())
	}
}

func TestLocalReceiveWindowGapTimeoutDeliversErrorAndCleansUp(t *testing.T) {
	clock := newFakeClock()
	queue := &fakeQueue{}
	manager := New(
		WithClock(clock),
		WithOutboundQueue(queue),
		WithSequenceGapTimeout(5*time.Second),
	)
	sub := &fakeSubscriber{}

	if err := manager.RegisterLocal(LocalStart{
		RequestID:     "req-1",
		RootMessageID: "om_root",
		ChatID:        "oc_chat",
		PeerBotOpenID: "ou_server",
	}, sub); err != nil {
		t.Fatalf("RegisterLocal returned error: %v", err)
	}

	if err := manager.ReceiveLocal(context.Background(), InboundMessage{
		RootMessageID: "om_root",
		ChatID:        "oc_chat",
		SenderOpenID:  "ou_server",
		Frames:        []protocol.Frame{{Seq: 2, Type: protocol.TypeStdout, Payload: []byte("late")}},
	}); err != nil {
		t.Fatalf("ReceiveLocal seq 2 returned error: %v", err)
	}
	snapshot, ok := manager.LocalSnapshot("om_root")
	if !ok || !reflect.DeepEqual(snapshot.RXPendingSeqs, []uint64{2}) {
		t.Fatalf("snapshot pending seqs = %#v/%v, want [2]", snapshot, ok)
	}

	clock.Advance(5 * time.Second)
	err := manager.Tick(context.Background())
	if !errors.Is(err, protocol.ErrSequenceGapTimeout) {
		t.Fatalf("Tick gap timeout error = %v, want ErrSequenceGapTimeout", err)
	}
	if !containsLocalEvent(sub.events, LocalEventError) {
		t.Fatalf("subscriber events = %#v, want error event", sub.events)
	}
	if len(manager.LocalSessions()) != 0 {
		t.Fatalf("LocalSessions after gap timeout = %#v, want none", manager.LocalSessions())
	}
}

func TestRemoteAcceptStartSendsAckAndRoutesControls(t *testing.T) {
	clock := newFakeClock()
	queue := &fakeQueue{}
	manager := New(WithClock(clock), WithOutboundQueue(queue))
	receiver := &fakeRemoteReceiver{}
	start := protocol.StartPayload{
		Cmd: "cat",
		Pty: false,
		Heartbeat: protocol.HeartbeatConfig{
			Interval: "10s",
			Timeout:  "40s",
		},
	}

	snapshot, err := manager.AcceptRemoteStart(context.Background(), InboundMessage{
		RootMessageID: "om_root",
		MessageID:     "om_root",
		ChatID:        "oc_chat",
		SenderOpenID:  "ou_client",
		IsRoot:        true,
		Frames:        []protocol.Frame{jsonFrame(t, 1, protocol.TypeStart, start)},
	}, receiver)
	if err != nil {
		t.Fatalf("AcceptRemoteStart returned error: %v", err)
	}
	if snapshot.ConnID != "om_root" || snapshot.PeerHeartbeatTimeout != 40*time.Second {
		t.Fatalf("snapshot = %#v, want root conn and 40s peer timeout", snapshot)
	}
	if len(queue.items) != 1 {
		t.Fatalf("queued frames = %d, want start_ack", len(queue.items))
	}
	assertFrame(t, queue.items[0].frames[0], 1, protocol.TypeStartAck)
	if len(receiver.events) != 1 || receiver.events[0].Type != RemoteEventStart || receiver.events[0].Start.Cmd != "cat" {
		t.Fatalf("receiver start events = %#v", receiver.events)
	}

	frames := []protocol.Frame{
		{Seq: 2, Type: protocol.TypeStdin, Payload: []byte("hello\n")},
		jsonFrame(t, 3, protocol.TypeResize, protocol.ResizePayload{Rows: 24, Cols: 80}),
		jsonFrame(t, 4, protocol.TypeSignal, protocol.SignalPayload{Name: "INT"}),
	}
	if err := manager.ReceiveRemote(context.Background(), InboundMessage{
		RootMessageID: "om_root",
		ChatID:        "oc_chat",
		SenderOpenID:  "ou_client",
		Frames:        frames,
	}); err != nil {
		t.Fatalf("ReceiveRemote returned error: %v", err)
	}

	gotTypes := remoteEventTypes(receiver.events)
	wantTypes := []RemoteEventType{RemoteEventStart, RemoteEventStdin, RemoteEventResize, RemoteEventSignal}
	if !reflect.DeepEqual(gotTypes, wantTypes) {
		t.Fatalf("remote event types = %v, want %v", gotTypes, wantTypes)
	}
	if string(receiver.events[1].Bytes) != "hello\n" || receiver.events[2].Rows != 24 || receiver.events[2].Cols != 80 || receiver.events[3].Name != "INT" {
		t.Fatalf("unexpected remote events = %#v", receiver.events)
	}
}

func TestRemoteGapTimeoutSendsErrorAndCleansUp(t *testing.T) {
	clock := newFakeClock()
	queue := &fakeQueue{}
	manager := New(
		WithClock(clock),
		WithOutboundQueue(queue),
		WithSequenceGapTimeout(5*time.Second),
	)
	receiver := &fakeRemoteReceiver{}

	if _, err := manager.AcceptRemoteStart(context.Background(), InboundMessage{
		RootMessageID: "om_root",
		ChatID:        "oc_chat",
		SenderOpenID:  "ou_client",
		IsRoot:        true,
		Frames:        []protocol.Frame{jsonFrame(t, 1, protocol.TypeStart, protocol.StartPayload{Heartbeat: protocol.HeartbeatConfig{Timeout: "30s"}})},
	}, receiver); err != nil {
		t.Fatalf("AcceptRemoteStart returned error: %v", err)
	}

	if err := manager.ReceiveRemote(context.Background(), InboundMessage{
		RootMessageID: "om_root",
		ChatID:        "oc_chat",
		SenderOpenID:  "ou_client",
		Frames:        []protocol.Frame{{Seq: 3, Type: protocol.TypeStdin, Payload: []byte("buffered")}},
	}); err != nil {
		t.Fatalf("ReceiveRemote seq 3 returned error: %v", err)
	}
	clock.Advance(5 * time.Second)
	err := manager.Tick(context.Background())
	if !errors.Is(err, protocol.ErrSequenceGapTimeout) {
		t.Fatalf("Tick error = %v, want ErrSequenceGapTimeout", err)
	}
	if !containsRemoteEvent(receiver.events, RemoteEventSequenceGapTimeout) {
		t.Fatalf("remote events = %#v, want sequence gap timeout", receiver.events)
	}
	if len(queue.items) != 2 {
		t.Fatalf("queued frames = %d, want start_ack and error", len(queue.items))
	}
	assertFrame(t, queue.items[1].frames[0], 2, protocol.TypeError)
	if len(manager.RemoteSessions()) != 0 {
		t.Fatalf("RemoteSessions after gap timeout = %#v, want none", manager.RemoteSessions())
	}
}

func TestUnauthorizedPeerIsRejected(t *testing.T) {
	manager := New(WithOutboundQueue(&fakeQueue{}))
	if err := manager.RegisterLocal(LocalStart{
		RequestID:     "req-1",
		RootMessageID: "om_root",
		ChatID:        "oc_chat",
		PeerBotOpenID: "ou_server",
	}, &fakeSubscriber{}); err != nil {
		t.Fatalf("RegisterLocal returned error: %v", err)
	}

	err := manager.ReceiveLocal(context.Background(), InboundMessage{
		RootMessageID: "om_root",
		ChatID:        "oc_other",
		SenderOpenID:  "ou_server",
		Frames:        []protocol.Frame{{Seq: 1, Type: protocol.TypeHeartbeat, Payload: []byte("{}")}},
	})
	if !errors.Is(err, ErrUnauthorizedPeer) {
		t.Fatalf("ReceiveLocal wrong chat error = %v, want ErrUnauthorizedPeer", err)
	}
}

type fakeClock struct {
	now time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Date(2026, 6, 6, 12, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.now = c.now.Add(d)
}

type queuedItem struct {
	target outbound.Target
	frames []protocol.Frame
}

type fakeQueue struct {
	items []queuedItem
}

func (q *fakeQueue) Enqueue(_ context.Context, target outbound.Target, frames ...protocol.Frame) error {
	q.items = append(q.items, queuedItem{
		target: target,
		frames: cloneFrames(frames),
	})
	return nil
}

type fakeSubscriber struct {
	events []LocalEvent
	closed bool
}

func (s *fakeSubscriber) Deliver(_ context.Context, event LocalEvent) error {
	event.Bytes = append([]byte(nil), event.Bytes...)
	s.events = append(s.events, event)
	return nil
}

func (s *fakeSubscriber) Close() error {
	s.closed = true
	return nil
}

type fakeRemoteReceiver struct {
	events []RemoteEvent
	closed bool
}

func (r *fakeRemoteReceiver) Deliver(_ context.Context, event RemoteEvent) error {
	event.Bytes = append([]byte(nil), event.Bytes...)
	r.events = append(r.events, event)
	return nil
}

func (r *fakeRemoteReceiver) Close() error {
	r.closed = true
	return nil
}

func jsonFrame(t *testing.T, seq uint64, typ protocol.FrameType, payload any) protocol.Frame {
	t.Helper()
	frame, err := protocol.NewJSONFrame(seq, typ, payload)
	if err != nil {
		t.Fatalf("NewJSONFrame returned error: %v", err)
	}
	return frame
}

func assertFrame(t *testing.T, frame protocol.Frame, seq uint64, typ protocol.FrameType) {
	t.Helper()
	if frame.Seq != seq || frame.Type != typ {
		t.Fatalf("frame = %#v, want seq %d type %s", frame, seq, typ)
	}
}

func cloneFrames(frames []protocol.Frame) []protocol.Frame {
	out := make([]protocol.Frame, len(frames))
	for i, frame := range frames {
		out[i] = protocol.Frame{
			Seq:     frame.Seq,
			Type:    frame.Type,
			Payload: append([]byte(nil), frame.Payload...),
		}
	}
	return out
}

func localEventTypes(events []LocalEvent) []LocalEventType {
	out := make([]LocalEventType, 0, len(events))
	for _, event := range events {
		out = append(out, event.Type)
	}
	return out
}

func remoteEventTypes(events []RemoteEvent) []RemoteEventType {
	out := make([]RemoteEventType, 0, len(events))
	for _, event := range events {
		out = append(out, event.Type)
	}
	return out
}

func containsLocalEvent(events []LocalEvent, typ LocalEventType) bool {
	for _, event := range events {
		if event.Type == typ {
			return true
		}
	}
	return false
}

func containsRemoteEvent(events []RemoteEvent, typ RemoteEventType) bool {
	for _, event := range events {
		if event.Type == typ {
			return true
		}
	}
	return false
}
