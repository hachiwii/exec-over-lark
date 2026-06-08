package outbound

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/hachiwii/exec-over-lark/internal/lark"
	"github.com/hachiwii/exec-over-lark/internal/protocol"
)

func TestManagerRetriesRetryableFailureWithoutDroppingFrames(t *testing.T) {
	sender := &managerTestSender{
		errs:  []error{&lark.APIError{Status: 500, Path: "/send"}},
		calls: make(chan error, 2),
	}
	manager := startTestManager(t, sender, ManagerOptions{SendCooldown: 10 * time.Millisecond})

	if err := manager.RegisterConnection(RegisterConnectionRequest{
		ConnID:            "om_root",
		Role:              RoleRemote,
		Target:            testTarget("om_root"),
		NextSeq:           1,
		HeartbeatInterval: time.Hour,
	}); err != nil {
		t.Fatalf("RegisterConnection returned error: %v", err)
	}
	if err := manager.Enqueue(context.Background(), "om_root", protocol.TypeStdout, []byte("retry me")); err != nil {
		t.Fatalf("Enqueue returned error: %v", err)
	}

	if err := waitManagerSend(t, sender.calls); !lark.IsRetryableSendError(err) {
		t.Fatalf("first send error = %v, want retryable", err)
	}
	if err := waitManagerSend(t, sender.calls); err != nil {
		t.Fatalf("retry send error = %v, want nil", err)
	}

	messages := sender.Messages()
	if got := len(messages); got != 2 {
		t.Fatalf("messages = %d, want initial attempt and retry", got)
	}
	assertMessageFrameSeqs(t, messages[1], 1)
}

func TestManagerDropsConnectionOnPermanentSendFailure(t *testing.T) {
	wantErr := errors.New("permission denied")
	sender := &managerTestSender{errs: []error{wantErr}, calls: make(chan error, 1)}
	manager := startTestManager(t, sender, ManagerOptions{SendCooldown: time.Millisecond})
	dropped := make(chan DropReason, 1)

	if err := manager.RegisterConnection(RegisterConnectionRequest{
		ConnID:            "om_root",
		Role:              RoleRemote,
		Target:            testTarget("om_root"),
		NextSeq:           1,
		HeartbeatInterval: time.Hour,
		OnDrop: func(_ context.Context, reason DropReason) {
			dropped <- reason
		},
	}); err != nil {
		t.Fatalf("RegisterConnection returned error: %v", err)
	}
	if err := manager.Enqueue(context.Background(), "om_root", protocol.TypeStdout, []byte("drop me")); err != nil {
		t.Fatalf("Enqueue returned error: %v", err)
	}
	if err := waitManagerSend(t, sender.calls); !errors.Is(err, wantErr) {
		t.Fatalf("send error = %v, want %v", err, wantErr)
	}
	select {
	case reason := <-dropped:
		if !errors.Is(reason.Err, wantErr) || reason.ConnID != "om_root" {
			t.Fatalf("drop reason = %#v, want conn om_root error %v", reason, wantErr)
		}
	case <-time.After(time.Second):
		t.Fatal("connection was not dropped")
	}
	if err := manager.Enqueue(context.Background(), "om_root", protocol.TypeStdout, []byte("after drop")); !errors.Is(err, ErrConnectionMissing) {
		t.Fatalf("Enqueue after drop error = %v, want ErrConnectionMissing", err)
	}
}

func TestManagerMergesFramesWithinLimitWithoutSplitting(t *testing.T) {
	frame1 := protocol.Frame{Seq: 1, Type: protocol.TypeStdout, Payload: []byte("one")}
	frame2 := protocol.Frame{Seq: 2, Type: protocol.TypeStdout, Payload: []byte("two")}
	frame3 := protocol.Frame{Seq: 3, Type: protocol.TypeStdout, Payload: []byte("three")}
	text12, err := protocol.EncodeFrames([]protocol.Frame{frame1, frame2})
	if err != nil {
		t.Fatalf("EncodeFrames returned error: %v", err)
	}
	sender := &managerTestSender{calls: make(chan error, 1)}
	manager, err := NewManager(ManagerOptions{
		Sender:            sender,
		SendCooldown:      time.Hour,
		RequestLimitBytes: len(text12),
		RequestSizer:      textSize,
		HeartbeatInterval: time.Hour,
	})
	if err != nil {
		t.Fatalf("NewManager returned error: %v", err)
	}
	if err := manager.RegisterConnection(RegisterConnectionRequest{
		ConnID:            "om_root",
		Role:              RoleRemote,
		Target:            testTarget("om_root"),
		NextSeq:           1,
		HeartbeatInterval: time.Hour,
	}); err != nil {
		t.Fatalf("RegisterConnection returned error: %v", err)
	}
	for _, frame := range []protocol.Frame{frame1, frame2, frame3} {
		if err := manager.Enqueue(context.Background(), "om_root", frame.Type, frame.Payload); err != nil {
			t.Fatalf("Enqueue seq %d returned error: %v", frame.Seq, err)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		_ = manager.Run(ctx)
	}()
	if err := waitManagerSend(t, sender.calls); err != nil {
		t.Fatalf("send error = %v, want nil", err)
	}

	messages := sender.Messages()
	if got := len(messages); got != 1 {
		t.Fatalf("messages = %d, want 1", got)
	}
	assertMessageFrameSeqs(t, messages[0], 1, 2)
}

func TestManagerDropsOversizedSingleFrame(t *testing.T) {
	frame := protocol.Frame{Seq: 1, Type: protocol.TypeStdout, Payload: []byte("too large")}
	line, err := protocol.EncodeFrames([]protocol.Frame{frame})
	if err != nil {
		t.Fatalf("EncodeFrames returned error: %v", err)
	}
	sender := &managerTestSender{calls: make(chan error, 1)}
	manager := startTestManager(t, sender, ManagerOptions{
		SendCooldown:      time.Millisecond,
		RequestLimitBytes: len(line) - 1,
		RequestSizer:      textSize,
	})
	dropped := make(chan DropReason, 1)

	if err := manager.RegisterConnection(RegisterConnectionRequest{
		ConnID:            "om_root",
		Role:              RoleRemote,
		Target:            testTarget("om_root"),
		NextSeq:           1,
		HeartbeatInterval: time.Hour,
		OnDrop: func(_ context.Context, reason DropReason) {
			dropped <- reason
		},
	}); err != nil {
		t.Fatalf("RegisterConnection returned error: %v", err)
	}
	if err := manager.Enqueue(context.Background(), "om_root", frame.Type, frame.Payload); err != nil {
		t.Fatalf("Enqueue returned error: %v", err)
	}
	select {
	case reason := <-dropped:
		if !errors.Is(reason.Err, ErrFrameTooLarge) {
			t.Fatalf("drop error = %v, want ErrFrameTooLarge", reason.Err)
		}
	case <-time.After(time.Second):
		t.Fatal("oversized frame did not drop connection")
	}
	if got := len(sender.Messages()); got != 0 {
		t.Fatalf("messages = %d, want 0", got)
	}
}

func TestChatReplanWaitsForCooldownBeforeHeartbeat(t *testing.T) {
	now := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)
	chat := &chatQueue{
		cooldown:       500 * time.Millisecond,
		lastAttemptAt:  now,
		hasLastAttempt: true,
		conns: map[string]*connQueue{
			"om_root": {
				connID:            "om_root",
				heartbeatInterval: 10 * time.Second,
				nextHeartbeatAt:   now.Add(100 * time.Millisecond),
			},
		},
	}

	chat.replanLocked(now)
	want := now.Add(500 * time.Millisecond)
	if !chat.hasNextFlush || !chat.nextFlushAt.Equal(want) {
		t.Fatalf("nextFlushAt = %s/%v, want %s/true", chat.nextFlushAt, chat.hasNextFlush, want)
	}
}

func TestChatReplanUsesCooldownReadyForPendingFrames(t *testing.T) {
	now := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)
	chat := &chatQueue{
		cooldown:       500 * time.Millisecond,
		lastAttemptAt:  now,
		hasLastAttempt: true,
		conns: map[string]*connQueue{
			"om_root": {
				connID:          "om_root",
				nextHeartbeatAt: now.Add(time.Minute),
				frames: []queuedFrame{{
					frame:     protocol.Frame{Seq: 1, Type: protocol.TypeStdout, Payload: []byte("pending")},
					createdAt: now,
				}},
			},
		},
	}

	chat.replanLocked(now)
	want := now.Add(500 * time.Millisecond)
	if !chat.hasNextFlush || !chat.nextFlushAt.Equal(want) {
		t.Fatalf("nextFlushAt = %s/%v, want %s/true", chat.nextFlushAt, chat.hasNextFlush, want)
	}
}

func TestChatSelectsOldestNormalPendingItemAcrossRootAndConnection(t *testing.T) {
	now := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)
	rootFrame := protocol.Frame{Seq: 1, Type: protocol.TypeStart, Payload: []byte("{}")}
	replyFrame := protocol.Frame{Seq: 1, Type: protocol.TypeStdout, Payload: []byte("older")}
	chat := &chatQueue{
		limit: DefaultLarkTextRequestLimitBytes,
		sizer: textSize,
		conns: make(map[string]*connQueue),
		rootJobs: map[string]*rootOpenJob{
			"root:newer": {
				id:        "root:newer",
				role:      RoleLocal,
				target:    testTarget(""),
				frame:     rootFrame,
				createdAt: now.Add(10 * time.Millisecond),
				resultCh:  make(chan rootOpenResult, 1),
			},
		},
	}
	chat.conns["om_root"] = &connQueue{
		connID: "om_root",
		role:   RoleRemote,
		target: testTarget("om_root"),
		frames: []queuedFrame{{
			frame:     replyFrame,
			createdAt: now,
		}},
	}

	batch, err := chat.selectBatchLocked(now)
	if err != nil {
		t.Fatalf("selectBatchLocked returned error: %v", err)
	}
	if batch.kind != sendKindReply || batch.connID != "om_root" {
		t.Fatalf("batch = %#v, want older reply connection", batch)
	}
}

type managerSentMessage struct {
	root          bool
	role          Role
	chatID        string
	rootMessageID string
	mentionOpenID string
	text          string
}

type managerTestSender struct {
	mu       sync.Mutex
	errs     []error
	messages []managerSentMessage
	calls    chan error
	nextRoot int
}

func (s *managerTestSender) SendRootMessage(ctx context.Context, role Role, chatID, mentionOpenID, text string) (RootMessage, error) {
	if err := ctx.Err(); err != nil {
		return RootMessage{}, err
	}
	err := s.record(managerSentMessage{
		root:          true,
		role:          role,
		chatID:        chatID,
		mentionOpenID: mentionOpenID,
		text:          text,
	})
	if err != nil {
		return RootMessage{}, err
	}
	s.mu.Lock()
	s.nextRoot++
	messageID := "om_root"
	if s.nextRoot > 1 {
		messageID = "om_root_" + time.Unix(int64(s.nextRoot), 0).UTC().Format("150405")
	}
	s.mu.Unlock()
	return RootMessage{MessageID: messageID}, nil
}

func (s *managerTestSender) ReplyRootMessage(ctx context.Context, role Role, chatID, rootMessageID, mentionOpenID, text string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	err := s.record(managerSentMessage{
		role:          role,
		chatID:        chatID,
		rootMessageID: rootMessageID,
		mentionOpenID: mentionOpenID,
		text:          text,
	})
	if err != nil {
		return "", err
	}
	return "om_reply", nil
}

func (s *managerTestSender) record(msg managerSentMessage) error {
	s.mu.Lock()
	s.messages = append(s.messages, msg)
	var err error
	if len(s.errs) > 0 {
		err = s.errs[0]
		s.errs = s.errs[1:]
	}
	s.mu.Unlock()
	if s.calls != nil {
		s.calls <- err
	}
	return err
}

func (s *managerTestSender) Messages() []managerSentMessage {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]managerSentMessage(nil), s.messages...)
}

func startTestManager(t *testing.T, sender *managerTestSender, opts ManagerOptions) *Manager {
	t.Helper()
	opts.Sender = sender
	if opts.RequestLimitBytes == 0 {
		opts.RequestLimitBytes = DefaultLarkTextRequestLimitBytes
	}
	if opts.HeartbeatInterval == 0 {
		opts.HeartbeatInterval = time.Hour
	}
	manager, err := NewManager(opts)
	if err != nil {
		t.Fatalf("NewManager returned error: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() {
		_ = manager.Run(ctx)
	}()
	return manager
}

func waitManagerSend(t *testing.T, calls <-chan error) error {
	t.Helper()
	select {
	case err := <-calls:
		return err
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for outbound send")
		return nil
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

func assertMessageFrameSeqs(t *testing.T, msg managerSentMessage, seqs ...uint64) {
	t.Helper()
	frames, err := protocol.DecodeFrames(msg.text)
	if err != nil {
		t.Fatalf("DecodeFrames returned error: %v", err)
	}
	got := make([]uint64, 0, len(frames))
	for _, frame := range frames {
		got = append(got, frame.Seq)
	}
	if !reflect.DeepEqual(got, seqs) {
		t.Fatalf("message seqs = %v, want %v", got, seqs)
	}
}
