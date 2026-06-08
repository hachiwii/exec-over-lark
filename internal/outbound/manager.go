package outbound

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/hachiwii/exec-over-lark/internal/lark"
	"github.com/hachiwii/exec-over-lark/internal/protocol"
)

type Role string

const (
	RoleLocal  Role = "local"
	RoleRemote Role = "remote"
)

var (
	ErrManagerNotRunning = errors.New("outbound manager is not running")
	ErrConnectionExists  = errors.New("outbound connection already exists")
	ErrConnectionMissing = errors.New("outbound connection not found")
	ErrRootOpenCanceled  = errors.New("outbound root open canceled")
)

type DropReason struct {
	Target Target
	ConnID string
	Role   Role
	Err    error
}

type ManagerOptions struct {
	Sender            ManagerSender
	Clock             Clock
	SendCooldown      time.Duration
	RequestLimitBytes int
	RequestSizer      RequestSizer
	HeartbeatInterval time.Duration
	Logger            *slog.Logger
}

type Manager struct {
	mu sync.Mutex

	chats map[string]*chatQueue
	conns map[string]*chatQueue

	sender            ManagerSender
	clock             Clock
	cooldown          time.Duration
	limit             int
	sizer             RequestSizer
	heartbeatInterval time.Duration
	logger            *slog.Logger

	runCtx  context.Context
	cancel  context.CancelFunc
	running bool
}

type RegisterConnectionRequest struct {
	ConnID            string
	Role              Role
	Target            Target
	NextSeq           uint64
	HeartbeatInterval time.Duration
	OnDrop            func(context.Context, DropReason)
	OnDrained         func(context.Context)
}

type OpenRootRequest struct {
	ChatID            string
	MentionOpenID     string
	Role              Role
	RequestID         string
	InitialType       protocol.FrameType
	InitialPayload    any
	HeartbeatInterval time.Duration
	OnDrop            func(context.Context, DropReason)
}

type OpenRootResult struct {
	RootMessageID string
}

type Status struct {
	Checked        bool
	PendingFrames  int
	PendingTargets []Target
	LastAttemptAt  time.Time
	HasLastAttempt bool
	NextFlushAt    time.Time
	HasNextFlush   bool
	Chats          []ChatStatus
}

type ChatStatus struct {
	ChatID         string
	PendingFrames  int
	PendingTargets []Target
	LastAttemptAt  time.Time
	HasLastAttempt bool
	NextFlushAt    time.Time
	HasNextFlush   bool
	Connections    int
}

type ManagerSender interface {
	SendRootMessage(ctx context.Context, role Role, chatID, mentionOpenID, text string) (RootMessage, error)
	ReplyRootMessage(ctx context.Context, role Role, chatID, rootMessageID, mentionOpenID, text string) (messageID string, err error)
}

type chatQueue struct {
	manager *Manager

	mu sync.Mutex

	chatID   string
	cooldown time.Duration

	lastAttemptAt  time.Time
	hasLastAttempt bool

	nextFlushAt  time.Time
	hasNextFlush bool

	conns    map[string]*connQueue
	rootSeq  uint64
	rootJobs map[string]*rootOpenJob

	rescheduleCh chan struct{}

	sender ManagerSender
	clock  Clock
	sizer  RequestSizer
	limit  int
	logger *slog.Logger
}

type connQueue struct {
	connID string
	role   Role

	target Target

	seq *protocol.Sequencer

	heartbeatInterval time.Duration
	nextHeartbeatAt   time.Time

	frames []queuedFrame

	closeAfterDrained bool
	dropped           bool
	inFlight          int

	onDrop    func(context.Context, DropReason)
	onDrained func(context.Context)
}

type queuedFrame struct {
	frame     protocol.Frame
	createdAt time.Time
}

type rootOpenJob struct {
	id string

	role   Role
	target Target

	frame     protocol.Frame
	createdAt time.Time

	resultCh chan rootOpenResult

	inFlight bool
	onDrop   func(context.Context, DropReason)
}

type rootOpenResult struct {
	root RootMessage
	err  error
}

type sendKind int

const (
	sendKindNone sendKind = iota
	sendKindRoot
	sendKindReply
)

type sendBatch struct {
	kind sendKind

	connID string
	role   Role
	target Target

	rootJobID string

	frames       []protocol.Frame
	queuedFrames []queuedFrame
	frameCount   int
}

func NewManager(opts ManagerOptions) (*Manager, error) {
	if opts.Sender == nil {
		return nil, ErrNoSender
	}
	clock := opts.Clock
	if clock == nil {
		clock = realClock{}
	}
	cooldown := opts.SendCooldown
	if cooldown <= 0 {
		cooldown = DefaultSendCooldown
	}
	limit := opts.RequestLimitBytes
	if limit <= 0 {
		limit = DefaultLarkTextRequestLimitBytes
	}
	sizer := opts.RequestSizer
	if sizer == nil {
		sizer = defaultRequestSizer
	}
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &Manager{
		chats:             make(map[string]*chatQueue),
		conns:             make(map[string]*chatQueue),
		sender:            opts.Sender,
		clock:             clock,
		cooldown:          cooldown,
		limit:             limit,
		sizer:             sizer,
		heartbeatInterval: opts.HeartbeatInterval,
		logger:            logger,
	}, nil
}

func (m *Manager) Run(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	runCtx, cancel := context.WithCancel(ctx)

	m.mu.Lock()
	if m.running {
		m.mu.Unlock()
		cancel()
		return errors.New("outbound manager is already running")
	}
	m.runCtx = runCtx
	m.cancel = cancel
	m.running = true
	for _, chat := range m.chats {
		go chat.run(runCtx)
	}
	m.mu.Unlock()

	<-runCtx.Done()

	m.mu.Lock()
	m.running = false
	m.runCtx = nil
	m.cancel = nil
	m.mu.Unlock()
	return runCtx.Err()
}

func (m *Manager) RegisterConnection(req RegisterConnectionRequest) error {
	connID := strings.TrimSpace(req.ConnID)
	if connID == "" {
		return fmt.Errorf("%w: conn_id is required", ErrInvalidTarget)
	}
	if err := req.Target.validate(); err != nil {
		return err
	}
	nextSeq := req.NextSeq
	if nextSeq == 0 {
		nextSeq = 1
	}
	seq, err := protocol.NewSequencerFrom(nextSeq)
	if err != nil {
		return err
	}
	interval := req.HeartbeatInterval
	if interval <= 0 {
		interval = m.heartbeatInterval
	}

	m.mu.Lock()
	if _, ok := m.conns[connID]; ok {
		m.mu.Unlock()
		return fmt.Errorf("%w: %s", ErrConnectionExists, connID)
	}
	chat := m.getOrCreateChatLocked(req.Target.ChatID)
	m.conns[connID] = chat
	m.mu.Unlock()

	now := m.clock.Now()
	chat.mu.Lock()
	chat.conns[connID] = &connQueue{
		connID:            connID,
		role:              req.Role,
		target:            req.Target,
		seq:               seq,
		heartbeatInterval: interval,
		nextHeartbeatAt:   now.Add(interval),
		onDrop:            req.OnDrop,
		onDrained:         req.OnDrained,
	}
	chat.replanLocked(now)
	chat.mu.Unlock()
	chat.notifyReschedule()
	return nil
}

func (m *Manager) Enqueue(ctx context.Context, connID string, typ protocol.FrameType, payload []byte) error {
	connID = strings.TrimSpace(connID)
	chat, err := m.chatForConn(connID)
	if err != nil {
		return err
	}
	now := m.clock.Now()
	chat.mu.Lock()
	conn, ok := chat.conns[connID]
	if !ok || conn.dropped {
		chat.mu.Unlock()
		return fmt.Errorf("%w: %s", ErrConnectionMissing, connID)
	}
	frame, err := conn.seq.Frame(typ, cloneBytes(payload))
	if err != nil {
		chat.mu.Unlock()
		return err
	}
	conn.frames = append(conn.frames, queuedFrame{frame: frame, createdAt: now})
	chat.scheduleReadyLocked(now)
	chat.mu.Unlock()
	chat.notifyReschedule()
	return nil
}

func (m *Manager) EnqueueJSON(ctx context.Context, connID string, typ protocol.FrameType, payload any) error {
	raw, err := protocol.MarshalJSONPayload(payload)
	if err != nil {
		return err
	}
	return m.Enqueue(ctx, connID, typ, raw)
}

func (m *Manager) MarkCloseAfterDrained(connID string) {
	chat, err := m.chatForConn(strings.TrimSpace(connID))
	if err != nil {
		return
	}
	var drained func(context.Context)
	removeConn := false
	chat.mu.Lock()
	if conn, ok := chat.conns[connID]; ok {
		conn.closeAfterDrained = true
		if len(conn.frames) == 0 && conn.inFlight == 0 {
			drained = conn.onDrained
			removeConn = true
			delete(chat.conns, connID)
			chat.replanLocked(chat.clock.Now())
		}
	}
	chat.mu.Unlock()
	if removeConn {
		m.forgetConn(connID)
	}
	if drained != nil {
		drained(context.Background())
	}
	chat.notifyReschedule()
}

func (m *Manager) DropConnection(connID string) {
	connID = strings.TrimSpace(connID)
	m.mu.Lock()
	chat := m.conns[connID]
	delete(m.conns, connID)
	m.mu.Unlock()
	if chat == nil {
		return
	}
	chat.mu.Lock()
	delete(chat.conns, connID)
	chat.replanLocked(chat.clock.Now())
	chat.mu.Unlock()
	chat.notifyReschedule()
}

func (m *Manager) OpenRoot(ctx context.Context, req OpenRootRequest) (OpenRootResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	chatID := strings.TrimSpace(req.ChatID)
	mentionOpenID := strings.TrimSpace(req.MentionOpenID)
	if chatID == "" {
		return OpenRootResult{}, fmt.Errorf("%w: chat id is required", ErrInvalidTarget)
	}
	if mentionOpenID == "" {
		return OpenRootResult{}, fmt.Errorf("%w: mention open id is required", ErrInvalidTarget)
	}
	frame, err := protocol.NewJSONFrame(1, req.InitialType, req.InitialPayload)
	if err != nil {
		return OpenRootResult{}, err
	}

	m.mu.Lock()
	chat := m.getOrCreateChatLocked(chatID)
	m.mu.Unlock()

	now := m.clock.Now()
	job := &rootOpenJob{
		role:      req.Role,
		target:    Target{ChatID: chatID, MentionOpenID: mentionOpenID},
		frame:     frame,
		createdAt: now,
		resultCh:  make(chan rootOpenResult, 1),
		onDrop:    req.OnDrop,
	}
	chat.mu.Lock()
	chat.rootSeq++
	job.id = fmt.Sprintf("root:%s:%d", firstNonEmpty(req.RequestID, chatID), chat.rootSeq)
	chat.rootJobs[job.id] = job
	chat.scheduleReadyLocked(now)
	chat.mu.Unlock()
	chat.notifyReschedule()

	select {
	case result := <-job.resultCh:
		if result.err != nil {
			return OpenRootResult{}, result.err
		}
		return OpenRootResult{RootMessageID: result.root.MessageID}, nil
	case <-ctx.Done():
		chat.mu.Lock()
		if _, ok := chat.rootJobs[job.id]; ok {
			delete(chat.rootJobs, job.id)
			chat.replanLocked(chat.clock.Now())
		}
		chat.mu.Unlock()
		chat.notifyReschedule()
		return OpenRootResult{}, fmt.Errorf("%w: %w", ErrRootOpenCanceled, ctx.Err())
	}
}

func (m *Manager) Status() Status {
	m.mu.Lock()
	chats := make([]*chatQueue, 0, len(m.chats))
	for _, chat := range m.chats {
		chats = append(chats, chat)
	}
	m.mu.Unlock()

	status := Status{Checked: true, Chats: make([]ChatStatus, 0, len(chats))}
	for _, chat := range chats {
		chatStatus := chat.status()
		status.Chats = append(status.Chats, chatStatus)
		status.PendingFrames += chatStatus.PendingFrames
		status.PendingTargets = append(status.PendingTargets, chatStatus.PendingTargets...)
		if chatStatus.HasLastAttempt && (!status.HasLastAttempt || chatStatus.LastAttemptAt.After(status.LastAttemptAt)) {
			status.LastAttemptAt = chatStatus.LastAttemptAt
			status.HasLastAttempt = true
		}
		if chatStatus.HasNextFlush && (!status.HasNextFlush || chatStatus.NextFlushAt.Before(status.NextFlushAt)) {
			status.NextFlushAt = chatStatus.NextFlushAt
			status.HasNextFlush = true
		}
	}
	return status
}

func (m *Manager) getOrCreateChatLocked(chatID string) *chatQueue {
	chatID = strings.TrimSpace(chatID)
	if chat := m.chats[chatID]; chat != nil {
		return chat
	}
	chat := &chatQueue{
		manager:      m,
		chatID:       chatID,
		cooldown:     m.cooldown,
		conns:        make(map[string]*connQueue),
		rootJobs:     make(map[string]*rootOpenJob),
		rescheduleCh: make(chan struct{}, 1),
		sender:       m.sender,
		clock:        m.clock,
		sizer:        m.sizer,
		limit:        m.limit,
		logger:       m.logger,
	}
	m.chats[chatID] = chat
	if m.running && m.runCtx != nil {
		go chat.run(m.runCtx)
	}
	return chat
}

func (c *chatQueue) status() ChatStatus {
	c.mu.Lock()
	defer c.mu.Unlock()

	status := ChatStatus{
		ChatID:         c.chatID,
		LastAttemptAt:  c.lastAttemptAt,
		HasLastAttempt: c.hasLastAttempt,
		NextFlushAt:    c.nextFlushAt,
		HasNextFlush:   c.hasNextFlush,
		Connections:    len(c.conns),
	}
	for _, job := range c.rootJobs {
		if job.inFlight {
			continue
		}
		status.PendingFrames++
		status.PendingTargets = append(status.PendingTargets, job.target)
	}
	for _, conn := range c.conns {
		if len(conn.frames) == 0 {
			continue
		}
		status.PendingFrames += len(conn.frames)
		status.PendingTargets = append(status.PendingTargets, conn.target)
	}
	return status
}

func (m *Manager) chatForConn(connID string) (*chatQueue, error) {
	m.mu.Lock()
	chat := m.conns[connID]
	m.mu.Unlock()
	if chat == nil {
		return nil, fmt.Errorf("%w: %s", ErrConnectionMissing, connID)
	}
	return chat, nil
}

func (c *chatQueue) run(ctx context.Context) {
	timer := time.NewTimer(time.Hour)
	if !timer.Stop() {
		<-timer.C
	}
	defer timer.Stop()

	for {
		now := c.clock.Now()
		c.mu.Lock()
		wait, ok := c.currentWaitLocked(now)
		c.mu.Unlock()
		if !ok {
			select {
			case <-ctx.Done():
				return
			case <-c.rescheduleCh:
				continue
			}
		}
		if wait <= 0 {
			c.flushOnce(ctx)
			continue
		}
		resetTimer(timer, wait)
		select {
		case <-ctx.Done():
			return
		case <-c.rescheduleCh:
			continue
		case <-timer.C:
			c.flushOnce(ctx)
		}
	}
}

func (c *chatQueue) currentWaitLocked(now time.Time) (time.Duration, bool) {
	if !c.hasNextFlush {
		return 0, false
	}
	if !now.Before(c.nextFlushAt) {
		return 0, true
	}
	return c.nextFlushAt.Sub(now), true
}

func (c *chatQueue) flushOnce(ctx context.Context) {
	now := c.clock.Now()
	batch, dropReason, dropCB := c.prepareBatch(now)
	if dropReason.Err != nil {
		c.logger.Error(
			"outbound connection dropped before send",
			"chat_id", dropReason.Target.ChatID,
			"root_message_id", dropReason.Target.RootMessageID,
			"mention_open_id", dropReason.Target.MentionOpenID,
			"conn_id", dropReason.ConnID,
			"role", dropReason.Role,
			"error", dropReason.Err,
		)
		if dropReason.ConnID != "" {
			c.manager.forgetConn(dropReason.ConnID)
		}
		if dropCB != nil {
			dropCB(ctx, dropReason)
		}
		return
	}
	if batch.kind == sendKindNone {
		c.mu.Lock()
		c.replanLocked(now)
		c.mu.Unlock()
		c.notifyReschedule()
		return
	}

	go c.sendBatch(ctx, batch)
	c.notifyReschedule()
}

func (c *chatQueue) sendBatch(ctx context.Context, batch sendBatch) {
	text, err := protocol.EncodeFrames(batch.frames)
	if err != nil {
		c.commitSendError(ctx, batch, err)
		return
	}

	switch batch.kind {
	case sendKindRoot:
		root, err := c.sender.SendRootMessage(ctx, batch.role, batch.target.ChatID, batch.target.MentionOpenID, text)
		c.commitRootResult(ctx, batch, root, err)
	case sendKindReply:
		_, err := c.sender.ReplyRootMessage(ctx, batch.role, batch.target.ChatID, batch.target.RootMessageID, batch.target.MentionOpenID, text)
		c.commitReplyResult(ctx, batch, err)
	default:
		c.notifyReschedule()
	}
}

func (c *chatQueue) commitSendError(ctx context.Context, batch sendBatch, err error) {
	switch batch.kind {
	case sendKindRoot:
		c.commitRootResult(ctx, batch, RootMessage{}, err)
	case sendKindReply:
		c.commitReplyResult(ctx, batch, err)
	default:
		c.notifyReschedule()
	}
}

func (c *chatQueue) prepareBatch(now time.Time) (sendBatch, DropReason, func(context.Context, DropReason)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	batch, err := c.selectAndPopBatchLocked(now)
	if err != nil {
		var tooLarge *FrameTooLargeError
		if errors.As(err, &tooLarge) {
			reason, callback := c.dropConnLocked(tooLarge.ConnID, err)
			c.replanLocked(now)
			return sendBatch{}, reason, callback
		}
		return sendBatch{}, DropReason{Err: err}, nil
	}
	if batch.kind == sendKindNone {
		c.replanLocked(now)
		return sendBatch{}, DropReason{}, nil
	}
	c.markAttemptLocked(now)
	c.replanLocked(now)
	return batch, DropReason{}, nil
}

func (c *chatQueue) selectAndPopBatchLocked(now time.Time) (sendBatch, error) {
	if conn := c.selectHeartbeatConnLocked(now); conn != nil {
		if len(conn.frames) == 0 {
			frame, err := conn.seq.JSONFrame(protocol.TypeHeartbeat, protocol.HeartbeatPayload{})
			if err != nil {
				return sendBatch{}, err
			}
			conn.frames = append(conn.frames, queuedFrame{frame: frame, createdAt: now})
		}
		return c.buildAndPopReplyBatchLocked(conn)
	}
	job := c.selectRootJobLocked()
	conn := c.selectPendingConnLocked()
	if job != nil && (conn == nil || !conn.frames[0].createdAt.Before(job.createdAt)) {
		return c.buildAndPopRootBatchLocked(now, job)
	}
	if conn != nil {
		return c.buildAndPopReplyBatchLocked(conn)
	}
	return sendBatch{}, nil
}

func (c *chatQueue) selectHeartbeatConnLocked(now time.Time) *connQueue {
	var selected *connQueue
	for _, conn := range c.conns {
		if conn.dropped || conn.heartbeatInterval <= 0 || now.Before(conn.nextHeartbeatAt) {
			continue
		}
		if selected == nil || conn.nextHeartbeatAt.Before(selected.nextHeartbeatAt) {
			selected = conn
		}
	}
	return selected
}

func (c *chatQueue) selectRootJobLocked() *rootOpenJob {
	var selected *rootOpenJob
	for _, job := range c.rootJobs {
		if job.inFlight {
			continue
		}
		if selected == nil || job.createdAt.Before(selected.createdAt) {
			selected = job
		}
	}
	return selected
}

func (c *chatQueue) selectPendingConnLocked() *connQueue {
	var selected *connQueue
	for _, conn := range c.conns {
		if conn.dropped || len(conn.frames) == 0 {
			continue
		}
		if selected == nil || conn.frames[0].createdAt.Before(selected.frames[0].createdAt) {
			selected = conn
		}
	}
	return selected
}

func (c *chatQueue) buildAndPopRootBatchLocked(now time.Time, job *rootOpenJob) (sendBatch, error) {
	fits, err := c.fits(job.target, []protocol.Frame{job.frame})
	if err != nil {
		return sendBatch{}, err
	}
	if !fits {
		delete(c.rootJobs, job.id)
		job.resultCh <- rootOpenResult{err: &FrameTooLargeError{
			Target: job.target,
			Role:   job.role,
			Seq:    job.frame.Seq,
			Type:   job.frame.Type,
			Limit:  c.limit,
		}}
		c.replanLocked(now)
		return sendBatch{}, nil
	}
	job.inFlight = true
	return sendBatch{
		kind:       sendKindRoot,
		role:       job.role,
		target:     job.target,
		rootJobID:  job.id,
		frames:     []protocol.Frame{cloneFrame(job.frame)},
		frameCount: 1,
	}, nil
}

func (c *chatQueue) buildAndPopReplyBatchLocked(conn *connQueue) (sendBatch, error) {
	if len(conn.frames) == 0 {
		return sendBatch{}, nil
	}
	frames := make([]protocol.Frame, 0, len(conn.frames))
	for i, queued := range conn.frames {
		candidate := append(cloneFrames(frames), queued.frame)
		fits, err := c.fits(conn.target, candidate)
		if err != nil {
			return sendBatch{}, err
		}
		if !fits {
			if len(frames) == 0 {
				return sendBatch{}, &FrameTooLargeError{
					Target: conn.target,
					ConnID: conn.connID,
					Role:   conn.role,
					Seq:    queued.frame.Seq,
					Type:   queued.frame.Type,
					Limit:  c.limit,
				}
			}
			batch := sendBatch{
				kind:       sendKindReply,
				connID:     conn.connID,
				role:       conn.role,
				target:     conn.target,
				frames:     cloneFrames(frames),
				frameCount: len(frames),
			}
			c.popReplyBatchLocked(conn, &batch)
			return batch, nil
		}
		frames = candidate
		if i == len(conn.frames)-1 {
			break
		}
	}
	batch := sendBatch{
		kind:       sendKindReply,
		connID:     conn.connID,
		role:       conn.role,
		target:     conn.target,
		frames:     cloneFrames(frames),
		frameCount: len(frames),
	}
	c.popReplyBatchLocked(conn, &batch)
	return batch, nil
}

func (c *chatQueue) popReplyBatchLocked(conn *connQueue, batch *sendBatch) {
	if batch.frameCount > len(conn.frames) {
		batch.frameCount = len(conn.frames)
	}
	batch.queuedFrames = cloneQueuedFrames(conn.frames[:batch.frameCount])
	conn.frames = cloneQueuedFrames(conn.frames[batch.frameCount:])
	conn.inFlight++
}

func (c *chatQueue) fits(target Target, frames []protocol.Frame) (bool, error) {
	text, err := protocol.EncodeFrames(frames)
	if err != nil {
		return false, err
	}
	size, err := c.sizer(target, text)
	if err != nil {
		return false, err
	}
	return size <= c.limit, nil
}

func (c *chatQueue) commitRootResult(ctx context.Context, batch sendBatch, root RootMessage, err error) {
	now := c.clock.Now()
	var resultCh chan rootOpenResult
	var dropReason DropReason
	var dropCB func(context.Context, DropReason)
	c.mu.Lock()
	if job := c.rootJobs[batch.rootJobID]; job != nil {
		job.inFlight = false
		if err == nil {
			delete(c.rootJobs, batch.rootJobID)
			resultCh = job.resultCh
		} else if lark.IsRetryableSendError(err) {
			c.logger.Warn("outbound root send failed; retrying after cooldown", "chat_id", batch.target.ChatID, "error", err)
		} else {
			delete(c.rootJobs, batch.rootJobID)
			resultCh = job.resultCh
			dropReason = DropReason{Target: batch.target, Role: batch.role, Err: err}
			dropCB = job.onDrop
		}
	}
	c.replanLocked(now)
	c.mu.Unlock()
	if resultCh != nil {
		resultCh <- rootOpenResult{root: root, err: err}
	}
	if dropCB != nil {
		dropCB(ctx, dropReason)
	}
	c.notifyReschedule()
}

func (c *chatQueue) commitReplyResult(ctx context.Context, batch sendBatch, err error) {
	now := c.clock.Now()
	var dropReason DropReason
	var dropCB func(context.Context, DropReason)
	var dropConnID string
	var drainedCB func(context.Context)
	var drainedConnID string
	c.mu.Lock()
	conn := c.conns[batch.connID]
	if conn != nil {
		if conn.inFlight > 0 {
			conn.inFlight--
		}
		switch {
		case err == nil:
			if conn.heartbeatInterval > 0 {
				conn.nextHeartbeatAt = now.Add(conn.heartbeatInterval)
			}
			if conn.closeAfterDrained && len(conn.frames) == 0 && conn.inFlight == 0 {
				drainedCB = conn.onDrained
				drainedConnID = conn.connID
				delete(c.conns, conn.connID)
			}
		case lark.IsRetryableSendError(err):
			conn.frames = append(cloneQueuedFrames(batch.queuedFrames), conn.frames...)
			c.logger.Warn(
				"outbound reply send failed; retrying after cooldown",
				"chat_id", batch.target.ChatID,
				"root_message_id", batch.target.RootMessageID,
				"conn_id", batch.connID,
				"role", batch.role,
				"error", err,
			)
		default:
			dropReason, dropCB = c.dropConnLocked(batch.connID, err)
			dropConnID = batch.connID
		}
	}
	c.replanLocked(now)
	c.mu.Unlock()
	if drainedConnID != "" {
		c.manager.forgetConn(drainedConnID)
	}
	if drainedCB != nil {
		drainedCB(ctx)
	}
	if dropConnID != "" {
		c.manager.forgetConn(dropConnID)
	}
	if dropCB != nil {
		dropCB(ctx, dropReason)
	}
	c.notifyReschedule()
}

func (c *chatQueue) dropConnLocked(connID string, err error) (DropReason, func(context.Context, DropReason)) {
	conn := c.conns[connID]
	if conn == nil {
		return DropReason{}, nil
	}
	conn.dropped = true
	delete(c.conns, connID)
	return DropReason{
		Target: conn.target,
		ConnID: conn.connID,
		Role:   conn.role,
		Err:    err,
	}, conn.onDrop
}

func (m *Manager) forgetConn(connID string) {
	m.mu.Lock()
	delete(m.conns, connID)
	m.mu.Unlock()
}

func (c *chatQueue) markAttemptLocked(now time.Time) {
	c.lastAttemptAt = now
	c.hasLastAttempt = true
}

func (c *chatQueue) scheduleReadyLocked(now time.Time) {
	readyAt := now
	if c.hasLastAttempt {
		readyAt = c.lastAttemptAt.Add(c.cooldown)
	}
	c.scheduleEarlierLocked(readyAt)
}

func (c *chatQueue) scheduleEarlierLocked(next time.Time) {
	if !c.hasNextFlush || next.Before(c.nextFlushAt) {
		c.nextFlushAt = next
		c.hasNextFlush = true
	}
}

func (c *chatQueue) replanLocked(now time.Time) {
	if c.hasPendingLocked() {
		next := now
		if c.hasLastAttempt {
			next = c.lastAttemptAt.Add(c.cooldown)
		}
		c.nextFlushAt = next
		c.hasNextFlush = true
		return
	}
	earliest, ok := c.earliestHeartbeatLocked()
	if !ok {
		c.hasNextFlush = false
		c.nextFlushAt = time.Time{}
		return
	}
	cooldownReadyAt := now
	if c.hasLastAttempt {
		cooldownReadyAt = c.lastAttemptAt.Add(c.cooldown)
	}
	if earliest.Before(cooldownReadyAt) {
		earliest = cooldownReadyAt
	}
	c.nextFlushAt = earliest
	c.hasNextFlush = true
}

func (c *chatQueue) hasPendingLocked() bool {
	for _, job := range c.rootJobs {
		if !job.inFlight {
			return true
		}
	}
	for _, conn := range c.conns {
		if len(conn.frames) > 0 {
			return true
		}
	}
	return false
}

func (c *chatQueue) earliestHeartbeatLocked() (time.Time, bool) {
	var earliest time.Time
	ok := false
	for _, conn := range c.conns {
		if conn.heartbeatInterval <= 0 {
			continue
		}
		if !ok || conn.nextHeartbeatAt.Before(earliest) {
			earliest = conn.nextHeartbeatAt
			ok = true
		}
	}
	return earliest, ok
}

func (c *chatQueue) notifyReschedule() {
	select {
	case c.rescheduleCh <- struct{}{}:
	default:
	}
}

func resetTimer(timer *time.Timer, wait time.Duration) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	timer.Reset(wait)
}

func cloneFrame(frame protocol.Frame) protocol.Frame {
	return protocol.Frame{
		Seq:     frame.Seq,
		Type:    frame.Type,
		Payload: cloneBytes(frame.Payload),
	}
}

func cloneQueuedFrames(frames []queuedFrame) []queuedFrame {
	if len(frames) == 0 {
		return nil
	}
	out := make([]queuedFrame, len(frames))
	for i, frame := range frames {
		out[i] = queuedFrame{
			frame:     cloneFrame(frame.frame),
			createdAt: frame.createdAt,
		}
	}
	return out
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}
