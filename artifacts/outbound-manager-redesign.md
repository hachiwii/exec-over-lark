# Outbound Manager Redesign

## Background

client daemon 和 server daemon 可能运行在同一个 `elarkd` 进程中，也可能只启动其中一侧。无论哪种模式，所有出站飞书消息都必须经过同一个 `outbound.Manager`，由它按 `chat_id` 统一调度发送。

旧实现的问题：

- client/server 各自维护 outbound queue，同一个 `chat_id` 内不能共享发送冷却。
- daemon 层维护 flush loop、失败重试和 connection drop，职责分散。
- heartbeat 由 session/daemon 触发，不属于统一发送调度。
- 失败重试使用指数退避，不符合按 chat cooldown 重试的目标。
- streaming frame 有拆分逻辑；新设计要求不拆分，单 frame 超限直接关闭 connection。

本重构不兼容旧 `outbound.Queue`/`FlushReady` API，以正确性为优先。

## Goals

1. 进程启动时创建一个共享 `outbound.Manager`，并作为必传参数注入 `daemon.NewLocal` 和 `daemon.NewRemote`。
2. `outbound.Manager` 内按 `chat_id` 动态维护 `chatQueue`，每个 chat 独立 cooldown。
3. 同一个 chat 内所有 client/server connection 共享一个 cooldown；不同 chat 互不影响。
4. daemon 层只负责投递业务 frame，不负责 flush、retry、cooldown、heartbeat。
5. 每个 chat 只维护一个 `nextFlushAt` 和一个 timer。
6. 每次 cooldown 到达，每个 chat 最多发送一条飞书消息。
7. heartbeat 由 outbound manager 托管。connection 只存 `nextHeartbeatAt`，不维护单独 heartbeat timer，也不需要 `needsHeartbeat` 字段。
8. heartbeat 不额外触发 flush；它只影响 cooldown 到达后的选择优先级和下一次 flush 规划。
9. flush 时只从队首合并尽可能多的 frame，不拆 frame。
10. 单个 frame 超过单条消息限制时，直接记录 error 并关闭该 connection。
11. 默认 `send_cooldown` 改为 `500ms`。

## Public Shape

构造函数命名：

```go
daemon.NewLocal(...)
daemon.NewRemote(...)
```

`daemon.NewRemoteDaemon` 被移除。

daemon options 必须传入 shared manager：

```go
type LocalOptions struct {
    Config *config.Config
    LarkClient LarkClient
    Outbound *outbound.Manager
}

type RemoteOptions struct {
    Config RemoteConfig
    Sender RemoteSender
    Outbound *outbound.Manager
}
```

`Outbound == nil` 直接返回错误，不自动创建私有 queue。

session 侧只依赖 outbound 投递接口：

```go
type Outbound interface {
    RegisterConnection(outbound.RegisterConnectionRequest) error
    Enqueue(ctx context.Context, connID string, typ protocol.FrameType, payload []byte) error
    EnqueueJSON(ctx context.Context, connID string, typ protocol.FrameType, payload any) error
    MarkCloseAfterDrained(connID string)
    DropConnection(connID string)
}
```

client start root message 通过 manager 打开：

```go
func (m *Manager) OpenRoot(ctx context.Context, req OpenRootRequest) (OpenRootResult, error)
```

`OpenRoot` 把 root open job 放入对应 chatQueue，等 chat cooldown 到达后发送。发送成功后返回 root message id，再由 local session 用这个 id 注册 connection。

## Top-Level Wiring

`cmd/elarkd/main.go` 在创建 daemon 前创建一个共享 manager：

```go
outboundMgr, err := outbound.NewManager(outbound.ManagerOptions{
    Sender: larkOutboundSender{client: larkClient},
    SendCooldown: cfg.Lark.SendCooldown.Duration(),
    RequestLimitBytes: cfg.Lark.LarkTextRequestLimitBytes,
    HeartbeatInterval: cfg.Connection.HeartbeatInterval.Duration(),
})
```

然后启动生命周期：

```go
go outboundMgr.Run(runCtx)
```

再注入 local/remote：

```go
local, err := daemon.NewLocal(daemon.LocalOptions{
    Config: cfg,
    LarkClient: larkClient,
    Outbound: outboundMgr,
})

remote, err := daemon.NewRemote(daemon.RemoteOptions{
    Config: daemon.RemoteConfigFromConfig(cfg),
    EventSource: eventSource,
    SelfBotOpenID: selfOpenID,
    Sender: larkClient,
    Outbound: outboundMgr,
})
```

只启用 client 或只启用 server 时也创建同一个 manager；未启用的一侧不会注册 connection。

## Data Model

`Manager`：

- `mu` 保护 `chats`、`conns` 和生命周期字段。
- `chats map[string]*chatQueue`，按 `chat_id` 动态增加。
- `conns map[string]*chatQueue`，用于按 `conn_id` 找到所属 chat。
- `sender ManagerSender`，发送时带 `Role`，方便测试和同进程双 bot 路由。

`chatQueue`：

- `chatID string`
- `cooldown time.Duration`
- `lastAttemptAt time.Time`
- `nextFlushAt time.Time`
- `conns map[string]*connQueue`
- `rootJobs map[string]*rootOpenJob`
- `flushing bool`
- `rescheduleCh chan struct{}`

`lastAttemptAt` 表示最近一次实际调用飞书发送接口的时间。可重试失败也算一次发送尝试，因此也更新它。

`connQueue`：

- `connID string`
- `role Role`
- `target Target`
- `seq *protocol.Sequencer`
- `heartbeatInterval time.Duration`
- `nextHeartbeatAt time.Time`
- `frames []queuedFrame`
- `closeAfterDrained bool`
- `onDrop func(context.Context, DropReason)`
- `onDrained func(context.Context)`

`queuedFrame.createdAt` 用于在同一个 chat 内选择等待最久的 connection。

## Scheduling

基本原则：

1. 每个 chat 独立调度。
2. 每个 chat 每次 timer 到达最多发送一条飞书消息。
3. 新消息入队不会绕过 cooldown，只会重新规划该 chat 的 `nextFlushAt`。
4. heartbeat 不触发额外 flush。

flush 成功后：

- pop 本次成功发送的 frames，或完成 root open job。
- 更新 `lastAttemptAt = now`。
- 如果是 connection flush，更新 `conn.nextHeartbeatAt = now + heartbeatInterval`。
- 如果 connection 标记了 `closeAfterDrained` 且队列已空，释放锁后调用 `onDrained`。
- 重新规划：
  - 如果还有 pending frame 或 root open job：`nextFlushAt = lastAttemptAt + cooldown`。
  - 否则如果还有 connection：`nextFlushAt = max(lastAttemptAt + cooldown, earliest nextHeartbeatAt)`。
  - 否则不规划。

重点修正：即使当前没有 pending frame，如果最近的 heartbeat 时间早于 cooldown 到达时间，也必须等到 cooldown 到达后再发送 heartbeat。

可重试失败后：

- warn 日志。
- 不 pop frame。
- 不 reset heartbeat。
- 更新 `lastAttemptAt = now`。
- `nextFlushAt = now + cooldown`。

不可重试失败后：

- error 日志。
- 丢弃对应 connection 或 root open job。
- 释放锁后调用 `onDrop`。
- 如果已经调用过飞书发送接口，更新 `lastAttemptAt = now`。
- 按剩余 pending/connection 重新规划。

没有发送任何消息时：

- 如果还有 connection：`nextFlushAt = max(cooldownReadyAt, earliest nextHeartbeatAt)`。
- 如果没有 connection：不规划。

新消息入队时：

```go
readyAt := now
if c.hasLastAttempt {
    readyAt = c.lastAttemptAt.Add(c.cooldown)
}
c.scheduleEarlierLocked(readyAt)
c.notifyReschedule()
```

如果 cooldown 已到，`readyAt <= now`，chat goroutine 会立即 flush。若当前 `nextFlushAt` 是更晚的 heartbeat，新 frame 会把计划提前到 cooldown ready 时间。

## Selection

timer 到达后只扫描当前 chat。

选择顺序：

1. heartbeat due 的 connection，条件是 `now >= conn.nextHeartbeatAt`；多个 due 时选 `nextHeartbeatAt` 最早的。
2. 如果选中的 heartbeat-due connection 队列非空，发送其队列；如果为空，生成 heartbeat frame 并发送。
3. 没有 heartbeat due 时，选择 root open job 和普通 pending connection 中等待最久的发送项。
4. 普通 pending connection 用队首 frame `createdAt` 比较。

不同 chat 之间不比较，也不会共享 timer。

## Batching

发送 connection queue 时：

1. 从队首 frame 开始。
2. 逐个尝试加入当前飞书消息。
3. 加入后不超过 request limit 就保留。
4. 加入下一个 frame 会超限就停止，本次只发送已保留的 frame。
5. 如果第一个 frame 自己就超限，返回 `ErrFrameTooLarge`，关闭该 connection。

不拆分 frame，删除旧 `splitStreamingPayload` 和 `splitFrameUntilFits` 路径。

## Retry Classification

可重试：

- 网络连接错误。
- HTTP `5xx`。
- HTTP `4xx` 且业务错误码为 `230020`。

不可重试：

- 其它 HTTP/API 错误。
- frame 超限。
- 协议编码错误。
- `context.Canceled` 和 `context.DeadlineExceeded`。

`internal/lark` 导出：

```go
type APIError struct {
    Status int
    Code int
    Msg string
    Path string
}

func IsRetryableSendError(err error) bool
```

网络连接错误通过 `*url.Error`、`*net.OpError`、`net.Error` 等类型判断。

## Locking

锁顺序固定为：

1. `Manager.mu`
2. `chatQueue.mu`

发送飞书请求时不能持有任何 manager/chat 锁。`onDrop` 和 `onDrained` 必须在释放 chat 锁后调用，避免回调进入 session/daemon 时死锁。

`rescheduleCh` 只表示状态变化，不表示绕过 cooldown 立即发送：

```go
func (c *chatQueue) notifyReschedule() {
    select {
    case c.rescheduleCh <- struct{}{}:
    default:
    }
}
```

## File-Level Changes

- `internal/outbound/common.go`：保留 `Target`、默认值、request sizing、clone helpers。
- `internal/outbound/manager.go`：实现 shared manager、per-chat scheduler、root open job、heartbeat、retry/drop。
- `internal/outbound/errors.go`：定义 `FrameTooLargeError` 等错误。
- 删除旧 `internal/outbound/queue.go` 的 `Queue/FlushReady` API 和旧 queue tests。
- 删除 `internal/daemon/flush_retry.go`。
- `internal/lark/client.go`：导出 `APIError`，新增 `IsRetryableSendError`。
- `internal/session/session.go`：移除 outbound seq 和 outbound heartbeat，改为投递到 manager。
- `internal/daemon/local.go`：移除 flush loop，`Outbound` 必传。
- `internal/daemon/remote.go`：`NewRemote`，移除 flush loop，`Outbound` 必传。
- `cmd/elarkd/main.go`：创建共享 manager 并注入 local/remote。
- `internal/config` 和 `README.md`：默认 `send_cooldown = "500ms"`。

## Testing

重点单测覆盖：

- 可重试失败不 pop frame，下个 cooldown 重试。
- 不可重试失败 drop connection。
- 单 frame 超限 drop connection。
- frame 只合并不拆分。
- heartbeat 时间早于 cooldown 到达时，下一次 flush 仍使用 cooldown 到达时间。
- session tick 不再生成 outbound heartbeat。
- local/remote daemon 不再有 flush loop。

loopback e2e 测试已移除。真实链路测试应使用本机真实 `elarkd`、真实 `elark` CLI 和真实飞书 websocket/OpenAPI 链路。
