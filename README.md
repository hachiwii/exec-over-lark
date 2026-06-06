# exec-over-lark

`exec-over-lark` 是一个用飞书话题群传输命令输入和执行输出的远程命令工具。它提供接近 `ssh host command`、`ssh host` 和 `ssh -t host command` 的命令行体验，但不实现 SSH wire protocol。

核心模型：

- 本地用户运行 `elark` CLI。
- 本地 `elarkd` 读取 host 配置，通过本地 Unix socket 接收 CLI 请求，并用 client bot 在飞书话题群里创建 root message。
- 远端 `elarkd` 使用 server bot 监听飞书消息，校验 bot 身份、群和发送方后，在远端机器执行命令或 PTY shell。
- 每个飞书 root message 对应一个连接，后续输入、输出、心跳和退出状态都通过该 root message 的回复消息传输。

## 功能边界

支持：

- 非交互命令：`elark macmini 'uname -a'`
- 默认登录 shell：`elark macmini`
- 强制 PTY：`elark -t macmini 'vim file'`
- stdin/stdout/stderr 与远端 exit code 传递
- 双 bot 身份、群成员关系、可选 chat/sender 白名单
- 发送限流、同连接消息聚合和大输出拆分

不支持：

- SSH wire protocol
- `scp`、`sftp`、`rsync -e ssh`
- SSH port forwarding、agent forwarding
- 把飞书当作高吞吐 TCP 隧道
- daemon 停机期间的飞书历史消息恢复

第一版协议不做应用层 ACK，也不做额外加密。消息正文只做 base64 编码，访问控制依赖飞书 bot 身份、群成员关系和配置中的 sender/chat 校验。

## 安装

### 从 Release 安装

推荐使用一键安装脚本。脚本会检测 macOS/Linux 和 CPU 架构，从 GitHub latest release 选择匹配的归档，安装 `elark` 和 `elarkd`，并在结束时输出安装位置、生成初始配置的命令和启动后台进程的命令。

```bash
curl -fsSL https://raw.githubusercontent.com/hachiwii/exec-over-lark/main/scripts/install.sh | sh
```

也可以先下载脚本再执行，或指定安装目录：

```bash
curl -fsSL https://raw.githubusercontent.com/hachiwii/exec-over-lark/main/scripts/install.sh -o install.sh
sh install.sh
ELARK_INSTALL_DIR="$HOME/.local/bin" sh install.sh
```

脚本优先安装到已经在 `PATH` 中的 `$HOME/.local/bin`，其次选择已有可写 `PATH` 目录；如果没有可写 `PATH` 目录，会创建 `$HOME/.local/bin` 并提示把它加入 `PATH`。

如果还没有发布 GitHub release，`releases/latest` 会返回 404；先发布一个包含对应系统归档的 release 后再执行脚本。

手动安装时，在 GitHub Releases 下载与你的系统匹配的压缩包，例如：

- `exec-over-lark_vX.Y.Z_darwin_amd64.tar.gz`
- `exec-over-lark_vX.Y.Z_darwin_arm64.tar.gz`
- `exec-over-lark_vX.Y.Z_linux_amd64.tar.gz`
- `exec-over-lark_vX.Y.Z_linux_arm64.tar.gz`

解压后把 `elark` 和 `elarkd` 放到 `PATH` 中：

```bash
tar -xzf exec-over-lark_vX.Y.Z_linux_amd64.tar.gz
mkdir -p "$HOME/.local/bin"
cp exec-over-lark_vX.Y.Z_linux_amd64/elark "$HOME/.local/bin/elark"
cp exec-over-lark_vX.Y.Z_linux_amd64/elarkd "$HOME/.local/bin/elarkd"
chmod 0755 "$HOME/.local/bin/elark" "$HOME/.local/bin/elarkd"
```

确认安装：

```bash
elark --help
elarkd init --help
```

生成初始配置并启动本地后台进程：

```bash
elarkd init --client
elark daemon start
```

远端机器通常生成 server 配置并直接运行 `elarkd`：

```bash
elarkd init --server
elarkd --config ~/.elark/config.toml
```

### 从源码安装

需要 Go 1.24 或更高版本。

```bash
git clone https://github.com/hachiwii/exec-over-lark.git
cd exec-over-lark
go install ./cmd/elark ./cmd/elarkd
```

也可以直接构建本地二进制：

```bash
go build -o ./bin/elark ./cmd/elark
go build -o ./bin/elarkd ./cmd/elarkd
```

## 飞书准备

部署需要两个飞书 bot：

- client bot：本地侧使用，负责发送命令和接收输出。
- server bot：远端侧使用，负责接收命令和发送输出。

准备步骤：

1. 创建两个飞书应用并启用 bot。
2. 为两个应用配置接收消息事件，至少需要消息接收事件；server bot 还需要 bot 入群事件用于 bootstrap。
3. 把两个 bot 都拉入同一个飞书话题群。
4. 在远端 `elarkd` 启动后，server bot 入群时会发送：

```text
exec-over-lark server ready
chat_id: oc_xxx
bot_openid: ou_server_bot
```

把其中的 `chat_id` 和 `bot_openid` 写入本地侧 host 配置。

## 配置文件

默认配置路径是：

```text
~/.elark/config.toml
```

`elark` 和 `elarkd` 都可以通过 `--config PATH` 使用其它配置文件。配置文件会保存飞书应用密钥，权限必须收紧：

```bash
chmod 700 ~/.elark
chmod 600 ~/.elark/config.toml
```

如果配置不存在，先生成模板：

```bash
elarkd init --client
elarkd init --server
elarkd init --client --config ./client.toml
elarkd init --server --config ~/.elark/server.toml --force
```

`init` 只生成模板，不连接飞书，不校验 `app_id` / `app_secret`，也不会打印 secret。目标文件已存在时默认拒绝覆盖，只有传 `--force` 才会覆盖。

### 本地侧配置示例

```toml
node_name = "local"
default_host = "macmini"

[ipc]
enabled = true
socket_path = "~/.local/run/exec-over-lark/elarkd.sock"

[lark]
app_id = "cli_client_xxx"
app_secret = "client_secret_xxx"
send_cooldown = "1000ms"
lark_text_request_limit_bytes = 153600

[connection]
heartbeat_interval = "10s"
heartbeat_timeout = "30s"
sequence_gap_timeout = "30s"

[exec]
enabled = false

[hosts.macmini]
chat_id = "oc_xxx"
peer_bot_open_id = "ou_server_bot"
shell = "/bin/zsh"
stream_chunk_bytes = 12000
default_cwd = "/Users/you"

[hosts.devbox]
chat_id = "oc_yyy"
peer_bot_open_id = "ou_server_bot_yyy"
shell = "/bin/bash"
stream_chunk_bytes = 12000
```

本地侧字段说明：

- `node_name`：当前节点名，用于诊断输出。
- `default_host`：默认 host 名称。
- `ipc.enabled = true`：允许 `elark` CLI 通过本地 Unix socket 调用本地 daemon。
- `ipc.socket_path`：本地 daemon socket 路径，所在目录权限不能宽于 `0700`。
- `lark.app_id` / `lark.app_secret`：client bot 的凭据。
- `lark.send_cooldown`：两次飞书发送之间的最小间隔，默认 `"1000ms"`。
- `lark.lark_text_request_limit_bytes`：飞书文本消息请求体上限，第一版固定为 `153600`。
- `connection.heartbeat_interval`：连接空闲多久后发送 heartbeat，默认 `"10s"`。
- `connection.heartbeat_timeout`：声明给对端的超时时间，默认 `"30s"`。
- `connection.sequence_gap_timeout`：收到跳号 frame 后等待缺失序号的最长时间，默认 `"30s"`。
- `exec.enabled = false`：本地侧不接受飞书消息触发本机命令执行。
- `hosts.<name>.chat_id`：该 host 使用的话题群 ID。
- `hosts.<name>.peer_bot_open_id`：该 host 的 server bot OpenID，本地只处理它发出的返回消息。
- `hosts.<name>.shell`：远端默认 shell。
- `hosts.<name>.stream_chunk_bytes`：流式输出进入协议层前的切片大小，默认 `12000`。
- `hosts.<name>.default_cwd`：可选。未通过 `--cwd` 覆盖时作为远端默认工作目录；不配置则远端使用执行用户 home。

### 远端侧配置示例

```toml
node_name = "macmini"

[ipc]
enabled = false
socket_path = "~/.local/run/exec-over-lark/elarkd.sock"

[lark]
app_id = "cli_server_xxx"
app_secret = "server_secret_xxx"
send_cooldown = "1000ms"
lark_text_request_limit_bytes = 153600

[connection]
heartbeat_interval = "10s"
heartbeat_timeout = "30s"
sequence_gap_timeout = "30s"

[exec]
enabled = true
default_shell = "/bin/zsh"
max_sessions = 8
stream_chunk_bytes = 12000
# 不设置表示允许所有 chat；设置后只处理列表内 chat。
# allowed_chat_ids = ["oc_xxx", "oc_yyy"]
# 不设置表示允许所有 sender；设置后只处理列表内 OpenID。
# allowed_sender_open_ids = ["ou_client_bot"]
```

远端侧字段说明：

- `ipc.enabled = false`：远端通常不暴露本地 CLI socket。
- `lark.app_id` / `lark.app_secret`：server bot 的凭据。
- `exec.enabled = true`：允许飞书消息触发远端命令执行。
- `exec.default_shell`：非交互命令使用的默认 shell。
- `exec.max_sessions`：远端同时执行的 session 上限，默认 `8`。
- `exec.stream_chunk_bytes`：远端 stdout/stderr 切片大小，默认 `12000`。
- `exec.allowed_chat_ids`：可选 chat 白名单。不配置表示允许所有 chat。
- `exec.allowed_sender_open_ids`：可选发送方 OpenID 白名单。不配置表示允许所有 sender。

所有时间字段都使用字符串，支持 `ms`、`s`、`m`、`h`、`d`，例如 `"1000ms"`、`"10s"`、`"5m"`、`"1h"`。

## 使用方法

### 启动 daemon

本地侧：

```bash
elark daemon start
elark daemon status
```

也可以直接运行：

```bash
elarkd --config ~/.elark/config.toml
```

远端侧通常用 systemd、launchd 或进程管理器长期运行：

```bash
elarkd --config ~/.elark/config.toml
```

### 查看和诊断

列出本地配置里的 host：

```bash
elark hosts
```

输出 doctor 报告。当前会检查配置路径、权限、加载和 host 配置，探测 daemon socket，刷新 Lark tenant token，查询本 bot `open_id`。指定 host 时，还会检查本 bot 是否在配置的群里，并发送一条 `doctor ping` root 消息提及 peer bot；peer bot 成员查询和 bootstrap 历史消息查询暂未接入，会在报告里显示 `skipped`：

```bash
elark doctor
elark doctor macmini
```

### 执行命令

非交互命令：

```bash
elark macmini 'uname -a'
elark macmini 'cd /srv/app && docker compose ps'
```

传递 stdin：

```bash
echo hello | elark macmini 'cat | wc -c'
```

指定远端工作目录：

```bash
elark --cwd /srv/app macmini 'git status --short'
```

指定超时：

```bash
elark --timeout 30s macmini 'long-running-command'
```

交互式 shell：

```bash
elark macmini
```

强制 PTY：

```bash
elark -t macmini
elark -t macmini 'vim file'
```

禁用 PTY：

```bash
elark -T macmini 'env | sort'
```

请求关闭连接：

```bash
elark kill om_xxx
```

当前 CLI 也保留了 `elark sessions`、`elark attach <conn_id>` 和 `elark daemon stop` 命令入口；它们依赖后续 daemon control RPC，在 IPC v1 中尚未开放完整能力。

## 协议摘要

飞书 text 消息中的协议 frame 格式为：

```text
EOL1 <seq> <type> <base64_payload>
```

一条飞书消息可以包含多个 frame，每行一个。`conn_id` 不放在 payload 中，固定由飞书 root message ID 决定。

常见 frame 类型：

- `start`：创建远端命令或 PTY session。
- `start_ack`：远端接受 session，并声明 heartbeat 设置。
- `stdin` / `stdout` / `stderr`：流式输入输出。
- `resize`：PTY 窗口大小变化。
- `signal`：转发 `INT`、`TERM` 等信号。
- `heartbeat`：连接保活。
- `exit`：远端进程退出码。
- `error`：协议或执行错误。
- `close`：请求关闭连接。

序列号按连接和方向单独递增，只用于进程内去重和顺序检查，不用于 ACK 或重传。

## 安全建议

- 始终保持配置文件权限为 `0600`，配置目录权限为 `0700`。
- 远端侧建议显式配置 `exec.allowed_chat_ids` 和 `exec.allowed_sender_open_ids`。
- 不要把 server bot 拉入不可信群。
- 不要在群里发送不希望群成员、管理员或合规审计看到的命令和输出。
- 不要把 `lark.app_secret` 写入日志、截图或公开文档。
- 远端 `elarkd` 以哪个系统用户启动，命令就以哪个用户执行。

## 开发

运行测试：

```bash
go test ./...
```

构建两个入口：

```bash
go build ./cmd/elark
go build ./cmd/elarkd
```

项目主要包：

- `cmd/elark`：命令行入口。
- `cmd/elarkd`：统一 daemon 入口。
- `internal/config`：TOML 配置、默认路径、模板生成和权限检查。
- `internal/ipc`：本地 Unix socket 协议。
- `internal/lark`：飞书 OpenAPI client、token、bot OpenID、bot 入群检查和消息发送。
- `internal/outbound`：发送队列、限流、聚合和拆分。
- `internal/protocol`：`EOL1` frame 编解码。
- `internal/session`：连接、序列窗口、heartbeat 和 session 分发。
- `internal/remoteexec`：远端非交互命令执行。
- `internal/pty`：Unix PTY 和终端控制。
- `internal/bootstrap`：server bot 入群 bootstrap 消息。
- `internal/doctor`：配置和运行状态诊断。

## License

MIT
