# Real Lark Test Report

## Design Baseline

Read `.local/exec-over-lark-technical-design.md` before implementation. The test work followed the design requirement that each connection is represented by a Lark root message and follow-up frames are sent as root replies with explicit bot mentions.

## Commands Run

- `go test ./...`
- `go test ./internal/lark`
- `ELARK_REAL_LARK_FIXTURE=/Users/bytedance/.elark/elark-test-fixture.toml go test ./internal/lark -run TestRealLarkBotMessaging -count=1 -v`
- `GOTOOLCHAIN=local go test ./internal/e2etest -run TestProcessE2ELocalElarkdAndCLI -count=1 -v`
- `ELARK_REAL_LARK_FIXTURE=/Users/bytedance/.elark/elark-test-fixture.toml GOTOOLCHAIN=local go test ./internal/e2etest -run TestRealLarkProcessE2ELocalElarkdAndCLIVisibleMessages -count=1 -v`
- `env -u ELARK_REAL_LARK_FIXTURE GOTOOLCHAIN=local go test ./... -count=1`
- Manual two-daemon real chain with locally installed binaries under `.local/real-run/bin`:
  - client `elarkd`: client bot profile, IPC enabled.
  - server `elarkd`: server bot profile, exec enabled.
  - both daemons consumed real `im.message.receive_v1` events through native Feishu websocket persistent connections.
  - real `elark` CLI sent commands to the local client daemon socket.
- Real chat membership setup was performed through Feishu APIs; secrets and raw tokens were not copied into this report.

## Sanitized Real Lark Setup

- Fixture path used: `/Users/bytedance/.elark/elark-test-fixture.toml`.
- Fixture contents were not copied into this repo and raw secrets were not printed.
- The fixture had a test chat, a client bot app, a server bot app, bot open IDs, and verification metadata.
- Initial chat robot membership check returned `bot_count=0`.
- Added the two fixture bot apps to the test chat with `member_id_type=app_id`.
- Add result was clean: invalid IDs `0`, not-existing IDs `0`, pending approvals `0`.
- Follow-up membership check returned `bot_count=2`, with both fixture bots present.

## Failures Fixed

- Added `internal/lark/real_lark_test.go`, guarded by `ELARK_REAL_LARK_FIXTURE`, to run real Lark-backed bot messaging only when explicitly requested.
- The first real run failed because fixture bot IDs differed from `bot/v3/info` results. The test now uses bot-info resolution as an auth check, while using fixture bot open IDs for chat mentions because those are the IDs accepted by the test chat.
- The next real run failed with only `HTTP status 400`. Fixed `internal/lark/client.go` to decode structured Lark error bodies on non-2xx responses. Added unit coverage in `internal/lark/client_test.go`.
- Improved diagnostics exposed Lark code `230002`, indicating the bots were not in the chat. Added the fixture bot apps to the test chat and verified membership before rerunning.
- Added process-level local e2e coverage in `internal/e2etest/process_test.go`. The test builds real `elarkd` and `elark` binaries, starts a real `elarkd` process with a local loopback transport over a real Unix socket, and sends requests through the real `elark` CLI.
- Fixed remote PTY execution in `internal/daemon/remote.go`; `pty=true` and empty-command login shell sessions now use `internal/pty` instead of returning an unsupported error.
- Propagated initial terminal rows and columns from CLI IPC start requests into protocol `start` frames so remote PTY sessions receive startup sizing.
- Added a gated real Lark-visible process e2e path. With `ELARK_REAL_LARK_FIXTURE` set, the same real `elarkd` process and real `elark` CLI path mirrors protocol root/reply messages into the fixture chat while preserving local loopback delivery for deterministic assertions.
- Fixed process output shutdown handling in the remote daemon so normal closed pipe/read-closed conditions during command teardown are treated as EOF instead of protocol failures.
- Wired the production `elarkd` path to a native Feishu websocket event source. It decodes persistent-connection protobuf frames, parses the raw V2 event payload directly, and acknowledges frames over the same websocket connection.
- Wired server-side `elarkd` startup to `RemoteDaemon` when `exec.enabled=true` and `ipc.enabled=false`.
- Removed OpenID-based sender matching and sender allowlists. Chat allowlists remain the supported server-side scope control.

## Manual Two-Daemon Real Chain

This run did not use loopback delivery. It installed the current `elarkd` and `elark` binaries into `.local/real-run/bin`, started two local daemon processes with separate client/server bot credentials, and sent requests through the real `elark` CLI to the local client daemon.

Commands executed through the real Feishu-backed chain:

- `ls -1 | head -8` returned `LICENSE`, `README.md`, `artifacts`, `cmd`, `go.mod`, `go.sum`, `internal`.
- Native websocket smoke command `printf native-ws-ok` returned `native-ws-ok`.
- `printf 'pear\napple\npear\nbanana\n' | sort | uniq -c | sed -E ...` returned `apple: 1`, `banana: 1`, `pear: 2`.
- Forced PTY `vim` command returned `ELARK_VIM_OK`.
- Default interactive shell opened a real zsh PTY and accepted multiple consecutive commands: `pwd`, `printf shell-one`, `ls -1 | head -3`, `printf shell-two`, `exit`.

Feishu-visible evidence:

- The test chat `Elark 测试` showed real client bot root protocol messages around `2026-06-06 22:32` to `22:34`.
- The server bot replied in those message threads with `start_ack`, `stdout`, and `exit` protocol frames.
- Latest manual roots included `om_x100b6d76b66b9ca0b30fd6d53afed8e`, `om_x100b6d76b71968a0b1ca07447d968c7`, `om_x100b6d76b59faca8b32daac1b52aaf8`, and `om_x100b6d76b33c98a4b13fb46c083f63f`.

## Local Process E2E Coverage

The local process e2e test starts `elarkd` on this machine and drives it only through the `elark` CLI. It covers:

- Common non-interactive commands: `printf`, `pwd` with `--cwd`, stdout/stderr separation, and nonzero exit propagation.
- Non-interactive stdin piping through the CLI and daemon IPC.
- Forced PTY command execution with `elark -t`, including remote TTY detection.
- Default interactive login shell behavior with `elark host` and piped shell input.
- Real Lark-visible process e2e: `elarkd` process + `elark` CLI command execution mirrored protocol messages into the fixture chat named `Elark 测试`.

## Final Pass Evidence

- Full local suite passed after changes:
  - `env -u ELARK_REAL_LARK_FIXTURE GOTOOLCHAIN=local go test ./... -count=1`
  - Result: all packages passed.
- Local process e2e passed:
  - `GOTOOLCHAIN=local go test ./internal/e2etest -run TestProcessE2ELocalElarkdAndCLI -count=1 -v`
  - Result: non-interactive command, stdin piping, forced PTY command, and default interactive login shell subtests passed.
- Real Lark-visible process e2e passed:
  - `ELARK_REAL_LARK_FIXTURE=/Users/bytedance/.elark/elark-test-fixture.toml GOTOOLCHAIN=local go test ./internal/e2etest -run TestRealLarkProcessE2ELocalElarkdAndCLIVisibleMessages -count=1 -v`
  - Result: real `elarkd` process and real `elark` CLI completed successfully; the fixture chat `Elark 测试` showed a new root protocol message at `2026-06-06 21:54` with four replies.
- Real Lark-backed test passed after sanitized setup:
  - `ELARK_REAL_LARK_FIXTURE=/Users/bytedance/.elark/elark-test-fixture.toml go test ./internal/lark -run TestRealLarkBotMessaging -count=1 -v`
  - Result: `--- PASS: TestRealLarkBotMessaging`, package `github.com/hachiwii/exec-over-lark/internal/lark` passed.
