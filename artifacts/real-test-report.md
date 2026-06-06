# Real Lark Test Report

## Design Baseline

Read `.local/exec-over-lark-technical-design.md` before implementation. The test work followed the design requirement that each connection is represented by a Lark root message and follow-up frames are sent as root replies with explicit bot mentions.

## Commands Run

- `go test ./...`
- `go test ./internal/lark`
- `ELARK_REAL_LARK_FIXTURE=/Users/bytedance/.elark/elark-test-fixture.toml go test ./internal/lark -run TestRealLarkBotMessaging -count=1 -v`
- `lark-cli --profile cli_a955176022381cc4 im chat.members bots --as user ...`
- `lark-cli --profile cli_a955176022381cc4 im chat.members create --as user ...`

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

## Final Pass Evidence

- Full local suite passed after changes:
  - `go test ./...`
  - Result: all packages passed.
- Real Lark-backed test passed after sanitized setup:
  - `ELARK_REAL_LARK_FIXTURE=/Users/bytedance/.elark/elark-test-fixture.toml go test ./internal/lark -run TestRealLarkBotMessaging -count=1 -v`
  - Result: `--- PASS: TestRealLarkBotMessaging`, package `github.com/hachiwii/exec-over-lark/internal/lark` passed.
