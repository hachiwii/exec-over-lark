# Release 0.1.0

## Scope

- Technical design read before release actions: `.local/exec-over-lark-technical-design.md`.
- Release commit pushed to the tracked remote branch `origin/main`.
- Git tag `0.1.0` created as an annotated tag and pushed to `origin`.
- Fixture files and local workflow state remained ignored under `.local/`.

## Verification

- Standard test command: `env -u ELARK_REAL_LARK_FIXTURE go test ./...`
- Standard test result: all packages passed.
- Real Lark dependency evidence: `artifacts/real-test-report.md` existed, was nonempty Markdown, and contained headings before this release node.
- Staged whitespace check: `git diff --cached --check` passed before commit.

## Pushed Commit

- Commit: `17aaf21674a39e973019f0fd5a81d4b8e9c2c03f`
- Subject: `Implement exec-over-lark 0.1.0`
- Commit date: `2026-06-06 21:30:09 +0800`
- Remote branch evidence: `17aaf21674a39e973019f0fd5a81d4b8e9c2c03f refs/heads/main`

## Pushed Tag

- Tag name: `0.1.0`
- Tag object: `569df2848fae46e34aefd8b80118724dc7c34f5c`
- Tagged commit: `17aaf21674a39e973019f0fd5a81d4b8e9c2c03f`
- Tag date: `2026-06-06 21:30:49 +0800`
- Tag message: `Release 0.1.0`
- Remote tag evidence: `569df2848fae46e34aefd8b80118724dc7c34f5c refs/tags/0.1.0`
- Remote peeled tag evidence: `17aaf21674a39e973019f0fd5a81d4b8e9c2c03f refs/tags/0.1.0^{}`

## Post-Push Status

- `git status --short --branch` after push and before writing this artifact: `## main...origin/main`
