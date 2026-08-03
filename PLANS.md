# Fubun implementation plan

## Assumptions

- 新規の独立OSS repositoryとして初期化する。
- Ubuntu 24.04 amd64を基準にし、Linux固有処理はUnix Socket境界へ閉じ込める。
- Protocol/Event spec versionはPhase 1で `1.0` とする。
- Synthetic Eventのdataは `label` と `counter` だけに限定する。
- `XDG_RUNTIME_DIR` は必須、data homeは `XDG_DATA_HOME`、未設定時は `$HOME/.local/share` とする。
- GitHub repositoryは作業開始時点で未作成のため、remote作成はこの実装範囲に含めない。

## Phase 0: product and security foundation

- [x] Product boundary, threat model, ADRs
- [x] Rust workspace and CI skeleton
- [x] Strict canonical JSON schemas and generated-schema workflow
- [x] XDG and permission policy

## Phase 1: core vertical slice

- [x] Strict Event and IPC domain types
- [x] 4-byte little-endian framed Unix Domain Socket protocol
- [x] Required handshake and protocol-major rejection
- [x] SQLite migrations, WAL, foreign keys, busy timeout
- [x] Single writer queue and duplicate detection
- [x] `fubund` graceful shutdown
- [x] `fubun status`, `dev emit-fixture`, `events list`, `doctor`
- [x] Unit, property, and integration coverage
- [ ] GitHub remote push and draft PR (blocked until repository/remote exists)

## Phase 2 and later

- Pattern Miner, Suggestion, Ritual, Rule, adapters, and GUI are explicitly deferred.
- Windows/macOS support, packaging, auto-update, cloud sync, plugin API, and AI are backlog only.
