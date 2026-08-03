# Fubun implementation plan

## Assumptions

- 新規の独立OSS repositoryとして初期化する。
- Ubuntu 24.04 amd64を基準にし、Linux固有処理はUnix Socket境界へ閉じ込める。
- Protocol/Event spec versionはPhase 1で `1.0` とする。
- Synthetic Eventのdataは `label` と `counter` だけに限定する。
- `XDG_RUNTIME_DIR` は必須、data homeは `XDG_DATA_HOME`、未設定時は `$HOME/.local/share` とする。
- Phase 1公開時点ではGitHub repositoryが未作成だったため、Phase 2開始時に `anythigok/fubun` を作成してmainへ初期公開する。

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
- [x] GitHub mainへPhase 1 root commitを初期公開

## Phase 2: explicit ritual manual execution

- [x] Resource、Ritual、Immutable RitualVersion、Approval、Execution domain
- [x] 固定Action RegistryとR0/R1 policy
- [x] v2 SQLite migrationとsingle writer CRUD
- [x] Adapter Hello、Capability Registry、双方向Action dispatch
- [x] Preview、明示Activation、Pause、Manual Run、Execution History
- [x] 固定ExecutableのみのLinux AdapterとFake Adapter E2E
- [x] CLI、Schema、ADR、Threat Model、Fixture、CI更新
- [x] GitHub feature branch pushとDraft PR（実装完了）

### Phase 2 assumptions

- Adapter statusは接続時のXDG Desktop Entry一覧をsnapshotとして扱う。
- `ensure_running`の/proc判定は同一uidのexe basenameだけを比較し、読取不能時は安全にlaunchへ進む。
- Canonical JSONは厳格なRust struct再シリアライズで生成し、入力WhitespaceはHash対象にしない。
- Previewのadapter接続・resource・desktop entry不在は実行不可のwarningとする。

### Deferred

- Pattern Miner、Suggestion、Rule、自動Trigger、自動Execution、GUI、Browser/VS Code連携はPhase 3以降。
- Windows/macOS、packaging、auto-update、cloud sync、plugin API、AIはbacklog only。
