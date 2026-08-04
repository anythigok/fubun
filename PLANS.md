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
- [x] v2/v3 SQLite migrationとsingle writer CRUD
- [x] Adapter Hello、Capability Registry、双方向Action dispatch
- [x] Preview、明示Activation、Pause、Manual Run、Execution History
- [x] 固定ExecutableのみのLinux AdapterとTest Scope限定Fake Adapter E2E
- [x] CLI、Schema、ADR、Threat Model、Fixture、CI更新
- [x] GitHub feature branch pushとDraft PR（実装完了）

### Phase 2 assumptions

- Adapter statusは接続時のXDG Desktop Entry一覧をsnapshotとして扱う。
- `ensure_running`の/proc判定は同一uidのexe basenameだけを比較し、読取不能時は安全にlaunchへ進む。
- Canonical JSONは厳格なRust struct再シリアライズで生成し、入力WhitespaceはHash対象にしない。
- Previewのadapter接続・required tool・resource・resource kind・desktop entry不在は実行不可のwarningとする。

### Phase 2 PR #1 hardening

PR #1の再現確認で、Production adapterが `--fake`、`FakeRunner`、偽のtool/desktop-entry statusを受理していたこと、Coreの外側Ritual timeoutがdispatch Futureをdropした場合にpending mapの削除を型で保証していなかったこと、既存Disconnect testが実行前切断だけだったことを確認した。加えて、domainとadapterのapp_id規則が重複しハイフンを拒否していたこと、Execution Stepのadapter_idが固定値だったこと、Preview/Preflightがrequired toolとResourceKindを検証していなかったこと、再起動時にExecution Stepがpending/runningのまま残ることをコードから再現した。

- [x] Production Fake経路を削除し、Test専用FakeをIntegration Testへ限定
- [x] 同期RAII Pending GuardとRitual Deadlineでcancel/timeout/disconnect/shutdown cleanupを保証
- [x] 実行中Adapter切断、再実行、adapter identity履歴の回帰Testを追加
- [x] app_id共有検証（`-`許可、1-128 bytes、path/control/whitespace拒否）
- [x] Adapter Helloの固定Registry・重複・長さ検証
- [x] Preview/Preflightのrequired tool、Desktop Entry、ResourceKind、Canonical Path再検証
- [x] Daemon restart時のExecutionとpending/running Step整合性を追加

### Phase 2 PR #1 final alignment

Head `776d4c82aad646665d40d7a49cf26ab6426b9d63` を確認した時点で、Preview/Preflightは個別のCapability・Tool・Desktop Entry照会を組み合わせる一方、DispatchはCapabilityだけをHashMap走査していたため、異なるInstanceの状態を合成し得た。Dispatch errorは選択済みAdapter Identityを返さず、Action failure後の後続Stepはpendingのまま残った。

- [x] `fubun-core::adapter` にAction由来の `AdapterRequirements` と単一Instanceの共通Selection APIを追加
- [x] Capability、固定Tool、Desktop Entryを同じInstanceで検証し、instance UUID文字列昇順で決定的に選択
- [x] Preview、Preflight、Dispatchを共通Selectionへ統合し、分割されたAdapter状態を実行可能としない
- [x] timeout、disconnect、protocol errorを含む選択後のDispatch errorへAdapter ID/Instance IDを付与
- [x] Execution StepへAdapter Instance IDを保存するv3 migration、Schema、Historyを追加
- [x] terminal Executionでpending/running Stepを残さず、後続未実行Stepを `stopped_after_failure` / `aborted` で確定
- [x] multi-adapter、timeout、disconnect、protocol error、first/partial failure、restartの回帰Testを追加または強化

### Phase 2 final assumptions

- PreviewとRunの間にAdapter状態が変わり得るため、Run直前PreflightとDispatch時に同じ要件で再選択する。Preview時のInstanceを固定予約しない。
- `adapter_instance_id` は監査用のExecution History項目であり、Ritual JSONやAction入力には含めない。
- terminal Step確定はsingle writer queue経由で行い、stdout/stderrやResource Pathを追加保存しない。

### Deferred

- Pattern Miner、Suggestion、Rule、自動Trigger、自動Execution、GUI、Browser/VS Code連携はPhase 3以降。
- Windows/macOS、packaging、auto-update、cloud sync、plugin API、AIはbacklog only。
