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

## Phase 3: explicit browser and VS Code integrations

- [x] Phase 2 PR #1をReady化し、Green CIを確認してmainへSquash Merge
- [x] `codex/integrations-v1`を統合後mainから作成
- [x] `ResourceKind::WebPage`、http/https canonicalization、query/fragment除去、SHA-256 hash
- [x] v4 migrationと`observation_scopes`（browser.chromium / vscode.workspace、active/paused）
- [x] Browser/VS Code Observation Enable、Pause、ListをCore IPCとCLIへ追加
- [x] Adapter HelloのEvent CapabilityとEvent-only Adapterを追加
- [x] Core-built semantic Event（Browser Resource Opened / VS Code Workspace Opened）とEvent Ack
- [x] Browser `browser.tab.ensure_open.v1` Action、Permission付き同一Adapter選択、Execution History
- [x] `fubun-native-host`（Chrome Native Messaging framing、Origin/Extension ID検証、stdout専用Protocol）
- [x] Manifest V3 Browser Extension（optional host permission、Storage hash/ID、navigation semantic event）
- [x] VS Code UI Extension（Local single-folder workspace、globalState hash/ID、Event-only Adapter）
- [x] Native Host install/status/uninstall、Integration status、README/ADR/Threat Model/CI更新
- [x] Rust/TypeScript unit・integration・schema drift・artifact build検証

### Phase 3 design decisions and assumptions

- BrowserとVS CodeのResource locatorはCoreでcanonicalizeする。URLのquery/fragment、Workspaceのraw pathはEvent、ログ、Extension Storageへ保存しない。
- Browser ExtensionはPopupのユーザーGesture中だけOrigin permissionを要求し、成功後にScopeを登録する。Permission/Scope変更時は新しいAdapter Instanceを再登録し、許可Resource snapshotを別Instanceと合成しない。
- Native HostはChromeのcaller originと設定ファイルの許可Extension IDを二重検証し、Native Messagingの32-bit native-endian framingとCoreの4-byte little-endian framingを変換する。
- VS Codeは本文やファイル名を読まず、Local/file/single-folderのcanonical hashとResource IDだけを保持する。Remote/Multi-root/Virtual/Untitledは対象外。
- Browser ActionはR1のResource ID-only Actionとし、Undoや自動Triggerは主張しない。
- `observation_scopes`は(source, resource_id)を一意にし、Enableは既存Scopeをactiveへ戻す。Pauseは冪等で、active Scopeをpausedへ遷移させ、既にpausedなら現在のScopeをそのまま返し、存在しないIDだけをNotFoundにする。

### Phase 3 verification notes

- 旧Phase 1 synthetic Eventのdataはtagなしだったため、v4 migrationで既存`data_json`/`canonical_json`へ`kind: synthetic`を補い、履歴を失わずstrict EventDataへ移行する回帰Testを追加した。
- Browser Observation Enable後はNative Portを再接続し、Coreが新しい`permitted_resource_ids`を持つ同一Adapter InstanceだけをEvent/Actionの適格候補にする。
- Browser/VS Codeの実Chrome/VS Code Profileは変更せず、Rust integration test、Fake API、Temporary XDG/HOME、生成artifactで検証する。

### Phase 3 deferred

- Phase 4のSessionizer、Pattern Miner、Suggestion、Evidence、Dismissal/Snooze、SuggestionからのRitual Draft生成。
- Firefox、Marketplace公開、Chrome実体のCI起動、Cross-platform Adapter、GUI、Performance tuning、複数Extension IDを跨ぐ高度なManifest管理。

### Phase 3 PR #2 final hardening

PR #2 head `27e1f5d43d9a092054b02c0970d7603a7c89e3c2` の再現確認では、通常Clientの`event.ingest`が完全な`Event`を受理するためBrowser/VS Code semantic eventを偽装できた。Native HostはEvent Emit後にAdapter socketを直接readして次のframeをEvent Ackと仮定しており、Action Executeとの逆順到着を処理できなかった。Browser ExtensionはCore Ack前にenableを成功扱いし、pauseやpermission removalでCore Scopeの状態確認前に再接続し、`Date.now()`をsequenceとしていた。Browser ActionはActionとResolved ResourceのID一致を確認せず、VS CodeはEventごとに短命Adapter connectionを作りHello/Event Ackを厳格検証していなかった。Browser/VS Codeの実行可能なLifecycle testも不足していた。

- [x] Client `event.ingest`をSynthetic Event専用に制限し、semantic eventはAdapter Event EmitだけでCoreが組み立てる
- [x] Native HostのAdapter streamを単一Reader、単一Writer queue、request_id別Pending Event Ackへ分離
- [x] Browser NativeBridgeの相関Request Manager、Observation enable/pause state machine、permission reconciliation、単調sequenceを追加
- [x] Browser Action envelopeのResource ID一致とstrict payload検証を追加
- [x] VS Codeの永続Event-only Adapter sessionとstrict framed response検証を追加
- [x] Browser/Native Host/VS Codeの実行可能なUnit/Integration regressionを追加

### Phase 3 PR #2 reconnect hardening

PR #2 head `fe7bac2187415ee6f1954f6db9229622a6c2f6c3` の再現確認では、Browser Eventのsequence払い出しがNative Hello完了前に行われ、再接続時に旧Instanceの番号を新Instanceへ持ち込む余地があった。Observation Pauseはactive専用更新で、応答喪失後の再送が失敗し得た。またAdapter接続の切断がinstance IDだけを鍵にしており、同一Instance IDの新接続を旧接続が削除し得た。

- [x] Browser NativeBridgeの`requestPrepared`でHello完了後にEvent payloadとsequenceを生成し、失敗しても同一Instance内で番号を再利用しない
- [x] Observation Pauseをactive→paused／paused→paused成功の冪等操作へ変更し、Browser/VS Codeの再送を可能にした
- [x] Adapter接続ごとにopaqueな`ConnectionToken`を発行し、`disconnect_if_current`で旧接続の終了が新接続を削除しないようにした
- [x] 接続置換時は旧Tokenに属するPending ActionだけをDisconnectedで解放し、新接続のPendingを保持する回帰Testを追加した
