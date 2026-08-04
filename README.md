# Fubun

Fubunは、許可された意味的なデスクトップEventから反復ワークフローを発見し、読めるRitualとRuleへ段階的に変換するためのローカルファースト基盤です。

Phase 2は、ユーザーが明示的に作成したRitualをPreview、Approval、手動実行し、Linux Adapterの固定ActionとExecution HistoryへつなぐVertical Sliceです。Phase 3ではChromiumとVS Codeの明示的なObservation Scope、意味Event、Browser Actionを追加します。自動発見、自動Trigger、GUIは含みません。

## Security boundary

- Linux IPCは `$XDG_RUNTIME_DIR/fubun/core.sock` のUnix Domain Socketのみ。
- IPC frameは4-byte little-endian length + UTF-8 JSON、上限256 KiB。
- キー入力、マウス、画面、クリップボード、ブラウザ本文を扱わない。
- localhost HTTP/TCP、任意シェル実行、telemetry、cloud syncを持たない。
- Ritualは固定Action RegistryのR0/R1だけを許可し、生Path・任意Executable・Shell Commandを受け取らない。
- Draft・Paused Ritualは実行せず、Version更新時は旧Approvalを失効させる。
- stdout/stderr全文を保存せず、短いredacted messageだけをExecution Historyへ記録する。
- Preview、Preflight、Dispatchは同じ単一Adapter InstanceのCapability・固定Tool・Desktop Entryを確認し、timeout/disconnect/protocol errorでも選択済みAdapterのIdentityを履歴へ残す。
- 終端したExecutionにはpending/running Stepを残さず、失敗後に未実行だったStepはabortedとして記録する。
- Browser/VS Code Eventは、Coreでactor、source、adapter identity、received_atを組み立て、Active Observation Scopeがある場合だけ保存する。
- 通常Clientの`event.ingest`は開発用Synthetic Eventだけを許可し、Browser/VS Code Semantic EventはAdapter Event Emit経路からしか保存できない。
- Browser Resourceはhttp/httpsだけを受け付け、query、fragment、userinfo、raw URL/PathをEventやExtension Storageへ保存しない。
- Browser Actionは登録済みResource IDだけを受け取り、Native Messaging経由で許可済み同一Originのタブだけを開く。
- Native HostのstdoutはNative Messaging frame専用で、許可Originは明示的なExtension IDだけに限定する。
- Native HostはAdapter UDSのReaderを一つに固定し、Event Ackをrequest_idで配送するため、Action ExecuteとAckの到着順に依存しない。
- BrowserのEnableはCore AckとPermission再確認後にだけLocal Mappingを保存する。Stop/Permission解除時は先にLocal Event送信を停止し、Core Pause失敗時もinactive状態を維持する。
- Browser/VS Code Event sequenceはAdapter Instanceごとに単調増加し、日時値をsequenceとして使わない。VS Codeは観察中のEvent-only Adapter接続を再利用する。
- VS Code連携はLocal、file scheme、single-folder workspaceだけを対象にし、本文、ファイル名、Terminal、Git差分を取得しない。
- data directoryは0700、databaseとsocketは0600。

## Phase 4A Discovery

Phase 4Aは、明示的に観察中のVS Code WorkspaceをAnchorに、開始から10分以内に
開かれたBrowser Resourceの安定したPrefixだけを決定論的に候補化します。対象は
`workspace-browser-start/v1`一種類で、Generic n-gram、AI、Scheduler、Rule、自動実行は
ありません。Raw URL、Workspace Path、本文、Title、Query、FragmentはSession／Evidenceへ
保存せず、Resource IDとラベルだけで説明します。新規SuggestionはRolling 24時間で最大1件、
未終端の候補は最大5件、Dismissは30日間抑止します。

```text
fubun discovery run
fubun discovery status
fubun sessions list
fubun session show <session-id>
fubun suggestions list --status pending
fubun suggestion show <suggestion-id>
fubun suggestion snooze <suggestion-id> --for 14d
fubun suggestion dismiss <suggestion-id>
fubun suggestion block <suggestion-id>
fubun suggestion accept <suggestion-id> --name "Research start"
```

Acceptは現在のResource／Scopeを再確認し、Browser ActionだけのDraft Ritualを一つ作ります。
Approval、Activation、Runはユーザーが既存のRitualコマンドで別途行います。同じFingerprintの
再提案やAcceptによるRitual重複はありません。FubunのBrowser Actionで新規Tabを作成した
Navigationは、Extensionの`chrome.storage.session`に最大60秒だけ保持するEphemeral情報で
一度抑止されます。Tab IDはCoreや永続Databaseへ送信されません。

## 必要環境

- Ubuntu 24.04 LTS（amd64）
- Rust 1.85.0（`rust-toolchain.toml`で固定）
- 有効な `XDG_RUNTIME_DIR`

## Buildと検証

```bash
cargo build --workspace
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
```

JSON Schemaの再生成:

```bash
cargo run -p fubun-protocol --example generate_schemas
```

## ローカル実行

通常のdesktop sessionでは既存の `XDG_RUNTIME_DIR` とdata homeを使います。

Terminal 1:

```bash
cargo run -p fubund
```

Terminal 2:

```bash
cargo run -p fubun-cli -- status
cargo run -p fubun-cli -- dev emit-fixture
cargo run -p fubun-cli -- events list
cargo run -p fubun-cli -- events list --since 1h
cargo run -p fubun-cli -- doctor
```

`dev emit-fixture` は重複検証用に固定の `adapter.instance_id` と `sequence_no` を送ります。同じdatabaseへ2回送ると `duplicate_event` になります。

隔離したsmoke testには、絶対パスの一時directoryを指定できます。

```bash
export XDG_RUNTIME_DIR=/tmp/fubun-smoke-runtime
export XDG_DATA_HOME=/tmp/fubun-smoke-data
mkdir -p "$XDG_RUNTIME_DIR" "$XDG_DATA_HOME"
cargo run -p fubund
```

Databaseは `$XDG_DATA_HOME/fubun/fubun.db`、未設定時は `$HOME/.local/share/fubun/fubun.db` です。daemon停止はCtrl-Cで行います。

## Phase 2: ResourceとRitual

まずDaemonとLinux Adapterを別Terminalで起動します。Production AdapterにはFake実行経路がなく、接続先の固定Executableと実環境の状態を使用します。実際のアプリやファイルを開かない検証は、Integration Test内のTest専用Fake Adapterを使ってください。

```bash
cargo run -p fubund
cargo run -p fubun-linux-adapter
```

Resourceは絶対Pathを登録し、登録時と実行直前にCanonical Pathを再確認します。

```bash
fubun resource add-path --label research --path /absolute/path/to/research
fubun resource list
fubun resource show <resource-id>
```

Fixtureの `resource_id` を登録したIDへ差し替え、保存前にValidateします。Fixtureは個人Pathを含みません。

```bash
fubun ritual validate --json-file fixtures/rituals/research-start.json
fubun ritual create --json-file /tmp/research-start.json
fubun ritual preview <ritual-id>
fubun ritual activate <ritual-id> --approve
fubun ritual run <ritual-id>
fubun execution list
fubun execution show <execution-id>
fubun ritual pause <ritual-id>
```

`activate` は `--approve` が必須です。PreviewでAdapter、Desktop Entry、ResourceのPreflightが一つでも失敗するとActiveにできません。Ritual更新は新しいImmutable Versionを作り、再承認が必要です。

AdapterとDaemonの状態は次で確認できます。

```bash
fubun adapter list
fubun adapter status
fubun doctor
```

## Phase 2で実装していない機能

Pattern Miner、Suggestion、Rule、Automatic Trigger/Execution、GUI、Browser Extension、VS Code Extension、Process/File Watcher、Windows/macOS、Cloud Sync、Login、Telemetry、LLM、Plugin API、Universal Undoは後続Phaseまたは明示的な非対象です。

## Workspace

- `crates/fubun-domain`: Eventとprivacy/domain型
- `crates/fubun-policy`: 固定Action RegistryとRisk Policy
- `crates/fubun-protocol`: versioned envelopeとbounded framing
- `crates/fubun-storage`: SQLite migrationとsingle writer
- `crates/fubun-core`: Unix Socket daemon、Preview、Approval、Execution orchestration
- `apps/fubun-linux-adapter`: 固定Executableだけを呼ぶLinux Adapter
- `apps/fubund`: daemon entrypoint
- `apps/fubun-cli`: `fubun` CLI
- `tests/integration`: restart、duplicate、malformed、oversize、permissionの実証

Architectureと判断根拠は [docs/architecture/overview.md](docs/architecture/overview.md) と [docs/adr](docs/adr) を参照してください。

## Phase 3: ChromiumとVS Code連携

Phase 3は、ユーザー操作で登録したページ／Workspaceから意味EventをCoreへ送るための統合層です。Browser ExtensionはManifest V3、optional host permission、Native Messagingを使い、Content Scriptや全サイト権限を使いません。VS Code ExtensionはUI extensionとしてLocalのsingle-folder workspaceだけを扱います。

### Chromium Extension（unpacked）

Node.jsとpnpmを用意した後、Repository rootでBundleを作成します。

```bash
pnpm install --frozen-lockfile
pnpm --filter @fubun/protocol-ts build
pnpm --filter @fubun/browser-extension package
```

`extensions/browser/dist`をChrome/ChromiumのDeveloper modeでLoad unpackedします。配布用の`artifacts/fubun-browser-extension.zip`も同じCommandで生成されます（Artifact自体はGitへCommitしません）。表示されたExtension ID（32文字の`a`〜`p`）を、Native Host Manifestへ明示登録します。実際のProfileを変更する前に、Temporary HOME/XDGディレクトリでCLIを試してください。

```bash
cargo build -p fubun-native-host
cargo run -p fubun-cli -- browser host install --browser chrome \
  --extension-id <extension-id> --host-path "$PWD/target/debug/fubun-native-host"
cargo run -p fubun-cli -- browser host status
```

Extension Popupの「このページを観察する」はユーザーのClick Handler内でOrigin permissionを要求します。許可後もCoreの`browser.observation.enabled`応答とPermission再確認が成功するまで登録成功とは表示せず、成功時だけcanonical URL hashとResource IDをExtension Storageへ保存します。停止またはPermission解除はLocal Event送信を即時停止してからCore Scope Pauseを同期します。Coreが一時的に不達でもLocal Mappingを再びactiveにはせず、次回Service Worker起動時にpending Pauseを一度だけ再同期します。

```bash
fubun observations list --source browser.chromium
fubun integrations status
```

登録済みページのNavigationが完了すると、EventにはResource IDだけが送られます。Browser Action Ritualでは`browser.tab.ensure_open.v1`にResource IDを記述し、PreviewでPermissionとScopeを確認してから手動Runします。既存タブは`already_open`、未開設なら`opened`としてExecution Historyへ短い結果だけが残ります。

Native Hostの解除は明示的に行います。

```bash
cargo run -p fubun-cli -- browser host uninstall --browser chrome
cargo run -p fubun-cli -- browser host uninstall --browser chromium
```

### VS Code Extension

```bash
pnpm --filter fubun-vscode-extension build
pnpm --filter fubun-vscode-extension package
code --install-extension artifacts/fubun-vscode-extension.vsix
```

VS Codeで「Fubun: Observe Current Workspace」を明示実行すると、Localかつfile schemeのsingle-folder workspaceだけが登録されます。globalStateにはResource ID、Scope ID、canonical hashだけを保存します。観察中はEvent-only Adapterを長時間接続し、Hello、Event Ack、request ID、protocol versionを検証して同じAdapter Instance内で単調なsequenceを送ります。Multi-root、Remote、Virtual、Untitled workspaceは状態表示だけで、Eventを送りません。

```bash
fubun observations list --source vscode.workspace
fubun observation pause <scope-id>
```

### Phase 3で実装していない機能

Pattern Miner、Sessionizer、Suggestion、Ritual自動生成、Rule、Automatic Trigger/Execution、GUI、Browser本文解析、Content Script、Cookie/History/Downloads/WebRequest、Firefox、Windows/macOS、Marketplace公開、Cloud Sync、Login、Telemetry、LLM、Plugin API、Universal Undoは対象外です。Bundle、VSIX、Manifestは生成可能ですが、Build ArtifactはGitへCommitしません。
