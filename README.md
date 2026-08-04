# Fubun

Fubunは、許可された意味的なデスクトップEventから反復ワークフローを発見し、読めるRitualとRuleへ段階的に変換するためのローカルファースト基盤です。

Phase 2は、ユーザーが明示的に作成したRitualをPreview、Approval、手動実行し、Linux Adapterの固定ActionとExecution HistoryへつなぐVertical Sliceです。自動発見、自動Trigger、GUIは含みません。

## Security boundary

- Linux IPCは `$XDG_RUNTIME_DIR/fubun/core.sock` のUnix Domain Socketのみ。
- IPC frameは4-byte little-endian length + UTF-8 JSON、上限256 KiB。
- キー入力、マウス、画面、クリップボード、ブラウザ本文を扱わない。
- localhost HTTP/TCP、任意シェル実行、telemetry、cloud syncを持たない。
- Ritualは固定Action RegistryのR0/R1だけを許可し、生Path・任意Executable・Shell Commandを受け取らない。
- Draft・Paused Ritualは実行せず、Version更新時は旧Approvalを失効させる。
- stdout/stderr全文を保存せず、短いredacted messageだけをExecution Historyへ記録する。
- data directoryは0700、databaseとsocketは0600。

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
