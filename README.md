# Fubun

Fubunは、許可された意味的なデスクトップEventから反復ワークフローを発見し、読めるRitualとRuleへ段階的に変換するためのローカルファースト基盤です。

Phase 1は最小Core Vertical Sliceです。Unix Domain SocketでSynthetic Eventを受信し、厳格なJSON検証後にSQLiteへ保存し、CLIから状態とEventを確認できます。Pattern Miner、Rule実行、GUI、Adapter、AI、外部ネットワーク通信はまだ含みません。

## Security boundary

- Linux IPCは `$XDG_RUNTIME_DIR/fubun/core.sock` のUnix Domain Socketのみ。
- IPC frameは4-byte little-endian length + UTF-8 JSON、上限256 KiB。
- キー入力、マウス、画面、クリップボード、ブラウザ本文を扱わない。
- localhost HTTP/TCP、任意シェル実行、telemetry、cloud syncを持たない。
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

## Workspace

- `crates/fubun-domain`: Eventとprivacy/domain型
- `crates/fubun-protocol`: versioned envelopeとbounded framing
- `crates/fubun-storage`: SQLite migrationとsingle writer
- `crates/fubun-core`: Unix Socket daemonとclient
- `apps/fubund`: daemon entrypoint
- `apps/fubun-cli`: `fubun` CLI
- `tests/integration`: restart、duplicate、malformed、oversize、permissionの実証

Architectureと判断根拠は [docs/architecture/overview.md](docs/architecture/overview.md) と [docs/adr](docs/adr) を参照してください。
