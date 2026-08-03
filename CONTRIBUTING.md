# Contributing

変更前に [AGENTS.md](AGENTS.md) と関連ADRを読んでください。

1. 小さなfeature branchを作る。
2. Schema変更時はMigration、Fixture、Testを同時に更新する。
3. `cargo fmt --all --check`、`cargo clippy --workspace --all-targets --all-features -- -D warnings`、`cargo test --workspace` を通す。
4. Security境界や権限を広げる変更にはADRを追加する。

禁止データ、localhost HTTP/TCP、任意シェル実行を追加するPRは受け入れません。
