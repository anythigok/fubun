# Core architecture

Phase 1のwrite/data flowは一本です。

```text
fubun-cli / future adapter
  -> Unix Domain Socket
  -> bounded frame decoder
  -> strict RequestEnvelope + client.hello
  -> Event validation
  -> single writer queue
  -> SQLite (WAL)
```

`fubun-domain` はCanonical Event、`fubun-protocol` はtransport framingとrequest/response、`fubun-storage` は永続化、`fubun-core` はprocess内のorchestrationを所有します。CLIのEvent一覧も必ずdaemonを通り、databaseを直接読みません。

受信Eventの `received_at` はdaemonがUTC現在時刻で上書きします。重複判定はEvent IDではなく `(adapter_instance_id, sequence_no)` が正本です。

Phase 1にoutbound network client、TCP listener、action executorはありません。
