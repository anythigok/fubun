# Core architecture

Phase 2のwrite/data flowもsingle writer queueへ集約されます。

```text
fubun-cli / fubun-linux-adapter
  -> Unix Domain Socket
  -> bounded frame decoder
  -> strict versioned Envelope + client.hello / adapter.hello
  -> Ritual validation / Preview / Approval / Execution plan
  -> single writer queue
  -> SQLite (WAL, v2 migration)
```

`fubun-domain` はEventとRitual/Resource/Execution型、`fubun-policy` は固定Action Registry、`fubun-protocol` はtransport framingと双方向envelope、`fubun-storage` は永続化、`fubun-core` はPreview・Approval・Execution orchestrationを所有します。Linux固有Commandは `fubun-linux-adapter` だけが固定引数配列で呼び出します。CLIは全てdaemonを通り、databaseを直接読みません。

受信Eventの `received_at` はdaemonがUTC現在時刻で上書きします。重複判定はEvent IDではなく `(adapter_instance_id, sequence_no)` が正本です。

Ritualは `draft -> active -> paused/archived` の状態を持ち、各Versionはimmutableです。ActivationはPreview成功と `--approve` を要求し、Versionのcontent hashが変われば旧Approvalを使えません。Executionは逐次Action、overall timeout、単一Ritual lock、redacted historyを持ちます。

CoreとAdapterは長時間の双方向Unix socket接続を使います。Coreはcapabilityを持つAdapterを選び、request/action execution IDごとのoneshot pending requestをtimeout・disconnect・shutdown時に必ず解放します。

外部network client、TCP listener、Pattern Miner、Rule、自動Trigger、GUIはありません。
