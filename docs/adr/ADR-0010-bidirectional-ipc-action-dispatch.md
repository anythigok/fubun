# ADR-0010: Bidirectional IPCによるAction Dispatch

- Status: Accepted
- Date: 2026-08-03

## Decision

AdapterはUnix Domain Socketで長時間接続し、Hello時にCapabilityを宣言する。CoreはAction Requestを送り、request IDとaction execution IDに対応するoneshotでResultを待つ。正常Response、Timeout、Disconnect、ShutdownでPendingを必ず解放する。

## Rationale

CoreとAdapterの責務を分離しつつ、固定Actionを逐次実行できる最小の双方向境界を作る。
