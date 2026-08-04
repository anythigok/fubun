# ADR-0010: Bidirectional IPCによるAction Dispatch

- Status: Accepted
- Date: 2026-08-03

## Decision

AdapterはUnix Domain Socketで長時間接続し、Hello時に固定Action RegistryのCapabilityを重複なく宣言する。ActionごとにCapability、固定Tool、必要ならDesktop Entry IDを持つ `AdapterRequirements` を生成し、Preview、Preflight、Dispatchは同じ単一Instance選択を使う。候補はinstance UUID文字列の昇順で選び、複数Adapterの状態を合成しない。CoreはAction Requestを送り、request IDとaction execution IDに対応するoneshotでResultを待つ。選択後のtimeout、disconnect、protocol errorは実Adapter IDとInstance IDを伴うdispatch errorとして返す。Pendingは同期RAII Guardが所有し、正常Response、Future cancellation、Timeout、Disconnect、Shutdown、send failureの全経路で必ず解放する。Ritual全体は外側の強制dropに依存せず、deadlineの残り時間をAction dispatchへ渡す。

## Rationale

CoreとAdapterの責務を分離しつつ、固定Actionを逐次実行できる最小の双方向境界を作る。
