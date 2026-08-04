# ADR-0009: CoreとLinux Adapterの責務分離

- Status: Accepted
- Date: 2026-08-03

## Decision

CoreはValidation、Policy、Plan、Historyだけを所有し、gtk-launch/xdg-open/notify-sendはLinux Adapterだけが固定引数で呼ぶ。CoreはOS Commandを直接実行しない。Production Linux Adapterは実環境の固定Executableだけを使い、Fake Runnerや偽のstatus経路を持たない。副作用なしのFake AdapterはIntegration Testのprotocol supportに限定する。

## Rationale

OS固有権限と実行面を小さなAdapterへ閉じ込め、Fake Adapterで副作用なしの検証を可能にする。
