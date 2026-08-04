# ADR-0019: Browser Tab ActionのResource ID境界

- Status: Accepted
- Date: 2026-08-04

## Decision

`browser.tab.ensure_open.v1`は登録済みWeb Page ResourceのIDだけを入力に持つ。Core PreflightとExtension実行直前にResource kind、Scope、Permission、canonical hashを再検証し、任意URLを受け付けない。

## Consequences

既存TabのExact hash一致はskipped、新規Tabはopenedとし、Tab IDはEphemeralでDatabaseへ保存しない。Universal Undoは主張しない。
