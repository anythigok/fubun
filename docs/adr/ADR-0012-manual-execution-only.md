# ADR-0012: Phase 2はManual Executionに限定する

- Status: Accepted
- Date: 2026-08-03

## Decision

Phase 2はCLIからの明示的なManual Runだけを実装する。Pattern Miner、Suggestion、Rule、Automatic Trigger/Execution、GUIは後続Phaseへ延期する。

## Rationale

PreviewとApprovalの安全境界を先に検証し、暗黙の相関から自動化を生成しない。
