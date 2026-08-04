# ADR-0007: RitualのVersioningとLifecycle

- Status: Accepted
- Date: 2026-08-03

## Decision

RitualはDraftを起点にし、ImmutableなRitualVersionを参照する。Previewと明示Approvalが成功したVersionだけActiveにでき、更新は常に新Versionを作ってDraftへ戻す。Paused/Archived/Draftは実行しない。

## Rationale

実行対象、承認根拠、履歴を同じJSONに固定し、stale approvalと部分的な更新を防ぐ。
