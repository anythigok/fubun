# ADR-0020: Raw URLとWorkspace PathをEventへ保存しない

- Status: Accepted
- Date: 2026-08-04

## Decision

Browser/VS Code Event dataはResource IDだけを許可する。Canonical URL/Pathは登録・Preflight・hash計算の短時間だけ使用し、Event、通常Log、Execution History、Extension Storageへ保存しない。

## Consequences

Event履歴を見ても個別URLやWorkspace Pathを復元できない。Resourceの現在値はCore DatabaseのResource recordで管理し、PermissionとScopeを別テーブルで検証する。
