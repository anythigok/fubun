# ADR-0013: Observation ScopeとExplicit Opt-in

- Status: Accepted
- Date: 2026-08-04

## Decision

Browser PageとVS Code Workspaceは、ユーザー操作でResourceを登録し、対応する`ObservationScope`をactiveにした場合だけ観察対象とする。Scopeは`(source, resource_id)`で一意、pauseと再Enableだけを提供し、暗黙の全体監視や自動発見は実装しない。

## Consequences

CoreはEvent保存前にScope、Resource kind、Adapter capabilityを検証できる。Permission解除や停止はScope pauseとして履歴境界を保つ。Pattern MinerはPhase 4以降で扱う。
