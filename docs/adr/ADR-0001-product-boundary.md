# ADR-0001: Fubunの製品境界

- Status: Accepted
- Date: 2026-08-03

## Decision

Fubunを「許可された意味的Eventから反復を発見し、承認可能な自動化へ段階的に変換するローカルファースト基盤」とする。Phase 1はSynthetic Eventのingest・保存・照会だけを実装する。

キーロガー、画面収集、汎用RPA、AI agent、OS distributionにはしない。PatternからRuleへ直接昇格させない。
