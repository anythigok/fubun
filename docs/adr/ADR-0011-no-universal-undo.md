# ADR-0011: Universal Undoを実装しない

- Status: Accepted
- Date: 2026-08-03

## Decision

Action Descriptorにはrevertabilityを記録するが、全Actionに効くUniversal Undo型や主張は実装しない。Phase 2のActionは削除・移動・上書きを含めない。

## Rationale

外部アプリ起動や通知に一般的な逆操作はなく、誤ったUndo保証は安全性を損なう。
