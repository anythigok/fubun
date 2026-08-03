# ADR-0002: Rust Coreを採用する

- Status: Accepted
- Date: 2026-08-03

## Decision

常駐Core、protocol、storageをRust workspaceとして実装する。明示的な型、所有権、bounded allocation、単一binary配布を安全境界に利用する。

crateはdomain、protocol、storage、coreの4境界に留め、Phase 1では細分化しない。
