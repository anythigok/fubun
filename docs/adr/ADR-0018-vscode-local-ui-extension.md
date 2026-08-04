# ADR-0018: VS Code Local UI Extension

- Status: Accepted
- Date: 2026-08-04

## Decision

VS Code連携は`extensionKind: ["ui"]`のNode Extensionとし、Local/file scheme/single-folder workspaceだけを対象にする。Remote、Multi-root、Virtual、Untitledは拒否する。本文、現在ファイル名、Terminal、Git差分、Settingsは取得しない。

## Consequences

Workspace登録は明示Commandだけで開始され、globalStateとEventはResource ID、Scope ID、canonical hashに限定される。VS Code側はEvent-only AdapterとしてCoreへ接続する。
