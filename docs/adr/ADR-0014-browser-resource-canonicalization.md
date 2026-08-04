# ADR-0014: Browser Resource Canonicalization

- Status: Accepted
- Date: 2026-08-04

## Decision

Web Page Resourceは検証済みURL Parserでhttp/https、userinfoなし、host正規化、default port除去、空Path`/`、query/fragment除去を行う。canonical URLからlowercase SHA-256を作り、Rust/TypeScript fixtureで一致を検証する。

## Consequences

Event、Execution、Extension Storageにはraw URLを複製しない。Exact hash照合でTabを判定し、URL tokenやfragmentを保存しない。URLの意味的なリダイレクト解決は行わない。
