# ADR-0004: Canonical FormatをJSONにする

- Status: Accepted
- Date: 2026-08-03

## Decision

Canonical EventとIPC envelopeはversion付きの厳格なJSONとし、全structで未知フィールドを拒否する。transportは4-byte little-endian length prefixとし、payloadを256 KiB以下に制限する。

JSONはdebuggability、schema generation、拡張実装の容易さを優先した判断である。binary formatへの変更は新しいprotocol majorとADRを必要とする。
