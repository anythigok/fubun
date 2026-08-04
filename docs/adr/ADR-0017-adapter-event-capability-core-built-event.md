# ADR-0017: Adapter Event CapabilityとCore-built Event

- Status: Accepted
- Date: 2026-08-04

## Decision

Adapter HelloにAction CapabilityとEvent Capabilityを分離して持たせ、VS CodeのEvent-only Adapterを許可する。Adapterはsequence、occurred_at、resource_id、event typeだけを送り、actor、source、received_at、Adapter identity、privacy、Event IDはCoreが接続済みHelloから組み立てる。

## Consequences

Adapterからactor/sourceを偽装できず、Active ScopeとResource kindをCoreの一箇所で検証できる。Duplicateはadapter instanceとsequenceでAckする。
