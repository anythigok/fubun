# ADR-0008: 固定Action RegistryとRisk Policy

- Status: Accepted
- Date: 2026-08-03

## Decision

Actionはcompile-timeの固定Registryから取得し、Phase 2はR0/R1だけを許可する。Adapter Helloも同じRegistryに照合し、未知・重複・過剰なCapabilityを受理しない。Runtime plugin登録、任意Executable、R2/R3は作らない。

## Rationale

Policy、Preview、Approval、Executionで同じDescriptorを使い、許可範囲の不一致を防ぐ。
