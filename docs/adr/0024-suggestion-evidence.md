# ADR-0024: Suggestion EvidenceとFingerprint

Status: accepted

EvidenceはAlgorithm Version、Resource ID、support、confidence basis points、観測時刻、
completion中央値だけを保存する。FingerprintはVersion、Workspace Resource ID、順序付き
Action Resource IDからSHA-256で作り、URLやPathを含めない。同一Fingerprintは一つだけ。
