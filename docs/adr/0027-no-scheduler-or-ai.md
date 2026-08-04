# ADR-0027: Phase 4AではBackground SchedulerとAIを使用しない

Status: accepted

Discoveryはユーザーが`fubun discovery run`またはIPCを明示的に呼んだ時だけ動く。LLM、
機械学習、通知、Background Scheduler、自動実行はPhase 4AのScope外であり、Evidenceの
説明可能性とローカルファースト境界を優先する。
