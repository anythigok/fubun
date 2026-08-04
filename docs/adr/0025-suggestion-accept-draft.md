# ADR-0025: Suggestion AcceptはRitual Draftだけを作る

Status: accepted

Acceptは一つのSQLite Transaction内で現在のResource／Scopeを再検証し、Browser Actionだけ
を持つDraft Ritualを作る。Approval、Activation、実行は別の明示操作とし、Accept再送は
既存Ritualを返して重複を防ぐ。
