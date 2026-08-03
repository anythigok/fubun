# Fubun agent rules

- 禁止データ（キー入力、マウス、画面、クリップボード、ブラウザ本文）を追加しない。
- localhost HTTP/TCPを追加しない。Linux IPCはUnix Domain Socketだけを使う。
- 任意シェル実行を追加しない。
- 将来のPattern Minerは `actor=fubun` のEventを学習対象から除外する。
- Schema変更時はMigration、Fixture、Testを同時に更新する。
- 権限拡大にはADRを要求する。
- FormattingとLintの最終判定はCIに任せる。
- Security境界を壊す変更はP1として扱う。
- Actionは固定Registryに登録された型だけを許可する。
- Shell経由実行、任意Executable、R2以上のActionを追加しない。
- Ritual JSONへ生Pathを置かず、Draft・Paused Ritualを実行しない。
- Material変更後はApprovalを失効させる。
- stdout・stderr全文を保存せず、CoreはOS Commandを直接実行しない。
- Phase 2では自動Trigger・自動Executionを追加しない。
