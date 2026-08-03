# Fubun agent rules

- 禁止データ（キー入力、マウス、画面、クリップボード、ブラウザ本文）を追加しない。
- localhost HTTP/TCPを追加しない。Linux IPCはUnix Domain Socketだけを使う。
- 任意シェル実行を追加しない。
- 将来のPattern Minerは `actor=fubun` のEventを学習対象から除外する。
- Schema変更時はMigration、Fixture、Testを同時に更新する。
- 権限拡大にはADRを要求する。
- FormattingとLintの最終判定はCIに任せる。
- Security境界を壊す変更はP1として扱う。
