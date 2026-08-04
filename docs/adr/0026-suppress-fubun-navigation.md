# ADR-0026: Fubun生成Browser NavigationをObservationから除外する

Status: accepted

Browser Actionで新規Tabを作った場合だけ、ExtensionのEphemeral session storageへTab ID、
Resource ID、60秒以内の期限を置く。次の一致Navigationを一度だけ消費し、Tab IDはCoreや
永続Storageへ送らない。既存Tabを再利用するActionにはSuppressionを作らない。
