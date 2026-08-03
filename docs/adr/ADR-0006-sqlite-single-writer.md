# ADR-0006: SQLiteとSingle Writerを採用する

- Status: Accepted
- Date: 2026-08-03

## Decision

永続化にSQLiteを使い、全操作をdatabase connectionを所有する専用threadへqueueする。write pathはこのsingle writerだけとする。

WAL、foreign keys、5秒のbusy timeoutを有効化する。migrationは `schema_migrations` で管理し、Event重複はdatabaseの `UNIQUE(adapter_instance_id, sequence_no)` を最終防衛線とする。
