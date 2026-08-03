# ADR-0005: Local-firstと禁止データ

- Status: Accepted
- Date: 2026-08-03

## Decision

Coreは外部ネットワーク通信を行わず、Eventと設定はlocal data directoryだけへ保存する。

キー入力、マウス座標、画面、クリップボード、ブラウザ本文、credentialを収集する型・adapter・fallbackを実装しない。権限拡大は新しいADRとsecurity reviewを必須とする。
