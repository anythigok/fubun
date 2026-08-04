# ADR-0015: Chromium Manifest V3とOptional Host Permission

- Status: Accepted
- Date: 2026-08-04

## Decision

Browser ExtensionはManifest V3、`activeTab`、`nativeMessaging`、`storage`だけを常時要求し、http/https Originはoptional host permissionとしてPopupのユーザーGesture中に要求する。Content Script、scripting、tabs permission、全サイト常時権限、Incognitoは使用しない。

## Consequences

ユーザーはページごとに許可範囲を確認できる。Permission変更時はMappingを停止しNative Portを再接続するため、CoreのAdapter Status snapshotが古いまま残らない。
