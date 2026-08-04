# ADR-0016: Native Messaging Host Bridge

- Status: Accepted
- Date: 2026-08-04

## Decision

`fubun-native-host`をChrome Native Messagingのstdio bridgeとする。Browser側の32-bit native-endian frameをCoreの4-byte little-endian Unix Socket frameへ変換し、caller origin、Extension Hello、0600設定ファイルを検証する。stdoutはProtocol frame専用、診断はstderrだけとする。

## Consequences

CoreはTCP/HTTPやBrowser固有APIを持たない。Native HostはShell、任意Executable、Network APIを持たず、Browser ActionはExtensionへ要求を転送する。
