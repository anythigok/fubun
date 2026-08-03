# ADR-0003: Local IPCとしてUnix Domain Socketを使う

- Status: Accepted
- Date: 2026-08-03

## Decision

Linux IPCは `$XDG_RUNTIME_DIR/fubun/core.sock` のUnix Domain Socketだけを使う。runtime directoryは0700、socketは0600にする。

localhostであってもTCP/HTTPはnetwork attack surface、port競合、proxy誤設定を生むため採用しない。Windows/macOS transportは将来別adapterとして判断する。
