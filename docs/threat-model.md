# Threat model

## Assets and trust boundary

保護対象はlocal Event history、database整合性、daemon availability、ユーザーのprivacyです。Phase 1は同一Linuxユーザーのclientだけを想定し、socket directory 0700とsocket 0600を第一境界にします。

## Threats and mitigations

### Malicious IPC Client

同一ユーザー権限を得たclientが不正requestを送る可能性があります。最初のrequestに `client.hello` を要求し、protocol major、strict envelope、Event validationを確認します。権限・認証の追加は将来ADR対象です。

### Malformed Message

不正UTF-8/JSON、truncated frame、未知フィールドはconnection単位で拒否します。connection errorはaccept loopへ伝播させず、daemonを継続します。

### Oversized Message

length prefixをallocation前に検査し、256 KiBを超えるconnectionを終了します。

### Duplicate Event

adapterの再送やreconnectを想定し、`adapter_instance_id + sequence_no` にdatabase unique constraintを置き、`duplicate_event` を明示的に返します。

### Same-user Attacker

同一uidで任意codeを実行できる攻撃者はsocket接続、database読取、process操作が可能です。Phase 1は同一ユーザー内の強い隔離を提供しません。OS account分離、disk encryption、session lockを利用してください。

### Database Theft

directory 0700、database 0600で他ユーザーのreadを制限します。保存内容の暗号化はPhase 1に含みません。offline theftへの防御はOS full-disk encryptionに依存します。

### Dependency Compromise

依存を最小化・version固定し、`Cargo.lock` とCIを使用します。release前にdependency auditとprovenance確認を行いますが、Phase 1は自動audit serviceを追加しません。

## Out of scope

Fubunはkernel/root compromise、同一ユーザーの完全な侵害、physical attacker、memory scraping、malicious compiler、disk encryption、backup policyを防御対象にしません。また禁止データを入力dataの意味から完全判定するDLP機能は提供しません。
