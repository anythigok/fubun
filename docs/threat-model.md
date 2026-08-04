# Threat model

## Assets and trust boundary

保護対象はlocal Event/Ritual/Execution history、database整合性、daemon availability、ユーザーのprivacyです。Socket directory 0700とsocket 0600を第一境界にし、Adapterは明示的なHelloと固定Capabilityで信頼範囲を宣言します。

## Threats and mitigations

### Malicious IPC Client

同一ユーザー権限を得たclientが不正requestを送る可能性があります。最初のrequestに `client.hello` または `adapter.hello` を要求し、protocol major、strict envelope、Domain validationを確認します。権限・認証の追加は将来ADR対象です。

### Malformed Message

不正UTF-8/JSON、truncated frame、未知フィールドはconnection単位で拒否します。connection errorはaccept loopへ伝播させず、daemonを継続します。

### Oversized Message

length prefixをallocation前に検査し、256 KiBを超えるconnectionを終了します。

### Duplicate Event

adapterの再送やreconnectを想定し、`adapter_instance_id + sequence_no` にdatabase unique constraintを置き、`duplicate_event` を明示的に返します。

### Malicious Ritual JSON / Unknown Action

`deny_unknown_fields`付きのversioned JSONとAction Registryで未知フィールド・未知Actionを拒否します。R2/R3はPolicyでmanual planへ入りません。

### Resource Path Substitution / Symlink Swap

RitualとAction RequestはResource IDだけを受け取り、登録時と実行直前にcanonical path、存在、file/directory typeを再検証します。差し替え時は実行せず `resource_changed` を返します。

### Malicious Desktop Entry ID

app_idは1-128 bytesの英数字・`.`・`_`・`-`だけでslash、backslash、空白、制御文字を拒否し、Desktop Entry標準ディレクトリ内の `<id>.desktop` だけを解決します。Coreから任意Executable pathは渡されません。

### Required Tool and Resource Type Drift

ActionからCapability、固定Tool、必要ならDesktop Entry IDを含むAdapter要件を作り、Preview、実行直前Preflight、Dispatchが同じ単一Instance選択を使います。異なるAdapterのcapability、Tool、Desktop Entry情報を合成しません。複数候補はinstance UUID文字列の昇順で選びます。Resourceは登録時と実行時のcanonical pathだけでなく、file/directory種別も比較し、symlink差し替えや種別変更時は実行しません。

### Adapter Impersonation / Disconnect

Socket権限、adapter Hello、固定Registry capability、重複・長さ・制御文字制約を確認します。Adapter認証はPhase 2では同一ユーザー境界に依存します。実行対象を選択した後は、timeout、disconnect、protocol errorでも実Adapter IDとInstance IDを履歴へ残します。実行中のDisconnect時はpending oneshotを同期的に解放し、Execution lockを解除して永久待機を防ぎます。

### Action Timeout / Partial Execution

ActionごとのtimeoutとRitual deadlineを持ち、失敗後の後続Actionを停止します。成功済みActionがある失敗は `partial` として記録し、Stepには短いredacted messageだけ保存します。failed/partial/aborted/succeededの終端Executionにpending/running Stepを残さず、失敗後に未実行だったStepは `stopped_after_failure` としてabortedにします。TimeoutでFutureがdropしてもPending Guardがrequestを残しません。

### Sensitive stdout/stderr / Stale Approval

Adapterのstdout・stderr全文はDatabaseへ保存せず、固定result codeと短い説明だけ返します。ApprovalはRitualVersion content hashと全Action identityに紐づき、Version更新で無効になります。

### Same-user Attacker

同一uidで任意codeを実行できる攻撃者はsocket接続、database読取、process操作が可能です。Phase 2は同一ユーザー内の強い隔離を提供しません。OS account分離、disk encryption、session lockを利用してください。

### Database Theft

directory 0700、database 0600で他ユーザーのreadを制限します。保存内容の暗号化はPhase 2に含みません。offline theftへの防御はOS full-disk encryptionに依存します。

### Dependency Compromise

依存を最小化・version固定し、`Cargo.lock` とCIを使用します。release前にdependency auditとprovenance確認を行いますが、自動audit serviceは追加しません。

### Restart Consistency

Daemon再起動時にrunning Executionをabortedへ遷移し、同じExecutionのpending/running Stepも `daemon_restarted` と終了時刻を付けてabortedにします。完了済みStepは保持し、実行Lockは削除します。

## Out of scope

Fubunはkernel/root compromise、同一ユーザーの完全な侵害、physical attacker、memory scraping、malicious compiler、disk encryption、backup policy、Adapter binary自体の侵害を防御対象にしません。また禁止データを入力dataの意味から完全判定するDLP機能、Universal Undo、外部network隔離を提供しません。
