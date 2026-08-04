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

### Malicious Web Page / Content Script相当の入力

Browser Extensionは本文、Content Script、Cookie、History、WebRequestを扱わず、`tabs.onUpdated`で取得できるhttp/https URLを登録済みhashと照合するだけです。Native Host/Coreへ未登録URL、query、fragment、title、tab IDは送信しません。Adapterから届くEventはCoreが固定Event Type、actor=user、source、identity、Resource kind、Active Scopeを検証して組み立てます。

### Native Host Origin Spoofing / Wildcard allowed_origins

Manifestの`allowed_origins`はexactな32文字Extension IDだけを原則とし、Wildcardを許可しません。Native Host自身も最初のcaller originを`chrome-extension://<id>/`として検証し、0600設定ファイルの許可IDとExtension HelloのIDが一致しなければ接続を拒否します。

### Native Host stdout corruption / Oversized or Partial Native Message

Native Host stdoutは32-bit native-endian frame以外を書きません。0長、256 KiB超、header/bodyの途中切断、不正UTF-8/JSON、未知Message Typeは安全に拒否し、CoreのUDS frame上限とは別に検査します。診断はstderrだけです。

### Browser Permission Revocation / Stale Resource Mapping

Permission解除を検出したExtensionはMappingをinactiveとして削除し、Scope Pauseを要求してNative Portを再接続します。Browser Adapterの新Instanceは許可済みResource ID snapshotを再宣言し、Coreは同一Instance内のPermission、Capability、Resource状態を同時に満たす場合だけAction/Eventを受け付けます。

### Raw URL Leakage / Query Token Leakage / Incognito Leakage

Canonical URLはquery/fragmentを除去してResource作成時だけ一時利用し、Event、通常Log、Execution History、Extension Storageには保存しません。Incognito Tabは無視し、許可されていないOriginへNative Messageを送信しません。

### Adapter Event Spoofing / Event Capability Spoofing

Adapter ID、Event Capability、Event Typeの組合せは固定Registryで検証し、未知・重複・空Capabilityを拒否します。Browser EventはBrowser Adapterかつ`dev.fubun.browser.resource.opened.v1`、VS Code EventはVS Code Event-only Adapterかつ`dev.fubun.vscode.workspace.opened.v1`だけを許可します。

### Inactive Scope / Wrong Resource Kind

Browser Eventはweb.page Resourceとactive browser Scope、VS Code Eventはfilesystem.directory Resourceとactive VS Code Scopeが必須です。paused Scope、別Source、Resource kind不一致、Browser StatusのPermission不足では保存しません。

### VS Code Remote / Multi-root / Untrusted Workspace

Extensionはsingle-folder Local file workspaceだけを対象にし、Multi-root、SSH/WSL/Dev Container/Codespaces、Virtual/Untitledは拒否します。Workspace本文、現在ファイル名、Terminal入力、Git差分、Settingsは読みません。Untrusted WorkspaceはVS Code manifest上でサポート宣言しますが、明示ObserveなしのEventは生成しません。

### Extension Storage Tampering / Browser Action URL Substitution

StorageにはResource ID、Scope ID、canonical hash、origin pattern、label、schema versionだけを保存します。Action RequestはResource IDとCore-resolved canonical Resourceだけを受け、ExtensionはLocal Mapping、現在Permission、URL hashを再照合してから既存Tab確認または新規Tab作成を行います。任意URLやResource ID mismatchは拒否します。

### Native Host Disconnect / Service Worker Restart / Reconnect Storm

Native Port切断時はpending操作を失敗させ、指数Backoff（上限30秒）で接続を一度だけ再試行します。Service WorkerはStorageからMappingを復元し、global stateだけを正本にしません。Core側のPending RequestはAdapter Disconnect、Timeout、Shutdownで解放されます。

## Out of scope

Fubunはkernel/root compromise、同一ユーザーの完全な侵害、physical attacker、memory scraping、malicious compiler、disk encryption、backup policy、Adapter binary自体の侵害を防御対象にしません。また禁止データを入力dataの意味から完全判定するDLP機能、Universal Undo、外部network隔離、Chrome Web Store/VS Code Marketplaceの配布審査、悪意あるBrowser/VS Code本体を提供しません。
