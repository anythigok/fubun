# Threat model

## Assets and trust boundary

保護対象はlocal Event/Ritual/Execution history、database整合性、daemon availability、ユーザーのprivacyです。Socket directory 0700とsocket 0600を第一境界にし、Adapterは明示的なHelloと固定Capabilityで信頼範囲を宣言します。

## Threats and mitigations

### Phase 4A discovery boundaries

Discovery accepts only Core-validated `actor=user` VS Code Workspace Opened and Browser
Resource Opened events. Synthetic, system, imported, Fubun-generated, inactive-scope, unknown,
and resource-missing events are excluded before Sessionization. Event order uses daemon
`received_at`; `occurred_at` cannot reorder a Session.

Sessions contain Resource IDs and event IDs only. Evidence and Fingerprints contain algorithm
version, workspace Resource ID, ordered action Resource IDs, counts, timestamps, and bounded
statistics. Raw URL, raw Workspace Path, browser title, query, fragment, cookie, tab ID, and file
contents are not representable in discovery records.

Discovery is an explicit CLI/IPC request with a 30-day and 100,000-event bound and a single-flight
guard. There is no background scheduler, machine learning model, LLM, suggestion notification,
automatic activation, or automatic execution. Accepting a Suggestion is transactional and creates
only a Draft Ritual; it never creates an Approval or Rule.

Browser tabs opened by a Fubun Action are suppressed from one subsequent navigation using an
ephemeral, 60-second session-store entry keyed by tab ID and Resource ID. The entry is consumed
once and is never sent to Core or persisted in SQLite. Expiry prevents permanent suppression.

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

Permission解除を検出したExtensionは、まずLocal Mappingをinactive/pause pendingへ永続化してEvent対象から外します。その後に各Core Scope Pauseを試行し、成功したMappingだけを削除します。Core不達時もLocal Event送信は再開せず、Storage更新が完了してからNative Portを再接続します。Service Worker起動時はPermissionを再照合し、残ったpause pending Scopeを一度だけ再同期します。Browser Adapterの新Instanceは許可済みResource ID snapshotを再宣言し、Coreは同一Instance内のPermission、Capability、Resource状態を同時に満たす場合だけAction/Eventを受け付けます。

PauseはCore側で冪等化されているため、Commit後に応答を失った再送でも既にpausedのScopeを成功として返します。Local mappingは再送成功後にだけ削除し、pause_pendingを永久に保持しません。

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

Native Port切断時はPending Native Requestをすべてrejectし、指数Backoff（上限30秒）で接続を一度だけ再試行します。Native HostのCore Adapter streamはReaderを一つに固定し、Event Ackをrequest ID別に配送するため、Action ExecuteがAckより先に到着しても誤配送しません。Service WorkerはStorageからMappingを復元し、global stateだけを正本にしません。Core側のPending RequestはAdapter Disconnect、Timeout、Shutdownで解放されます。

CoreのAdapter接続は`adapter_instance_id`とは別のConnection Tokenで世代識別します。同じInstance IDの新接続が登録された後に旧接続が終了しても、条件付きDisconnectは新接続を削除せず、旧TokenのPendingだけを解放します。Browser EventのsequenceはHello完了後の現Adapter Instanceに対して発行し、旧Instanceで払い出した番号を新接続へ持ち込まず、新Instanceでは1から開始します。

### Semantic Event Injection / VS Code Adapter Session

通常Clientの`event.ingest`は開発用Synthetic Eventだけを許可し、Browser/VS CodeのSemantic Eventを受理しません。Semantic EventはAdapter Helloの固定ID/Event Capability、Resource kind、Active Scope、Browser Permission snapshotを検証した後にCoreが構築します。VS Code Event-only AdapterはEventごとの短命接続を使わず、Extension activation中の長時間UDS sessionでHello、Event Ack、request ID、protocol version、bounded frameを検証します。切断時はPending Eventを解放し、観察対象がない場合は再接続しません。

## Out of scope

Fubunはkernel/root compromise、同一ユーザーの完全な侵害、physical attacker、memory scraping、malicious compiler、disk encryption、backup policy、Adapter binary自体の侵害を防御対象にしません。また禁止データを入力dataの意味から完全判定するDLP機能、Universal Undo、外部network隔離、Chrome Web Store/VS Code Marketplaceの配布審査、悪意あるBrowser/VS Code本体を提供しません。
