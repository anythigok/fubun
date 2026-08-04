# Core architecture

Phase 3でも永続化write/data flowはsingle writer queueへ集約されます。

```text
fubun-cli / fubun-linux-adapter / fubun-native-host / VS Code Extension
  -> Unix Domain Socket
  -> bounded frame decoder
  -> strict versioned Envelope + client.hello / adapter.hello
  -> Ritual validation / Preview / Approval / Execution plan
  -> single writer queue
  -> SQLite (WAL, v4 migration)
```

`fubun-domain` はEvent、Ritual、Resource、Observation Scope、Execution型、URL canonicalizationを所有します。`fubun-policy` は固定Action Registry、`fubun-protocol` はtransport framingと双方向envelope、`fubun-storage` はv4 migrationとsingle writer、`fubun-core` はScope検証、Preview・Approval・Execution orchestration、Core-built Eventを所有します。Linux固有Commandは `fubun-linux-adapter` だけが固定引数配列で呼び出します。BrowserのOS境界は `fubun-native-host`、Browser APIはManifest V3 Extension、VS Code APIはUI Extensionだけが担当します。CLIは全てdaemonを通り、databaseを直接読みません。

受信Eventの `received_at` はdaemonがUTC現在時刻で上書きします。重複判定はEvent IDではなく `(adapter_instance_id, sequence_no)` が正本です。

Ritualは `draft -> active -> paused/archived` の状態を持ち、各Versionはimmutableです。ActivationはPreview成功と `--approve` を要求し、Versionのcontent hashが変われば旧Approvalを使えません。Executionは逐次Action、overall timeout、単一Ritual lock、redacted historyを持ちます。

CoreとAdapterは長時間の双方向Unix socket接続を使います。Actionから `AdapterRequirements`（Capability、固定Tool、必要ならDesktop Entry ID）を作り、Preview、Preflight、Dispatchが同じ単一Instance適格性判定を使用します。候補はinstance UUID文字列の昇順で決定し、異なるAdapterのToolとDesktop Entryを合成しません。Dispatchを開始した後は成功、timeout、disconnect、protocol errorのいずれでも実Adapter IDとInstance IDをExecution Stepへ保存します。request/action execution IDごとのpending requestは同期RAII Guardで所有し、Futureのdropを含むcancel、timeout、disconnect、shutdown時に必ず解放します。Ritual全体は外側Futureを強制dropせず、deadlineの残り時間をAction timeoutへ渡します。

Previewと実行直前Preflightは、同じ単一Adapterのrequired executable、Desktop Entry、Resourceのcanonical pathとfile/directory種別を確認します。Executionがsucceeded、failed、partial、abortedのいずれかへ終端すると、すべてのStepも終端状態になります。未実行の後続Stepは `stopped_after_failure` でabortedにします。Production Linux Adapterは固定Executableだけを使用し、Fake実行経路を持ちません。

外部network client、TCP listener、Pattern Miner、Rule、自動Trigger、GUIはありません。

## Observation ScopeとSemantic Event

BrowserページまたはVS Code Workspaceは、ユーザーが明示的にEnableしたときだけResourceと`observation_scopes`へ登録されます。Scopeがpausedまたは存在しない場合、AdapterがEventを送ってもCoreは保存しません。Scopeの一意性は`(source, resource_id)`です。

Browser Resourceは`http`/`https`のcanonical URLだけを保持します。userinfo、query、fragment、default portを除去し、空Pathを`/`へ正規化した文字列からSHA-256を計算します。URLはEventやログへ複製せず、Browser ActionはResource IDだけを受け取ります。

Adapter Helloは固定Adapter RegistryとAction/Event Capabilityを検証します。通常Clientの`event.ingest`は開発用Synthetic Eventに限り、Browser/VS Code Semantic EventはAdapter Event EmitでのみCoreがactor、source、received_at、adapter identityを確定して保存します。VS CodeはAction CapabilityなしのEvent-only Adapterとして登録できます。Browser Adapterの`permitted_resource_ids`は同一Adapter InstanceのStatus snapshotに紐づき、Coreは別InstanceのPermissionやCapabilityを合成しません。Browser Enable後やPermission解除後はExtensionがPortを再接続し、新しいInstanceでStatusを更新します。

## Native Messaging bridge

Chrome/ChromiumからNative Hostへは標準の32-bit native-endian length framing、HostからCoreへは既存4-byte little-endian framingを使います。Native Hostは最初のcaller originとstrict `extension.hello`を検証し、設定ファイルに明示されたExtension IDだけを許可します。Adapter socketは単一Reader taskと単一Writer queueへ分離し、Readerが`event.ack`をrequest ID別Pending mapへ、`action.execute`をExtension outbound queueへ配送します。したがってEvent AckとAction Executeは到着順に依存せず、timeout/disconnect/cancelはPending mapを解放します。stdoutはNative Messaging frame専用、診断はstderrだけです。Native HostはShell、任意Executable、Network APIを持たず、Browser API呼出しはExtensionへAction Requestとして渡します。

## VS Code boundary

VS Code Extensionは`extensionKind: ["ui"]`で、Local/file scheme/single-folder workspaceだけを対象にします。realpathとcanonical hashは一時的にCoreへ渡しますが、globalStateとEventにはResource ID、Scope ID、hashだけを保存します。観察中は一つのEvent-only Adapter Sessionを再利用し、stable instance ID、単調sequence、strict Hello/Event Ack/request ID/protocol検証、bounded reconnect backoffを持ちます。Remote、Multi-root、Virtual、Untitled workspaceではEventを生成しません。
