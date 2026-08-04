//! Local-only Fubun daemon, ritual orchestration, and IPC client.

pub mod adapter;
pub mod paths;

use std::{
    fs,
    os::unix::fs::{FileTypeExt, PermissionsExt},
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use adapter::{
    AdapterDispatchErrorKind, AdapterEligibilityError, AdapterManager, AdapterRequirements,
};
use fubun_domain::{
    canonical_web_url_hash, canonicalize_web_url, web_origin_pattern, ActionSpec, Actor,
    AdapterIdentity, Approval, EventData, EventType, Execution, ExecutionMode, ExecutionStatus,
    ExecutionStep, ExecutionStepStatus, FailureMode, ObservationScope, ObservationSource,
    ObservationStatus, PrivacyClass, Resource, ResourceKind, ResourceScope, Ritual,
    RitualDefinition, RitualStatus, RitualVersion, TriggerKind, EVENT_SPEC_VERSION,
    RITUAL_SCHEMA_VERSION,
};
use fubun_mining::{
    self, DiscoveryInput, DiscoveryRun, DiscoveryRunStatus, SuggestionStatus, ALGORITHM_VERSION,
};
use fubun_policy::{approval_fields, descriptor, validate_action};
use fubun_protocol::{
    read_json_frame, write_json_frame, ActionResult, AdapterEventAck, AdapterEventEmitRequest,
    AdapterHello, AdapterRequestBody, AdapterRequestEnvelope, AdapterResponseBody,
    AdapterResponseEnvelope, ClientHello, ClientHelloAck, DoctorReport, EmptyRequest, EventAck,
    EventIngested, EventList, FrameError, PreviewAction, RequestBody, RequestEnvelope,
    ResolvedResource, ResponseBody, ResponseEnvelope, ResponsePayload, RitualPreview, StatusReport,
    CURRENT_PROTOCOL_VERSION,
};
use fubun_storage::{Storage, StorageError, StorageHandle};
use thiserror::Error;
use time::OffsetDateTime;
use tokio::{
    net::{UnixListener, UnixStream},
    sync::{mpsc, watch},
    task::{JoinHandle, JoinSet},
};
use tracing::{debug, warn};
use uuid::Uuid;

use crate::paths::FubunPaths;

const MAX_RITUAL_JSON_BYTES: usize = 128 * 1024;

#[derive(Debug, Error)]
pub enum CoreError {
    #[error("filesystem or socket error: {0}")]
    Io(#[from] std::io::Error),
    #[error("storage error: {0}")]
    Storage(#[from] StorageError),
    #[error("IPC frame error: {0}")]
    Frame(#[from] FrameError),
    #[error("socket path exists and is not a Unix socket: {0}")]
    SocketPathOccupied(PathBuf),
    #[error("daemon task failed: {0}")]
    Task(#[from] tokio::task::JoinError),
}

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("socket error: {0}")]
    Io(#[from] std::io::Error),
    #[error("IPC frame error: {0}")]
    Frame(#[from] FrameError),
    #[error("daemon closed the connection")]
    ConnectionClosed,
    #[error("response request ID mismatch")]
    RequestIdMismatch,
    #[error("daemon rejected request ({code}): {message}")]
    Rejected { code: String, message: String },
    #[error("unexpected response payload")]
    UnexpectedPayload,
}

pub struct RunningServer {
    shutdown: Option<watch::Sender<bool>>,
    task: Option<JoinHandle<Result<(), CoreError>>>,
    paths: FubunPaths,
}

impl RunningServer {
    #[must_use]
    pub fn paths(&self) -> &FubunPaths {
        &self.paths
    }

    pub async fn shutdown(mut self) -> Result<(), CoreError> {
        if let Some(sender) = self.shutdown.take() {
            let _ = sender.send(true);
        }
        if let Some(task) = self.task.take() {
            task.await??;
        }
        Ok(())
    }
}

impl Drop for RunningServer {
    fn drop(&mut self) {
        if let Some(sender) = self.shutdown.take() {
            let _ = sender.send(true);
        }
    }
}

pub async fn start_server(paths: FubunPaths) -> Result<RunningServer, CoreError> {
    prepare_socket_directory(&paths.runtime_directory)?;
    remove_stale_socket(&paths.socket_path)?;
    let storage = Storage::open(&paths.database_path)?;
    storage.handle().abort_running_executions().await?;
    storage.handle().abort_running_discovery_runs().await?;
    let listener = UnixListener::bind(&paths.socket_path)?;
    fs::set_permissions(&paths.socket_path, fs::Permissions::from_mode(0o600))?;

    let (shutdown, shutdown_receiver) = watch::channel(false);
    let server_paths = paths.clone();
    let task = tokio::spawn(async move {
        serve(
            listener,
            storage,
            shutdown_receiver,
            &server_paths.socket_path,
        )
        .await
    });
    Ok(RunningServer {
        shutdown: Some(shutdown),
        task: Some(task),
        paths,
    })
}

fn prepare_socket_directory(path: &Path) -> Result<(), CoreError> {
    fs::create_dir_all(path)?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    Ok(())
}

fn remove_stale_socket(path: &Path) -> Result<(), CoreError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_socket() => fs::remove_file(path)?,
        Ok(_) => return Err(CoreError::SocketPathOccupied(path.to_path_buf())),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(CoreError::Io(error)),
    }
    Ok(())
}

async fn serve(
    listener: UnixListener,
    storage: Storage,
    mut shutdown: watch::Receiver<bool>,
    socket_path: &Path,
) -> Result<(), CoreError> {
    let storage_handle = storage.handle();
    let adapters = AdapterManager::default();
    let mut connections = JoinSet::new();

    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() { break; }
            }
            accepted = listener.accept() => {
                let (stream, _) = accepted?;
                let handle = storage_handle.clone();
                let manager = adapters.clone();
                let connection_shutdown = shutdown.clone();
                connections.spawn(async move {
                    if let Err(error) = handle_connection(stream, handle, manager, connection_shutdown).await {
                        debug!(%error, "closing invalid or failed IPC connection");
                    }
                });
            }
            completed = connections.join_next(), if !connections.is_empty() => {
                if let Some(Err(error)) = completed { warn!(%error, "IPC connection task panicked"); }
            }
        }
    }

    adapters.shutdown().await;
    while let Some(result) = connections.join_next().await {
        if let Err(error) = result {
            warn!(%error, "IPC connection task panicked during shutdown");
        }
    }
    storage.shutdown().await?;
    match fs::remove_file(socket_path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(CoreError::Io(error)),
    }
    Ok(())
}

async fn handle_connection(
    mut stream: UnixStream,
    storage: StorageHandle,
    adapters: AdapterManager,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), CoreError> {
    let Some(first) = read_request_or_shutdown(&mut stream, &mut shutdown).await? else {
        return Ok(());
    };
    if first.protocol_version.major != CURRENT_PROTOCOL_VERSION.major {
        write_json_frame(
            &mut stream,
            &ResponseEnvelope::error(
                first.request_id,
                "protocol_major_mismatch",
                "client and daemon protocol major versions differ",
            ),
        )
        .await?;
        return Ok(());
    }
    match first.body {
        RequestBody::AdapterHello(hello) => {
            handle_adapter_connection(stream, first.request_id, hello, adapters, storage, shutdown)
                .await
        }
        RequestBody::ClientHello(hello) => {
            if hello.protocol_version.major != CURRENT_PROTOCOL_VERSION.major {
                write_json_frame(
                    &mut stream,
                    &ResponseEnvelope::error(
                        first.request_id,
                        "protocol_major_mismatch",
                        "client hello and daemon protocol major versions differ",
                    ),
                )
                .await?;
                return Ok(());
            }
            let ack = ClientHelloAck {
                server_name: "fubund".to_owned(),
                server_version: env!("CARGO_PKG_VERSION").to_owned(),
                protocol_version: CURRENT_PROTOCOL_VERSION,
            };
            write_json_frame(
                &mut stream,
                &ResponseEnvelope::ok(first.request_id, ResponsePayload::ClientHelloAck(ack)),
            )
            .await?;
            while let Some(request) = read_request_or_shutdown(&mut stream, &mut shutdown).await? {
                let response = process_request(request, &storage, &adapters).await;
                write_json_frame(&mut stream, &response).await?;
            }
            Ok(())
        }
        _ => {
            write_json_frame(
                &mut stream,
                &ResponseEnvelope::error(
                    first.request_id,
                    "handshake_required",
                    "client.hello or adapter.hello must be the first request",
                ),
            )
            .await?;
            Ok(())
        }
    }
}

async fn handle_adapter_connection(
    mut stream: UnixStream,
    request_id: Uuid,
    hello: AdapterHello,
    adapters: AdapterManager,
    storage: StorageHandle,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), CoreError> {
    if let Err(error) = AdapterManager::validate_hello(&hello) {
        write_json_frame(
            &mut stream,
            &ResponseEnvelope::error(request_id, "protocol_error", error.to_string()),
        )
        .await?;
        return Ok(());
    }
    let ack = ClientHelloAck {
        server_name: "fubund".to_owned(),
        server_version: env!("CARGO_PKG_VERSION").to_owned(),
        protocol_version: CURRENT_PROTOCOL_VERSION,
    };
    write_json_frame(
        &mut stream,
        &ResponseEnvelope::ok(request_id, ResponsePayload::AdapterHelloAck(ack)),
    )
    .await?;
    let (reader, mut writer) = stream.into_split();
    let (sender, mut receiver) = mpsc::channel(32);
    let instance_id = hello.instance_id;
    let connection_token = match adapters.register(hello.clone(), sender.clone()).await {
        Ok(connection_token) => connection_token,
        Err(error) => {
            write_json_frame(
                &mut writer,
                &ResponseEnvelope::error(request_id, "protocol_error", error.to_string()),
            )
            .await?;
            return Ok(());
        }
    };
    let writer_task = tokio::spawn(async move {
        while let Some(request) = receiver.recv().await {
            if write_json_frame(&mut writer, &request).await.is_err() {
                break;
            }
        }
    });
    let mut reader = reader;
    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() { break; }
            }
            response = read_json_frame::<_, AdapterResponseEnvelope>(&mut reader) => {
                match response {
                    Ok(Some(response)) if response.protocol_version.major == CURRENT_PROTOCOL_VERSION.major => {
                        if let AdapterResponseBody::EventEmit(event) = &response.body {
                            let ack = ingest_adapter_event(
                                response.request_id,
                                &hello,
                                event.clone(),
                                &storage,
                            )
                            .await;
                            let _ = sender.send(AdapterRequestEnvelope {
                                protocol_version: CURRENT_PROTOCOL_VERSION,
                                request_id: response.request_id,
                                action_execution_id: Uuid::nil(),
                                body: AdapterRequestBody::EventAck(AdapterEventAck {
                                    event_id: ack.event_id,
                                    stored: ack.stored,
                                    duplicate: ack.duplicate,
                                }),
                            }).await;
                        } else {
                            adapters.resolve(response).await;
                        }
                    }
                    Ok(Some(_)) => warn!("adapter response protocol major mismatch"),
                    Ok(None) | Err(_) => break,
                }
            }
        }
    }
    adapters
        .disconnect_if_current(instance_id, connection_token)
        .await;
    writer_task.abort();
    Ok(())
}

async fn read_request_or_shutdown(
    stream: &mut UnixStream,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<Option<RequestEnvelope>, CoreError> {
    tokio::select! {
        _ = shutdown.changed() => Ok(None),
        request = read_json_frame(stream) => request.map_err(CoreError::from),
    }
}

async fn process_request(
    request: RequestEnvelope,
    storage: &StorageHandle,
    adapters: &AdapterManager,
) -> ResponseEnvelope {
    if request.protocol_version.major != CURRENT_PROTOCOL_VERSION.major {
        return ResponseEnvelope::error(
            request.request_id,
            "protocol_major_mismatch",
            "client and daemon protocol major versions differ",
        );
    }
    let id = request.request_id;
    match request.body {
        RequestBody::ClientHello(_) | RequestBody::AdapterHello(_) => ResponseEnvelope::error(
            id,
            "already_initialized",
            "handshake is only valid as the first request",
        ),
        RequestBody::EventIngest(payload) => ingest_response(id, payload.event, storage).await,
        RequestBody::BrowserObservationEnable(payload) => {
            browser_observation_enable_response(id, payload, storage).await
        }
        RequestBody::VscodeObservationEnable(payload) => {
            vscode_observation_enable_response(id, payload, storage).await
        }
        RequestBody::ObservationPause(payload) => {
            match storage.pause_observation_scope(payload.scope_id).await {
                Ok(scope) => ResponseEnvelope::ok(id, ResponsePayload::ObservationPaused(scope)),
                Err(StorageError::NotFound) => ResponseEnvelope::error(
                    id,
                    "scope_not_found",
                    "observation scope was not found",
                ),
                Err(error) => storage_error(id, error),
            }
        }
        RequestBody::ObservationsList(payload) => {
            match storage.list_observation_scopes(payload.source).await {
                Ok(scopes) => ResponseEnvelope::ok(
                    id,
                    ResponsePayload::ObservationList(fubun_protocol::ObservationList { scopes }),
                ),
                Err(error) => storage_error(id, error),
            }
        }
        RequestBody::EventsList(payload) => {
            match storage.list_events(payload.since, payload.limit).await {
                Ok(events) => {
                    ResponseEnvelope::ok(id, ResponsePayload::EventList(EventList { events }))
                }
                Err(error) => storage_error(id, error),
            }
        }
        RequestBody::SystemStatus(_) => status_response(id, storage).await,
        RequestBody::SystemDoctor(_) => doctor_response(id, storage, adapters).await,
        RequestBody::ResourceCreate(payload) => {
            resource_create_response(id, payload, storage).await
        }
        RequestBody::ResourceList(_) => match storage.list_resources().await {
            Ok(resources) => ResponseEnvelope::ok(
                id,
                ResponsePayload::ResourceList(fubun_protocol::ResourceList { resources }),
            ),
            Err(error) => storage_error(id, error),
        },
        RequestBody::ResourceShow(payload) => match storage.get_resource(payload.resource_id).await
        {
            Ok(resource) => ResponseEnvelope::ok(id, ResponsePayload::ResourceShow(resource)),
            Err(StorageError::NotFound) => {
                ResponseEnvelope::error(id, "resource_not_found", "resource was not found")
            }
            Err(error) => storage_error(id, error),
        },
        RequestBody::RitualCreate(payload) => {
            ritual_create_response(id, payload.definition, storage).await
        }
        RequestBody::RitualUpdate(payload) => {
            ritual_update_response(id, payload.ritual_id, payload.definition, storage).await
        }
        RequestBody::RitualList(_) => match storage.list_rituals().await {
            Ok(rituals) => ResponseEnvelope::ok(
                id,
                ResponsePayload::RitualList(fubun_protocol::RitualList { rituals }),
            ),
            Err(error) => storage_error(id, error),
        },
        RequestBody::RitualShow(payload) => {
            ritual_show_response(id, payload.ritual_id, storage).await
        }
        RequestBody::RitualPreview(payload) => {
            ritual_preview_response(id, payload.ritual_id, storage, adapters).await
        }
        RequestBody::RitualActivate(payload) => {
            ritual_activate_response(id, payload.ritual_id, payload.approve, storage, adapters)
                .await
        }
        RequestBody::RitualPause(payload) => {
            ritual_pause_response(id, payload.ritual_id, storage).await
        }
        RequestBody::RitualRun(payload) => {
            ritual_run_response(id, payload.ritual_id, storage, adapters).await
        }
        RequestBody::AdapterList(_) | RequestBody::AdapterStatus(_) => {
            let reports = adapters.reports().await;
            ResponseEnvelope::ok(
                id,
                ResponsePayload::AdapterList(fubun_protocol::AdapterList { adapters: reports }),
            )
        }
        RequestBody::ExecutionList(_) => match storage.list_executions().await {
            Ok(executions) => ResponseEnvelope::ok(
                id,
                ResponsePayload::ExecutionList(fubun_protocol::ExecutionList { executions }),
            ),
            Err(error) => storage_error(id, error),
        },
        RequestBody::ExecutionShow(payload) => {
            match storage.get_execution(payload.execution_id).await {
                Ok((execution, steps)) => ResponseEnvelope::ok(
                    id,
                    ResponsePayload::ExecutionShow(fubun_protocol::ExecutionRecord {
                        execution,
                        steps,
                    }),
                ),
                Err(StorageError::NotFound) => {
                    ResponseEnvelope::error(id, "execution_not_found", "execution was not found")
                }
                Err(error) => storage_error(id, error),
            }
        }
        RequestBody::IntegrationsStatus(_) => {
            let reports = adapters.reports().await;
            let scopes = storage
                .list_observation_scopes(None)
                .await
                .unwrap_or_default();
            let browser_adapters = reports
                .iter()
                .filter(|report| report.adapter_id == "dev.fubun.browser.chromium")
                .count();
            let vscode_adapters = reports
                .iter()
                .filter(|report| report.adapter_id == "dev.fubun.vscode")
                .count();
            ResponseEnvelope::ok(
                id,
                ResponsePayload::IntegrationsStatus(fubun_protocol::IntegrationReport {
                    core_connected: true,
                    linux_adapters: reports
                        .iter()
                        .filter(|report| report.adapter_id == "dev.fubun.linux")
                        .count(),
                    browser_adapters,
                    vscode_adapters,
                    native_host_manifest: "unknown".to_owned(),
                    permitted_browser_resources: reports
                        .iter()
                        .filter(|report| report.adapter_id == "dev.fubun.browser.chromium")
                        .map(|report| report.permitted_resource_ids.len())
                        .sum(),
                    active_browser_scopes: scopes
                        .iter()
                        .filter(|scope| {
                            scope.source == ObservationSource::BrowserChromium
                                && scope.status == ObservationStatus::Active
                        })
                        .count(),
                    active_vscode_scopes: scopes
                        .iter()
                        .filter(|scope| {
                            scope.source == ObservationSource::VscodeWorkspace
                                && scope.status == ObservationStatus::Active
                        })
                        .count(),
                    protocol_version: CURRENT_PROTOCOL_VERSION,
                    database_version: storage.schema_version().await.unwrap_or_default(),
                }),
            )
        }
        RequestBody::DiscoveryRun(_) => discovery_run_response(id, storage).await,
        RequestBody::DiscoveryStatus(_) => discovery_status_response(id, storage).await,
        RequestBody::SessionsList(payload) => {
            match storage.list_sessions(payload.workspace_resource_id).await {
                Ok(sessions) => ResponseEnvelope::ok(
                    id,
                    ResponsePayload::SessionsList(fubun_protocol::SessionsList { sessions }),
                ),
                Err(error) => storage_error(id, error),
            }
        }
        RequestBody::SessionShow(payload) => match storage.get_session(payload.ritual_id).await {
            Ok(session) => ResponseEnvelope::ok(
                id,
                ResponsePayload::SessionShow(fubun_protocol::SessionShow { session }),
            ),
            Err(StorageError::SessionNotFound) => {
                ResponseEnvelope::error(id, "session_not_found", "session was not found")
            }
            Err(error) => storage_error(id, error),
        },
        RequestBody::SuggestionsList(payload) => match storage
            .list_suggestions(payload.status, payload.workspace_resource_id)
            .await
        {
            Ok(suggestions) => ResponseEnvelope::ok(
                id,
                ResponsePayload::SuggestionsList(fubun_protocol::SuggestionsList { suggestions }),
            ),
            Err(error) => storage_error(id, error),
        },
        RequestBody::SuggestionShow(payload) => match storage
            .get_suggestion(payload.ritual_id)
            .await
        {
            Ok((suggestion, status, snoozed_until, accepted_ritual_id)) => ResponseEnvelope::ok(
                id,
                ResponsePayload::SuggestionShow(fubun_protocol::SuggestionShow {
                    suggestion,
                    status,
                    snoozed_until: snoozed_until.map(|value| {
                        value
                            .format(&time::format_description::well_known::Rfc3339)
                            .unwrap_or_default()
                    }),
                    accepted_ritual_id,
                }),
            ),
            Err(StorageError::SuggestionNotFound) => {
                ResponseEnvelope::error(id, "suggestion_not_found", "suggestion was not found")
            }
            Err(error) => storage_error(id, error),
        },
        RequestBody::SuggestionSnooze(payload) => {
            suggestion_snooze_response(id, payload, storage).await
        }
        RequestBody::SuggestionDismiss(payload) => {
            suggestion_state_response(
                id,
                payload.ritual_id,
                SuggestionStatus::Dismissed,
                None,
                storage,
            )
            .await
        }
        RequestBody::SuggestionBlock(payload) => {
            suggestion_state_response(
                id,
                payload.ritual_id,
                SuggestionStatus::Blocked,
                None,
                storage,
            )
            .await
        }
        RequestBody::SuggestionAccept(payload) => {
            suggestion_accept_response(id, payload, storage).await
        }
    }
}

async fn discovery_run_response(id: Uuid, storage: &StorageHandle) -> ResponseEnvelope {
    let started_at = OffsetDateTime::now_utc();
    let run_id = Uuid::new_v4();
    let events = match storage
        .list_events(Some(started_at - time::Duration::days(30)), 100_001)
        .await
    {
        Ok(events) => events,
        Err(error) => return storage_error(id, error),
    };
    if events.len() > 100_000 {
        return ResponseEnvelope::error(
            id,
            "discovery_input_too_large",
            "discovery input exceeds the maximum event count",
        );
    }
    let run = DiscoveryRun {
        id: run_id,
        algorithm_version: ALGORITHM_VERSION.to_owned(),
        status: DiscoveryRunStatus::Running,
        started_at,
        finished_at: None,
        input_event_count: events.len() as u32,
        sessions_upserted: 0,
        candidates_evaluated: 0,
        suggestions_created: 0,
        failure_code: None,
    };
    if let Err(error) = storage.start_discovery_run(run.clone()).await {
        return storage_error(id, error);
    }
    let resources = match storage.list_resources().await {
        Ok(resources) => resources,
        Err(error) => return storage_error(id, error),
    };
    let scopes = match storage.list_observation_scopes(None).await {
        Ok(scopes) => scopes,
        Err(error) => return storage_error(id, error),
    };
    let output = fubun_mining::discover(DiscoveryInput {
        events,
        resources,
        scopes,
    });
    let mut run = run;
    run.candidates_evaluated = output.candidates_evaluated;
    match storage
        .persist_discovery(run, output.sessions, output.suggestions)
        .await
    {
        Ok(run) => ResponseEnvelope::ok(
            id,
            ResponsePayload::DiscoveryRun(fubun_protocol::DiscoveryRunReport { run }),
        ),
        Err(error) => storage_error(id, error),
    }
}

async fn discovery_status_response(id: Uuid, storage: &StorageHandle) -> ResponseEnvelope {
    match storage.list_discovery_runs().await {
        Ok(mut runs) => {
            let run = runs.drain(..).next().unwrap_or(DiscoveryRun {
                id: Uuid::nil(),
                algorithm_version: ALGORITHM_VERSION.to_owned(),
                status: DiscoveryRunStatus::Aborted,
                started_at: OffsetDateTime::UNIX_EPOCH,
                finished_at: None,
                input_event_count: 0,
                sessions_upserted: 0,
                candidates_evaluated: 0,
                suggestions_created: 0,
                failure_code: None,
            });
            ResponseEnvelope::ok(
                id,
                ResponsePayload::DiscoveryStatus(fubun_protocol::DiscoveryRunReport { run }),
            )
        }
        Err(error) => storage_error(id, error),
    }
}

fn parse_discovery_duration(value: &str) -> Result<time::Duration, ()> {
    let (number, suffix) = value.split_at(value.len().saturating_sub(1));
    let amount: i64 = number.parse().map_err(|_| ())?;
    if amount <= 0 {
        return Err(());
    }
    match suffix {
        "d" => Ok(time::Duration::days(amount)),
        "h" => Ok(time::Duration::hours(amount)),
        "m" => Ok(time::Duration::minutes(amount)),
        _ => Err(()),
    }
}

async fn suggestion_snooze_response(
    id: Uuid,
    payload: fubun_protocol::SuggestionSnoozeRequest,
    storage: &StorageHandle,
) -> ResponseEnvelope {
    let duration = match parse_discovery_duration(&payload.for_duration) {
        Ok(duration) if duration <= time::Duration::days(90) => duration,
        _ => {
            return ResponseEnvelope::error(
                id,
                "invalid_snooze_duration",
                "snooze duration must be between 1m and 90d",
            )
        }
    };
    suggestion_state_response(
        id,
        payload.suggestion_id,
        SuggestionStatus::Snoozed,
        Some(OffsetDateTime::now_utc() + duration),
        storage,
    )
    .await
}

async fn suggestion_state_response(
    id: Uuid,
    suggestion_id: Uuid,
    status: SuggestionStatus,
    until: Option<OffsetDateTime>,
    storage: &StorageHandle,
) -> ResponseEnvelope {
    match storage
        .set_suggestion_status(suggestion_id, status, until)
        .await
    {
        Ok((suggestion, status, snoozed_until, accepted_ritual_id)) => ResponseEnvelope::ok(
            id,
            match status {
                SuggestionStatus::Snoozed => {
                    ResponsePayload::SuggestionSnoozed(fubun_protocol::SuggestionShow {
                        suggestion,
                        status,
                        snoozed_until: snoozed_until.map(|value| {
                            value
                                .format(&time::format_description::well_known::Rfc3339)
                                .unwrap_or_default()
                        }),
                        accepted_ritual_id,
                    })
                }
                SuggestionStatus::Dismissed => {
                    ResponsePayload::SuggestionDismissed(fubun_protocol::SuggestionShow {
                        suggestion,
                        status,
                        snoozed_until: snoozed_until.map(|value| {
                            value
                                .format(&time::format_description::well_known::Rfc3339)
                                .unwrap_or_default()
                        }),
                        accepted_ritual_id,
                    })
                }
                SuggestionStatus::Blocked => {
                    ResponsePayload::SuggestionBlocked(fubun_protocol::SuggestionShow {
                        suggestion,
                        status,
                        snoozed_until: snoozed_until.map(|value| {
                            value
                                .format(&time::format_description::well_known::Rfc3339)
                                .unwrap_or_default()
                        }),
                        accepted_ritual_id,
                    })
                }
                _ => ResponsePayload::SuggestionShow(fubun_protocol::SuggestionShow {
                    suggestion,
                    status,
                    snoozed_until: snoozed_until.map(|value| {
                        value
                            .format(&time::format_description::well_known::Rfc3339)
                            .unwrap_or_default()
                    }),
                    accepted_ritual_id,
                }),
            },
        ),
        Err(StorageError::SuggestionNotFound) => {
            ResponseEnvelope::error(id, "suggestion_not_found", "suggestion was not found")
        }
        Err(StorageError::SuggestionInvalidState) => ResponseEnvelope::error(
            id,
            "suggestion_invalid_state",
            "suggestion state does not allow this operation",
        ),
        Err(error) => storage_error(id, error),
    }
}

async fn suggestion_accept_response(
    id: Uuid,
    payload: fubun_protocol::SuggestionAcceptRequest,
    storage: &StorageHandle,
) -> ResponseEnvelope {
    let (suggestion, status, _, accepted_ritual) = match storage
        .get_suggestion(payload.suggestion_id)
        .await
    {
        Ok(value) => value,
        Err(StorageError::SuggestionNotFound) => {
            return ResponseEnvelope::error(id, "suggestion_not_found", "suggestion was not found")
        }
        Err(error) => return storage_error(id, error),
    };
    if status == SuggestionStatus::Accepted {
        if let Some(ritual_id) = accepted_ritual {
            return ritual_show_response(id, ritual_id, storage).await;
        }
    }
    let workspace = match storage.get_resource(suggestion.workspace_resource_id).await {
        Ok(resource) => resource,
        Err(_) => {
            return ResponseEnvelope::error(
                id,
                "suggestion_stale",
                "workspace resource is no longer available",
            )
        }
    };
    let name = payload
        .name
        .unwrap_or_else(|| format!("Open resources for {}", workspace.label));
    let definition = RitualDefinition {
        schema_version: RITUAL_SCHEMA_VERSION.to_owned(),
        name,
        actions: suggestion
            .action_resource_ids
            .iter()
            .map(|resource_id| ActionSpec::BrowserTabEnsureOpen {
                resource_id: *resource_id,
            })
            .collect(),
        execution: fubun_domain::RitualExecutionConfig {
            mode: ExecutionMode::Sequential,
            on_failure: FailureMode::Stop,
            timeout_seconds: 30,
        },
    };
    let canonical = match validate_ritual_definition(&definition) {
        Ok(value) => value,
        Err(message) => return ResponseEnvelope::error(id, "invalid_ritual", message),
    };
    let content_hash = match definition.content_hash() {
        Ok(value) => value,
        Err(_) => {
            return ResponseEnvelope::error(
                id,
                "internal_error",
                "ritual hash could not be computed",
            )
        }
    };
    let now = OffsetDateTime::now_utc();
    let ritual_id = Uuid::new_v4();
    let version = RitualVersion {
        id: Uuid::new_v4(),
        ritual_id,
        version: 1,
        schema_version: RITUAL_SCHEMA_VERSION.to_owned(),
        canonical_json: canonical,
        content_hash,
        created_at: now,
    };
    let ritual = Ritual {
        id: ritual_id,
        name: definition.name.clone(),
        status: RitualStatus::Draft,
        current_version_id: version.id,
        created_at: now,
        updated_at: now,
    };
    match storage
        .accept_suggestion(payload.suggestion_id, ritual, version, definition)
        .await
    {
        Ok((ritual, version, definition)) => ResponseEnvelope::ok(
            id,
            ResponsePayload::SuggestionAccepted(fubun_protocol::RitualRecord {
                ritual,
                version,
                definition,
            }),
        ),
        Err(StorageError::SuggestionStale) => ResponseEnvelope::error(
            id,
            "suggestion_stale",
            "suggestion resources or scopes are no longer active",
        ),
        Err(StorageError::SuggestionInvalidState) => ResponseEnvelope::error(
            id,
            "suggestion_invalid_state",
            "suggestion cannot be accepted in its current state",
        ),
        Err(error) => storage_error(id, error),
    }
}

async fn ingest_response(
    id: Uuid,
    mut event: fubun_domain::Event,
    storage: &StorageHandle,
) -> ResponseEnvelope {
    // `event.ingest` is deliberately retained for the Phase 1 development
    // fixture only. Semantic events are constructed exclusively from an
    // authenticated Adapter Event Emit connection below, so a regular client
    // cannot choose their actor, source, adapter identity, or scope bypass.
    if event.event_type != EventType::SyntheticV1
        || !matches!(&event.data, EventData::Synthetic { .. })
    {
        return ResponseEnvelope::error(
            id,
            "invalid_event",
            "client event ingest only accepts dev synthetic events",
        );
    }
    event.received_at = OffsetDateTime::now_utc();
    if let Err(error) = event.validate() {
        return ResponseEnvelope::error(id, "invalid_event", error.to_string());
    }
    let event_id = event.id;
    match storage.insert_event(event).await {
        Ok(()) => ResponseEnvelope::ok(
            id,
            ResponsePayload::EventIngested(EventIngested {
                event_id,
                stored: true,
                duplicate: false,
            }),
        ),
        Err(StorageError::DuplicateEvent) => ResponseEnvelope::error(
            id,
            "duplicate_event",
            "adapter_instance_id and sequence_no have already been stored",
        ),
        Err(error) => storage_error(id, error),
    }
}

async fn ingest_adapter_event(
    _request_id: Uuid,
    hello: &AdapterHello,
    payload: AdapterEventEmitRequest,
    storage: &StorageHandle,
) -> EventAck {
    let event_id = Uuid::new_v4();
    let expected = match hello.adapter_id.as_str() {
        "dev.fubun.browser.chromium" => (
            EventType::BrowserResourceOpenedV1,
            "browser.chromium",
            ObservationSource::BrowserChromium,
            ResourceKind::WebPage,
            "dev.fubun.browser.resource.opened.v1",
        ),
        "dev.fubun.vscode" => (
            EventType::VscodeWorkspaceOpenedV1,
            "vscode.workspace",
            ObservationSource::VscodeWorkspace,
            ResourceKind::Directory,
            "dev.fubun.vscode.workspace.opened.v1",
        ),
        _ => {
            return EventAck {
                event_id,
                stored: false,
                duplicate: false,
            }
        }
    };
    if payload.event_type != expected.0
        || !hello
            .event_capabilities
            .iter()
            .any(|value| value == expected.4)
        || (hello.adapter_id == "dev.fubun.browser.chromium"
            && !hello
                .status
                .permitted_resource_ids
                .contains(&payload.resource_id))
    {
        return EventAck {
            event_id,
            stored: false,
            duplicate: false,
        };
    }
    let Ok(resource) = storage.get_resource(payload.resource_id).await else {
        return EventAck {
            event_id,
            stored: false,
            duplicate: false,
        };
    };
    if resource.kind != expected.3 {
        return EventAck {
            event_id,
            stored: false,
            duplicate: false,
        };
    }
    let Ok(scopes) = storage.list_observation_scopes(Some(expected.2)).await else {
        return EventAck {
            event_id,
            stored: false,
            duplicate: false,
        };
    };
    if !scopes.iter().any(|scope| {
        scope.resource_id == payload.resource_id && scope.status == ObservationStatus::Active
    }) {
        return EventAck {
            event_id,
            stored: false,
            duplicate: false,
        };
    }
    let event = fubun_domain::Event {
        spec_version: EVENT_SPEC_VERSION.to_owned(),
        id: event_id,
        event_type: expected.0,
        source: expected.1.to_owned(),
        occurred_at: payload.occurred_at,
        received_at: OffsetDateTime::now_utc(),
        actor: Actor::User,
        adapter: AdapterIdentity {
            id: hello.adapter_id.clone(),
            version: hello.adapter_version.clone(),
            instance_id: hello.instance_id,
            sequence_no: payload.sequence_no,
        },
        context: None,
        privacy: PrivacyClass::Normal,
        data: match expected.0 {
            EventType::BrowserResourceOpenedV1 => EventData::BrowserResourceOpened {
                resource_id: payload.resource_id,
            },
            EventType::VscodeWorkspaceOpenedV1 => EventData::VscodeWorkspaceOpened {
                resource_id: payload.resource_id,
            },
            EventType::SyntheticV1 => {
                return EventAck {
                    event_id,
                    stored: false,
                    duplicate: false,
                }
            }
        },
    };
    match storage.insert_event(event).await {
        Ok(()) => EventAck {
            event_id,
            stored: true,
            duplicate: false,
        },
        Err(StorageError::DuplicateEvent) => EventAck {
            event_id,
            stored: false,
            duplicate: true,
        },
        Err(_) => EventAck {
            event_id,
            stored: false,
            duplicate: false,
        },
    }
}

async fn resource_create_response(
    id: Uuid,
    payload: fubun_protocol::ResourceCreateRequest,
    storage: &StorageHandle,
) -> ResponseEnvelope {
    let input = PathBuf::from(&payload.path);
    if !input.is_absolute()
        || payload.path.len() > 4096
        || payload.path.bytes().any(|byte| byte == 0 || byte < 0x20)
    {
        return ResponseEnvelope::error(
            id,
            "invalid_resource",
            "resource path must be an absolute existing path",
        );
    }
    let metadata = match fs::metadata(&input) {
        Ok(value) => value,
        Err(_) => {
            return ResponseEnvelope::error(id, "invalid_resource", "resource path does not exist")
        }
    };
    let canonical = match fs::canonicalize(&input) {
        Ok(value) => value,
        Err(_) => {
            return ResponseEnvelope::error(
                id,
                "invalid_resource",
                "resource path cannot be canonicalized",
            )
        }
    };
    let kind = if metadata.is_file() {
        ResourceKind::File
    } else if metadata.is_dir() {
        ResourceKind::Directory
    } else {
        return ResponseEnvelope::error(
            id,
            "invalid_resource",
            "resource must be a file or directory",
        );
    };
    let now = OffsetDateTime::now_utc();
    let resource = Resource {
        id: Uuid::new_v4(),
        kind,
        label: payload.label,
        locator: payload.path,
        canonical_locator: canonical.to_string_lossy().into_owned(),
        sensitivity: payload.sensitivity,
        scope: ResourceScope::Exact,
        created_at: now,
        updated_at: now,
    };
    if let Err(error) = resource.validate() {
        return ResponseEnvelope::error(id, "invalid_resource", error.to_string());
    }
    match storage.create_resource(resource).await {
        Ok(resource) => ResponseEnvelope::ok(id, ResponsePayload::ResourceCreated(resource)),
        Err(StorageError::DuplicateResource) => ResponseEnvelope::error(
            id,
            "duplicate_resource",
            "a resource with the same canonical path already exists",
        ),
        Err(error) => storage_error(id, error),
    }
}

async fn browser_observation_enable_response(
    id: Uuid,
    payload: fubun_protocol::BrowserObservationEnableRequest,
    storage: &StorageHandle,
) -> ResponseEnvelope {
    let canonical = match canonicalize_web_url(&payload.url) {
        Ok(value) => value,
        Err(_) => return ResponseEnvelope::error(id, "invalid_resource", "invalid web URL"),
    };
    let now = OffsetDateTime::now_utc();
    let candidate = Resource {
        id: Uuid::new_v4(),
        kind: ResourceKind::WebPage,
        label: payload.label,
        locator: canonical.clone(),
        canonical_locator: canonical.clone(),
        sensitivity: fubun_domain::Sensitivity::Normal,
        scope: ResourceScope::Exact,
        created_at: now,
        updated_at: now,
    };
    if candidate.validate().is_err() {
        return ResponseEnvelope::error(id, "invalid_resource", "invalid web resource");
    }
    let resource = match storage.create_resource(candidate).await {
        Ok(resource) => resource,
        Err(StorageError::DuplicateResource) => match storage.list_resources().await {
            Ok(resources) => match resources.into_iter().find(|resource| {
                resource.kind == ResourceKind::WebPage && resource.canonical_locator == canonical
            }) {
                Some(resource) => resource,
                None => {
                    return ResponseEnvelope::error(id, "invalid_resource", "web resource conflict")
                }
            },
            Err(error) => return storage_error(id, error),
        },
        Err(error) => return storage_error(id, error),
    };
    let scope = ObservationScope {
        id: Uuid::new_v4(),
        source: ObservationSource::BrowserChromium,
        resource_id: resource.id,
        status: ObservationStatus::Active,
        created_at: now,
        updated_at: now,
    };
    let scope = match storage.ensure_observation_scope(scope).await {
        Ok(scope) => scope,
        Err(error) => return storage_error(id, error),
    };
    let origin_pattern = match web_origin_pattern(&canonical) {
        Ok(pattern) => pattern,
        Err(_) => return ResponseEnvelope::error(id, "invalid_resource", "invalid web origin"),
    };
    ResponseEnvelope::ok(
        id,
        ResponsePayload::BrowserObservationEnabled(fubun_protocol::BrowserObservationEnabled {
            resource,
            scope,
            canonical_url_hash: canonical_web_url_hash(&canonical),
            origin_pattern,
        }),
    )
}

async fn vscode_observation_enable_response(
    id: Uuid,
    payload: fubun_protocol::VscodeObservationEnableRequest,
    storage: &StorageHandle,
) -> ResponseEnvelope {
    let input = PathBuf::from(&payload.absolute_path);
    if !input.is_absolute() {
        return ResponseEnvelope::error(id, "invalid_resource", "workspace path must be absolute");
    }
    let Ok(canonical) = fs::canonicalize(&input) else {
        return ResponseEnvelope::error(
            id,
            "invalid_resource",
            "workspace path cannot be canonicalized",
        );
    };
    if !canonical.is_dir() {
        return ResponseEnvelope::error(id, "invalid_resource", "workspace must be a directory");
    }
    let now = OffsetDateTime::now_utc();
    let candidate = Resource {
        id: Uuid::new_v4(),
        kind: ResourceKind::Directory,
        label: payload.label,
        locator: payload.absolute_path,
        canonical_locator: canonical.to_string_lossy().into_owned(),
        sensitivity: fubun_domain::Sensitivity::Normal,
        scope: ResourceScope::Exact,
        created_at: now,
        updated_at: now,
    };
    if candidate.validate().is_err() {
        return ResponseEnvelope::error(id, "invalid_resource", "invalid workspace resource");
    }
    let resource = match storage.create_resource(candidate).await {
        Ok(resource) => resource,
        Err(StorageError::DuplicateResource) => match storage.list_resources().await {
            Ok(resources) => match resources.into_iter().find(|resource| {
                resource.kind == ResourceKind::Directory
                    && resource.canonical_locator == canonical.to_string_lossy()
            }) {
                Some(resource) => resource,
                None => {
                    return ResponseEnvelope::error(id, "invalid_resource", "workspace conflict")
                }
            },
            Err(error) => return storage_error(id, error),
        },
        Err(error) => return storage_error(id, error),
    };
    let scope = ObservationScope {
        id: Uuid::new_v4(),
        source: ObservationSource::VscodeWorkspace,
        resource_id: resource.id,
        status: ObservationStatus::Active,
        created_at: now,
        updated_at: now,
    };
    match storage.ensure_observation_scope(scope).await {
        Ok(scope) => ResponseEnvelope::ok(
            id,
            ResponsePayload::VscodeObservationEnabled(fubun_protocol::VscodeObservationEnabled {
                canonical_path_hash: canonical_web_url_hash(&format!(
                    "file://{}",
                    resource.canonical_locator
                )),
                resource,
                scope,
            }),
        ),
        Err(error) => storage_error(id, error),
    }
}

fn validate_ritual_definition(definition: &RitualDefinition) -> Result<String, String> {
    definition.validate().map_err(|error| error.to_string())?;
    for action in &definition.actions {
        validate_action(action).map_err(|error| error.to_string())?;
    }
    let canonical = definition
        .canonical_json()
        .map_err(|_| "ritual JSON could not be canonicalized".to_owned())?;
    if canonical.len() > MAX_RITUAL_JSON_BYTES {
        return Err("ritual JSON exceeds the size limit".to_owned());
    }
    Ok(canonical)
}

async fn ritual_create_response(
    id: Uuid,
    definition: RitualDefinition,
    storage: &StorageHandle,
) -> ResponseEnvelope {
    let canonical = match validate_ritual_definition(&definition) {
        Ok(value) => value,
        Err(message) => return ResponseEnvelope::error(id, "invalid_ritual", message),
    };
    let content_hash = match definition.content_hash() {
        Ok(value) => value,
        Err(_) => {
            return ResponseEnvelope::error(
                id,
                "invalid_ritual",
                "ritual hash could not be computed",
            )
        }
    };
    let now = OffsetDateTime::now_utc();
    let ritual_id = Uuid::new_v4();
    let version = RitualVersion {
        id: Uuid::new_v4(),
        ritual_id,
        version: 1,
        schema_version: RITUAL_SCHEMA_VERSION.to_owned(),
        canonical_json: canonical,
        content_hash,
        created_at: now,
    };
    let ritual = Ritual {
        id: ritual_id,
        name: definition.name.clone(),
        status: RitualStatus::Draft,
        current_version_id: version.id,
        created_at: now,
        updated_at: now,
    };
    match storage.create_ritual(ritual, version, definition).await {
        Ok((ritual, version, definition)) => ResponseEnvelope::ok(
            id,
            ResponsePayload::RitualCreated(fubun_protocol::RitualRecord {
                ritual,
                version,
                definition,
            }),
        ),
        Err(error) => storage_error(id, error),
    }
}

async fn ritual_update_response(
    id: Uuid,
    ritual_id: Uuid,
    definition: RitualDefinition,
    storage: &StorageHandle,
) -> ResponseEnvelope {
    let canonical = match validate_ritual_definition(&definition) {
        Ok(value) => value,
        Err(message) => return ResponseEnvelope::error(id, "invalid_ritual", message),
    };
    let content_hash = match definition.content_hash() {
        Ok(value) => value,
        Err(_) => {
            return ResponseEnvelope::error(
                id,
                "invalid_ritual",
                "ritual hash could not be computed",
            )
        }
    };
    let (mut ritual, current, _) = match storage.get_ritual(ritual_id).await {
        Ok(value) => value,
        Err(StorageError::NotFound) => {
            return ResponseEnvelope::error(id, "ritual_not_found", "ritual was not found")
        }
        Err(error) => return storage_error(id, error),
    };
    let now = OffsetDateTime::now_utc();
    let version = RitualVersion {
        id: Uuid::new_v4(),
        ritual_id,
        version: current.version.saturating_add(1),
        schema_version: RITUAL_SCHEMA_VERSION.to_owned(),
        canonical_json: canonical,
        content_hash,
        created_at: now,
    };
    ritual.name = definition.name.clone();
    ritual.status = RitualStatus::Draft;
    ritual.current_version_id = version.id;
    ritual.updated_at = now;
    match storage.update_ritual(ritual, version, definition).await {
        Ok((ritual, version, definition)) => ResponseEnvelope::ok(
            id,
            ResponsePayload::RitualUpdated(fubun_protocol::RitualRecord {
                ritual,
                version,
                definition,
            }),
        ),
        Err(error) => storage_error(id, error),
    }
}

async fn ritual_show_response(
    id: Uuid,
    ritual_id: Uuid,
    storage: &StorageHandle,
) -> ResponseEnvelope {
    match storage.get_ritual(ritual_id).await {
        Ok((ritual, version, definition)) => ResponseEnvelope::ok(
            id,
            ResponsePayload::RitualShow(fubun_protocol::RitualRecord {
                ritual,
                version,
                definition,
            }),
        ),
        Err(StorageError::NotFound) => {
            ResponseEnvelope::error(id, "ritual_not_found", "ritual was not found")
        }
        Err(error) => storage_error(id, error),
    }
}

async fn ritual_preview_response(
    id: Uuid,
    ritual_id: Uuid,
    storage: &StorageHandle,
    adapters: &AdapterManager,
) -> ResponseEnvelope {
    let (ritual, version, definition) = match storage.get_ritual(ritual_id).await {
        Ok(value) => value,
        Err(StorageError::NotFound) => {
            return ResponseEnvelope::error(id, "ritual_not_found", "ritual was not found")
        }
        Err(error) => return storage_error(id, error),
    };
    match build_preview(&ritual, &version, &definition, storage, adapters).await {
        Ok(preview) => ResponseEnvelope::ok(id, ResponsePayload::RitualPreview(preview)),
        Err((code, message)) => ResponseEnvelope::error(id, code, message),
    }
}

async fn build_preview(
    ritual: &Ritual,
    version: &RitualVersion,
    definition: &RitualDefinition,
    storage: &StorageHandle,
    adapters: &AdapterManager,
) -> Result<RitualPreview, (String, String)> {
    let mut actions = Vec::with_capacity(definition.actions.len());
    let mut warnings = Vec::new();
    for (index, action) in definition.actions.iter().enumerate() {
        let descriptor = descriptor(action);
        let requirements = AdapterRequirements::from_action(action);
        let (eligible, mut action_warnings) = match adapters.find_eligible(&requirements).await {
            Ok(_) => (true, Vec::new()),
            Err(error) => (false, vec![eligibility_warning(error).to_owned()]),
        };
        let mut resource_exists = None;
        let mut resource_path_matches = None;
        let mut resource_kind_matches = None;
        let mut desktop_entry_exists = None;
        match action {
            ActionSpec::LinuxPathOpen { resource_id } => match storage
                .get_resource(*resource_id)
                .await
            {
                Ok(resource) => {
                    let path = Path::new(&resource.canonical_locator);
                    resource_exists = Some(path.exists());
                    resource_path_matches = Some(
                        path.exists()
                            && fs::canonicalize(path)
                                .map(|value| value.to_string_lossy() == resource.canonical_locator)
                                .unwrap_or(false),
                    );
                    resource_kind_matches = Some(
                        path.exists()
                            && match fs::metadata(path) {
                                Ok(metadata) if metadata.is_file() => {
                                    resource.kind == ResourceKind::File
                                }
                                Ok(metadata) if metadata.is_dir() => {
                                    resource.kind == ResourceKind::Directory
                                }
                                _ => false,
                            },
                    );
                    if resource_exists != Some(true) {
                        action_warnings.push("resource does not exist".to_owned());
                    }
                    if resource_path_matches != Some(true) {
                        action_warnings.push("resource canonical path changed".to_owned());
                    }
                    if resource_kind_matches != Some(true) {
                        action_warnings.push("resource type changed".to_owned());
                    }
                }
                Err(_) => {
                    action_warnings.push("resource was not found".to_owned());
                }
            },
            ActionSpec::LinuxAppEnsureRunning { .. } => {
                desktop_entry_exists = Some(eligible);
                if desktop_entry_exists != Some(true) {
                    action_warnings
                        .push("desktop entry was not found on an eligible adapter".to_owned());
                }
            }
            ActionSpec::DesktopNotificationShow { .. } => {}
            ActionSpec::BrowserTabEnsureOpen { resource_id } => {
                match storage.get_resource(*resource_id).await {
                    Ok(resource) => {
                        resource_exists = Some(true);
                        resource_kind_matches = Some(resource.kind == ResourceKind::WebPage);
                        resource_path_matches = Some(
                            resource.kind == ResourceKind::WebPage
                                && canonicalize_web_url(&resource.canonical_locator)
                                    .map(|value| value == resource.canonical_locator)
                                    .unwrap_or(false),
                        );
                        if resource_kind_matches != Some(true) {
                            action_warnings.push("resource is not a web page".to_owned());
                        }
                        if resource_path_matches != Some(true) {
                            action_warnings
                                .push("web resource canonical URL is invalid".to_owned());
                        }
                        let scopes = storage
                            .list_observation_scopes(Some(ObservationSource::BrowserChromium))
                            .await
                            .map_err(|_| {
                                (
                                    "internal_error".to_owned(),
                                    "observation scope lookup failed".to_owned(),
                                )
                            })?;
                        if !scopes.iter().any(|scope| {
                            scope.resource_id == *resource_id
                                && scope.status == ObservationStatus::Active
                        }) {
                            action_warnings
                                .push("browser observation scope is not active".to_owned());
                        }
                    }
                    Err(_) => {
                        resource_exists = Some(false);
                        action_warnings.push("resource was not found".to_owned());
                    }
                }
            }
        }
        if action.validate().is_err() {
            action_warnings.push("action validation failed".to_owned());
        }
        for message in &action_warnings {
            warnings.push(format!("action {}: {message}", index + 1));
        }
        let warning = action_warnings.into_iter().next();
        actions.push(PreviewAction {
            step_index: index as u32,
            action_type: descriptor.action_type.to_owned(),
            risk_level: format!("{:?}", descriptor.risk_level),
            idempotency: format!("{:?}", descriptor.idempotency),
            revertability: format!("{:?}", descriptor.revertability),
            required_capability: descriptor.required_capability.to_owned(),
            adapter_connected: eligible,
            required_tool_available: requirements.required_tool.map(|_| eligible),
            resource_exists,
            resource_path_matches,
            resource_kind_matches,
            desktop_entry_exists,
            warning,
        });
    }
    Ok(RitualPreview {
        ritual_id: ritual.id,
        ritual_version_id: version.id,
        actions,
        timeout_seconds: definition.execution.timeout_seconds,
        warnings: warnings.clone(),
        executable: warnings.is_empty(),
    })
}

const fn eligibility_warning(error: AdapterEligibilityError) -> &'static str {
    match error {
        AdapterEligibilityError::Unavailable => "no adapter is connected",
        AdapterEligibilityError::Ineligible => {
            "no single adapter instance satisfies the action requirements"
        }
    }
}

async fn ritual_activate_response(
    id: Uuid,
    ritual_id: Uuid,
    approve: bool,
    storage: &StorageHandle,
    adapters: &AdapterManager,
) -> ResponseEnvelope {
    if !approve {
        return ResponseEnvelope::error(id, "approval_required", "activation requires --approve");
    }
    let (mut ritual, version, definition) = match storage.get_ritual(ritual_id).await {
        Ok(value) => value,
        Err(StorageError::NotFound) => {
            return ResponseEnvelope::error(id, "ritual_not_found", "ritual was not found")
        }
        Err(error) => return storage_error(id, error),
    };
    let preview = match build_preview(&ritual, &version, &definition, storage, adapters).await {
        Ok(value) => value,
        Err((code, message)) => return ResponseEnvelope::error(id, code, message),
    };
    if !preview.executable {
        return ResponseEnvelope::error(
            id,
            "preflight_failed",
            "ritual preview contains blocking warnings",
        );
    }
    let now = OffsetDateTime::now_utc();
    let approvals = definition
        .actions
        .iter()
        .map(|action| {
            let fields = approval_fields(action);
            Approval {
                id: Uuid::new_v4(),
                ritual_version_id: version.id,
                action_type: fields.action_type.to_owned(),
                capability: fields.capability.to_owned(),
                resource_id: fields.resource_id,
                app_id: fields.app_id,
                content_hash: version.content_hash.clone(),
                approved_at: now,
            }
        })
        .collect();
    if let Err(error) = storage.create_approvals(approvals).await {
        return storage_error(id, error);
    }
    ritual.status = RitualStatus::Active;
    if let Err(error) = storage
        .set_ritual_status(ritual_id, RitualStatus::Active)
        .await
    {
        return storage_error(id, error);
    }
    ResponseEnvelope::ok(id, ResponsePayload::RitualActivated(ritual))
}

async fn ritual_pause_response(
    id: Uuid,
    ritual_id: Uuid,
    storage: &StorageHandle,
) -> ResponseEnvelope {
    let (ritual, _, _) = match storage.get_ritual(ritual_id).await {
        Ok(value) => value,
        Err(StorageError::NotFound) => {
            return ResponseEnvelope::error(id, "ritual_not_found", "ritual was not found")
        }
        Err(error) => return storage_error(id, error),
    };
    if ritual.status == RitualStatus::Archived {
        return ResponseEnvelope::error(id, "ritual_archived", "archived rituals cannot be paused");
    }
    if let Err(error) = storage
        .set_ritual_status(ritual_id, RitualStatus::Paused)
        .await
    {
        return storage_error(id, error);
    }
    let mut paused = ritual;
    paused.status = RitualStatus::Paused;
    ResponseEnvelope::ok(id, ResponsePayload::RitualPaused(paused))
}

async fn ritual_run_response(
    id: Uuid,
    ritual_id: Uuid,
    storage: &StorageHandle,
    adapters: &AdapterManager,
) -> ResponseEnvelope {
    let (ritual, version, definition) = match storage.get_ritual(ritual_id).await {
        Ok(value) => value,
        Err(StorageError::NotFound) => {
            return ResponseEnvelope::error(id, "ritual_not_found", "ritual was not found")
        }
        Err(error) => return storage_error(id, error),
    };
    match ritual.status {
        RitualStatus::Active => {}
        RitualStatus::Draft => {
            return ResponseEnvelope::error(
                id,
                "ritual_not_active",
                "draft rituals must be activated before execution",
            )
        }
        RitualStatus::Paused => {
            return ResponseEnvelope::error(
                id,
                "ritual_not_active",
                "paused rituals cannot execute",
            )
        }
        RitualStatus::Archived => {
            return ResponseEnvelope::error(
                id,
                "ritual_not_active",
                "archived rituals cannot execute",
            )
        }
    }
    if let Err((code, message)) = preflight(&version, &definition, storage, adapters).await {
        return ResponseEnvelope::error(id, code, message);
    }
    let now = OffsetDateTime::now_utc();
    let execution_id = Uuid::new_v4();
    let execution = Execution {
        id: execution_id,
        ritual_id,
        ritual_version_id: version.id,
        status: ExecutionStatus::Running,
        trigger_kind: TriggerKind::Manual,
        started_at: now,
        finished_at: None,
        failure_code: None,
        created_at: now,
    };
    let steps: Vec<ExecutionStep> = definition
        .actions
        .iter()
        .enumerate()
        .map(|(index, action)| ExecutionStep {
            id: Uuid::new_v4(),
            execution_id,
            step_index: index as u32,
            action_type: action.action_type().to_owned(),
            status: ExecutionStepStatus::Pending,
            adapter_id: None,
            adapter_instance_id: None,
            started_at: now,
            finished_at: None,
            result_code: None,
            redacted_message: None,
        })
        .collect();
    if let Err(error) = storage
        .start_execution(execution.clone(), steps.clone())
        .await
    {
        return match error {
            StorageError::RitualAlreadyRunning => ResponseEnvelope::error(
                id,
                "ritual_already_running",
                "the ritual already has a running execution",
            ),
            other => storage_error(id, other),
        };
    }
    let result = execute_steps(execution, steps, definition, storage, adapters).await;
    match result {
        Ok((execution, steps)) => ResponseEnvelope::ok(
            id,
            ResponsePayload::RitualRun(fubun_protocol::ExecutionRecord { execution, steps }),
        ),
        Err((code, message)) => ResponseEnvelope::error(id, code, message),
    }
}

async fn preflight(
    version: &RitualVersion,
    definition: &RitualDefinition,
    storage: &StorageHandle,
    adapters: &AdapterManager,
) -> Result<(), (String, String)> {
    let approvals = storage.list_approvals(version.id).await.map_err(|_| {
        (
            "internal_error".to_owned(),
            "approval lookup failed".to_owned(),
        )
    })?;
    for action in &definition.actions {
        action.validate().map_err(|_| {
            (
                "invalid_ritual".to_owned(),
                "action validation failed".to_owned(),
            )
        })?;
        validate_action(action).map_err(|_| {
            (
                "unknown_action".to_owned(),
                "action is not registered".to_owned(),
            )
        })?;
        let requirements = AdapterRequirements::from_action(action);
        adapters
            .find_eligible(&requirements)
            .await
            .map_err(preflight_eligibility_error)?;
        let fields = approval_fields(action);
        let approved = approvals.iter().any(|approval| {
            approval.ritual_version_id == version.id
                && approval.content_hash == version.content_hash
                && approval.action_type == fields.action_type
                && approval.capability == fields.capability
                && approval.resource_id == fields.resource_id
                && approval.app_id == fields.app_id
        });
        if !approved {
            return Err((
                "ritual_not_approved".to_owned(),
                "ritual version approval is missing or stale".to_owned(),
            ));
        }
        match action {
            ActionSpec::LinuxPathOpen { resource_id } => {
                let resource = storage.get_resource(*resource_id).await.map_err(|_| {
                    (
                        "resource_not_found".to_owned(),
                        "resource was not found".to_owned(),
                    )
                })?;
                let path = Path::new(&resource.canonical_locator);
                if !path.exists() {
                    return Err((
                        "resource_changed".to_owned(),
                        "resource no longer exists".to_owned(),
                    ));
                }
                if fs::canonicalize(path)
                    .map(|value| value.to_string_lossy() != resource.canonical_locator)
                    .unwrap_or(true)
                {
                    return Err((
                        "resource_changed".to_owned(),
                        "resource canonical path changed".to_owned(),
                    ));
                }
                let kind_matches = match fs::metadata(path) {
                    Ok(metadata) if metadata.is_file() => resource.kind == ResourceKind::File,
                    Ok(metadata) if metadata.is_dir() => resource.kind == ResourceKind::Directory,
                    _ => false,
                };
                if !kind_matches {
                    return Err((
                        "resource_changed".to_owned(),
                        "resource type changed".to_owned(),
                    ));
                }
            }
            ActionSpec::LinuxAppEnsureRunning { .. } => {}
            ActionSpec::DesktopNotificationShow { .. } => {}
            ActionSpec::BrowserTabEnsureOpen { resource_id } => {
                let resource = storage.get_resource(*resource_id).await.map_err(|_| {
                    (
                        "resource_not_found".to_owned(),
                        "web resource was not found".to_owned(),
                    )
                })?;
                if resource.kind != ResourceKind::WebPage
                    || canonicalize_web_url(&resource.canonical_locator)
                        .map(|value| value != resource.canonical_locator)
                        .unwrap_or(true)
                {
                    return Err((
                        "resource_changed".to_owned(),
                        "web resource canonical URL changed".to_owned(),
                    ));
                }
                let scopes = storage
                    .list_observation_scopes(Some(ObservationSource::BrowserChromium))
                    .await
                    .map_err(|_| {
                        (
                            "internal_error".to_owned(),
                            "observation scope lookup failed".to_owned(),
                        )
                    })?;
                if !scopes.iter().any(|scope| {
                    scope.resource_id == *resource_id && scope.status == ObservationStatus::Active
                }) {
                    return Err((
                        "observation_inactive".to_owned(),
                        "browser observation scope is not active".to_owned(),
                    ));
                }
            }
        }
    }
    Ok(())
}

fn preflight_eligibility_error(error: AdapterEligibilityError) -> (String, String) {
    match error {
        AdapterEligibilityError::Unavailable => (
            "adapter_unavailable".to_owned(),
            "no adapter is connected".to_owned(),
        ),
        AdapterEligibilityError::Ineligible => (
            "capability_unavailable".to_owned(),
            "no single adapter instance satisfies the action requirements".to_owned(),
        ),
    }
}

async fn execute_steps(
    mut execution: Execution,
    mut steps: Vec<ExecutionStep>,
    definition: RitualDefinition,
    storage: &StorageHandle,
    adapters: &AdapterManager,
) -> Result<(Execution, Vec<ExecutionStep>), (String, String)> {
    let overall = Duration::from_secs(u64::from(definition.execution.timeout_seconds));
    let deadline = Instant::now() + overall;
    let mut successful = 0usize;
    for (index, action) in definition.actions.into_iter().enumerate() {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            execution.failure_code = Some("ritual_timeout".to_owned());
            break;
        }
        let started = OffsetDateTime::now_utc();
        steps[index].status = ExecutionStepStatus::Running;
        steps[index].started_at = started;
        storage
            .update_step(steps[index].clone())
            .await
            .map_err(|_| {
                (
                    "internal_error".to_owned(),
                    "execution step could not be updated".to_owned(),
                )
            })?;
        let resolved = match &action {
            ActionSpec::LinuxPathOpen { resource_id }
            | ActionSpec::BrowserTabEnsureOpen { resource_id } => {
                let resource = storage.get_resource(*resource_id).await.map_err(|_| {
                    (
                        "resource_not_found".to_owned(),
                        "resource was not found".to_owned(),
                    )
                })?;
                Some(ResolvedResource {
                    resource_id: *resource_id,
                    kind: resource.kind,
                    canonical_locator: resource.canonical_locator,
                })
            }
            _ => None,
        };
        let default_timeout = Duration::from_millis(descriptor(&action).default_timeout_ms);
        let deadline_bound = remaining <= default_timeout;
        let action_timeout = default_timeout.min(remaining);
        let action_result = adapters.execute(action, resolved, action_timeout).await;
        let finished = OffsetDateTime::now_utc();
        match action_result {
            Ok(dispatched) => {
                steps[index].adapter_id = Some(dispatched.identity.adapter_id);
                steps[index].adapter_instance_id = Some(dispatched.identity.instance_id);
                let ActionResult {
                    status,
                    result_code,
                    redacted_message,
                } = dispatched.result;
                steps[index].status = match status {
                    fubun_protocol::AdapterActionStatus::Succeeded => {
                        ExecutionStepStatus::Succeeded
                    }
                    fubun_protocol::AdapterActionStatus::Skipped => ExecutionStepStatus::Skipped,
                    fubun_protocol::AdapterActionStatus::Failed => ExecutionStepStatus::Failed,
                };
                steps[index].result_code = Some(result_code);
                steps[index].redacted_message = Some(redact_message(&redacted_message));
                steps[index].finished_at = Some(finished);
                storage
                    .update_step(steps[index].clone())
                    .await
                    .map_err(|_| {
                        (
                            "internal_error".to_owned(),
                            "execution step could not be updated".to_owned(),
                        )
                    })?;
                if matches!(
                    steps[index].status,
                    ExecutionStepStatus::Succeeded | ExecutionStepStatus::Skipped
                ) {
                    successful += 1;
                } else {
                    execution.status = if successful == 0 {
                        ExecutionStatus::Failed
                    } else {
                        ExecutionStatus::Partial
                    };
                    execution.failure_code = Some("action_failed".to_owned());
                    break;
                }
            }
            Err(error) => {
                if let Some(identity) = error.identity {
                    steps[index].adapter_id = Some(identity.adapter_id);
                    steps[index].adapter_instance_id = Some(identity.instance_id);
                }
                if error.kind == AdapterDispatchErrorKind::Timeout && deadline_bound {
                    execution.failure_code = Some("ritual_timeout".to_owned());
                    break;
                }
                steps[index].status = ExecutionStepStatus::Failed;
                steps[index].result_code = Some(dispatch_error_code(error.kind).to_owned());
                steps[index].redacted_message = Some(dispatch_error_message(error.kind).to_owned());
                steps[index].finished_at = Some(finished);
                storage
                    .update_step(steps[index].clone())
                    .await
                    .map_err(|_| {
                        (
                            "internal_error".to_owned(),
                            "execution step could not be updated".to_owned(),
                        )
                    })?;
                execution.status = if successful == 0 {
                    ExecutionStatus::Failed
                } else {
                    ExecutionStatus::Partial
                };
                execution.failure_code = Some(dispatch_error_code(error.kind).to_owned());
                break;
            }
        }
    }
    let execution_finished = OffsetDateTime::now_utc();
    if execution.failure_code.as_deref() == Some("ritual_timeout") {
        execution.status = if successful == 0 {
            ExecutionStatus::Failed
        } else {
            ExecutionStatus::Partial
        };
        finalize_unfinished_steps(
            &mut steps,
            storage,
            execution_finished,
            "ritual_timeout",
            "ritual timed out",
        )
        .await?;
    } else if execution.failure_code.is_none() {
        execution.status = ExecutionStatus::Succeeded;
    } else {
        finalize_unfinished_steps(
            &mut steps,
            storage,
            execution_finished,
            "stopped_after_failure",
            "action was not executed because an earlier action failed",
        )
        .await?;
    }
    execution.finished_at = Some(execution_finished);
    if !terminal_step_invariant(execution.status, &steps) {
        return Err((
            "internal_error".to_owned(),
            "terminal execution has a non-terminal step".to_owned(),
        ));
    }
    storage
        .update_execution(execution.clone())
        .await
        .map_err(|_| {
            (
                "internal_error".to_owned(),
                "execution could not be finalized".to_owned(),
            )
        })?;
    if matches!(
        execution.status,
        ExecutionStatus::Failed | ExecutionStatus::Partial
    ) && execution.failure_code.is_none()
    {
        return Err(("action_failed".to_owned(), "execution failed".to_owned()));
    }
    Ok((execution, steps))
}

async fn finalize_unfinished_steps(
    steps: &mut [ExecutionStep],
    storage: &StorageHandle,
    finished_at: OffsetDateTime,
    result_code: &str,
    redacted_message: &str,
) -> Result<(), (String, String)> {
    for step in steps {
        if matches!(
            step.status,
            ExecutionStepStatus::Pending | ExecutionStepStatus::Running
        ) {
            step.status = ExecutionStepStatus::Aborted;
            step.finished_at = Some(finished_at);
            step.result_code = Some(result_code.to_owned());
            step.redacted_message = Some(redacted_message.to_owned());
            storage.update_step(step.clone()).await.map_err(|_| {
                (
                    "internal_error".to_owned(),
                    "execution step could not be finalized".to_owned(),
                )
            })?;
        }
    }
    Ok(())
}

fn terminal_step_invariant(status: ExecutionStatus, steps: &[ExecutionStep]) -> bool {
    if !matches!(
        status,
        ExecutionStatus::Succeeded
            | ExecutionStatus::Failed
            | ExecutionStatus::Partial
            | ExecutionStatus::Aborted
    ) {
        return true;
    }
    steps.iter().all(|step| {
        matches!(
            step.status,
            ExecutionStepStatus::Succeeded
                | ExecutionStepStatus::Skipped
                | ExecutionStepStatus::Failed
                | ExecutionStepStatus::Aborted
        )
    })
}

fn redact_message(message: &str) -> String {
    message
        .chars()
        .filter(|character| !character.is_control())
        .take(160)
        .collect()
}

const fn dispatch_error_code(error: AdapterDispatchErrorKind) -> &'static str {
    match error {
        AdapterDispatchErrorKind::Unavailable => "adapter_unavailable",
        AdapterDispatchErrorKind::CapabilityUnavailable => "capability_unavailable",
        AdapterDispatchErrorKind::Timeout => "action_timeout",
        AdapterDispatchErrorKind::Disconnected => "adapter_disconnected",
        AdapterDispatchErrorKind::Protocol => "protocol_error",
    }
}
const fn dispatch_error_message(error: AdapterDispatchErrorKind) -> &'static str {
    match error {
        AdapterDispatchErrorKind::Unavailable => "no adapter is connected",
        AdapterDispatchErrorKind::CapabilityUnavailable => "required capability is unavailable",
        AdapterDispatchErrorKind::Timeout => "adapter action timed out",
        AdapterDispatchErrorKind::Disconnected => "adapter disconnected",
        AdapterDispatchErrorKind::Protocol => "adapter protocol error",
    }
}

async fn status_response(request_id: Uuid, storage: &StorageHandle) -> ResponseEnvelope {
    match storage.schema_version().await {
        Ok(schema_version) => ResponseEnvelope::ok(
            request_id,
            ResponsePayload::Status(StatusReport {
                daemon: "ok".to_owned(),
                protocol_version: CURRENT_PROTOCOL_VERSION,
                database_status: "ok".to_owned(),
                schema_version,
            }),
        ),
        Err(error) => storage_error(request_id, error),
    }
}

async fn doctor_response(
    request_id: Uuid,
    storage: &StorageHandle,
    adapters: &AdapterManager,
) -> ResponseEnvelope {
    match (storage.schema_version().await, storage.counts().await) {
        (Ok(schema_version), Ok((running, draft, active))) => ResponseEnvelope::ok(
            request_id,
            ResponsePayload::Doctor(DoctorReport {
                database_path: storage.database_path().display().to_string(),
                database_status: "ok".to_owned(),
                schema_version,
                gtk_launch_available: adapters.tool_available("gtk-launch").await,
                xdg_open_available: adapters.tool_available("xdg-open").await,
                notify_send_available: adapters.tool_available("notify-send").await,
                connected_adapters: adapters.connected_count().await,
                action_registry_version: "phase2-fixed-v1".to_owned(),
                ritual_schema_version: RITUAL_SCHEMA_VERSION.to_owned(),
                running_executions: running,
                draft_rituals: draft,
                active_rituals: active,
                browser_adapters: adapters
                    .connected_count_by_id("dev.fubun.browser.chromium")
                    .await,
                vscode_adapters: adapters.connected_count_by_id("dev.fubun.vscode").await,
                active_browser_scopes: storage
                    .list_observation_scopes(Some(ObservationSource::BrowserChromium))
                    .await
                    .unwrap_or_default()
                    .iter()
                    .filter(|scope| scope.status == ObservationStatus::Active)
                    .count(),
                active_vscode_scopes: storage
                    .list_observation_scopes(Some(ObservationSource::VscodeWorkspace))
                    .await
                    .unwrap_or_default()
                    .iter()
                    .filter(|scope| scope.status == ObservationStatus::Active)
                    .count(),
            }),
        ),
        _ => ResponseEnvelope::error(request_id, "storage_error", "database check failed"),
    }
}

fn storage_error(id: Uuid, error: StorageError) -> ResponseEnvelope {
    let (code, message) = match error {
        StorageError::NotFound => ("internal_error", "requested record was not found"),
        StorageError::DuplicateResource => ("duplicate_resource", "resource already exists"),
        StorageError::RitualAlreadyRunning => {
            ("ritual_already_running", "ritual is already running")
        }
        StorageError::DiscoveryAlreadyRunning => (
            "discovery_already_running",
            "a discovery run is already running",
        ),
        StorageError::DiscoveryInputTooLarge => {
            ("discovery_input_too_large", "discovery input is too large")
        }
        StorageError::SessionNotFound => ("session_not_found", "session was not found"),
        StorageError::SuggestionNotFound => ("suggestion_not_found", "suggestion was not found"),
        StorageError::SuggestionInvalidState => (
            "suggestion_invalid_state",
            "suggestion state does not allow this operation",
        ),
        StorageError::SuggestionStale => ("suggestion_stale", "suggestion is stale"),
        StorageError::InvalidSnoozeDuration => {
            ("invalid_snooze_duration", "snooze duration is invalid")
        }
        _ => ("internal_error", "storage operation failed"),
    };
    ResponseEnvelope::error(id, code, message)
}

pub struct FubunClient {
    stream: UnixStream,
}

impl FubunClient {
    pub async fn connect(
        socket_path: impl AsRef<Path>,
        client_name: &str,
        client_version: &str,
    ) -> Result<Self, ClientError> {
        let stream = UnixStream::connect(socket_path).await?;
        let mut client = Self { stream };
        let payload = client
            .request(RequestBody::ClientHello(ClientHello {
                client_name: client_name.to_owned(),
                client_version: client_version.to_owned(),
                protocol_version: CURRENT_PROTOCOL_VERSION,
            }))
            .await?;
        if !matches!(payload, ResponsePayload::ClientHelloAck(_)) {
            return Err(ClientError::UnexpectedPayload);
        }
        Ok(client)
    }

    pub async fn request(&mut self, body: RequestBody) -> Result<ResponsePayload, ClientError> {
        let request_id = Uuid::new_v4();
        let request = RequestEnvelope {
            protocol_version: CURRENT_PROTOCOL_VERSION,
            request_id,
            body,
        };
        write_json_frame(&mut self.stream, &request).await?;
        let response: ResponseEnvelope = read_json_frame(&mut self.stream)
            .await?
            .ok_or(ClientError::ConnectionClosed)?;
        if response.request_id != request_id {
            return Err(ClientError::RequestIdMismatch);
        }
        match response.body {
            ResponseBody::Ok(payload) => Ok(payload),
            ResponseBody::Error(error) => Err(ClientError::Rejected {
                code: error.code,
                message: error.message,
            }),
        }
    }

    pub async fn status(&mut self) -> Result<StatusReport, ClientError> {
        match self
            .request(RequestBody::SystemStatus(EmptyRequest::default()))
            .await?
        {
            ResponsePayload::Status(report) => Ok(report),
            _ => Err(ClientError::UnexpectedPayload),
        }
    }
}
