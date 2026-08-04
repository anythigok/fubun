//! Local-only Fubun daemon, ritual orchestration, and IPC client.

pub mod adapter;
pub mod paths;

use std::{
    fs,
    os::unix::fs::{FileTypeExt, PermissionsExt},
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use adapter::{AdapterDispatchError, AdapterManager};
use fubun_domain::{
    ActionSpec, Approval, Execution, ExecutionStatus, ExecutionStep, ExecutionStepStatus, Resource,
    ResourceKind, ResourceScope, Ritual, RitualDefinition, RitualStatus, RitualVersion,
    TriggerKind, RITUAL_SCHEMA_VERSION,
};
use fubun_policy::{approval_fields, descriptor, validate_action};
use fubun_protocol::{
    read_json_frame, write_json_frame, ActionResult, AdapterHello, AdapterResponseEnvelope,
    ClientHello, ClientHelloAck, DoctorReport, EmptyRequest, EventIngested, EventList, FrameError,
    PreviewAction, RequestBody, RequestEnvelope, ResolvedResource, ResponseBody, ResponseEnvelope,
    ResponsePayload, RitualPreview, StatusReport, CURRENT_PROTOCOL_VERSION,
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
            handle_adapter_connection(stream, first.request_id, hello, adapters, shutdown).await
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
    if let Err(error) = adapters.register(hello, sender).await {
        write_json_frame(
            &mut writer,
            &ResponseEnvelope::error(request_id, "protocol_error", error.to_string()),
        )
        .await?;
        return Ok(());
    }
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
                    Ok(Some(response)) if response.protocol_version.major == CURRENT_PROTOCOL_VERSION.major => adapters.resolve(response).await,
                    Ok(Some(_)) => warn!("adapter response protocol major mismatch"),
                    Ok(None) | Err(_) => break,
                }
            }
        }
    }
    adapters.disconnect(instance_id).await;
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
    }
}

async fn ingest_response(
    id: Uuid,
    mut event: fubun_domain::Event,
    storage: &StorageHandle,
) -> ResponseEnvelope {
    event.received_at = OffsetDateTime::now_utc();
    if let Err(error) = event.validate() {
        return ResponseEnvelope::error(id, "invalid_event", error.to_string());
    }
    let event_id = event.id;
    match storage.insert_event(event).await {
        Ok(()) => ResponseEnvelope::ok(
            id,
            ResponsePayload::EventIngested(EventIngested { event_id }),
        ),
        Err(StorageError::DuplicateEvent) => ResponseEnvelope::error(
            id,
            "duplicate_event",
            "adapter_instance_id and sequence_no have already been stored",
        ),
        Err(error) => storage_error(id, error),
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
        let connected = adapters
            .capability_available(descriptor.required_capability)
            .await;
        let required_tool = required_tool(action);
        let required_tool_available = Some(
            adapters
                .capability_tool_available(descriptor.required_capability, required_tool)
                .await,
        );
        let mut resource_exists = None;
        let mut resource_path_matches = None;
        let mut resource_kind_matches = None;
        let mut desktop_entry_exists = None;
        let mut warning = None;
        if !connected {
            warning = Some("required adapter capability is unavailable".to_owned());
        }
        if required_tool_available == Some(false) {
            warning = Some("required executable is unavailable".to_owned());
        }
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
                        warning = Some("resource does not exist".to_owned());
                    }
                    if resource_path_matches != Some(true) {
                        warning = Some("resource canonical path changed".to_owned());
                    }
                    if resource_kind_matches != Some(true) {
                        warning = Some("resource type changed".to_owned());
                    }
                }
                Err(_) => {
                    warning = Some("resource was not found".to_owned());
                }
            },
            ActionSpec::LinuxAppEnsureRunning { app_id } => {
                desktop_entry_exists = adapters.desktop_entry_available(app_id).await;
                if desktop_entry_exists != Some(true) {
                    warning = Some("desktop entry was not found".to_owned());
                }
            }
            ActionSpec::DesktopNotificationShow { .. } => {}
        }
        if action.validate().is_err() {
            warning = Some("action validation failed".to_owned());
        }
        if let Some(message) = &warning {
            warnings.push(format!("action {}: {message}", index + 1));
        }
        actions.push(PreviewAction {
            step_index: index as u32,
            action_type: descriptor.action_type.to_owned(),
            risk_level: format!("{:?}", descriptor.risk_level),
            idempotency: format!("{:?}", descriptor.idempotency),
            revertability: format!("{:?}", descriptor.revertability),
            required_capability: descriptor.required_capability.to_owned(),
            adapter_connected: connected,
            required_tool_available,
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

const fn required_tool(action: &ActionSpec) -> &'static str {
    match action {
        ActionSpec::LinuxAppEnsureRunning { .. } => "gtk-launch",
        ActionSpec::LinuxPathOpen { .. } => "xdg-open",
        ActionSpec::DesktopNotificationShow { .. } => "notify-send",
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
        let descriptor = validate_action(action).map_err(|_| {
            (
                "unknown_action".to_owned(),
                "action is not registered".to_owned(),
            )
        })?;
        if !adapters
            .capability_available(descriptor.required_capability)
            .await
        {
            return Err((
                "capability_unavailable".to_owned(),
                "required adapter capability is unavailable".to_owned(),
            ));
        }
        if !adapters
            .capability_tool_available(descriptor.required_capability, required_tool(action))
            .await
        {
            return Err((
                "tool_unavailable".to_owned(),
                "required executable is unavailable".to_owned(),
            ));
        }
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
            ActionSpec::LinuxAppEnsureRunning { app_id } => {
                if adapters.desktop_entry_available(app_id).await != Some(true) {
                    return Err((
                        "desktop_entry_not_found".to_owned(),
                        "desktop entry was not found".to_owned(),
                    ));
                }
            }
            ActionSpec::DesktopNotificationShow { .. } => {}
        }
    }
    Ok(())
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
            ActionSpec::LinuxPathOpen { resource_id } => {
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
                let adapter_id = dispatched.adapter_id;
                let ActionResult {
                    status,
                    result_code,
                    redacted_message,
                } = dispatched.result;
                steps[index].adapter_id = Some(adapter_id);
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
                if matches!(error, AdapterDispatchError::Timeout) && deadline_bound {
                    execution.failure_code = Some("ritual_timeout".to_owned());
                    break;
                }
                steps[index].status = ExecutionStepStatus::Failed;
                steps[index].result_code = Some(dispatch_error_code(&error).to_owned());
                steps[index].redacted_message = Some(dispatch_error_message(&error).to_owned());
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
                execution.failure_code = Some(dispatch_error_code(&error).to_owned());
                break;
            }
        }
    }
    if execution.failure_code.as_deref() == Some("ritual_timeout") {
        execution.status = if successful == 0 {
            ExecutionStatus::Failed
        } else {
            ExecutionStatus::Partial
        };
        let finished = OffsetDateTime::now_utc();
        for step in &mut steps {
            if matches!(
                step.status,
                ExecutionStepStatus::Pending | ExecutionStepStatus::Running
            ) {
                step.status = ExecutionStepStatus::Aborted;
                step.finished_at = Some(finished);
                step.result_code = Some("ritual_timeout".to_owned());
                step.redacted_message = Some("ritual timed out".to_owned());
                let _ = storage.update_step(step.clone()).await;
            }
        }
    } else if execution.failure_code.is_none() {
        execution.status = ExecutionStatus::Succeeded;
    }
    execution.finished_at = Some(OffsetDateTime::now_utc());
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

fn redact_message(message: &str) -> String {
    message
        .chars()
        .filter(|character| !character.is_control())
        .take(160)
        .collect()
}

const fn dispatch_error_code(error: &AdapterDispatchError) -> &'static str {
    match error {
        AdapterDispatchError::Unavailable => "adapter_unavailable",
        AdapterDispatchError::CapabilityUnavailable => "capability_unavailable",
        AdapterDispatchError::Timeout => "action_timeout",
        AdapterDispatchError::Disconnected => "adapter_disconnected",
        AdapterDispatchError::Protocol => "protocol_error",
    }
}
const fn dispatch_error_message(error: &AdapterDispatchError) -> &'static str {
    match error {
        AdapterDispatchError::Unavailable => "no adapter is connected",
        AdapterDispatchError::CapabilityUnavailable => "required capability is unavailable",
        AdapterDispatchError::Timeout => "adapter action timed out",
        AdapterDispatchError::Disconnected => "adapter disconnected",
        AdapterDispatchError::Protocol => "adapter protocol error",
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
