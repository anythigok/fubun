//! Local-only Fubun daemon and client.

pub mod paths;

use std::{
    fs,
    os::unix::fs::{FileTypeExt, PermissionsExt},
    path::{Path, PathBuf},
};

use fubun_protocol::{
    read_json_frame, write_json_frame, ClientHello, ClientHelloAck, DoctorReport, EmptyRequest,
    EventIngested, EventList, FrameError, RequestBody, RequestEnvelope, ResponseBody,
    ResponseEnvelope, ResponsePayload, StatusReport, CURRENT_PROTOCOL_VERSION,
};
use fubun_storage::{Storage, StorageError, StorageHandle};
use thiserror::Error;
use time::OffsetDateTime;
use tokio::{
    net::{UnixListener, UnixStream},
    sync::watch,
    task::{JoinHandle, JoinSet},
};
use tracing::{debug, warn};
use uuid::Uuid;

use crate::paths::FubunPaths;

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
    let mut connections = JoinSet::new();

    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
            }
            accepted = listener.accept() => {
                let (stream, _) = accepted?;
                let handle = storage_handle.clone();
                let connection_shutdown = shutdown.clone();
                connections.spawn(async move {
                    if let Err(error) = handle_connection(stream, handle, connection_shutdown).await {
                        debug!(%error, "closing invalid or failed IPC connection");
                    }
                });
            }
            completed = connections.join_next(), if !connections.is_empty() => {
                if let Some(Err(error)) = completed {
                    warn!(%error, "IPC connection task panicked");
                }
            }
        }
    }

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

    let RequestBody::ClientHello(hello) = first.body else {
        write_json_frame(
            &mut stream,
            &ResponseEnvelope::error(
                first.request_id,
                "handshake_required",
                "client.hello must be the first request",
            ),
        )
        .await?;
        return Ok(());
    };

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
        let response = process_request(request, &storage).await;
        write_json_frame(&mut stream, &response).await?;
    }
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

async fn process_request(request: RequestEnvelope, storage: &StorageHandle) -> ResponseEnvelope {
    if request.protocol_version.major != CURRENT_PROTOCOL_VERSION.major {
        return ResponseEnvelope::error(
            request.request_id,
            "protocol_major_mismatch",
            "client and daemon protocol major versions differ",
        );
    }

    match request.body {
        RequestBody::ClientHello(_) => ResponseEnvelope::error(
            request.request_id,
            "already_initialized",
            "client.hello is only valid as the first request",
        ),
        RequestBody::EventIngest(payload) => {
            let mut event = payload.event;
            event.received_at = OffsetDateTime::now_utc();
            if let Err(error) = event.validate() {
                return ResponseEnvelope::error(
                    request.request_id,
                    "invalid_event",
                    error.to_string(),
                );
            }
            let event_id = event.id;
            match storage.insert_event(event).await {
                Ok(()) => ResponseEnvelope::ok(
                    request.request_id,
                    ResponsePayload::EventIngested(EventIngested { event_id }),
                ),
                Err(StorageError::DuplicateEvent) => ResponseEnvelope::error(
                    request.request_id,
                    "duplicate_event",
                    "adapter_instance_id and sequence_no have already been stored",
                ),
                Err(error) => {
                    warn!(%error, "event storage failed");
                    ResponseEnvelope::error(
                        request.request_id,
                        "storage_error",
                        "event could not be stored",
                    )
                }
            }
        }
        RequestBody::EventsList(payload) => {
            match storage.list_events(payload.since, payload.limit).await {
                Ok(events) => ResponseEnvelope::ok(
                    request.request_id,
                    ResponsePayload::EventList(EventList { events }),
                ),
                Err(error) => {
                    warn!(%error, "event listing failed");
                    ResponseEnvelope::error(
                        request.request_id,
                        "storage_error",
                        "events could not be listed",
                    )
                }
            }
        }
        RequestBody::SystemStatus(_) => status_response(request.request_id, storage).await,
        RequestBody::SystemDoctor(_) => doctor_response(request.request_id, storage).await,
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
        Err(error) => {
            warn!(%error, "status database check failed");
            ResponseEnvelope::error(request_id, "storage_error", "database check failed")
        }
    }
}

async fn doctor_response(request_id: Uuid, storage: &StorageHandle) -> ResponseEnvelope {
    match storage.schema_version().await {
        Ok(schema_version) => ResponseEnvelope::ok(
            request_id,
            ResponsePayload::Doctor(DoctorReport {
                database_path: storage.database_path().display().to_string(),
                database_status: "ok".to_owned(),
                schema_version,
            }),
        ),
        Err(error) => {
            warn!(%error, "doctor database check failed");
            ResponseEnvelope::error(request_id, "storage_error", "database check failed")
        }
    }
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
