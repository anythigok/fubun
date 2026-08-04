//! Chromium Native Messaging bridge for Fubun.
//!
//! stdout is reserved for native-messaging protocol frames. Diagnostics are
//! never written to stdout. The host does not execute commands or contact the
//! network; it only bridges an explicitly allowed extension to the Core UDS.

use std::{
    collections::{HashMap, HashSet},
    env,
    io::{self, Read, Write},
    os::unix::fs::PermissionsExt,
    path::PathBuf,
    sync::{Arc, Mutex as StdMutex},
    time::Duration,
};

use fubun_core::{paths::FubunPaths, FubunClient};
use fubun_protocol::{
    read_json_frame, write_json_frame, ActionExecuteRequest, AdapterEventAck,
    AdapterEventEmitRequest, AdapterHello, AdapterRequestBody, AdapterRequestEnvelope,
    AdapterResponseBody, AdapterResponseEnvelope, AdapterStatusSnapshot,
    BrowserObservationEnableRequest, ClientHelloAck, RequestBody, ResponseBody, ResponseEnvelope,
    ResponsePayload, CURRENT_PROTOCOL_VERSION, MAX_MESSAGE_SIZE,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::{
    net::{unix::OwnedReadHalf, UnixStream},
    sync::{mpsc, oneshot},
    task::JoinSet,
};
use uuid::Uuid;

const HOST_NAME: &str = "dev.fubun.browser";
const ADAPTER_ID: &str = "dev.fubun.browser.chromium";
const ACTION_CAPABILITIES: [&str; 1] = ["browser.tab.ensure_open.v1"];
const EVENT_CAPABILITIES: [&str; 1] = ["dev.fubun.browser.resource.opened.v1"];
const EVENT_ACK_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct NativeEnvelope {
    protocol_version: fubun_protocol::ProtocolVersion,
    request_id: Uuid,
    #[serde(rename = "type")]
    message_type: String,
    payload: serde_json::Value,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ExtensionHello {
    extension_id: String,
    extension_version: String,
    extension_instance_id: Uuid,
    permitted_resource_ids: Vec<Uuid>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct BrowserEvent {
    sequence_no: u64,
    occurred_at: time::OffsetDateTime,
    resource_id: Uuid,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct BrowserActionResult {
    request_id: Uuid,
    action_execution_id: Uuid,
    result: fubun_protocol::ActionResult,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AllowedOrigins {
    allowed_extension_ids: Vec<String>,
}

#[derive(Debug, Error)]
enum HostMessageError {
    #[error("native protocol mismatch")]
    Protocol,
    #[error("invalid native message payload")]
    InvalidPayload,
    #[error("core request failed")]
    Core,
    #[error("adapter connection is unavailable")]
    AdapterUnavailable,
    #[error("event acknowledgement timed out")]
    EventAckTimeout,
    #[error("event acknowledgement connection closed")]
    EventAckClosed,
    #[error("event acknowledgement protocol error")]
    EventAckProtocol,
    #[error("duplicate native event request")]
    DuplicateEventRequest,
}

#[derive(Debug, Clone, Copy)]
enum PendingAckError {
    Disconnected,
    Protocol,
}

type PendingEventAcks =
    Arc<StdMutex<HashMap<Uuid, oneshot::Sender<Result<AdapterEventAck, PendingAckError>>>>>;

struct PendingEventAckGuard {
    pending: PendingEventAcks,
    request_id: Uuid,
    armed: bool,
}

impl PendingEventAckGuard {
    fn new(pending: PendingEventAcks, request_id: Uuid) -> Self {
        Self {
            pending,
            request_id,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for PendingEventAckGuard {
    fn drop(&mut self) {
        if self.armed {
            self.pending
                .lock()
                .expect("pending event acknowledgement mutex poisoned")
                .remove(&self.request_id);
        }
    }
}

enum CoreInbound {
    Action {
        request_id: Uuid,
        action_execution_id: Uuid,
        payload: ActionExecuteRequest,
    },
    Status {
        request_id: Uuid,
        action_execution_id: Uuid,
    },
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let caller_origin = env::args().nth(1).ok_or("caller origin is required")?;
    let extension_id = validate_origin(&caller_origin)?;
    let allowed = load_allowed_ids()?;
    if !allowed.iter().any(|value| value == &extension_id) {
        return Err("extension origin is not registered".into());
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(run_host(extension_id))
}

fn validate_origin(origin: &str) -> Result<String, &'static str> {
    let prefix = "chrome-extension://";
    let value = origin.strip_prefix(prefix).ok_or("invalid caller scheme")?;
    let id = value
        .strip_suffix('/')
        .ok_or("caller origin must end with /")?;
    if id.len() != 32 || !id.bytes().all(|byte| (b'a'..=b'p').contains(&byte)) {
        return Err("invalid extension id");
    }
    Ok(id.to_owned())
}

fn load_allowed_ids() -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let config_home = env::var_os("XDG_CONFIG_HOME")
        .map(PathBuf::from)
        .or_else(|| env::var_os("HOME").map(|home| PathBuf::from(home).join(".config")))
        .ok_or("XDG_CONFIG_HOME or HOME is required")?;
    let path = config_home.join("fubun/browser-native-host.json");
    let parent_metadata =
        std::fs::metadata(path.parent().ok_or("invalid native host config path")?)?;
    if !parent_metadata.is_dir() || parent_metadata.permissions().mode() & 0o077 != 0 {
        return Err("native host config directory must be private".into());
    }
    let metadata = std::fs::metadata(&path)?;
    if !metadata.is_file() || metadata.permissions().mode() & 0o077 != 0 {
        return Err("native host config must not be group/world accessible".into());
    }
    let value: AllowedOrigins = serde_json::from_slice(&std::fs::read(path)?)?;
    let unique_ids = value.allowed_extension_ids.iter().collect::<HashSet<_>>();
    if value.allowed_extension_ids.is_empty()
        || value.allowed_extension_ids.len() > 32
        || unique_ids.len() != value.allowed_extension_ids.len()
        || value
            .allowed_extension_ids
            .iter()
            .any(|id| id.len() != 32 || !id.bytes().all(|byte| (b'a'..=b'p').contains(&byte)))
    {
        return Err("invalid allowed extension id configuration".into());
    }
    Ok(value.allowed_extension_ids)
}

async fn run_host(extension_id: String) -> Result<(), Box<dyn std::error::Error>> {
    let paths = FubunPaths::discover()?;
    let mut native = NativeIo::new();
    let Some(hello) = native.read().await? else {
        return Ok(());
    };
    if hello.protocol_version.major != CURRENT_PROTOCOL_VERSION.major
        || hello.message_type != "extension.hello"
    {
        return Err("extension hello is required".into());
    }
    let hello_payload: ExtensionHello = serde_json::from_value(hello.payload)?;
    validate_extension_hello(&hello_payload, &extension_id)?;

    let (native_writer, native_writer_receiver) = mpsc::channel(32);
    let native_writer_task = tokio::spawn(native_writer_task(native_writer_receiver));
    let mut adapter = UnixStream::connect(&paths.socket_path).await?;
    let adapter_status = AdapterStatusSnapshot {
        tools: Vec::new(),
        desktop_entry_ids: Vec::new(),
        permitted_resource_ids: hello_payload.permitted_resource_ids.clone(),
    };
    let adapter_hello = fubun_protocol::RequestEnvelope {
        protocol_version: CURRENT_PROTOCOL_VERSION,
        request_id: Uuid::new_v4(),
        body: RequestBody::AdapterHello(AdapterHello {
            adapter_id: ADAPTER_ID.to_owned(),
            adapter_version: env!("CARGO_PKG_VERSION").to_owned(),
            instance_id: hello_payload.extension_instance_id,
            action_capabilities: ACTION_CAPABILITIES
                .iter()
                .map(|value| (*value).to_owned())
                .collect(),
            event_capabilities: EVENT_CAPABILITIES
                .iter()
                .map(|value| (*value).to_owned())
                .collect(),
            status: adapter_status.clone(),
        }),
    };
    let hello_id = adapter_hello.request_id;
    write_json_frame(&mut adapter, &adapter_hello).await?;
    let ack: ResponseEnvelope = read_json_frame(&mut adapter).await?.ok_or("core closed")?;
    if ack.request_id != hello_id
        || !matches!(
            ack.body,
            ResponseBody::Ok(ResponsePayload::AdapterHelloAck(ClientHelloAck { .. }))
        )
    {
        return Err("core rejected adapter hello".into());
    }
    native_writer
        .send(NativeEnvelope {
            protocol_version: CURRENT_PROTOCOL_VERSION,
            request_id: hello.request_id,
            message_type: "extension.hello.ack".to_owned(),
            payload: serde_json::json!({ "host": HOST_NAME, "adapter_id": ADAPTER_ID }),
        })
        .await
        .map_err(|_| "native messaging output closed")?;

    let (adapter_reader, adapter_writer) = adapter.into_split();
    let (adapter_writer_sender, adapter_writer_receiver) = mpsc::channel(32);
    let adapter_writer_task =
        tokio::spawn(adapter_writer_task(adapter_writer, adapter_writer_receiver));
    let pending = Arc::new(StdMutex::new(HashMap::new()));
    let (core_sender, mut core_receiver) = mpsc::channel(32);
    let adapter_reader_task = tokio::spawn(adapter_reader_task(
        adapter_reader,
        pending.clone(),
        core_sender,
    ));
    let mut client =
        FubunClient::connect(&paths.socket_path, HOST_NAME, env!("CARGO_PKG_VERSION")).await?;
    let mut event_tasks = JoinSet::new();

    loop {
        tokio::select! {
            native_message = native.read() => {
                let Some(message) = native_message? else { break; };
                let request_id = message.request_id;
                if handle_native_message(
                    message,
                    &native_writer,
                    &adapter_writer_sender,
                    pending.clone(),
                    &mut client,
                    &mut event_tasks,
                ).await.is_err() {
                    let _ = native_writer.send(integration_error(request_id)).await;
                }
            }
            core_message = core_receiver.recv() => {
                let Some(core_message) = core_message else { break; };
                match core_message {
                    CoreInbound::Action { request_id, action_execution_id, payload } => {
                        let _ = native_writer.send(NativeEnvelope {
                            protocol_version: CURRENT_PROTOCOL_VERSION,
                            request_id,
                            message_type: "browser.action.execute".to_owned(),
                            payload: serde_json::json!({
                                "request_id": request_id,
                                "action_execution_id": action_execution_id,
                                "action": payload.action,
                                "resolved_resource": payload.resolved_resource,
                            }),
                        }).await;
                    }
                    CoreInbound::Status { request_id, action_execution_id } => {
                        let _ = adapter_writer_sender.send(AdapterResponseEnvelope {
                            protocol_version: CURRENT_PROTOCOL_VERSION,
                            request_id,
                            action_execution_id,
                            body: AdapterResponseBody::Status(adapter_status.clone()),
                        }).await;
                    }
                }
            }
            completed = event_tasks.join_next(), if !event_tasks.is_empty() => {
                let _ = completed;
            }
        }
    }

    event_tasks.abort_all();
    drain_pending_event_acks(&pending, PendingAckError::Disconnected);
    drop(adapter_writer_sender);
    adapter_reader_task.abort();
    adapter_writer_task.abort();
    native_writer_task.abort();
    Ok(())
}

fn validate_extension_hello(
    hello: &ExtensionHello,
    expected_extension_id: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    if hello.extension_id != expected_extension_id {
        return Err("extension hello identity mismatch".into());
    }
    if hello.extension_version.is_empty()
        || hello.extension_version.len() > 64
        || hello.extension_version.chars().any(char::is_control)
    {
        return Err("invalid extension version".into());
    }
    let unique = hello.permitted_resource_ids.iter().collect::<HashSet<_>>();
    if hello.permitted_resource_ids.len() > 256
        || unique.len() != hello.permitted_resource_ids.len()
    {
        return Err("invalid permitted browser resources".into());
    }
    Ok(())
}

async fn handle_native_message(
    message: NativeEnvelope,
    native_writer: &mpsc::Sender<NativeEnvelope>,
    adapter_writer: &mpsc::Sender<AdapterResponseEnvelope>,
    pending: PendingEventAcks,
    client: &mut FubunClient,
    event_tasks: &mut JoinSet<()>,
) -> Result<(), HostMessageError> {
    if message.protocol_version.major != CURRENT_PROTOCOL_VERSION.major {
        return Err(HostMessageError::Protocol);
    }
    match message.message_type.as_str() {
        "browser.observation.enable" => {
            let payload: BrowserObservationEnableRequest = serde_json::from_value(message.payload)
                .map_err(|_| HostMessageError::InvalidPayload)?;
            let response = client
                .request(RequestBody::BrowserObservationEnable(payload))
                .await
                .map_err(|_| HostMessageError::Core)?;
            let ResponsePayload::BrowserObservationEnabled(value) = response else {
                return Err(HostMessageError::Core);
            };
            native_writer
                .send(NativeEnvelope {
                    protocol_version: CURRENT_PROTOCOL_VERSION,
                    request_id: message.request_id,
                    message_type: "browser.observation.enabled".to_owned(),
                    payload: serde_json::to_value(value).map_err(|_| HostMessageError::Core)?,
                })
                .await
                .map_err(|_| HostMessageError::AdapterUnavailable)?;
        }
        "browser.observation.pause" => {
            let payload: fubun_protocol::ObservationPauseRequest =
                serde_json::from_value(message.payload)
                    .map_err(|_| HostMessageError::InvalidPayload)?;
            let response = client
                .request(RequestBody::ObservationPause(payload))
                .await
                .map_err(|_| HostMessageError::Core)?;
            let ResponsePayload::ObservationPaused(value) = response else {
                return Err(HostMessageError::Core);
            };
            native_writer
                .send(NativeEnvelope {
                    protocol_version: CURRENT_PROTOCOL_VERSION,
                    request_id: message.request_id,
                    message_type: "browser.observation.paused".to_owned(),
                    payload: serde_json::to_value(value).map_err(|_| HostMessageError::Core)?,
                })
                .await
                .map_err(|_| HostMessageError::AdapterUnavailable)?;
        }
        "browser.event.emit" => {
            let event: BrowserEvent = serde_json::from_value(message.payload)
                .map_err(|_| HostMessageError::InvalidPayload)?;
            let request_id = message.request_id;
            let event_sender = adapter_writer.clone();
            let event_pending = pending.clone();
            let event_native_writer = native_writer.clone();
            event_tasks.spawn(async move {
                let response = match emit_event_and_wait(
                    request_id,
                    event,
                    event_sender,
                    event_pending,
                    EVENT_ACK_TIMEOUT,
                )
                .await
                {
                    Ok(value) => NativeEnvelope {
                        protocol_version: CURRENT_PROTOCOL_VERSION,
                        request_id,
                        message_type: "browser.event.ack".to_owned(),
                        payload: serde_json::to_value(value)
                            .unwrap_or_else(|_| serde_json::json!({})),
                    },
                    Err(_) => integration_error(request_id),
                };
                let _ = event_native_writer.send(response).await;
            });
        }
        "browser.action.result" => {
            let result: BrowserActionResult = serde_json::from_value(message.payload)
                .map_err(|_| HostMessageError::InvalidPayload)?;
            adapter_writer
                .send(AdapterResponseEnvelope {
                    protocol_version: CURRENT_PROTOCOL_VERSION,
                    request_id: result.request_id,
                    action_execution_id: result.action_execution_id,
                    body: AdapterResponseBody::ActionResult(result.result),
                })
                .await
                .map_err(|_| HostMessageError::AdapterUnavailable)?;
        }
        "integration.ping" => {
            native_writer
                .send(NativeEnvelope {
                    protocol_version: CURRENT_PROTOCOL_VERSION,
                    request_id: message.request_id,
                    message_type: "integration.pong".to_owned(),
                    payload: serde_json::json!({}),
                })
                .await
                .map_err(|_| HostMessageError::AdapterUnavailable)?;
        }
        _ => return Err(HostMessageError::InvalidPayload),
    }
    Ok(())
}

async fn emit_event_and_wait(
    request_id: Uuid,
    event: BrowserEvent,
    adapter_writer: mpsc::Sender<AdapterResponseEnvelope>,
    pending: PendingEventAcks,
    timeout: Duration,
) -> Result<AdapterEventAck, HostMessageError> {
    let (sender, receiver) = oneshot::channel();
    {
        let mut entries = pending
            .lock()
            .expect("pending event acknowledgement mutex poisoned");
        if entries.contains_key(&request_id) {
            return Err(HostMessageError::DuplicateEventRequest);
        }
        entries.insert(request_id, sender);
    }
    let mut guard = PendingEventAckGuard::new(pending, request_id);
    adapter_writer
        .send(AdapterResponseEnvelope {
            protocol_version: CURRENT_PROTOCOL_VERSION,
            request_id,
            action_execution_id: Uuid::nil(),
            body: AdapterResponseBody::EventEmit(AdapterEventEmitRequest {
                sequence_no: event.sequence_no,
                occurred_at: event.occurred_at,
                event_type: fubun_domain::EventType::BrowserResourceOpenedV1,
                resource_id: event.resource_id,
            }),
        })
        .await
        .map_err(|_| HostMessageError::AdapterUnavailable)?;
    let result = tokio::time::timeout(timeout, receiver)
        .await
        .map_err(|_| HostMessageError::EventAckTimeout)?
        .map_err(|_| HostMessageError::EventAckClosed)?;
    guard.disarm();
    result.map_err(|error| match error {
        PendingAckError::Disconnected => HostMessageError::EventAckClosed,
        PendingAckError::Protocol => HostMessageError::EventAckProtocol,
    })
}

async fn adapter_reader_task(
    mut reader: OwnedReadHalf,
    pending: PendingEventAcks,
    core_sender: mpsc::Sender<CoreInbound>,
) {
    loop {
        let request = match read_json_frame::<_, AdapterRequestEnvelope>(&mut reader).await {
            Ok(Some(request))
                if request.protocol_version.major == CURRENT_PROTOCOL_VERSION.major =>
            {
                request
            }
            Ok(Some(_)) | Ok(None) | Err(_) => break,
        };
        if !route_adapter_request(request, &pending, &core_sender).await {
            break;
        }
    }
    drain_pending_event_acks(&pending, PendingAckError::Disconnected);
}

async fn route_adapter_request(
    request: AdapterRequestEnvelope,
    pending: &PendingEventAcks,
    core_sender: &mpsc::Sender<CoreInbound>,
) -> bool {
    match request.body {
        AdapterRequestBody::EventAck(value) => {
            let result = if request.action_execution_id == Uuid::nil() {
                Ok(value)
            } else {
                Err(PendingAckError::Protocol)
            };
            if let Some(sender) = pending
                .lock()
                .expect("pending event acknowledgement mutex poisoned")
                .remove(&request.request_id)
            {
                let _ = sender.send(result);
            }
            true
        }
        AdapterRequestBody::ActionExecute(payload) => core_sender
            .send(CoreInbound::Action {
                request_id: request.request_id,
                action_execution_id: request.action_execution_id,
                payload,
            })
            .await
            .is_ok(),
        AdapterRequestBody::Status(_) => core_sender
            .send(CoreInbound::Status {
                request_id: request.request_id,
                action_execution_id: request.action_execution_id,
            })
            .await
            .is_ok(),
    }
}

fn drain_pending_event_acks(pending: &PendingEventAcks, error: PendingAckError) {
    let entries = pending
        .lock()
        .expect("pending event acknowledgement mutex poisoned")
        .drain()
        .collect::<Vec<_>>();
    for (_, sender) in entries {
        let _ = sender.send(Err(error));
    }
}

async fn adapter_writer_task(
    mut writer: tokio::net::unix::OwnedWriteHalf,
    mut receiver: mpsc::Receiver<AdapterResponseEnvelope>,
) {
    while let Some(response) = receiver.recv().await {
        if write_json_frame(&mut writer, &response).await.is_err() {
            break;
        }
    }
}

async fn native_writer_task(mut receiver: mpsc::Receiver<NativeEnvelope>) {
    while let Some(value) = receiver.recv().await {
        if write_native_envelope(value).await.is_err() {
            break;
        }
    }
}

fn integration_error(request_id: Uuid) -> NativeEnvelope {
    NativeEnvelope {
        protocol_version: CURRENT_PROTOCOL_VERSION,
        request_id,
        message_type: "integration.error".to_owned(),
        payload: serde_json::json!({ "code": "request_failed" }),
    }
}

struct NativeIo {
    receiver: mpsc::Receiver<io::Result<Option<NativeEnvelope>>>,
}

impl NativeIo {
    fn new() -> Self {
        let (sender, receiver) = mpsc::channel(8);
        std::thread::spawn(move || loop {
            let result = read_native_frame();
            let done = matches!(result, Ok(None) | Err(_));
            if sender.blocking_send(result).is_err() || done {
                break;
            }
        });
        Self { receiver }
    }

    async fn read(&mut self) -> io::Result<Option<NativeEnvelope>> {
        self.receiver.recv().await.unwrap_or(Ok(None))
    }
}

async fn write_native_envelope(value: NativeEnvelope) -> io::Result<()> {
    let bytes = serde_json::to_vec(&value).map_err(io::Error::other)?;
    if bytes.len() > MAX_MESSAGE_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "native message oversized",
        ));
    }
    tokio::task::spawn_blocking(move || write_native_frame(&bytes))
        .await
        .map_err(io::Error::other)?
}

fn read_native_frame() -> io::Result<Option<NativeEnvelope>> {
    let mut header = [0_u8; 4];
    let mut stdin = io::stdin().lock();
    match stdin.read_exact(&mut header[..1]) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(error) => return Err(error),
    }
    stdin.read_exact(&mut header[1..])?;
    let length = u32::from_ne_bytes(header) as usize;
    if length == 0 || length > MAX_MESSAGE_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid native frame length",
        ));
    }
    let mut payload = vec![0_u8; length];
    stdin.read_exact(&mut payload)?;
    serde_json::from_slice(&payload)
        .map(Some)
        .map_err(io::Error::other)
}

fn write_native_frame(payload: &[u8]) -> io::Result<()> {
    let mut stdout = io::stdout().lock();
    stdout.write_all(&(payload.len() as u32).to_ne_bytes())?;
    stdout.write_all(payload)?;
    stdout.flush()
}

#[cfg(test)]
mod tests {
    use super::{
        adapter_writer_task, drain_pending_event_acks, emit_event_and_wait, route_adapter_request,
        validate_origin, AdapterEventAck, AdapterRequestBody, AdapterRequestEnvelope, BrowserEvent,
        CoreInbound, PendingAckError, PendingEventAcks, CURRENT_PROTOCOL_VERSION, MAX_MESSAGE_SIZE,
    };
    use fubun_protocol::{
        read_json_frame, ActionResult, AdapterActionStatus, AdapterEventEmitRequest,
        AdapterResponseBody, AdapterResponseEnvelope,
    };
    use std::{
        collections::HashMap,
        io::Cursor,
        sync::{Arc, Mutex},
        time::Duration,
    };
    use tokio::sync::{mpsc, oneshot};
    use uuid::Uuid;

    fn pending() -> PendingEventAcks {
        Arc::new(Mutex::new(HashMap::new()))
    }

    fn event_ack(request_id: Uuid) -> AdapterRequestEnvelope {
        AdapterRequestEnvelope {
            protocol_version: CURRENT_PROTOCOL_VERSION,
            request_id,
            action_execution_id: Uuid::nil(),
            body: AdapterRequestBody::EventAck(AdapterEventAck {
                event_id: Uuid::new_v4(),
                stored: true,
                duplicate: false,
            }),
        }
    }

    #[test]
    fn validates_exact_chrome_origin() {
        let id = "a".repeat(32);
        assert_eq!(
            validate_origin(&format!("chrome-extension://{id}/")).unwrap(),
            id
        );
        assert!(validate_origin(&format!("chrome-extension://{}/x", "a".repeat(32))).is_err());
        assert!(validate_origin("http://abcdefghijklmnopabcdefghijklmnop/").is_err());
    }

    #[test]
    fn native_frame_length_is_utf8_bytes_and_bounded() {
        let payload = "é".repeat(8);
        let bytes = payload.as_bytes();
        assert_eq!(
            u32::from_ne_bytes((bytes.len() as u32).to_ne_bytes()) as usize,
            bytes.len()
        );
        assert!(MAX_MESSAGE_SIZE > bytes.len());
        let mut partial = Cursor::new(vec![1_u8, 0, 0]);
        let mut header = [0_u8; 4];
        assert!(std::io::Read::read_exact(&mut partial, &mut header).is_err());
    }

    #[tokio::test]
    async fn routes_action_before_event_ack_without_misdelivery() {
        let pending = pending();
        let event_request_id = Uuid::new_v4();
        let (event_sender, event_receiver) = oneshot::channel();
        pending
            .lock()
            .expect("pending")
            .insert(event_request_id, event_sender);
        let (core_sender, mut core_receiver) = mpsc::channel(2);
        let action_request_id = Uuid::new_v4();
        let action = AdapterRequestEnvelope {
            protocol_version: CURRENT_PROTOCOL_VERSION,
            request_id: action_request_id,
            action_execution_id: Uuid::new_v4(),
            body: AdapterRequestBody::ActionExecute(fubun_protocol::ActionExecuteRequest {
                action: fubun_domain::ActionSpec::BrowserTabEnsureOpen {
                    resource_id: Uuid::new_v4(),
                },
                resolved_resource: None,
            }),
        };
        assert!(route_adapter_request(action, &pending, &core_sender).await);
        let Some(CoreInbound::Action { request_id, .. }) = core_receiver.recv().await else {
            panic!("action must be routed");
        };
        assert_eq!(request_id, action_request_id);
        assert!(route_adapter_request(event_ack(event_request_id), &pending, &core_sender).await);
        assert!(event_receiver.await.expect("ack receiver").is_ok());
        assert!(pending.lock().expect("pending").is_empty());
    }

    #[tokio::test]
    async fn routes_event_ack_before_action_without_misdelivery() {
        let pending = pending();
        let event_request_id = Uuid::new_v4();
        let (event_sender, event_receiver) = oneshot::channel();
        pending
            .lock()
            .expect("pending")
            .insert(event_request_id, event_sender);
        let (core_sender, mut core_receiver) = mpsc::channel(2);
        assert!(route_adapter_request(event_ack(event_request_id), &pending, &core_sender).await);
        assert!(event_receiver.await.expect("ack receiver").is_ok());
        let action_request_id = Uuid::new_v4();
        let action = AdapterRequestEnvelope {
            protocol_version: CURRENT_PROTOCOL_VERSION,
            request_id: action_request_id,
            action_execution_id: Uuid::new_v4(),
            body: AdapterRequestBody::ActionExecute(fubun_protocol::ActionExecuteRequest {
                action: fubun_domain::ActionSpec::BrowserTabEnsureOpen {
                    resource_id: Uuid::new_v4(),
                },
                resolved_resource: None,
            }),
        };
        assert!(route_adapter_request(action, &pending, &core_sender).await);
        let Some(CoreInbound::Action { request_id, .. }) = core_receiver.recv().await else {
            panic!("action must be routed");
        };
        assert_eq!(request_id, action_request_id);
    }

    #[tokio::test]
    async fn event_ack_timeout_and_disconnect_release_pending() {
        let pending = pending();
        let (adapter_sender, mut adapter_receiver) = mpsc::channel(1);
        let request_id = Uuid::new_v4();
        let task = tokio::spawn(emit_event_and_wait(
            request_id,
            BrowserEvent {
                sequence_no: 1,
                occurred_at: time::OffsetDateTime::now_utc(),
                resource_id: Uuid::new_v4(),
            },
            adapter_sender,
            pending.clone(),
            Duration::from_millis(1),
        ));
        let _ = adapter_receiver.recv().await.expect("event emit");
        assert!(task.await.expect("task").is_err());
        assert!(pending.lock().expect("pending").is_empty());

        let (sender, receiver) = oneshot::channel();
        pending
            .lock()
            .expect("pending")
            .insert(Uuid::new_v4(), sender);
        drain_pending_event_acks(&pending, PendingAckError::Disconnected);
        assert!(matches!(
            receiver.await,
            Ok(Err(PendingAckError::Disconnected))
        ));
        assert!(pending.lock().expect("pending").is_empty());
    }

    #[tokio::test]
    async fn unknown_and_duplicate_event_ack_are_ignored_without_stopping_the_router() {
        let pending = pending();
        let (core_sender, _core_receiver) = mpsc::channel(1);
        assert!(route_adapter_request(event_ack(Uuid::new_v4()), &pending, &core_sender).await);
        let request_id = Uuid::new_v4();
        let (sender, receiver) = oneshot::channel();
        pending.lock().expect("pending").insert(request_id, sender);
        assert!(route_adapter_request(event_ack(request_id), &pending, &core_sender).await);
        assert!(receiver.await.expect("ack receiver").is_ok());
        assert!(route_adapter_request(event_ack(request_id), &pending, &core_sender).await);
        assert!(pending.lock().expect("pending").is_empty());
    }

    #[tokio::test]
    async fn adapter_writer_queue_serializes_event_and_action_result_frames() {
        let (writer_stream, mut reader_stream) = tokio::net::UnixStream::pair().expect("pair");
        let (_, writer) = writer_stream.into_split();
        let (sender, receiver) = mpsc::channel(2);
        let task = tokio::spawn(adapter_writer_task(writer, receiver));
        let event_id = Uuid::new_v4();
        sender
            .send(AdapterResponseEnvelope {
                protocol_version: CURRENT_PROTOCOL_VERSION,
                request_id: event_id,
                action_execution_id: Uuid::nil(),
                body: AdapterResponseBody::EventEmit(AdapterEventEmitRequest {
                    sequence_no: 1,
                    occurred_at: time::OffsetDateTime::now_utc(),
                    event_type: fubun_domain::EventType::BrowserResourceOpenedV1,
                    resource_id: Uuid::new_v4(),
                }),
            })
            .await
            .expect("event queued");
        let result_id = Uuid::new_v4();
        sender
            .send(AdapterResponseEnvelope {
                protocol_version: CURRENT_PROTOCOL_VERSION,
                request_id: result_id,
                action_execution_id: Uuid::new_v4(),
                body: AdapterResponseBody::ActionResult(ActionResult {
                    status: AdapterActionStatus::Succeeded,
                    result_code: "ok".to_owned(),
                    redacted_message: "ok".to_owned(),
                }),
            })
            .await
            .expect("result queued");
        let first: AdapterResponseEnvelope = read_json_frame(&mut reader_stream)
            .await
            .expect("first frame")
            .expect("first value");
        let second: AdapterResponseEnvelope = read_json_frame(&mut reader_stream)
            .await
            .expect("second frame")
            .expect("second value");
        assert_eq!(first.request_id, event_id);
        assert_eq!(second.request_id, result_id);
        drop(sender);
        task.await.expect("writer task");
    }
}
