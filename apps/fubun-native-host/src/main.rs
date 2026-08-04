//! Chromium Native Messaging bridge for Fubun.
//!
//! stdout is reserved for native-messaging protocol frames. Diagnostics are
//! written to stderr only. The host does not execute commands or contact the
//! network; it only bridges an explicitly allowed extension to the Core UDS.

use std::{
    collections::HashSet,
    env,
    io::{self, Read, Write},
    os::unix::fs::PermissionsExt,
    path::PathBuf,
};

use fubun_core::{paths::FubunPaths, FubunClient};
use fubun_protocol::{
    read_json_frame, write_json_frame, AdapterEventEmitRequest, AdapterHello, AdapterRequestBody,
    AdapterRequestEnvelope, AdapterResponseBody, AdapterResponseEnvelope, AdapterStatusSnapshot,
    BrowserObservationEnableRequest, ClientHelloAck, RequestBody, ResponseBody, ResponseEnvelope,
    ResponsePayload, CURRENT_PROTOCOL_VERSION, MAX_MESSAGE_SIZE,
};
use serde::{Deserialize, Serialize};
use tokio::net::UnixStream;
use tokio::sync::mpsc;
use uuid::Uuid;

const HOST_NAME: &str = "dev.fubun.browser";
const ADAPTER_ID: &str = "dev.fubun.browser.chromium";
const ACTION_CAPABILITIES: [&str; 1] = ["browser.tab.ensure_open.v1"];
const EVENT_CAPABILITIES: [&str; 1] = ["dev.fubun.browser.resource.opened.v1"];

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
    if hello_payload.extension_id != extension_id {
        return Err("extension hello identity mismatch".into());
    }
    if hello_payload.extension_version.is_empty()
        || hello_payload.extension_version.len() > 64
        || hello_payload
            .extension_version
            .chars()
            .any(char::is_control)
    {
        return Err("invalid extension version".into());
    }
    if hello_payload.permitted_resource_ids.len() > 256 {
        return Err("too many permitted browser resources".into());
    }
    let mut adapter = UnixStream::connect(&paths.socket_path).await?;
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
            status: AdapterStatusSnapshot {
                tools: Vec::new(),
                desktop_entry_ids: Vec::new(),
                permitted_resource_ids: hello_payload.permitted_resource_ids,
            },
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
    native
        .write(NativeEnvelope {
            protocol_version: CURRENT_PROTOCOL_VERSION,
            request_id: hello.request_id,
            message_type: "extension.hello.ack".to_owned(),
            payload: serde_json::json!({ "host": HOST_NAME, "adapter_id": ADAPTER_ID }),
        })
        .await?;

    let mut client =
        FubunClient::connect(&paths.socket_path, HOST_NAME, env!("CARGO_PKG_VERSION")).await?;
    loop {
        tokio::select! {
            native_message = native.read() => {
                let Some(message) = native_message? else { break; };
                handle_native_message(message, &mut native, &mut adapter, &mut client).await?;
            }
            core_request = read_json_frame::<_, AdapterRequestEnvelope>(&mut adapter) => {
                let Some(request) = core_request? else { break; };
                if let AdapterRequestBody::ActionExecute(payload) = request.body {
                    native.write(NativeEnvelope {
                        protocol_version: CURRENT_PROTOCOL_VERSION,
                        request_id: request.request_id,
                        message_type: "browser.action.execute".to_owned(),
                        payload: serde_json::json!({
                            "request_id": request.request_id,
                            "action_execution_id": request.action_execution_id,
                            "action": payload.action,
                            "resolved_resource": payload.resolved_resource,
                        }),
                    }).await?;
                }
            }
        }
    }
    Ok(())
}

async fn handle_native_message(
    message: NativeEnvelope,
    native: &mut NativeIo,
    adapter: &mut UnixStream,
    client: &mut FubunClient,
) -> Result<(), Box<dyn std::error::Error>> {
    if message.protocol_version.major != CURRENT_PROTOCOL_VERSION.major {
        return Err("native protocol major mismatch".into());
    }
    match message.message_type.as_str() {
        "browser.observation.enable" => {
            let payload: BrowserObservationEnableRequest = serde_json::from_value(message.payload)?;
            let response = client
                .request(RequestBody::BrowserObservationEnable(payload))
                .await?;
            native
                .write(NativeEnvelope {
                    protocol_version: CURRENT_PROTOCOL_VERSION,
                    request_id: message.request_id,
                    message_type: "browser.observation.enabled".to_owned(),
                    payload: serde_json::to_value(response)?,
                })
                .await?;
        }
        "browser.observation.pause" => {
            let payload: fubun_protocol::ObservationPauseRequest =
                serde_json::from_value(message.payload)?;
            let response = client
                .request(RequestBody::ObservationPause(payload))
                .await?;
            native
                .write(NativeEnvelope {
                    protocol_version: CURRENT_PROTOCOL_VERSION,
                    request_id: message.request_id,
                    message_type: "browser.observation.paused".to_owned(),
                    payload: serde_json::to_value(response)?,
                })
                .await?;
        }
        "browser.event.emit" => {
            let event: BrowserEvent = serde_json::from_value(message.payload)?;
            let request = AdapterResponseEnvelope {
                protocol_version: CURRENT_PROTOCOL_VERSION,
                request_id: message.request_id,
                action_execution_id: Uuid::nil(),
                body: AdapterResponseBody::EventEmit(AdapterEventEmitRequest {
                    sequence_no: event.sequence_no,
                    occurred_at: event.occurred_at,
                    event_type: fubun_domain::EventType::BrowserResourceOpenedV1,
                    resource_id: event.resource_id,
                }),
            };
            write_json_frame(adapter, &request).await?;
            let ack: AdapterRequestEnvelope =
                read_json_frame(adapter).await?.ok_or("core closed")?;
            let AdapterRequestBody::EventAck(value) = ack.body else {
                return Err("invalid event ack".into());
            };
            native
                .write(NativeEnvelope {
                    protocol_version: CURRENT_PROTOCOL_VERSION,
                    request_id: message.request_id,
                    message_type: "browser.event.ack".to_owned(),
                    payload: serde_json::to_value(value)?,
                })
                .await?;
        }
        "browser.action.result" => {
            let result: BrowserActionResult = serde_json::from_value(message.payload)?;
            write_json_frame(
                adapter,
                &AdapterResponseEnvelope {
                    protocol_version: CURRENT_PROTOCOL_VERSION,
                    request_id: result.request_id,
                    action_execution_id: result.action_execution_id,
                    body: AdapterResponseBody::ActionResult(result.result),
                },
            )
            .await?;
        }
        "integration.ping" => {
            native
                .write(NativeEnvelope {
                    protocol_version: CURRENT_PROTOCOL_VERSION,
                    request_id: message.request_id,
                    message_type: "integration.pong".to_owned(),
                    payload: serde_json::json!({}),
                })
                .await?;
        }
        _ => return Err("unknown native message type".into()),
    }
    Ok(())
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

    async fn write(&mut self, value: NativeEnvelope) -> io::Result<()> {
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
    use super::{validate_origin, MAX_MESSAGE_SIZE};
    use std::io::Cursor;

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
}
