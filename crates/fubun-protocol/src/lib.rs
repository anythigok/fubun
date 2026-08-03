//! Versioned IPC envelopes and bounded little-endian framing.

use std::io;

use fubun_domain::Event;
use schemars::JsonSchema;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use thiserror::Error;
use time::OffsetDateTime;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use uuid::Uuid;

pub const MAX_MESSAGE_SIZE: usize = 256 * 1024;
pub const CURRENT_PROTOCOL_VERSION: ProtocolVersion = ProtocolVersion { major: 1, minor: 0 };

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ProtocolVersion {
    pub major: u16,
    pub minor: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ClientHello {
    pub client_name: String,
    pub client_version: String,
    pub protocol_version: ProtocolVersion,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ClientHelloAck {
    pub server_name: String,
    pub server_version: String,
    pub protocol_version: ProtocolVersion,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct EventIngestRequest {
    pub event: Event,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct EventsListRequest {
    #[serde(default, with = "time::serde::rfc3339::option")]
    #[schemars(with = "Option<String>")]
    pub since: Option<OffsetDateTime>,
    #[serde(default = "default_event_limit")]
    pub limit: u32,
}

const fn default_event_limit() -> u32 {
    100
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(deny_unknown_fields)]
pub struct EmptyRequest {}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "method", content = "params")]
pub enum RequestBody {
    #[serde(rename = "client.hello")]
    ClientHello(ClientHello),
    #[serde(rename = "event.ingest")]
    EventIngest(EventIngestRequest),
    #[serde(rename = "events.list")]
    EventsList(EventsListRequest),
    #[serde(rename = "system.status")]
    SystemStatus(EmptyRequest),
    #[serde(rename = "system.doctor")]
    SystemDoctor(EmptyRequest),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RequestEnvelope {
    pub protocol_version: ProtocolVersion,
    pub request_id: Uuid,
    pub body: RequestBody,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct EventIngested {
    pub event_id: Uuid,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct EventList {
    pub events: Vec<Event>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct StatusReport {
    pub daemon: String,
    pub protocol_version: ProtocolVersion,
    pub database_status: String,
    pub schema_version: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DoctorReport {
    pub database_path: String,
    pub database_status: String,
    pub schema_version: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", content = "data")]
pub enum ResponsePayload {
    #[serde(rename = "client.hello_ack")]
    ClientHelloAck(ClientHelloAck),
    #[serde(rename = "event.ingested")]
    EventIngested(EventIngested),
    #[serde(rename = "events.list")]
    EventList(EventList),
    #[serde(rename = "system.status")]
    Status(StatusReport),
    #[serde(rename = "system.doctor")]
    Doctor(DoctorReport),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ErrorBody {
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "status", content = "payload", rename_all = "snake_case")]
pub enum ResponseBody {
    Ok(ResponsePayload),
    Error(ErrorBody),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ResponseEnvelope {
    pub protocol_version: ProtocolVersion,
    pub request_id: Uuid,
    pub body: ResponseBody,
}

impl ResponseEnvelope {
    pub const fn ok(request_id: Uuid, payload: ResponsePayload) -> Self {
        Self {
            protocol_version: CURRENT_PROTOCOL_VERSION,
            request_id,
            body: ResponseBody::Ok(payload),
        }
    }

    pub fn error(request_id: Uuid, code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            protocol_version: CURRENT_PROTOCOL_VERSION,
            request_id,
            body: ResponseBody::Error(ErrorBody {
                code: code.into(),
                message: message.into(),
            }),
        }
    }
}

#[derive(Debug, Error)]
pub enum FrameError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    #[error("message length {actual} exceeds maximum {maximum}")]
    Oversized { actual: usize, maximum: usize },
    #[error("invalid UTF-8 JSON: {0}")]
    Json(#[from] serde_json::Error),
}

pub async fn read_json_frame<R, T>(reader: &mut R) -> Result<Option<T>, FrameError>
where
    R: AsyncRead + Unpin,
    T: DeserializeOwned,
{
    let Some(bytes) = read_frame(reader).await? else {
        return Ok(None);
    };
    Ok(Some(serde_json::from_slice(&bytes)?))
}

pub async fn write_json_frame<W, T>(writer: &mut W, value: &T) -> Result<(), FrameError>
where
    W: AsyncWrite + Unpin,
    T: Serialize,
{
    let frame = encode_json_frame(value)?;
    writer.write_all(&frame).await?;
    writer.flush().await?;
    Ok(())
}

pub fn encode_json_frame<T: Serialize>(value: &T) -> Result<Vec<u8>, FrameError> {
    let payload = serde_json::to_vec(value)?;
    if payload.len() > MAX_MESSAGE_SIZE {
        return Err(FrameError::Oversized {
            actual: payload.len(),
            maximum: MAX_MESSAGE_SIZE,
        });
    }
    let length = u32::try_from(payload.len()).expect("maximum message size fits in u32");
    let mut frame = Vec::with_capacity(payload.len() + 4);
    frame.extend_from_slice(&length.to_le_bytes());
    frame.extend_from_slice(&payload);
    Ok(frame)
}

async fn read_frame<R: AsyncRead + Unpin>(reader: &mut R) -> Result<Option<Vec<u8>>, FrameError> {
    let mut header = [0_u8; 4];
    let read = reader.read(&mut header[..1]).await?;
    if read == 0 {
        return Ok(None);
    }
    reader.read_exact(&mut header[1..]).await?;
    let length = u32::from_le_bytes(header) as usize;
    if length > MAX_MESSAGE_SIZE {
        return Err(FrameError::Oversized {
            actual: length,
            maximum: MAX_MESSAGE_SIZE,
        });
    }
    let mut payload = vec![0_u8; length];
    reader.read_exact(&mut payload).await?;
    Ok(Some(payload))
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn frame_prefix_matches_json_length(client_name in "[a-zA-Z0-9_-]{1,64}") {
            let request = RequestEnvelope {
                protocol_version: CURRENT_PROTOCOL_VERSION,
                request_id: Uuid::nil(),
                body: RequestBody::ClientHello(ClientHello {
                    client_name,
                    client_version: "0.1.0".to_owned(),
                    protocol_version: CURRENT_PROTOCOL_VERSION,
                }),
            };
            let frame = encode_json_frame(&request).expect("request fits");
            let declared = u32::from_le_bytes(frame[..4].try_into().expect("four bytes")) as usize;
            prop_assert_eq!(declared, frame.len() - 4);
        }
    }

    #[test]
    fn unknown_envelope_field_is_rejected() {
        let json = r#"{
            "protocol_version":{"major":1,"minor":0},
            "request_id":"00000000-0000-0000-0000-000000000000",
            "body":{"method":"system.status","params":{}},
            "unexpected":true
        }"#;
        assert!(serde_json::from_str::<RequestEnvelope>(json).is_err());
    }

    #[test]
    fn encoder_rejects_oversized_payload() {
        let oversized = "x".repeat(MAX_MESSAGE_SIZE + 1);
        let error = encode_json_frame(&oversized).expect_err("payload must be rejected");
        assert!(matches!(error, FrameError::Oversized { .. }));
    }
}
