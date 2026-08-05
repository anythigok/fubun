//! Versioned IPC envelopes and bounded little-endian framing.

use std::io;

use fubun_domain::{
    ActionSpec, Event, EventType, Execution, ExecutionStep, ObservationScope, ObservationSource,
    Resource, ResourceKind, Ritual, RitualDefinition, RitualVersion, Sensitivity,
};
use fubun_mining::{DiscoveredSession, DiscoveredSuggestion, DiscoveryRun, SuggestionStatus};
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
pub struct AdapterEventEmitRequest {
    pub sequence_no: u64,
    #[serde(with = "time::serde::rfc3339")]
    #[schemars(with = "String")]
    pub occurred_at: OffsetDateTime,
    #[serde(rename = "type")]
    pub event_type: EventType,
    pub resource_id: Uuid,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AdapterStatusSnapshot {
    pub tools: Vec<AdapterToolStatus>,
    pub desktop_entry_ids: Vec<String>,
    #[serde(default)]
    pub permitted_resource_ids: Vec<Uuid>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AdapterToolStatus {
    pub name: String,
    pub available: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AdapterHello {
    pub adapter_id: String,
    pub adapter_version: String,
    pub instance_id: Uuid,
    pub action_capabilities: Vec<String>,
    #[serde(default)]
    pub event_capabilities: Vec<String>,
    pub status: AdapterStatusSnapshot,
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
#[serde(deny_unknown_fields)]
pub struct ResourceCreateRequest {
    pub label: String,
    pub path: String,
    pub sensitivity: Sensitivity,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct BrowserObservationEnableRequest {
    pub label: String,
    pub url: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct VscodeObservationEnableRequest {
    pub label: String,
    pub absolute_path: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ObservationPauseRequest {
    pub scope_id: Uuid,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ObservationListRequest {
    #[serde(default)]
    pub source: Option<ObservationSource>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SessionsListRequest {
    #[serde(default)]
    pub workspace_resource_id: Option<Uuid>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SuggestionsListRequest {
    #[serde(default)]
    pub status: Option<SuggestionStatus>,
    #[serde(default)]
    pub workspace_resource_id: Option<Uuid>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SuggestionSnoozeRequest {
    pub suggestion_id: Uuid,
    pub for_duration: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SuggestionAcceptRequest {
    pub suggestion_id: Uuid,
    #[serde(default)]
    pub name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DiscoveryRunReport {
    pub run: DiscoveryRun,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SessionsList {
    pub sessions: Vec<DiscoveredSession>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SessionShow {
    pub session: DiscoveredSession,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SuggestionsList {
    pub suggestions: Vec<DiscoveredSuggestion>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SuggestionShow {
    pub suggestion: DiscoveredSuggestion,
    pub status: SuggestionStatus,
    #[serde(default)]
    pub snoozed_until: Option<String>,
    #[serde(default)]
    pub accepted_ritual_id: Option<Uuid>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct BrowserObservationEnabled {
    pub resource: Resource,
    pub scope: ObservationScope,
    pub canonical_url_hash: String,
    pub origin_pattern: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct VscodeObservationEnabled {
    pub resource: Resource,
    pub scope: ObservationScope,
    pub canonical_path_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ObservationList {
    pub scopes: Vec<ObservationScope>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct EventAck {
    pub event_id: Uuid,
    pub stored: bool,
    pub duplicate: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ResourceIdRequest {
    pub resource_id: Uuid,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RitualIdRequest {
    pub ritual_id: Uuid,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SessionIdRequest {
    pub session_id: Uuid,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SuggestionIdRequest {
    pub suggestion_id: Uuid,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RitualCreateRequest {
    pub definition: RitualDefinition,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RitualUpdateRequest {
    pub ritual_id: Uuid,
    pub definition: RitualDefinition,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RitualActivateRequest {
    pub ritual_id: Uuid,
    pub approve: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ExecutionIdRequest {
    pub execution_id: Uuid,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ActionExecuteRequest {
    pub action: ActionSpec,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resolved_resource: Option<ResolvedResource>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ResolvedResource {
    pub resource_id: Uuid,
    pub kind: ResourceKind,
    pub canonical_locator: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AdapterStatusRequest {}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AdapterEventAck {
    pub event_id: Uuid,
    pub stored: bool,
    pub duplicate: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "method", content = "params")]
pub enum AdapterRequestBody {
    #[serde(rename = "action.execute")]
    ActionExecute(ActionExecuteRequest),
    #[serde(rename = "adapter.status")]
    Status(AdapterStatusRequest),
    #[serde(rename = "event.ack")]
    EventAck(AdapterEventAck),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AdapterRequestEnvelope {
    pub protocol_version: ProtocolVersion,
    pub request_id: Uuid,
    pub action_execution_id: Uuid,
    pub body: AdapterRequestBody,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum AdapterActionStatus {
    Succeeded,
    Skipped,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ActionResult {
    pub status: AdapterActionStatus,
    pub result_code: String,
    pub redacted_message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", content = "data")]
pub enum AdapterResponseBody {
    #[serde(rename = "action.result")]
    ActionResult(ActionResult),
    #[serde(rename = "adapter.status")]
    Status(AdapterStatusSnapshot),
    #[serde(rename = "event.emit")]
    EventEmit(AdapterEventEmitRequest),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AdapterResponseEnvelope {
    pub protocol_version: ProtocolVersion,
    pub request_id: Uuid,
    pub action_execution_id: Uuid,
    pub body: AdapterResponseBody,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "method", content = "params")]
pub enum RequestBody {
    #[serde(rename = "client.hello")]
    ClientHello(ClientHello),
    #[serde(rename = "event.ingest")]
    EventIngest(EventIngestRequest),
    #[serde(rename = "browser.observation.enable")]
    BrowserObservationEnable(BrowserObservationEnableRequest),
    #[serde(rename = "vscode.observation.enable")]
    VscodeObservationEnable(VscodeObservationEnableRequest),
    #[serde(rename = "observation.pause")]
    ObservationPause(ObservationPauseRequest),
    #[serde(rename = "observations.list")]
    ObservationsList(ObservationListRequest),
    #[serde(rename = "events.list")]
    EventsList(EventsListRequest),
    #[serde(rename = "system.status")]
    SystemStatus(EmptyRequest),
    #[serde(rename = "system.doctor")]
    SystemDoctor(EmptyRequest),
    #[serde(rename = "adapter.hello")]
    AdapterHello(AdapterHello),
    #[serde(rename = "resource.create")]
    ResourceCreate(ResourceCreateRequest),
    #[serde(rename = "resource.list")]
    ResourceList(EmptyRequest),
    #[serde(rename = "resource.show")]
    ResourceShow(ResourceIdRequest),
    #[serde(rename = "ritual.create")]
    RitualCreate(RitualCreateRequest),
    #[serde(rename = "ritual.update")]
    RitualUpdate(RitualUpdateRequest),
    #[serde(rename = "ritual.list")]
    RitualList(EmptyRequest),
    #[serde(rename = "ritual.show")]
    RitualShow(RitualIdRequest),
    #[serde(rename = "ritual.preview")]
    RitualPreview(RitualIdRequest),
    #[serde(rename = "ritual.activate")]
    RitualActivate(RitualActivateRequest),
    #[serde(rename = "ritual.pause")]
    RitualPause(RitualIdRequest),
    #[serde(rename = "ritual.run")]
    RitualRun(RitualIdRequest),
    #[serde(rename = "adapter.list")]
    AdapterList(EmptyRequest),
    #[serde(rename = "adapter.status")]
    AdapterStatus(EmptyRequest),
    #[serde(rename = "execution.list")]
    ExecutionList(EmptyRequest),
    #[serde(rename = "execution.show")]
    ExecutionShow(ExecutionIdRequest),
    #[serde(rename = "integrations.status")]
    IntegrationsStatus(EmptyRequest),
    #[serde(rename = "discovery.run")]
    DiscoveryRun(EmptyRequest),
    #[serde(rename = "discovery.status")]
    DiscoveryStatus(EmptyRequest),
    #[serde(rename = "sessions.list")]
    SessionsList(SessionsListRequest),
    #[serde(rename = "session.show")]
    SessionShow(SessionIdRequest),
    #[serde(rename = "suggestions.list")]
    SuggestionsList(SuggestionsListRequest),
    #[serde(rename = "suggestion.show")]
    SuggestionShow(SuggestionIdRequest),
    #[serde(rename = "suggestion.snooze")]
    SuggestionSnooze(SuggestionSnoozeRequest),
    #[serde(rename = "suggestion.dismiss")]
    SuggestionDismiss(SuggestionIdRequest),
    #[serde(rename = "suggestion.block")]
    SuggestionBlock(SuggestionIdRequest),
    #[serde(rename = "suggestion.accept")]
    SuggestionAccept(SuggestionAcceptRequest),
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
    pub stored: bool,
    pub duplicate: bool,
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
    pub gtk_launch_available: bool,
    pub xdg_open_available: bool,
    pub notify_send_available: bool,
    pub connected_adapters: usize,
    pub action_registry_version: String,
    pub ritual_schema_version: String,
    pub running_executions: usize,
    pub draft_rituals: usize,
    pub active_rituals: usize,
    pub browser_adapters: usize,
    pub vscode_adapters: usize,
    pub active_browser_scopes: usize,
    pub active_vscode_scopes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct IntegrationReport {
    pub core_connected: bool,
    pub linux_adapters: usize,
    pub browser_adapters: usize,
    pub vscode_adapters: usize,
    pub native_host_manifest: String,
    pub permitted_browser_resources: usize,
    pub active_browser_scopes: usize,
    pub active_vscode_scopes: usize,
    pub protocol_version: ProtocolVersion,
    pub database_version: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ResourceList {
    pub resources: Vec<Resource>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RitualRecord {
    pub ritual: Ritual,
    pub version: RitualVersion,
    pub definition: RitualDefinition,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RitualList {
    pub rituals: Vec<Ritual>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PreviewAction {
    pub step_index: u32,
    pub action_type: String,
    pub risk_level: String,
    pub idempotency: String,
    pub revertability: String,
    pub required_capability: String,
    pub adapter_connected: bool,
    pub required_tool_available: Option<bool>,
    pub resource_exists: Option<bool>,
    pub resource_path_matches: Option<bool>,
    pub resource_kind_matches: Option<bool>,
    pub desktop_entry_exists: Option<bool>,
    pub warning: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RitualPreview {
    pub ritual_id: Uuid,
    pub ritual_version_id: Uuid,
    pub actions: Vec<PreviewAction>,
    pub timeout_seconds: u32,
    pub warnings: Vec<String>,
    pub executable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AdapterReport {
    pub adapter_id: String,
    pub version: String,
    pub instance_id: Uuid,
    pub connected: bool,
    pub capabilities: Vec<String>,
    pub event_capabilities: Vec<String>,
    pub permitted_resource_ids: Vec<Uuid>,
    pub connected_at: String,
    pub last_seen_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AdapterList {
    pub adapters: Vec<AdapterReport>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ExecutionRecord {
    pub execution: Execution,
    pub steps: Vec<ExecutionStep>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ExecutionList {
    pub executions: Vec<Execution>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", content = "data")]
pub enum ResponsePayload {
    #[serde(rename = "client.hello_ack")]
    ClientHelloAck(ClientHelloAck),
    #[serde(rename = "event.ingested")]
    EventIngested(EventIngested),
    #[serde(rename = "event.ack")]
    EventAck(EventAck),
    #[serde(rename = "events.list")]
    EventList(EventList),
    #[serde(rename = "system.status")]
    Status(StatusReport),
    #[serde(rename = "system.doctor")]
    Doctor(DoctorReport),
    #[serde(rename = "adapter.hello_ack")]
    AdapterHelloAck(ClientHelloAck),
    #[serde(rename = "resource.created")]
    ResourceCreated(Resource),
    #[serde(rename = "resource.list")]
    ResourceList(ResourceList),
    #[serde(rename = "resource.show")]
    ResourceShow(Resource),
    #[serde(rename = "ritual.created")]
    RitualCreated(RitualRecord),
    #[serde(rename = "ritual.updated")]
    RitualUpdated(RitualRecord),
    #[serde(rename = "ritual.list")]
    RitualList(RitualList),
    #[serde(rename = "ritual.show")]
    RitualShow(RitualRecord),
    #[serde(rename = "ritual.preview")]
    RitualPreview(RitualPreview),
    #[serde(rename = "ritual.activated")]
    RitualActivated(Ritual),
    #[serde(rename = "ritual.paused")]
    RitualPaused(Ritual),
    #[serde(rename = "ritual.run")]
    RitualRun(ExecutionRecord),
    #[serde(rename = "adapter.list")]
    AdapterList(AdapterList),
    #[serde(rename = "adapter.status")]
    AdapterStatus(AdapterList),
    #[serde(rename = "execution.list")]
    ExecutionList(ExecutionList),
    #[serde(rename = "execution.show")]
    ExecutionShow(ExecutionRecord),
    #[serde(rename = "observation.enabled")]
    BrowserObservationEnabled(BrowserObservationEnabled),
    #[serde(rename = "vscode.observation.enabled")]
    VscodeObservationEnabled(VscodeObservationEnabled),
    #[serde(rename = "observation.paused")]
    ObservationPaused(ObservationScope),
    #[serde(rename = "observations.list")]
    ObservationList(ObservationList),
    #[serde(rename = "integrations.status")]
    IntegrationsStatus(IntegrationReport),
    #[serde(rename = "discovery.run")]
    DiscoveryRun(DiscoveryRunReport),
    #[serde(rename = "discovery.status")]
    DiscoveryStatus(DiscoveryRunReport),
    #[serde(rename = "sessions.list")]
    SessionsList(SessionsList),
    #[serde(rename = "session.show")]
    SessionShow(SessionShow),
    #[serde(rename = "suggestions.list")]
    SuggestionsList(SuggestionsList),
    #[serde(rename = "suggestion.show")]
    SuggestionShow(SuggestionShow),
    #[serde(rename = "suggestion.snooze")]
    SuggestionSnoozed(SuggestionShow),
    #[serde(rename = "suggestion.dismiss")]
    SuggestionDismissed(SuggestionShow),
    #[serde(rename = "suggestion.block")]
    SuggestionBlocked(SuggestionShow),
    #[serde(rename = "suggestion.accept")]
    SuggestionAccepted(RitualRecord),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ErrorBody {
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "status", content = "payload", rename_all = "snake_case")]
#[allow(clippy::large_enum_variant)]
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

    #[test]
    fn discovery_ids_are_strictly_separate_from_ritual_ids() {
        let session_json = r#"{"protocol_version":{"major":1,"minor":0},"request_id":"00000000-0000-0000-0000-000000000000","body":{"method":"session.show","params":{"session_id":"11111111-1111-4111-8111-111111111111"}}}"#.to_string();
        let envelope: RequestEnvelope = serde_json::from_str(&session_json).expect("session id");
        assert!(matches!(
            envelope.body,
            RequestBody::SessionShow(SessionIdRequest { .. })
        ));

        let wrong_field = session_json.replace("session_id", "ritual_id");
        assert!(serde_json::from_str::<RequestEnvelope>(&wrong_field).is_err());

        let suggestion_json = session_json
            .replace("session.show", "suggestion.dismiss")
            .replace("session_id", "suggestion_id");
        let suggestion: RequestEnvelope =
            serde_json::from_str(&suggestion_json).expect("suggestion id");
        assert!(matches!(
            suggestion.body,
            RequestBody::SuggestionDismiss(SuggestionIdRequest { .. })
        ));
    }
}
