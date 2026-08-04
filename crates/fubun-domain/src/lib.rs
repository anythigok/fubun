//! Strict domain types for canonical Fubun events.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use time::OffsetDateTime;
use uuid::Uuid;

pub const EVENT_SPEC_VERSION: &str = "1.0";
pub const SYNTHETIC_EVENT_TYPE: &str = "dev.fubun.dev.synthetic.v1";
pub const BROWSER_RESOURCE_OPENED_EVENT_TYPE: &str = "dev.fubun.browser.resource.opened.v1";
pub const VSCODE_WORKSPACE_OPENED_EVENT_TYPE: &str = "dev.fubun.vscode.workspace.opened.v1";
pub const RITUAL_SCHEMA_VERSION: &str = "dev.fubun.ritual/1";
pub const MAX_RITUAL_ACTIONS: usize = 16;
pub const WEB_URL_MAX_BYTES: usize = 2048;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum Actor {
    User,
    Fubun,
    System,
    Imported,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PrivacyClass {
    Normal,
    Sensitive,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum EventType {
    #[serde(rename = "dev.fubun.dev.synthetic.v1")]
    SyntheticV1,
    #[serde(rename = "dev.fubun.browser.resource.opened.v1")]
    BrowserResourceOpenedV1,
    #[serde(rename = "dev.fubun.vscode.workspace.opened.v1")]
    VscodeWorkspaceOpenedV1,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AdapterIdentity {
    pub id: String,
    pub version: String,
    pub instance_id: Uuid,
    pub sequence_no: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ContextReference {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub project: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SyntheticEventData {
    pub label: String,
    pub counter: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ResourceEventData {
    pub resource_id: Uuid,
}

/// Event payloads are intentionally small and semantic.  Raw URLs, paths,
/// titles, tab identifiers, and file names are not representable here.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind")]
pub enum EventData {
    #[serde(rename = "synthetic")]
    Synthetic { label: String, counter: i64 },
    #[serde(rename = "browser.resource.opened")]
    BrowserResourceOpened { resource_id: Uuid },
    #[serde(rename = "vscode.workspace.opened")]
    VscodeWorkspaceOpened { resource_id: Uuid },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Event {
    pub spec_version: String,
    pub id: Uuid,
    #[serde(rename = "type")]
    pub event_type: EventType,
    pub source: String,
    #[serde(with = "time::serde::rfc3339")]
    #[schemars(with = "String")]
    pub occurred_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    #[schemars(with = "String")]
    pub received_at: OffsetDateTime,
    pub actor: Actor,
    pub adapter: AdapterIdentity,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context: Option<ContextReference>,
    pub privacy: PrivacyClass,
    pub data: EventData,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ValidationError {
    #[error("unsupported event spec version: {0}")]
    UnsupportedSpecVersion(String),
    #[error("field {0} must not be empty")]
    EmptyField(&'static str),
    #[error("adapter sequence number exceeds SQLite's signed integer range")]
    SequenceOutOfRange,
    #[error("synthetic event label exceeds 128 bytes")]
    LabelTooLong,
    #[error("event type and data kind do not match")]
    TypeDataMismatch,
}

impl Event {
    pub fn validate(&self) -> Result<(), ValidationError> {
        if self.spec_version != EVENT_SPEC_VERSION {
            return Err(ValidationError::UnsupportedSpecVersion(
                self.spec_version.clone(),
            ));
        }
        validate_non_empty("source", &self.source)?;
        validate_non_empty("adapter.id", &self.adapter.id)?;
        validate_non_empty("adapter.version", &self.adapter.version)?;
        if let EventData::Synthetic { label, .. } = &self.data {
            validate_non_empty("data.label", label)?;
        }
        if self.adapter.sequence_no > i64::MAX as u64 {
            return Err(ValidationError::SequenceOutOfRange);
        }
        match &self.data {
            EventData::Synthetic { label, .. } if label.len() > 128 => {
                return Err(ValidationError::LabelTooLong)
            }
            EventData::Synthetic { .. }
            | EventData::BrowserResourceOpened { .. }
            | EventData::VscodeWorkspaceOpened { .. } => {}
        }
        let type_matches = matches!(
            (&self.event_type, &self.data),
            (EventType::SyntheticV1, EventData::Synthetic { .. })
                | (
                    EventType::BrowserResourceOpenedV1,
                    EventData::BrowserResourceOpened { .. }
                )
                | (
                    EventType::VscodeWorkspaceOpenedV1,
                    EventData::VscodeWorkspaceOpened { .. }
                )
        );
        if !type_matches {
            return Err(ValidationError::TypeDataMismatch);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum ResourceKind {
    #[serde(rename = "filesystem.file")]
    File,
    #[serde(rename = "filesystem.directory")]
    Directory,
    #[serde(rename = "web.page")]
    WebPage,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum ResourceScope {
    Exact,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum Sensitivity {
    Normal,
    Sensitive,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Resource {
    pub id: Uuid,
    pub kind: ResourceKind,
    pub label: String,
    pub locator: String,
    pub canonical_locator: String,
    pub sensitivity: Sensitivity,
    pub scope: ResourceScope,
    #[serde(with = "time::serde::rfc3339")]
    #[schemars(with = "String")]
    pub created_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    #[schemars(with = "String")]
    pub updated_at: OffsetDateTime,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ResourceValidationError {
    #[error("resource label must be 1-128 characters")]
    InvalidLabel,
    #[error("resource locator must be an absolute path")]
    RelativePath,
    #[error("resource locator contains a NUL or control character")]
    ControlCharacter,
    #[error("resource locator exceeds 4096 bytes")]
    LocatorTooLong,
    #[error("resource canonical locator must be an absolute path")]
    InvalidCanonicalPath,
    #[error("web page locator is not a valid canonical http(s) URL")]
    InvalidWebUrl,
}

impl Resource {
    pub fn validate(&self) -> Result<(), ResourceValidationError> {
        let label_len = self.label.chars().count();
        if !(1..=128).contains(&label_len) {
            return Err(ResourceValidationError::InvalidLabel);
        }
        if self.kind == ResourceKind::WebPage {
            let canonical = canonicalize_web_url(&self.locator)
                .map_err(|_| ResourceValidationError::InvalidWebUrl)?;
            if canonical != self.locator || canonical != self.canonical_locator {
                return Err(ResourceValidationError::InvalidWebUrl);
            }
        } else {
            validate_path_string(&self.locator)?;
            validate_path_string(&self.canonical_locator)?;
            if !std::path::Path::new(&self.locator).is_absolute() {
                return Err(ResourceValidationError::RelativePath);
            }
            if !std::path::Path::new(&self.canonical_locator).is_absolute() {
                return Err(ResourceValidationError::InvalidCanonicalPath);
            }
        }
        Ok(())
    }
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum WebUrlError {
    #[error("URL must use http or https")]
    UnsupportedScheme,
    #[error("URL must contain a host")]
    MissingHost,
    #[error("URL userinfo is not allowed")]
    UserInfo,
    #[error("URL exceeds {WEB_URL_MAX_BYTES} bytes")]
    TooLong,
    #[error("URL parser rejected the value")]
    Parse,
}

/// Canonicalize a web resource without retaining query strings or fragments.
pub fn canonicalize_web_url(input: &str) -> Result<String, WebUrlError> {
    if input.is_empty() || input.len() > WEB_URL_MAX_BYTES {
        return Err(if input.len() > WEB_URL_MAX_BYTES {
            WebUrlError::TooLong
        } else {
            WebUrlError::Parse
        });
    }
    let mut url = url::Url::parse(input).map_err(|_| WebUrlError::Parse)?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err(WebUrlError::UnsupportedScheme);
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(WebUrlError::UserInfo);
    }
    if url.host_str().is_none() {
        return Err(WebUrlError::MissingHost);
    }
    url.set_query(None);
    url.set_fragment(None);
    if url.path().is_empty() {
        url.set_path("/");
    }
    if matches!(
        (url.scheme(), url.port()),
        ("http", Some(80)) | ("https", Some(443))
    ) {
        url.set_port(None).map_err(|_| WebUrlError::Parse)?;
    }
    let canonical = url.to_string();
    if canonical.len() > WEB_URL_MAX_BYTES {
        return Err(WebUrlError::TooLong);
    }
    Ok(canonical)
}

#[must_use]
pub fn canonical_web_url_hash(canonical_url: &str) -> String {
    let digest = Sha256::digest(canonical_url.as_bytes());
    let mut hash = String::with_capacity(digest.len() * 2);
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(&mut hash, "{byte:02x}");
    }
    hash
}

pub fn web_origin_pattern(canonical_url: &str) -> Result<String, WebUrlError> {
    let url = url::Url::parse(canonical_url).map_err(|_| WebUrlError::Parse)?;
    let host = url.host_str().ok_or(WebUrlError::MissingHost)?;
    let mut origin = format!("{}://{host}", url.scheme());
    if let Some(port) = url.port() {
        origin.push(':');
        origin.push_str(&port.to_string());
    }
    origin.push_str("/*");
    Ok(origin)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum ObservationSource {
    #[serde(rename = "browser.chromium")]
    BrowserChromium,
    #[serde(rename = "vscode.workspace")]
    VscodeWorkspace,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum ObservationStatus {
    Active,
    Paused,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ObservationScope {
    pub id: Uuid,
    pub source: ObservationSource,
    pub resource_id: Uuid,
    pub status: ObservationStatus,
    #[serde(with = "time::serde::rfc3339")]
    #[schemars(with = "String")]
    pub created_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    #[schemars(with = "String")]
    pub updated_at: OffsetDateTime,
}

fn validate_path_string(value: &str) -> Result<(), ResourceValidationError> {
    if value.is_empty() || value.len() > 4096 || value.bytes().any(|byte| byte == 0 || byte < 0x20)
    {
        return Err(if value.len() > 4096 {
            ResourceValidationError::LocatorTooLong
        } else {
            ResourceValidationError::ControlCharacter
        });
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type")]
#[serde(deny_unknown_fields)]
pub enum ActionSpec {
    #[serde(rename = "linux.app.ensure_running.v1")]
    LinuxAppEnsureRunning { app_id: String },
    #[serde(rename = "linux.path.open.v1")]
    LinuxPathOpen { resource_id: Uuid },
    #[serde(rename = "desktop.notification.show.v1")]
    DesktopNotificationShow { title: String, body: String },
    #[serde(rename = "browser.tab.ensure_open.v1")]
    BrowserTabEnsureOpen { resource_id: Uuid },
}

impl ActionSpec {
    #[must_use]
    pub const fn action_type(&self) -> &'static str {
        match self {
            Self::LinuxAppEnsureRunning { .. } => "linux.app.ensure_running.v1",
            Self::LinuxPathOpen { .. } => "linux.path.open.v1",
            Self::DesktopNotificationShow { .. } => "desktop.notification.show.v1",
            Self::BrowserTabEnsureOpen { .. } => "browser.tab.ensure_open.v1",
        }
    }

    pub fn validate(&self) -> Result<(), ActionValidationError> {
        match self {
            Self::LinuxAppEnsureRunning { app_id } => validate_app_id(app_id),
            Self::LinuxPathOpen { .. } => Ok(()),
            Self::DesktopNotificationShow { title, body } => {
                validate_notification("title", title, 128)?;
                validate_notification("body", body, 1024)
            }
            Self::BrowserTabEnsureOpen { .. } => Ok(()),
        }
    }
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ActionValidationError {
    #[error("app_id must be 1-128 bytes from A-Z, a-z, 0-9, '.', '_', or '-'")]
    InvalidAppId,
    #[error("notification {field} must be 1-{maximum} characters without control characters")]
    InvalidNotification { field: &'static str, maximum: usize },
}

/// Validate an XDG Desktop Entry ID used by the fixed Linux adapter.
///
/// The same validator is used by the domain and the adapter so that a value
/// accepted during ritual validation cannot be rejected or reinterpreted at
/// execution time.  Desktop Entry IDs are deliberately narrower than paths:
/// only ASCII alphanumerics, `.`, `_`, and `-` are accepted.
pub fn validate_app_id(app_id: &str) -> Result<(), ActionValidationError> {
    if app_id.is_empty()
        || app_id.len() > 128
        || !app_id.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || byte == b'.' || byte == b'_' || byte == b'-'
        })
    {
        return Err(ActionValidationError::InvalidAppId);
    }
    Ok(())
}

fn validate_notification(
    field: &'static str,
    value: &str,
    maximum: usize,
) -> Result<(), ActionValidationError> {
    if value.is_empty() || value.chars().count() > maximum || value.chars().any(char::is_control) {
        return Err(ActionValidationError::InvalidNotification { field, maximum });
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum ExecutionMode {
    Sequential,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum FailureMode {
    Stop,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RitualExecutionConfig {
    pub mode: ExecutionMode,
    pub on_failure: FailureMode,
    pub timeout_seconds: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RitualDefinition {
    pub schema_version: String,
    pub name: String,
    pub actions: Vec<ActionSpec>,
    pub execution: RitualExecutionConfig,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum RitualValidationError {
    #[error("unsupported ritual schema version: {0}")]
    UnsupportedSchemaVersion(String),
    #[error("ritual name must be 1-128 characters")]
    InvalidName,
    #[error("ritual must contain 1-{MAX_RITUAL_ACTIONS} actions")]
    InvalidActionCount,
    #[error("ritual action {index} is invalid: {source}")]
    InvalidAction {
        index: usize,
        source: ActionValidationError,
    },
    #[error("execution.timeout_seconds must be between 1 and 120")]
    InvalidTimeout,
    #[error("consecutive duplicate action at index {index}")]
    ConsecutiveDuplicate { index: usize },
}

impl RitualDefinition {
    pub fn validate(&self) -> Result<(), RitualValidationError> {
        if self.schema_version != RITUAL_SCHEMA_VERSION {
            return Err(RitualValidationError::UnsupportedSchemaVersion(
                self.schema_version.clone(),
            ));
        }
        let name_len = self.name.chars().count();
        if !(1..=128).contains(&name_len) || self.name.chars().any(char::is_control) {
            return Err(RitualValidationError::InvalidName);
        }
        if self.actions.is_empty() || self.actions.len() > MAX_RITUAL_ACTIONS {
            return Err(RitualValidationError::InvalidActionCount);
        }
        if self.execution.timeout_seconds == 0 || self.execution.timeout_seconds > 120 {
            return Err(RitualValidationError::InvalidTimeout);
        }
        for (index, action) in self.actions.iter().enumerate() {
            action
                .validate()
                .map_err(|source| RitualValidationError::InvalidAction { index, source })?;
            if index > 0 && self.actions[index - 1] == *action {
                return Err(RitualValidationError::ConsecutiveDuplicate { index });
            }
        }
        Ok(())
    }

    pub fn canonical_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }

    pub fn content_hash(&self) -> Result<String, serde_json::Error> {
        let canonical = self.canonical_json()?;
        let digest = Sha256::digest(canonical.as_bytes());
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let mut hash = String::with_capacity(digest.len() * 2);
        for byte in digest {
            hash.push(char::from(HEX[(byte >> 4) as usize]));
            hash.push(char::from(HEX[(byte & 0x0f) as usize]));
        }
        Ok(hash)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum RitualStatus {
    Draft,
    Active,
    Paused,
    Archived,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Ritual {
    pub id: Uuid,
    pub name: String,
    pub status: RitualStatus,
    pub current_version_id: Uuid,
    #[serde(with = "time::serde::rfc3339")]
    #[schemars(with = "String")]
    pub created_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    #[schemars(with = "String")]
    pub updated_at: OffsetDateTime,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RitualVersion {
    pub id: Uuid,
    pub ritual_id: Uuid,
    pub version: u32,
    pub schema_version: String,
    pub canonical_json: String,
    pub content_hash: String,
    #[serde(with = "time::serde::rfc3339")]
    #[schemars(with = "String")]
    pub created_at: OffsetDateTime,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Approval {
    pub id: Uuid,
    pub ritual_version_id: Uuid,
    pub action_type: String,
    pub capability: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resource_id: Option<Uuid>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub app_id: Option<String>,
    pub content_hash: String,
    #[serde(with = "time::serde::rfc3339")]
    #[schemars(with = "String")]
    pub approved_at: OffsetDateTime,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum TriggerKind {
    Manual,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum ExecutionStatus {
    Planned,
    Running,
    Succeeded,
    Failed,
    Partial,
    Aborted,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Execution {
    pub id: Uuid,
    pub ritual_id: Uuid,
    pub ritual_version_id: Uuid,
    pub status: ExecutionStatus,
    pub trigger_kind: TriggerKind,
    #[serde(with = "time::serde::rfc3339")]
    #[schemars(with = "String")]
    pub started_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339::option")]
    #[schemars(with = "Option<String>")]
    pub finished_at: Option<OffsetDateTime>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failure_code: Option<String>,
    #[serde(with = "time::serde::rfc3339")]
    #[schemars(with = "String")]
    pub created_at: OffsetDateTime,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum ExecutionStepStatus {
    Pending,
    Running,
    Succeeded,
    Skipped,
    Failed,
    Aborted,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ExecutionStep {
    pub id: Uuid,
    pub execution_id: Uuid,
    pub step_index: u32,
    pub action_type: String,
    pub status: ExecutionStepStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub adapter_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub adapter_instance_id: Option<Uuid>,
    #[serde(with = "time::serde::rfc3339")]
    #[schemars(with = "String")]
    pub started_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339::option")]
    #[schemars(with = "Option<String>")]
    pub finished_at: Option<OffsetDateTime>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result_code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub redacted_message: Option<String>,
}

fn validate_non_empty(field: &'static str, value: &str) -> Result<(), ValidationError> {
    if value.trim().is_empty() {
        return Err(ValidationError::EmptyField(field));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event() -> Event {
        Event {
            spec_version: EVENT_SPEC_VERSION.to_owned(),
            id: Uuid::new_v4(),
            event_type: EventType::SyntheticV1,
            source: "fubun-cli".to_owned(),
            occurred_at: OffsetDateTime::now_utc(),
            received_at: OffsetDateTime::now_utc(),
            actor: Actor::User,
            adapter: AdapterIdentity {
                id: "dev.fixture".to_owned(),
                version: "0.1.0".to_owned(),
                instance_id: Uuid::new_v4(),
                sequence_no: 1,
            },
            context: None,
            privacy: PrivacyClass::Normal,
            data: EventData::Synthetic {
                label: "smoke".to_owned(),
                counter: 1,
            },
        }
    }

    #[test]
    fn unknown_event_fields_are_rejected() {
        let mut value = serde_json::to_value(event()).expect("event serializes");
        value
            .as_object_mut()
            .expect("event is an object")
            .insert("unknown".to_owned(), serde_json::Value::Bool(true));

        let result = serde_json::from_value::<Event>(value);
        assert!(result.is_err());
    }

    #[test]
    fn validates_required_text() {
        let mut candidate = event();
        candidate.source.clear();
        assert_eq!(
            candidate.validate(),
            Err(ValidationError::EmptyField("source"))
        );
    }

    #[test]
    fn ritual_action_unknown_fields_are_rejected() {
        let json = r#"{"type":"linux.app.ensure_running.v1","app_id":"code","extra":true}"#;
        assert!(serde_json::from_str::<ActionSpec>(json).is_err());
    }

    #[test]
    fn ritual_hash_ignores_input_whitespace() {
        let first = r#"{"schema_version":"dev.fubun.ritual/1","name":"x","actions":[{"type":"desktop.notification.show.v1","title":"a","body":"b"}],"execution":{"mode":"sequential","on_failure":"stop","timeout_seconds":1}}"#;
        let second = r#"{
          "schema_version": "dev.fubun.ritual/1",
          "name": "x",
          "actions": [{"type":"desktop.notification.show.v1", "title":"a", "body":"b"}],
          "execution": {"mode":"sequential", "on_failure":"stop", "timeout_seconds":1}
        }"#;
        let first: RitualDefinition = serde_json::from_str(first).expect("first");
        let second: RitualDefinition = serde_json::from_str(second).expect("second");
        assert_eq!(
            first.content_hash().expect("hash"),
            second.content_hash().expect("hash")
        );
    }

    #[test]
    fn desktop_entry_ids_share_the_strict_cross_crate_rules() {
        for value in [
            "code",
            "google-chrome",
            "org.example.App",
            "org_example-App-1",
        ] {
            assert!(validate_app_id(value).is_ok(), "{value} should be valid");
        }
        for value in ["../app", "dir/app", r"dir\app", "app name", "", "app\nname"] {
            assert!(
                validate_app_id(value).is_err(),
                "{value:?} should be invalid"
            );
        }
        assert!(validate_app_id(&"a".repeat(129)).is_err());
    }

    #[test]
    fn canonicalizes_web_urls_without_query_or_fragment() {
        let canonical = canonicalize_web_url("https://Example.COM:443/research?id=123#section")
            .expect("canonical URL");
        assert_eq!(canonical, "https://example.com/research");
        assert_eq!(
            canonical_web_url_hash(&canonical),
            "468164e75ba0e4cf47eee057fe3459fa5bc5fd4ba259de08f117c780e261da59"
        );
        assert!(canonicalize_web_url("file:///tmp/a").is_err());
        assert!(canonicalize_web_url("https://user@example.com/").is_err());
    }

    #[test]
    fn semantic_event_payloads_cannot_carry_raw_locations() {
        let data = EventData::BrowserResourceOpened {
            resource_id: Uuid::nil(),
        };
        let json = serde_json::to_value(data).expect("event data");
        assert!(json.get("url").is_none());
        assert!(json.get("path").is_none());
    }

    #[test]
    fn event_type_and_data_kind_must_match() {
        let mut event = event();
        event.event_type = EventType::BrowserResourceOpenedV1;
        assert!(matches!(
            event.validate(),
            Err(ValidationError::TypeDataMismatch)
        ));
    }
}
