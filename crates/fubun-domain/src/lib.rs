//! Strict domain types for canonical Fubun events.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use time::OffsetDateTime;
use uuid::Uuid;

pub const EVENT_SPEC_VERSION: &str = "1.0";
pub const SYNTHETIC_EVENT_TYPE: &str = "dev.fubun.dev.synthetic.v1";

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
    pub data: SyntheticEventData,
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
        validate_non_empty("data.label", &self.data.label)?;
        if self.adapter.sequence_no > i64::MAX as u64 {
            return Err(ValidationError::SequenceOutOfRange);
        }
        if self.data.label.len() > 128 {
            return Err(ValidationError::LabelTooLong);
        }
        Ok(())
    }
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
            data: SyntheticEventData {
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
}
