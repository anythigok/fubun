//! Fixed, non-plugin Action Registry and the Phase 2 risk policy.

use fubun_domain::ActionSpec;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum RiskLevel {
    R0,
    R1,
    R2,
    R3,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Idempotency {
    Idempotent,
    BestEffort,
    NonIdempotent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Revertability {
    NoStateChange,
    PartiallyRevertable,
    NotRevertable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ActionDescriptor {
    pub action_type: &'static str,
    pub schema_version: &'static str,
    pub risk_level: RiskLevel,
    pub idempotency: Idempotency,
    pub revertability: Revertability,
    pub required_capability: &'static str,
    pub default_timeout_ms: u64,
}

pub const APP_ENSURE_RUNNING: ActionDescriptor = ActionDescriptor {
    action_type: "linux.app.ensure_running.v1",
    schema_version: "1",
    risk_level: RiskLevel::R1,
    idempotency: Idempotency::BestEffort,
    revertability: Revertability::NotRevertable,
    required_capability: "linux.app.ensure_running.v1",
    default_timeout_ms: 10_000,
};

pub const PATH_OPEN: ActionDescriptor = ActionDescriptor {
    action_type: "linux.path.open.v1",
    schema_version: "1",
    risk_level: RiskLevel::R1,
    idempotency: Idempotency::BestEffort,
    revertability: Revertability::NoStateChange,
    required_capability: "linux.path.open.v1",
    default_timeout_ms: 10_000,
};

pub const NOTIFICATION_SHOW: ActionDescriptor = ActionDescriptor {
    action_type: "desktop.notification.show.v1",
    schema_version: "1",
    risk_level: RiskLevel::R0,
    idempotency: Idempotency::NonIdempotent,
    revertability: Revertability::NoStateChange,
    required_capability: "desktop.notification.show.v1",
    default_timeout_ms: 10_000,
};

pub const BROWSER_TAB_ENSURE_OPEN: ActionDescriptor = ActionDescriptor {
    action_type: "browser.tab.ensure_open.v1",
    schema_version: "1",
    risk_level: RiskLevel::R1,
    idempotency: Idempotency::BestEffort,
    revertability: Revertability::PartiallyRevertable,
    required_capability: "browser.tab.ensure_open.v1",
    default_timeout_ms: 10_000,
};

#[must_use]
pub fn descriptors() -> [ActionDescriptor; 4] {
    [
        APP_ENSURE_RUNNING,
        PATH_OPEN,
        NOTIFICATION_SHOW,
        BROWSER_TAB_ENSURE_OPEN,
    ]
}

#[must_use]
pub fn descriptor(action: &ActionSpec) -> &'static ActionDescriptor {
    match action {
        ActionSpec::LinuxAppEnsureRunning { .. } => &APP_ENSURE_RUNNING,
        ActionSpec::LinuxPathOpen { .. } => &PATH_OPEN,
        ActionSpec::DesktopNotificationShow { .. } => &NOTIFICATION_SHOW,
        ActionSpec::BrowserTabEnsureOpen { .. } => &BROWSER_TAB_ENSURE_OPEN,
    }
}

#[must_use]
pub fn descriptor_by_type(action_type: &str) -> Option<&'static ActionDescriptor> {
    match action_type {
        "linux.app.ensure_running.v1" => Some(&APP_ENSURE_RUNNING),
        "linux.path.open.v1" => Some(&PATH_OPEN),
        "desktop.notification.show.v1" => Some(&NOTIFICATION_SHOW),
        "browser.tab.ensure_open.v1" => Some(&BROWSER_TAB_ENSURE_OPEN),
        _ => None,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApprovalFields {
    pub action_type: &'static str,
    pub capability: &'static str,
    pub resource_id: Option<Uuid>,
    pub app_id: Option<String>,
}

#[must_use]
pub fn approval_fields(action: &ActionSpec) -> ApprovalFields {
    let descriptor = descriptor(action);
    match action {
        ActionSpec::LinuxAppEnsureRunning { app_id } => ApprovalFields {
            action_type: descriptor.action_type,
            capability: descriptor.required_capability,
            resource_id: None,
            app_id: Some(app_id.clone()),
        },
        ActionSpec::LinuxPathOpen { resource_id } => ApprovalFields {
            action_type: descriptor.action_type,
            capability: descriptor.required_capability,
            resource_id: Some(*resource_id),
            app_id: None,
        },
        ActionSpec::DesktopNotificationShow { .. } => ApprovalFields {
            action_type: descriptor.action_type,
            capability: descriptor.required_capability,
            resource_id: None,
            app_id: None,
        },
        ActionSpec::BrowserTabEnsureOpen { resource_id } => ApprovalFields {
            action_type: descriptor.action_type,
            capability: descriptor.required_capability,
            resource_id: Some(*resource_id),
            app_id: None,
        },
    }
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum PolicyError {
    #[error("action is not registered")]
    UnknownAction,
    #[error("risk level is not permitted for Phase 2 manual execution")]
    RiskNotPermitted,
}

pub fn validate_action(action: &ActionSpec) -> Result<&'static ActionDescriptor, PolicyError> {
    let descriptor = descriptor(action);
    if descriptor.risk_level as u8 > RiskLevel::R1 as u8 {
        return Err(PolicyError::RiskNotPermitted);
    }
    Ok(descriptor)
}

#[cfg(test)]
mod tests {
    use super::*;
    use fubun_domain::ActionSpec;

    #[test]
    fn registry_contains_only_r0_or_r1_actions() {
        assert!(descriptors()
            .iter()
            .all(|descriptor| matches!(descriptor.risk_level, RiskLevel::R0 | RiskLevel::R1)));
    }

    #[test]
    fn approval_fields_never_expose_a_raw_path() {
        let fields = approval_fields(&ActionSpec::LinuxPathOpen {
            resource_id: Uuid::nil(),
        });
        assert!(fields.resource_id.is_some());
    }
}
