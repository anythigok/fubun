//! Bidirectional adapter registry and bounded action dispatch.

use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex as StdMutex},
    time::Duration,
};

use fubun_domain::ActionSpec;
use fubun_policy::descriptor_by_type;
use fubun_protocol::{
    ActionExecuteRequest, ActionResult, AdapterHello, AdapterReport, AdapterRequestBody,
    AdapterRequestEnvelope, AdapterResponseBody, AdapterResponseEnvelope, ResolvedResource,
};
use thiserror::Error;
use time::OffsetDateTime;
use tokio::sync::{mpsc, oneshot, Mutex};
use uuid::Uuid;

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum AdapterDispatchErrorKind {
    #[error("adapter is unavailable")]
    Unavailable,
    #[error("required adapter capability is unavailable")]
    CapabilityUnavailable,
    #[error("adapter action timed out")]
    Timeout,
    #[error("adapter disconnected")]
    Disconnected,
    #[error("adapter protocol error")]
    Protocol,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdapterRequirements {
    pub capability: &'static str,
    pub required_tool: &'static str,
    pub desktop_entry_id: Option<String>,
}

impl AdapterRequirements {
    #[must_use]
    pub fn from_action(action: &ActionSpec) -> Self {
        match action {
            ActionSpec::LinuxAppEnsureRunning { app_id } => Self {
                capability: "linux.app.ensure_running.v1",
                required_tool: "gtk-launch",
                desktop_entry_id: Some(app_id.clone()),
            },
            ActionSpec::LinuxPathOpen { .. } => Self {
                capability: "linux.path.open.v1",
                required_tool: "xdg-open",
                desktop_entry_id: None,
            },
            ActionSpec::DesktopNotificationShow { .. } => Self {
                capability: "desktop.notification.show.v1",
                required_tool: "notify-send",
                desktop_entry_id: None,
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EligibleAdapter {
    pub adapter_id: String,
    pub instance_id: Uuid,
}

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum AdapterEligibilityError {
    #[error("adapter is unavailable")]
    Unavailable,
    #[error("no connected adapter instance satisfies the action requirements")]
    Ineligible,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DispatchedActionError {
    pub identity: Option<EligibleAdapter>,
    pub kind: AdapterDispatchErrorKind,
}

impl std::fmt::Display for DispatchedActionError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.kind.fmt(formatter)
    }
}

impl std::error::Error for DispatchedActionError {}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum AdapterRegistrationError {
    #[error("adapter id is not registered for Phase 2")]
    UnknownAdapter,
    #[error("adapter version is empty, too long, or contains control characters")]
    InvalidVersion,
    #[error("adapter capability list is empty or too large")]
    InvalidCapabilityCount,
    #[error("adapter capability is empty, too long, or contains control characters")]
    InvalidCapability,
    #[error("adapter capability is not in the fixed action registry")]
    UnknownCapability,
    #[error("adapter capability is duplicated")]
    DuplicateCapability,
}

const LINUX_ADAPTER_ID: &str = "dev.fubun.linux";
const MAX_ADAPTER_VERSION_BYTES: usize = 128;
const MAX_CAPABILITIES: usize = 16;
const MAX_CAPABILITY_BYTES: usize = 128;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DispatchedActionResult {
    pub identity: EligibleAdapter,
    pub result: ActionResult,
}

struct PendingRequest {
    identity: EligibleAdapter,
    action_execution_id: Uuid,
    sender: oneshot::Sender<Result<ActionResult, AdapterDispatchErrorKind>>,
}

struct AdapterConnection {
    hello: AdapterHello,
    sender: mpsc::Sender<AdapterRequestEnvelope>,
    connected_at: OffsetDateTime,
    last_seen_at: OffsetDateTime,
}

#[derive(Clone, Default)]
pub struct AdapterManager {
    connections: Arc<Mutex<HashMap<Uuid, AdapterConnection>>>,
    pending: Arc<StdMutex<HashMap<Uuid, PendingRequest>>>,
}

impl AdapterManager {
    pub fn validate_hello(hello: &AdapterHello) -> Result<(), AdapterRegistrationError> {
        if hello.adapter_id != LINUX_ADAPTER_ID {
            return Err(AdapterRegistrationError::UnknownAdapter);
        }
        if hello.adapter_version.is_empty()
            || hello.adapter_version.len() > MAX_ADAPTER_VERSION_BYTES
            || hello
                .adapter_version
                .chars()
                .any(|character| character.is_control())
        {
            return Err(AdapterRegistrationError::InvalidVersion);
        }
        if hello.action_capabilities.is_empty()
            || hello.action_capabilities.len() > MAX_CAPABILITIES
        {
            return Err(AdapterRegistrationError::InvalidCapabilityCount);
        }
        let mut seen = HashSet::new();
        for capability in &hello.action_capabilities {
            if capability.is_empty()
                || capability.len() > MAX_CAPABILITY_BYTES
                || capability
                    .chars()
                    .any(|character| character.is_control() || character.is_whitespace())
            {
                return Err(AdapterRegistrationError::InvalidCapability);
            }
            if descriptor_by_type(capability).is_none() {
                return Err(AdapterRegistrationError::UnknownCapability);
            }
            if !seen.insert(capability) {
                return Err(AdapterRegistrationError::DuplicateCapability);
            }
        }
        Ok(())
    }

    pub async fn register(
        &self,
        hello: AdapterHello,
        sender: mpsc::Sender<AdapterRequestEnvelope>,
    ) -> Result<(), AdapterRegistrationError> {
        Self::validate_hello(&hello)?;
        let now = OffsetDateTime::now_utc();
        self.connections.lock().await.insert(
            hello.instance_id,
            AdapterConnection {
                hello,
                sender,
                connected_at: now,
                last_seen_at: now,
            },
        );
        Ok(())
    }

    pub async fn touch(&self, instance_id: Uuid) {
        if let Some(connection) = self.connections.lock().await.get_mut(&instance_id) {
            connection.last_seen_at = OffsetDateTime::now_utc();
        }
    }

    pub async fn disconnect(&self, instance_id: Uuid) {
        self.connections.lock().await.remove(&instance_id);
        let mut pending = self.pending.lock().expect("pending mutex poisoned");
        let requests: Vec<_> = pending
            .iter()
            .filter_map(|(id, request)| {
                (request.identity.instance_id == instance_id).then_some(*id)
            })
            .collect();
        for request_id in requests {
            if let Some(request) = pending.remove(&request_id) {
                let _ = request
                    .sender
                    .send(Err(AdapterDispatchErrorKind::Disconnected));
            }
        }
    }

    pub async fn shutdown(&self) {
        let instances: Vec<Uuid> = self.connections.lock().await.keys().copied().collect();
        for instance_id in instances {
            self.disconnect(instance_id).await;
        }
        let mut pending = self.pending.lock().expect("pending mutex poisoned");
        for (_, request) in pending.drain() {
            let _ = request
                .sender
                .send(Err(AdapterDispatchErrorKind::Disconnected));
        }
    }

    /// Selects one connected adapter instance that satisfies every requirement.
    ///
    /// The ordering is stable by UUID text so that HashMap iteration cannot
    /// influence a dispatch decision.
    pub async fn find_eligible(
        &self,
        requirements: &AdapterRequirements,
    ) -> Result<EligibleAdapter, AdapterEligibilityError> {
        self.select_eligible(requirements)
            .await
            .map(|(identity, _)| identity)
    }

    pub async fn execute(
        &self,
        action: ActionSpec,
        resolved_resource: Option<ResolvedResource>,
        timeout: Duration,
    ) -> Result<DispatchedActionResult, DispatchedActionError> {
        let requirements = AdapterRequirements::from_action(&action);
        let (identity, sender) =
            self.select_eligible(&requirements)
                .await
                .map_err(|error| DispatchedActionError {
                    identity: None,
                    kind: match error {
                        AdapterEligibilityError::Unavailable => {
                            AdapterDispatchErrorKind::Unavailable
                        }
                        AdapterEligibilityError::Ineligible => {
                            AdapterDispatchErrorKind::CapabilityUnavailable
                        }
                    },
                })?;

        let request_id = Uuid::new_v4();
        let action_execution_id = Uuid::new_v4();
        let request = AdapterRequestEnvelope {
            protocol_version: fubun_protocol::CURRENT_PROTOCOL_VERSION,
            request_id,
            action_execution_id,
            body: AdapterRequestBody::ActionExecute(ActionExecuteRequest {
                action,
                resolved_resource,
            }),
        };
        let (sender_reply, receiver_reply) = oneshot::channel();
        self.pending.lock().expect("pending mutex poisoned").insert(
            request_id,
            PendingRequest {
                identity: identity.clone(),
                action_execution_id,
                sender: sender_reply,
            },
        );
        let mut pending_guard = PendingRequestGuard::new(self.pending.clone(), request_id);
        if sender.send(request).await.is_err() {
            return Err(DispatchedActionError {
                identity: Some(identity),
                kind: AdapterDispatchErrorKind::Disconnected,
            });
        }
        match tokio::time::timeout(timeout, receiver_reply).await {
            Ok(Ok(Ok(result))) => {
                pending_guard.disarm();
                Ok(DispatchedActionResult { identity, result })
            }
            Ok(Ok(Err(kind))) => {
                pending_guard.disarm();
                Err(DispatchedActionError {
                    identity: Some(identity),
                    kind,
                })
            }
            Ok(Err(_)) => {
                pending_guard.disarm();
                Err(DispatchedActionError {
                    identity: Some(identity),
                    kind: AdapterDispatchErrorKind::Disconnected,
                })
            }
            Err(_) => Err(DispatchedActionError {
                identity: Some(identity),
                kind: AdapterDispatchErrorKind::Timeout,
            }),
        }
    }

    pub async fn resolve(&self, response: AdapterResponseEnvelope) {
        self.touch_by_request(response.request_id).await;
        let pending = self
            .pending
            .lock()
            .expect("pending mutex poisoned")
            .remove(&response.request_id);
        let Some(pending) = pending else {
            tracing::warn!(request_id = %response.request_id, "adapter response for unknown request");
            return;
        };
        let result = if pending.action_execution_id != response.action_execution_id {
            Err(AdapterDispatchErrorKind::Protocol)
        } else {
            match response.body {
                AdapterResponseBody::ActionResult(result) => Ok(result),
                AdapterResponseBody::Status(_) => Err(AdapterDispatchErrorKind::Protocol),
            }
        };
        let _ = pending.sender.send(result);
    }

    async fn touch_by_request(&self, request_id: Uuid) {
        let instance_id = self
            .pending
            .lock()
            .expect("pending mutex poisoned")
            .get(&request_id)
            .map(|pending| pending.identity.instance_id);
        if let Some(instance_id) = instance_id {
            self.touch(instance_id).await;
        }
    }

    pub async fn reports(&self) -> Vec<AdapterReport> {
        let mut reports: Vec<_> = self
            .connections
            .lock()
            .await
            .values()
            .map(|connection| AdapterReport {
                adapter_id: connection.hello.adapter_id.clone(),
                version: connection.hello.adapter_version.clone(),
                instance_id: connection.hello.instance_id,
                connected: true,
                capabilities: connection.hello.action_capabilities.clone(),
                connected_at: connection
                    .connected_at
                    .format(&time::format_description::well_known::Rfc3339)
                    .unwrap_or_default(),
                last_seen_at: connection
                    .last_seen_at
                    .format(&time::format_description::well_known::Rfc3339)
                    .unwrap_or_default(),
            })
            .collect();
        reports.sort_by_key(|report| report.instance_id.to_string());
        reports
    }

    pub async fn connected_count(&self) -> usize {
        self.connections.lock().await.len()
    }

    pub async fn tool_available(&self, tool: &str) -> bool {
        self.connections.lock().await.values().any(|connection| {
            connection
                .hello
                .status
                .tools
                .iter()
                .any(|candidate| candidate.name == tool && candidate.available)
        })
    }

    #[cfg(test)]
    pub fn pending_count(&self) -> usize {
        self.pending.lock().expect("pending mutex poisoned").len()
    }

    async fn select_eligible(
        &self,
        requirements: &AdapterRequirements,
    ) -> Result<(EligibleAdapter, mpsc::Sender<AdapterRequestEnvelope>), AdapterEligibilityError>
    {
        let connections = self.connections.lock().await;
        if connections.is_empty() {
            return Err(AdapterEligibilityError::Unavailable);
        }
        let mut candidates: Vec<_> = connections
            .iter()
            .filter(|(_, connection)| connection_satisfies(connection, requirements))
            .map(|(instance_id, connection)| {
                (
                    EligibleAdapter {
                        adapter_id: connection.hello.adapter_id.clone(),
                        instance_id: *instance_id,
                    },
                    connection.sender.clone(),
                )
            })
            .collect();
        candidates.sort_by_key(|(identity, _)| identity.instance_id.to_string());
        candidates
            .into_iter()
            .next()
            .ok_or(AdapterEligibilityError::Ineligible)
    }
}

fn connection_satisfies(
    connection: &AdapterConnection,
    requirements: &AdapterRequirements,
) -> bool {
    connection.hello.adapter_id == LINUX_ADAPTER_ID
        && connection
            .hello
            .action_capabilities
            .iter()
            .any(|candidate| candidate == requirements.capability)
        && connection
            .hello
            .status
            .tools
            .iter()
            .any(|candidate| candidate.name == requirements.required_tool && candidate.available)
        && requirements
            .desktop_entry_id
            .as_ref()
            .is_none_or(|entry_id| {
                connection
                    .hello
                    .status
                    .desktop_entry_ids
                    .iter()
                    .any(|candidate| candidate == entry_id)
            })
}

struct PendingRequestGuard {
    pending: Arc<StdMutex<HashMap<Uuid, PendingRequest>>>,
    request_id: Uuid,
    armed: bool,
}

impl PendingRequestGuard {
    fn new(pending: Arc<StdMutex<HashMap<Uuid, PendingRequest>>>, request_id: Uuid) -> Self {
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

impl Drop for PendingRequestGuard {
    fn drop(&mut self) {
        if self.armed {
            if let Ok(mut pending) = self.pending.lock() {
                pending.remove(&self.request_id);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fubun_domain::ActionSpec;
    use fubun_protocol::{
        AdapterResponseBody, AdapterResponseEnvelope, AdapterStatusSnapshot, AdapterToolStatus,
    };

    fn hello(instance_id: Uuid, capabilities: Vec<&str>) -> AdapterHello {
        hello_with_status(
            instance_id,
            capabilities,
            vec![("notify-send", true)],
            Vec::new(),
        )
    }

    fn hello_with_status(
        instance_id: Uuid,
        capabilities: Vec<&str>,
        tools: Vec<(&str, bool)>,
        desktop_entry_ids: Vec<&str>,
    ) -> AdapterHello {
        AdapterHello {
            adapter_id: "dev.fubun.linux".to_owned(),
            adapter_version: "test".to_owned(),
            instance_id,
            action_capabilities: capabilities.into_iter().map(str::to_owned).collect(),
            status: AdapterStatusSnapshot {
                tools: tools
                    .into_iter()
                    .map(|(name, available)| AdapterToolStatus {
                        name: name.to_owned(),
                        available,
                    })
                    .collect(),
                desktop_entry_ids: desktop_entry_ids.into_iter().map(str::to_owned).collect(),
            },
        }
    }

    #[tokio::test]
    async fn pending_guard_removes_request_when_dispatch_future_is_cancelled() {
        let manager = AdapterManager::default();
        let instance_id = Uuid::new_v4();
        let (sender, mut receiver) = mpsc::channel(1);
        manager
            .register(
                hello(instance_id, vec!["desktop.notification.show.v1"]),
                sender,
            )
            .await
            .expect("valid hello");
        let task_manager = manager.clone();
        let task = tokio::spawn(async move {
            task_manager
                .execute(
                    ActionSpec::DesktopNotificationShow {
                        title: "title".to_owned(),
                        body: "body".to_owned(),
                    },
                    None,
                    Duration::from_secs(30),
                )
                .await
        });
        receiver.recv().await.expect("request");
        assert_eq!(manager.pending_count(), 1);
        task.abort();
        let _ = task.await;
        assert_eq!(manager.pending_count(), 0);
    }

    #[tokio::test]
    async fn successful_response_disarms_pending_guard_and_returns_dispatch_identity() {
        let manager = AdapterManager::default();
        let instance_id = Uuid::new_v4();
        let (sender, mut receiver) = mpsc::channel(1);
        manager
            .register(
                hello(instance_id, vec!["desktop.notification.show.v1"]),
                sender,
            )
            .await
            .expect("valid hello");
        let execute_manager = manager.clone();
        let task = tokio::spawn(async move {
            execute_manager
                .execute(
                    ActionSpec::DesktopNotificationShow {
                        title: "title".to_owned(),
                        body: "body".to_owned(),
                    },
                    None,
                    Duration::from_secs(1),
                )
                .await
        });
        let request = receiver.recv().await.expect("request");
        manager
            .resolve(AdapterResponseEnvelope {
                protocol_version: fubun_protocol::CURRENT_PROTOCOL_VERSION,
                request_id: request.request_id,
                action_execution_id: request.action_execution_id,
                body: AdapterResponseBody::ActionResult(ActionResult {
                    status: fubun_protocol::AdapterActionStatus::Succeeded,
                    result_code: "ok".to_owned(),
                    redacted_message: "ok".to_owned(),
                }),
            })
            .await;
        let result = task.await.expect("task").expect("result");
        assert_eq!(result.identity.adapter_id, "dev.fubun.linux");
        assert_eq!(result.identity.instance_id, instance_id);
        assert_eq!(manager.pending_count(), 0);
    }

    #[tokio::test]
    async fn timeout_and_disconnect_drain_pending_requests() {
        let manager = AdapterManager::default();
        let instance_id = Uuid::new_v4();
        let (sender, mut receiver) = mpsc::channel(1);
        manager
            .register(
                hello(instance_id, vec!["desktop.notification.show.v1"]),
                sender,
            )
            .await
            .expect("valid hello");
        let timeout_manager = manager.clone();
        let timeout = tokio::spawn(async move {
            timeout_manager
                .execute(
                    ActionSpec::DesktopNotificationShow {
                        title: "title".to_owned(),
                        body: "body".to_owned(),
                    },
                    None,
                    Duration::from_millis(1),
                )
                .await
        });
        receiver.recv().await.expect("request");
        let timeout_error = timeout.await.expect("task").expect_err("timeout");
        assert_eq!(timeout_error.kind, AdapterDispatchErrorKind::Timeout);
        assert_eq!(
            timeout_error.identity.map(|identity| identity.instance_id),
            Some(instance_id)
        );
        assert_eq!(manager.pending_count(), 0);

        let disconnect_manager = manager.clone();
        let disconnect = tokio::spawn(async move {
            disconnect_manager
                .execute(
                    ActionSpec::DesktopNotificationShow {
                        title: "title".to_owned(),
                        body: "body".to_owned(),
                    },
                    None,
                    Duration::from_secs(30),
                )
                .await
        });
        receiver.recv().await.expect("request");
        manager.disconnect(instance_id).await;
        let disconnect_error = disconnect.await.expect("task").expect_err("disconnect");
        assert_eq!(
            disconnect_error.kind,
            AdapterDispatchErrorKind::Disconnected
        );
        assert_eq!(
            disconnect_error
                .identity
                .map(|identity| identity.instance_id),
            Some(instance_id)
        );
        assert_eq!(manager.pending_count(), 0);

        let shutdown_instance = Uuid::new_v4();
        let (shutdown_sender, mut shutdown_receiver) = mpsc::channel(1);
        manager
            .register(
                hello(shutdown_instance, vec!["desktop.notification.show.v1"]),
                shutdown_sender,
            )
            .await
            .expect("valid hello");
        let shutdown_manager = manager.clone();
        let shutdown_task = tokio::spawn(async move {
            shutdown_manager
                .execute(
                    ActionSpec::DesktopNotificationShow {
                        title: "title".to_owned(),
                        body: "body".to_owned(),
                    },
                    None,
                    Duration::from_secs(30),
                )
                .await
        });
        shutdown_receiver.recv().await.expect("request");
        manager.shutdown().await;
        let shutdown_error = shutdown_task.await.expect("task").expect_err("shutdown");
        assert_eq!(shutdown_error.kind, AdapterDispatchErrorKind::Disconnected);
        assert_eq!(
            shutdown_error.identity.map(|identity| identity.instance_id),
            Some(shutdown_instance)
        );
        assert_eq!(manager.pending_count(), 0);

        let send_failure_instance = Uuid::new_v4();
        let (send_failure_sender, send_failure_receiver) = mpsc::channel(1);
        drop(send_failure_receiver);
        manager
            .register(
                hello(send_failure_instance, vec!["desktop.notification.show.v1"]),
                send_failure_sender,
            )
            .await
            .expect("valid hello");
        let result = manager
            .execute(
                ActionSpec::DesktopNotificationShow {
                    title: "title".to_owned(),
                    body: "body".to_owned(),
                },
                None,
                Duration::from_secs(1),
            )
            .await;
        let send_failure = result.expect_err("send failure");
        assert_eq!(send_failure.kind, AdapterDispatchErrorKind::Disconnected);
        assert_eq!(
            send_failure.identity.map(|identity| identity.instance_id),
            Some(send_failure_instance)
        );
        assert_eq!(manager.pending_count(), 0);
    }

    #[tokio::test]
    async fn selection_requires_one_eligible_instance_and_is_deterministic() {
        let manager = AdapterManager::default();
        let unavailable_instance = Uuid::from_u128(1);
        let selected_instance = Uuid::from_u128(2);
        let later_instance = Uuid::from_u128(3);
        let (unavailable_sender, _unavailable_receiver) = mpsc::channel(1);
        let (selected_sender, mut selected_receiver) = mpsc::channel(1);
        let (later_sender, mut later_receiver) = mpsc::channel(1);
        manager
            .register(
                hello_with_status(
                    unavailable_instance,
                    vec!["desktop.notification.show.v1"],
                    vec![("notify-send", false)],
                    Vec::new(),
                ),
                unavailable_sender,
            )
            .await
            .expect("register unavailable tool");
        manager
            .register(
                hello_with_status(
                    selected_instance,
                    vec!["desktop.notification.show.v1"],
                    vec![("notify-send", true)],
                    Vec::new(),
                ),
                selected_sender,
            )
            .await
            .expect("register selected adapter");
        manager
            .register(
                hello_with_status(
                    later_instance,
                    vec!["desktop.notification.show.v1"],
                    vec![("notify-send", true)],
                    Vec::new(),
                ),
                later_sender,
            )
            .await
            .expect("register later adapter");
        let notification = AdapterRequirements::from_action(&ActionSpec::DesktopNotificationShow {
            title: "title".to_owned(),
            body: "body".to_owned(),
        });
        let selected = manager
            .find_eligible(&notification)
            .await
            .expect("eligible notification adapter");
        assert_eq!(selected.instance_id, selected_instance);
        let execute_manager = manager.clone();
        let dispatch = tokio::spawn(async move {
            execute_manager
                .execute(
                    ActionSpec::DesktopNotificationShow {
                        title: "title".to_owned(),
                        body: "body".to_owned(),
                    },
                    None,
                    Duration::from_secs(1),
                )
                .await
        });
        let request = selected_receiver.recv().await.expect("selected request");
        assert!(matches!(
            later_receiver.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ));
        manager
            .resolve(AdapterResponseEnvelope {
                protocol_version: fubun_protocol::CURRENT_PROTOCOL_VERSION,
                request_id: request.request_id,
                action_execution_id: request.action_execution_id,
                body: AdapterResponseBody::ActionResult(ActionResult {
                    status: fubun_protocol::AdapterActionStatus::Succeeded,
                    result_code: "ok".to_owned(),
                    redacted_message: "ok".to_owned(),
                }),
            })
            .await;
        assert_eq!(
            dispatch
                .await
                .expect("dispatch task")
                .expect("dispatch result")
                .identity
                .instance_id,
            selected_instance
        );

        let desktop_manager = AdapterManager::default();
        let missing_entry = Uuid::from_u128(10);
        let matching_entry = Uuid::from_u128(11);
        let (missing_sender, _missing_receiver) = mpsc::channel(1);
        let (matching_sender, _matching_receiver) = mpsc::channel(1);
        desktop_manager
            .register(
                hello_with_status(
                    missing_entry,
                    vec!["linux.app.ensure_running.v1"],
                    vec![("gtk-launch", true)],
                    Vec::new(),
                ),
                missing_sender,
            )
            .await
            .expect("register missing desktop entry");
        desktop_manager
            .register(
                hello_with_status(
                    matching_entry,
                    vec!["linux.app.ensure_running.v1"],
                    vec![("gtk-launch", true)],
                    vec!["code"],
                ),
                matching_sender,
            )
            .await
            .expect("register matching desktop entry");
        let app = AdapterRequirements::from_action(&ActionSpec::LinuxAppEnsureRunning {
            app_id: "code".to_owned(),
        });
        assert_eq!(
            desktop_manager
                .find_eligible(&app)
                .await
                .expect("matching desktop entry")
                .instance_id,
            matching_entry
        );

        let split_manager = AdapterManager::default();
        let (tool_sender, _tool_receiver) = mpsc::channel(1);
        let (entry_sender, _entry_receiver) = mpsc::channel(1);
        split_manager
            .register(
                hello_with_status(
                    Uuid::from_u128(20),
                    vec!["linux.app.ensure_running.v1"],
                    vec![("gtk-launch", true)],
                    Vec::new(),
                ),
                tool_sender,
            )
            .await
            .expect("register tool-only adapter");
        split_manager
            .register(
                hello_with_status(
                    Uuid::from_u128(21),
                    vec!["linux.app.ensure_running.v1"],
                    vec![("gtk-launch", false)],
                    vec!["code"],
                ),
                entry_sender,
            )
            .await
            .expect("register entry-only adapter");
        assert_eq!(
            split_manager.find_eligible(&app).await,
            Err(AdapterEligibilityError::Ineligible)
        );
    }

    #[tokio::test]
    async fn protocol_error_retains_selected_adapter_identity() {
        let manager = AdapterManager::default();
        let instance_id = Uuid::new_v4();
        let (sender, mut receiver) = mpsc::channel(1);
        manager
            .register(
                hello(instance_id, vec!["desktop.notification.show.v1"]),
                sender,
            )
            .await
            .expect("valid hello");
        let execute_manager = manager.clone();
        let task = tokio::spawn(async move {
            execute_manager
                .execute(
                    ActionSpec::DesktopNotificationShow {
                        title: "title".to_owned(),
                        body: "body".to_owned(),
                    },
                    None,
                    Duration::from_secs(1),
                )
                .await
        });
        let request = receiver.recv().await.expect("request");
        manager
            .resolve(AdapterResponseEnvelope {
                protocol_version: fubun_protocol::CURRENT_PROTOCOL_VERSION,
                request_id: request.request_id,
                action_execution_id: Uuid::new_v4(),
                body: AdapterResponseBody::ActionResult(ActionResult {
                    status: fubun_protocol::AdapterActionStatus::Succeeded,
                    result_code: "ok".to_owned(),
                    redacted_message: "ok".to_owned(),
                }),
            })
            .await;
        let error = task.await.expect("task").expect_err("protocol error");
        assert_eq!(error.kind, AdapterDispatchErrorKind::Protocol);
        assert_eq!(
            error.identity.map(|identity| identity.instance_id),
            Some(instance_id)
        );
        assert_eq!(manager.pending_count(), 0);
    }

    #[tokio::test]
    async fn unavailable_dispatch_has_no_adapter_identity() {
        let manager = AdapterManager::default();
        let error = manager
            .execute(
                ActionSpec::DesktopNotificationShow {
                    title: "title".to_owned(),
                    body: "body".to_owned(),
                },
                None,
                Duration::from_secs(1),
            )
            .await
            .expect_err("no adapter is connected");
        assert_eq!(error.kind, AdapterDispatchErrorKind::Unavailable);
        assert_eq!(error.identity, None);
        assert_eq!(manager.pending_count(), 0);
    }

    #[test]
    fn adapter_hello_rejects_unknown_and_duplicate_capabilities() {
        let id = Uuid::new_v4();
        assert_eq!(
            AdapterManager::validate_hello(&hello(id, vec!["unknown.v1"])),
            Err(AdapterRegistrationError::UnknownCapability)
        );
        assert_eq!(
            AdapterManager::validate_hello(&hello(
                id,
                vec![
                    "desktop.notification.show.v1",
                    "desktop.notification.show.v1"
                ]
            )),
            Err(AdapterRegistrationError::DuplicateCapability)
        );
        let mut invalid_version = hello(id, vec!["desktop.notification.show.v1"]);
        invalid_version.adapter_version.clear();
        assert_eq!(
            AdapterManager::validate_hello(&invalid_version),
            Err(AdapterRegistrationError::InvalidVersion)
        );
        let mut invalid_adapter = hello(id, vec!["desktop.notification.show.v1"]);
        invalid_adapter.adapter_id = "dev.fubun.unknown".to_owned();
        assert_eq!(
            AdapterManager::validate_hello(&invalid_adapter),
            Err(AdapterRegistrationError::UnknownAdapter)
        );
        let mut empty_capabilities = hello(id, vec!["desktop.notification.show.v1"]);
        empty_capabilities.action_capabilities.clear();
        assert_eq!(
            AdapterManager::validate_hello(&empty_capabilities),
            Err(AdapterRegistrationError::InvalidCapabilityCount)
        );
    }
}
