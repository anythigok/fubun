//! Bidirectional adapter registry and bounded action dispatch.

use std::{collections::HashMap, sync::Arc, time::Duration};

use fubun_domain::ActionSpec;
use fubun_protocol::{
    ActionExecuteRequest, ActionResult, AdapterHello, AdapterReport, AdapterRequestBody,
    AdapterRequestEnvelope, AdapterResponseBody, AdapterResponseEnvelope, ResolvedResource,
};
use thiserror::Error;
use time::OffsetDateTime;
use tokio::sync::{mpsc, oneshot, Mutex};
use uuid::Uuid;

#[derive(Debug, Error)]
pub enum AdapterDispatchError {
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

struct PendingRequest {
    adapter_instance_id: Uuid,
    action_execution_id: Uuid,
    sender: oneshot::Sender<Result<ActionResult, AdapterDispatchError>>,
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
    pending: Arc<Mutex<HashMap<Uuid, PendingRequest>>>,
}

impl AdapterManager {
    pub async fn register(
        &self,
        hello: AdapterHello,
        sender: mpsc::Sender<AdapterRequestEnvelope>,
    ) {
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
    }

    pub async fn touch(&self, instance_id: Uuid) {
        if let Some(connection) = self.connections.lock().await.get_mut(&instance_id) {
            connection.last_seen_at = OffsetDateTime::now_utc();
        }
    }

    pub async fn disconnect(&self, instance_id: Uuid) {
        self.connections.lock().await.remove(&instance_id);
        let mut pending = self.pending.lock().await;
        let requests: Vec<_> = pending
            .iter()
            .filter_map(|(id, request)| (request.adapter_instance_id == instance_id).then_some(*id))
            .collect();
        for request_id in requests {
            if let Some(request) = pending.remove(&request_id) {
                let _ = request.sender.send(Err(AdapterDispatchError::Disconnected));
            }
        }
    }

    pub async fn execute(
        &self,
        action: ActionSpec,
        resolved_resource: Option<ResolvedResource>,
        timeout: Duration,
    ) -> Result<ActionResult, AdapterDispatchError> {
        let capability = action.action_type();
        let (instance_id, sender) = {
            let connections = self.connections.lock().await;
            let Some((instance_id, connection)) = connections.iter().find(|(_, connection)| {
                connection
                    .hello
                    .action_capabilities
                    .iter()
                    .any(|candidate| candidate == capability)
            }) else {
                if connections.is_empty() {
                    return Err(AdapterDispatchError::Unavailable);
                }
                return Err(AdapterDispatchError::CapabilityUnavailable);
            };
            (*instance_id, connection.sender.clone())
        };

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
        self.pending.lock().await.insert(
            request_id,
            PendingRequest {
                adapter_instance_id: instance_id,
                action_execution_id,
                sender: sender_reply,
            },
        );
        if sender.send(request).await.is_err() {
            self.pending.lock().await.remove(&request_id);
            return Err(AdapterDispatchError::Disconnected);
        }
        match tokio::time::timeout(timeout, receiver_reply).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(AdapterDispatchError::Disconnected),
            Err(_) => {
                self.pending.lock().await.remove(&request_id);
                Err(AdapterDispatchError::Timeout)
            }
        }
    }

    pub async fn resolve(&self, response: AdapterResponseEnvelope) {
        self.touch_by_request(response.request_id).await;
        let pending = self.pending.lock().await.remove(&response.request_id);
        let Some(pending) = pending else {
            tracing::warn!(request_id = %response.request_id, "adapter response for unknown request");
            return;
        };
        let result = if pending.action_execution_id != response.action_execution_id {
            Err(AdapterDispatchError::Protocol)
        } else {
            match response.body {
                AdapterResponseBody::ActionResult(result) => Ok(result),
                AdapterResponseBody::Status(_) => Err(AdapterDispatchError::Protocol),
            }
        };
        let _ = pending.sender.send(result);
    }

    async fn touch_by_request(&self, request_id: Uuid) {
        let instance_id = self
            .pending
            .lock()
            .await
            .get(&request_id)
            .map(|pending| pending.adapter_instance_id);
        if let Some(instance_id) = instance_id {
            self.touch(instance_id).await;
        }
    }

    pub async fn reports(&self) -> Vec<AdapterReport> {
        self.connections
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
            .collect()
    }

    pub async fn capability_available(&self, capability: &str) -> bool {
        self.connections.lock().await.values().any(|connection| {
            connection
                .hello
                .action_capabilities
                .iter()
                .any(|candidate| candidate == capability)
        })
    }

    pub async fn desktop_entry_available(&self, app_id: &str) -> Option<bool> {
        let connections = self.connections.lock().await;
        let mut connected = false;
        for connection in connections.values() {
            if connection
                .hello
                .action_capabilities
                .iter()
                .any(|candidate| candidate == "linux.app.ensure_running.v1")
            {
                connected = true;
                if connection
                    .hello
                    .status
                    .desktop_entry_ids
                    .iter()
                    .any(|candidate| candidate == app_id)
                {
                    return Some(true);
                }
            }
        }
        connected.then_some(false)
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
}
