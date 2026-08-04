use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use fubun_core::{paths::FubunPaths, start_server, ClientError, FubunClient};
use fubun_domain::{
    ActionSpec, ExecutionMode, ExecutionStatus, ExecutionStep, ExecutionStepStatus, FailureMode,
    ResourceKind, RitualDefinition, RitualExecutionConfig, Sensitivity, RITUAL_SCHEMA_VERSION,
};
use fubun_protocol::{
    read_json_frame, write_json_frame, ActionResult, AdapterActionStatus, AdapterHello,
    AdapterRequestBody, AdapterRequestEnvelope, AdapterResponseBody, AdapterResponseEnvelope,
    AdapterStatusSnapshot, AdapterToolStatus, ClientHelloAck, RequestBody, RequestEnvelope,
    ResourceCreateRequest, ResponseBody, ResponseEnvelope, ResponsePayload, RitualActivateRequest,
    RitualCreateRequest, RitualIdRequest, RitualUpdateRequest, CURRENT_PROTOCOL_VERSION,
};
use tempfile::TempDir;
use tokio::net::UnixStream;
use uuid::Uuid;

fn paths(temp: &TempDir) -> FubunPaths {
    FubunPaths::from_xdg_roots(temp.path().join("runtime"), temp.path().join("data"))
        .expect("paths")
}

struct TestAdapter {
    instance_id: Uuid,
    task: tokio::task::JoinHandle<()>,
}

impl TestAdapter {
    fn abort(&self) {
        self.task.abort();
    }
}

fn assert_terminal_steps(steps: &[ExecutionStep]) {
    assert!(steps.iter().all(|step| {
        matches!(
            step.status,
            ExecutionStepStatus::Succeeded
                | ExecutionStepStatus::Skipped
                | ExecutionStepStatus::Failed
                | ExecutionStepStatus::Aborted
        )
    }));
}

async fn fake_adapter(
    paths: &FubunPaths,
    failures: Arc<AtomicUsize>,
    fail_at: Option<usize>,
) -> TestAdapter {
    let mut stream = UnixStream::connect(&paths.socket_path)
        .await
        .expect("adapter socket");
    let instance_id = Uuid::new_v4();
    let hello = RequestEnvelope {
        protocol_version: CURRENT_PROTOCOL_VERSION,
        request_id: Uuid::new_v4(),
        body: RequestBody::AdapterHello(AdapterHello {
            adapter_id: "dev.fubun.linux".to_owned(),
            adapter_version: "test".to_owned(),
            instance_id,
            action_capabilities: vec![
                "linux.app.ensure_running.v1".to_owned(),
                "linux.path.open.v1".to_owned(),
                "desktop.notification.show.v1".to_owned(),
            ],
            event_capabilities: Vec::new(),
            status: AdapterStatusSnapshot {
                tools: vec![
                    AdapterToolStatus {
                        name: "gtk-launch".to_owned(),
                        available: true,
                    },
                    AdapterToolStatus {
                        name: "xdg-open".to_owned(),
                        available: true,
                    },
                    AdapterToolStatus {
                        name: "notify-send".to_owned(),
                        available: true,
                    },
                ],
                desktop_entry_ids: vec!["code".to_owned()],
                permitted_resource_ids: Vec::new(),
            },
        }),
    };
    let id = hello.request_id;
    write_json_frame(&mut stream, &hello).await.expect("hello");
    let ack: ResponseEnvelope = read_json_frame(&mut stream)
        .await
        .expect("ack")
        .expect("ack value");
    assert_eq!(ack.request_id, id);
    assert!(matches!(
        ack.body,
        ResponseBody::Ok(ResponsePayload::AdapterHelloAck(ClientHelloAck { .. }))
    ));
    let task = tokio::spawn(async move {
        while let Ok(Some(request)) =
            read_json_frame::<_, AdapterRequestEnvelope>(&mut stream).await
        {
            let index = failures.fetch_add(1, Ordering::SeqCst);
            let (status, code) = if fail_at == Some(index) {
                (AdapterActionStatus::Failed, "failed")
            } else {
                (AdapterActionStatus::Succeeded, "ok")
            };
            let AdapterRequestBody::ActionExecute(_) = request.body else {
                continue;
            };
            let response = AdapterResponseEnvelope {
                protocol_version: CURRENT_PROTOCOL_VERSION,
                request_id: request.request_id,
                action_execution_id: request.action_execution_id,
                body: AdapterResponseBody::ActionResult(ActionResult {
                    status,
                    result_code: code.to_owned(),
                    redacted_message: "fake result".to_owned(),
                }),
            };
            if write_json_frame(&mut stream, &response).await.is_err() {
                break;
            }
        }
    });
    TestAdapter { instance_id, task }
}

async fn disconnecting_adapter(paths: &FubunPaths) -> TestAdapter {
    let mut stream = UnixStream::connect(&paths.socket_path)
        .await
        .expect("adapter socket");
    let instance_id = Uuid::new_v4();
    let hello = RequestEnvelope {
        protocol_version: CURRENT_PROTOCOL_VERSION,
        request_id: Uuid::new_v4(),
        body: RequestBody::AdapterHello(AdapterHello {
            adapter_id: "dev.fubun.linux".to_owned(),
            adapter_version: "disconnect-test".to_owned(),
            instance_id,
            action_capabilities: vec!["desktop.notification.show.v1".to_owned()],
            event_capabilities: Vec::new(),
            status: AdapterStatusSnapshot {
                tools: vec![AdapterToolStatus {
                    name: "notify-send".to_owned(),
                    available: true,
                }],
                desktop_entry_ids: vec![],
                permitted_resource_ids: Vec::new(),
            },
        }),
    };
    let id = hello.request_id;
    write_json_frame(&mut stream, &hello).await.expect("hello");
    let ack: ResponseEnvelope = read_json_frame(&mut stream)
        .await
        .expect("ack")
        .expect("ack value");
    assert_eq!(ack.request_id, id);
    assert!(matches!(
        ack.body,
        ResponseBody::Ok(ResponsePayload::AdapterHelloAck(ClientHelloAck { .. }))
    ));
    let task = tokio::spawn(async move {
        let _ = read_json_frame::<_, AdapterRequestEnvelope>(&mut stream).await;
        drop(stream);
    });
    TestAdapter { instance_id, task }
}

async fn limited_adapter(
    paths: &FubunPaths,
    capabilities: Vec<&str>,
    unavailable_tool: &str,
) -> TestAdapter {
    let mut stream = UnixStream::connect(&paths.socket_path)
        .await
        .expect("adapter socket");
    let instance_id = Uuid::new_v4();
    let hello = RequestEnvelope {
        protocol_version: CURRENT_PROTOCOL_VERSION,
        request_id: Uuid::new_v4(),
        body: RequestBody::AdapterHello(AdapterHello {
            adapter_id: "dev.fubun.linux".to_owned(),
            adapter_version: "limited-test".to_owned(),
            instance_id,
            action_capabilities: capabilities.into_iter().map(str::to_owned).collect(),
            event_capabilities: Vec::new(),

            status: AdapterStatusSnapshot {
                tools: ["gtk-launch", "xdg-open", "notify-send"]
                    .into_iter()
                    .map(|name| AdapterToolStatus {
                        name: name.to_owned(),
                        available: name != unavailable_tool,
                    })
                    .collect(),
                desktop_entry_ids: vec!["code".to_owned()],
                permitted_resource_ids: Vec::new(),
            },
        }),
    };
    let id = hello.request_id;
    write_json_frame(&mut stream, &hello).await.expect("hello");
    let ack: ResponseEnvelope = read_json_frame(&mut stream)
        .await
        .expect("ack")
        .expect("ack value");
    assert_eq!(ack.request_id, id);
    assert!(matches!(
        ack.body,
        ResponseBody::Ok(ResponsePayload::AdapterHelloAck(ClientHelloAck { .. }))
    ));
    let task = tokio::spawn(async move {
        while let Ok(Some(request)) =
            read_json_frame::<_, AdapterRequestEnvelope>(&mut stream).await
        {
            let AdapterRequestBody::ActionExecute(_) = request.body else {
                continue;
            };
            let response = AdapterResponseEnvelope {
                protocol_version: CURRENT_PROTOCOL_VERSION,
                request_id: request.request_id,
                action_execution_id: request.action_execution_id,
                body: AdapterResponseBody::ActionResult(ActionResult {
                    status: AdapterActionStatus::Succeeded,
                    result_code: "ok".to_owned(),
                    redacted_message: "limited fake result".to_owned(),
                }),
            };
            if write_json_frame(&mut stream, &response).await.is_err() {
                break;
            }
        }
    });
    TestAdapter { instance_id, task }
}

async fn status_adapter(
    paths: &FubunPaths,
    adapter_version: &str,
    capabilities: Vec<&str>,
    tools: Vec<(&str, bool)>,
    desktop_entry_ids: Vec<&str>,
    requests: Arc<AtomicUsize>,
) -> TestAdapter {
    let mut stream = UnixStream::connect(&paths.socket_path)
        .await
        .expect("adapter socket");
    let instance_id = Uuid::new_v4();
    let hello = RequestEnvelope {
        protocol_version: CURRENT_PROTOCOL_VERSION,
        request_id: Uuid::new_v4(),
        body: RequestBody::AdapterHello(AdapterHello {
            adapter_id: "dev.fubun.linux".to_owned(),
            adapter_version: adapter_version.to_owned(),
            instance_id,
            action_capabilities: capabilities.into_iter().map(str::to_owned).collect(),
            event_capabilities: Vec::new(),

            status: AdapterStatusSnapshot {
                tools: tools
                    .into_iter()
                    .map(|(name, available)| AdapterToolStatus {
                        name: name.to_owned(),
                        available,
                    })
                    .collect(),
                desktop_entry_ids: desktop_entry_ids.into_iter().map(str::to_owned).collect(),
                permitted_resource_ids: Vec::new(),
            },
        }),
    };
    let id = hello.request_id;
    write_json_frame(&mut stream, &hello).await.expect("hello");
    let ack: ResponseEnvelope = read_json_frame(&mut stream)
        .await
        .expect("ack")
        .expect("ack value");
    assert_eq!(ack.request_id, id);
    assert!(matches!(
        ack.body,
        ResponseBody::Ok(ResponsePayload::AdapterHelloAck(ClientHelloAck { .. }))
    ));
    let task = tokio::spawn(async move {
        while let Ok(Some(request)) =
            read_json_frame::<_, AdapterRequestEnvelope>(&mut stream).await
        {
            let AdapterRequestBody::ActionExecute(_) = request.body else {
                continue;
            };
            requests.fetch_add(1, Ordering::SeqCst);
            let response = AdapterResponseEnvelope {
                protocol_version: CURRENT_PROTOCOL_VERSION,
                request_id: request.request_id,
                action_execution_id: request.action_execution_id,
                body: AdapterResponseBody::ActionResult(ActionResult {
                    status: AdapterActionStatus::Succeeded,
                    result_code: "ok".to_owned(),
                    redacted_message: "status adapter result".to_owned(),
                }),
            };
            if write_json_frame(&mut stream, &response).await.is_err() {
                break;
            }
        }
    });
    TestAdapter { instance_id, task }
}

async fn protocol_error_adapter(paths: &FubunPaths) -> TestAdapter {
    let mut stream = UnixStream::connect(&paths.socket_path)
        .await
        .expect("adapter socket");
    let instance_id = Uuid::new_v4();
    let hello = RequestEnvelope {
        protocol_version: CURRENT_PROTOCOL_VERSION,
        request_id: Uuid::new_v4(),
        body: RequestBody::AdapterHello(AdapterHello {
            adapter_id: "dev.fubun.linux".to_owned(),
            adapter_version: "protocol-error-test".to_owned(),
            instance_id,
            action_capabilities: vec!["desktop.notification.show.v1".to_owned()],
            event_capabilities: Vec::new(),
            status: AdapterStatusSnapshot {
                tools: vec![AdapterToolStatus {
                    name: "notify-send".to_owned(),
                    available: true,
                }],
                desktop_entry_ids: Vec::new(),
                permitted_resource_ids: Vec::new(),
            },
        }),
    };
    let id = hello.request_id;
    write_json_frame(&mut stream, &hello).await.expect("hello");
    let ack: ResponseEnvelope = read_json_frame(&mut stream)
        .await
        .expect("ack")
        .expect("ack value");
    assert_eq!(ack.request_id, id);
    assert!(matches!(
        ack.body,
        ResponseBody::Ok(ResponsePayload::AdapterHelloAck(ClientHelloAck { .. }))
    ));
    let task = tokio::spawn(async move {
        let Ok(Some(request)) = read_json_frame::<_, AdapterRequestEnvelope>(&mut stream).await
        else {
            return;
        };
        let AdapterRequestBody::ActionExecute(_) = request.body else {
            return;
        };
        let response = AdapterResponseEnvelope {
            protocol_version: CURRENT_PROTOCOL_VERSION,
            request_id: request.request_id,
            action_execution_id: Uuid::new_v4(),
            body: AdapterResponseBody::ActionResult(ActionResult {
                status: AdapterActionStatus::Succeeded,
                result_code: "ok".to_owned(),
                redacted_message: "protocol error fixture".to_owned(),
            }),
        };
        let _ = write_json_frame(&mut stream, &response).await;
    });
    TestAdapter { instance_id, task }
}

async fn silent_adapter(paths: &FubunPaths) -> TestAdapter {
    let mut stream = UnixStream::connect(&paths.socket_path)
        .await
        .expect("adapter socket");
    let instance_id = Uuid::new_v4();
    let hello = RequestEnvelope {
        protocol_version: CURRENT_PROTOCOL_VERSION,
        request_id: Uuid::new_v4(),
        body: RequestBody::AdapterHello(AdapterHello {
            adapter_id: "dev.fubun.linux".to_owned(),
            adapter_version: "silent-test".to_owned(),
            instance_id,
            action_capabilities: vec!["desktop.notification.show.v1".to_owned()],
            event_capabilities: Vec::new(),
            status: AdapterStatusSnapshot {
                tools: vec![AdapterToolStatus {
                    name: "notify-send".to_owned(),
                    available: true,
                }],
                desktop_entry_ids: Vec::new(),
                permitted_resource_ids: Vec::new(),
            },
        }),
    };
    let id = hello.request_id;
    write_json_frame(&mut stream, &hello).await.expect("hello");
    let ack: ResponseEnvelope = read_json_frame(&mut stream)
        .await
        .expect("ack")
        .expect("ack value");
    assert_eq!(ack.request_id, id);
    assert!(matches!(
        ack.body,
        ResponseBody::Ok(ResponsePayload::AdapterHelloAck(ClientHelloAck { .. }))
    ));
    let task = tokio::spawn(async move {
        let _ = read_json_frame::<_, AdapterRequestEnvelope>(&mut stream).await;
        tokio::time::sleep(Duration::from_secs(15)).await;
    });
    TestAdapter { instance_id, task }
}

#[tokio::test]
async fn ritual_preview_activation_manual_run_and_stale_approval() {
    let temp = TempDir::new().expect("tempdir");
    let paths = paths(&temp);
    let server = start_server(paths.clone()).await.expect("server");
    let counter = Arc::new(AtomicUsize::new(0));
    let adapter = fake_adapter(&paths, counter.clone(), None).await;
    let resource_dir = temp.path().join("research");
    std::fs::create_dir(&resource_dir).expect("resource");
    let mut client = FubunClient::connect(&paths.socket_path, "integration", "test")
        .await
        .expect("client");
    let resource = match client
        .request(RequestBody::ResourceCreate(ResourceCreateRequest {
            label: "research".to_owned(),
            path: resource_dir.to_string_lossy().into_owned(),
            sensitivity: Sensitivity::Normal,
        }))
        .await
        .expect("resource")
    {
        ResponsePayload::ResourceCreated(resource) => resource,
        _ => panic!("resource payload"),
    };
    assert_eq!(resource.kind, ResourceKind::Directory);
    let definition = RitualDefinition {
        schema_version: RITUAL_SCHEMA_VERSION.to_owned(),
        name: "Research Start".to_owned(),
        actions: vec![
            ActionSpec::LinuxAppEnsureRunning {
                app_id: "code".to_owned(),
            },
            ActionSpec::LinuxPathOpen {
                resource_id: resource.id,
            },
            ActionSpec::DesktopNotificationShow {
                title: "Fubun".to_owned(),
                body: "done".to_owned(),
            },
        ],
        execution: RitualExecutionConfig {
            mode: ExecutionMode::Sequential,
            on_failure: FailureMode::Stop,
            timeout_seconds: 30,
        },
    };
    let record = match client
        .request(RequestBody::RitualCreate(RitualCreateRequest {
            definition: definition.clone(),
        }))
        .await
        .expect("create")
    {
        ResponsePayload::RitualCreated(record) => record,
        _ => panic!("ritual payload"),
    };
    let preview = match client
        .request(RequestBody::RitualPreview(RitualIdRequest {
            ritual_id: record.ritual.id,
        }))
        .await
        .expect("preview")
    {
        ResponsePayload::RitualPreview(preview) => preview,
        _ => panic!("preview payload"),
    };
    assert!(preview.executable);
    assert_eq!(counter.load(Ordering::SeqCst), 0);
    let no_approval = client
        .request(RequestBody::RitualActivate(RitualActivateRequest {
            ritual_id: record.ritual.id,
            approve: false,
        }))
        .await
        .expect_err("approval required");
    assert!(
        matches!(no_approval, ClientError::Rejected { ref code, .. } if code == "approval_required")
    );
    let activated = client
        .request(RequestBody::RitualActivate(RitualActivateRequest {
            ritual_id: record.ritual.id,
            approve: true,
        }))
        .await
        .expect("activate");
    assert!(matches!(activated, ResponsePayload::RitualActivated(_)));
    let run = client
        .request(RequestBody::RitualRun(RitualIdRequest {
            ritual_id: record.ritual.id,
        }))
        .await
        .expect("run");
    let ResponsePayload::RitualRun(result) = run else {
        panic!("run payload")
    };
    assert_eq!(result.execution.status, ExecutionStatus::Succeeded);
    assert_eq!(result.steps.len(), 3);
    assert_eq!(counter.load(Ordering::SeqCst), 3);
    assert!(result
        .steps
        .iter()
        .all(|step| step.adapter_id.as_deref() == Some("dev.fubun.linux")));
    assert!(result
        .steps
        .iter()
        .all(|step| step.adapter_instance_id == Some(adapter.instance_id)));
    assert_terminal_steps(&result.steps);
    let mut updated_definition = definition;
    updated_definition.name = "Research Start Updated".to_owned();
    let updated = client
        .request(RequestBody::RitualUpdate(RitualUpdateRequest {
            ritual_id: record.ritual.id,
            definition: updated_definition,
        }))
        .await
        .expect("update");
    let ResponsePayload::RitualUpdated(updated) = updated else {
        panic!("updated ritual payload")
    };
    assert_eq!(updated.ritual.status, fubun_domain::RitualStatus::Draft);
    let stale = client
        .request(RequestBody::RitualRun(RitualIdRequest {
            ritual_id: record.ritual.id,
        }))
        .await
        .expect_err("updated ritual needs approval");
    assert!(matches!(stale, ClientError::Rejected { ref code, .. } if code == "ritual_not_active"));
    drop(client);
    adapter.abort();
    server.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn paused_ritual_is_not_executable() {
    let temp = TempDir::new().expect("tempdir");
    let paths = paths(&temp);
    let server = start_server(paths.clone()).await.expect("server");
    let adapter = fake_adapter(&paths, Arc::new(AtomicUsize::new(0)), None).await;
    let mut client = FubunClient::connect(&paths.socket_path, "integration", "test")
        .await
        .expect("client");
    let resource_dir = temp.path().join("research");
    std::fs::create_dir(&resource_dir).expect("resource");
    let resource = match client
        .request(RequestBody::ResourceCreate(ResourceCreateRequest {
            label: "research".to_owned(),
            path: resource_dir.to_string_lossy().into_owned(),
            sensitivity: Sensitivity::Normal,
        }))
        .await
        .expect("resource")
    {
        ResponsePayload::ResourceCreated(resource) => resource,
        _ => panic!("resource"),
    };
    let definition = RitualDefinition {
        schema_version: RITUAL_SCHEMA_VERSION.to_owned(),
        name: "Pause".to_owned(),
        actions: vec![ActionSpec::LinuxPathOpen {
            resource_id: resource.id,
        }],
        execution: RitualExecutionConfig {
            mode: ExecutionMode::Sequential,
            on_failure: FailureMode::Stop,
            timeout_seconds: 10,
        },
    };
    let record = match client
        .request(RequestBody::RitualCreate(RitualCreateRequest {
            definition,
        }))
        .await
        .expect("create")
    {
        ResponsePayload::RitualCreated(record) => record,
        _ => panic!("ritual"),
    };
    client
        .request(RequestBody::RitualActivate(RitualActivateRequest {
            ritual_id: record.ritual.id,
            approve: true,
        }))
        .await
        .expect("activate");
    client
        .request(RequestBody::RitualPause(RitualIdRequest {
            ritual_id: record.ritual.id,
        }))
        .await
        .expect("pause");
    let result = client
        .request(RequestBody::RitualRun(RitualIdRequest {
            ritual_id: record.ritual.id,
        }))
        .await
        .expect_err("paused run");
    assert!(
        matches!(result, ClientError::Rejected { ref code, .. } if code == "ritual_not_active")
    );
    drop(client);
    adapter.abort();
    server.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn action_failure_stops_following_steps_and_records_partial() {
    let temp = TempDir::new().expect("tempdir");
    let paths = paths(&temp);
    let server = start_server(paths.clone()).await.expect("server");
    let counter = Arc::new(AtomicUsize::new(0));
    let adapter = fake_adapter(&paths, counter.clone(), Some(1)).await;
    let resource_dir = temp.path().join("research");
    std::fs::create_dir(&resource_dir).expect("resource");
    let mut client = FubunClient::connect(&paths.socket_path, "integration", "test")
        .await
        .expect("client");
    let resource = match client
        .request(RequestBody::ResourceCreate(ResourceCreateRequest {
            label: "research".to_owned(),
            path: resource_dir.to_string_lossy().into_owned(),
            sensitivity: Sensitivity::Normal,
        }))
        .await
        .expect("resource")
    {
        ResponsePayload::ResourceCreated(resource) => resource,
        _ => panic!("resource"),
    };
    let definition = RitualDefinition {
        schema_version: RITUAL_SCHEMA_VERSION.to_owned(),
        name: "Partial".to_owned(),
        actions: vec![
            ActionSpec::DesktopNotificationShow {
                title: "one".to_owned(),
                body: "one".to_owned(),
            },
            ActionSpec::LinuxPathOpen {
                resource_id: resource.id,
            },
            ActionSpec::DesktopNotificationShow {
                title: "three".to_owned(),
                body: "three".to_owned(),
            },
        ],
        execution: RitualExecutionConfig {
            mode: ExecutionMode::Sequential,
            on_failure: FailureMode::Stop,
            timeout_seconds: 10,
        },
    };
    let record = match client
        .request(RequestBody::RitualCreate(RitualCreateRequest {
            definition,
        }))
        .await
        .expect("create")
    {
        ResponsePayload::RitualCreated(record) => record,
        _ => panic!("ritual"),
    };
    client
        .request(RequestBody::RitualActivate(RitualActivateRequest {
            ritual_id: record.ritual.id,
            approve: true,
        }))
        .await
        .expect("activate");
    let result = client
        .request(RequestBody::RitualRun(RitualIdRequest {
            ritual_id: record.ritual.id,
        }))
        .await
        .expect("execution history");
    let ResponsePayload::RitualRun(result) = result else {
        panic!("execution")
    };
    assert_eq!(result.execution.status, ExecutionStatus::Partial);
    assert_eq!(counter.load(Ordering::SeqCst), 2);
    assert_eq!(result.steps.len(), 3);
    assert_eq!(
        result.steps[0].adapter_id.as_deref(),
        Some("dev.fubun.linux")
    );
    assert_eq!(
        result.steps[1].adapter_id.as_deref(),
        Some("dev.fubun.linux")
    );
    assert_eq!(result.steps[2].adapter_id, None);
    assert_eq!(result.steps[2].status, ExecutionStepStatus::Aborted);
    assert_eq!(
        result.steps[2].result_code.as_deref(),
        Some("stopped_after_failure")
    );
    assert_eq!(
        result.steps[2].redacted_message.as_deref(),
        Some("action was not executed because an earlier action failed")
    );
    assert_terminal_steps(&result.steps);
    drop(client);
    adapter.abort();
    server.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn first_action_failure_aborts_every_unstarted_step() {
    let temp = TempDir::new().expect("tempdir");
    let paths = paths(&temp);
    let server = start_server(paths.clone()).await.expect("server");
    let adapter = fake_adapter(&paths, Arc::new(AtomicUsize::new(0)), Some(0)).await;
    let mut client = FubunClient::connect(&paths.socket_path, "integration", "test")
        .await
        .expect("client");
    let definition = RitualDefinition {
        schema_version: RITUAL_SCHEMA_VERSION.to_owned(),
        name: "First failure".to_owned(),
        actions: vec![
            ActionSpec::DesktopNotificationShow {
                title: "one".to_owned(),
                body: "one".to_owned(),
            },
            ActionSpec::DesktopNotificationShow {
                title: "two".to_owned(),
                body: "two".to_owned(),
            },
        ],
        execution: RitualExecutionConfig {
            mode: ExecutionMode::Sequential,
            on_failure: FailureMode::Stop,
            timeout_seconds: 10,
        },
    };
    let record = match client
        .request(RequestBody::RitualCreate(RitualCreateRequest {
            definition,
        }))
        .await
        .expect("create")
    {
        ResponsePayload::RitualCreated(record) => record,
        _ => panic!("ritual"),
    };
    client
        .request(RequestBody::RitualActivate(RitualActivateRequest {
            ritual_id: record.ritual.id,
            approve: true,
        }))
        .await
        .expect("activate");
    let result = client
        .request(RequestBody::RitualRun(RitualIdRequest {
            ritual_id: record.ritual.id,
        }))
        .await
        .expect("run");
    let ResponsePayload::RitualRun(result) = result else {
        panic!("execution")
    };
    assert_eq!(result.execution.status, ExecutionStatus::Failed);
    assert_eq!(result.steps[0].status, ExecutionStepStatus::Failed);
    assert_eq!(result.steps[1].status, ExecutionStepStatus::Aborted);
    assert_eq!(
        result.steps[1].result_code.as_deref(),
        Some("stopped_after_failure")
    );
    assert_terminal_steps(&result.steps);
    drop(client);
    adapter.abort();
    server.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn adapter_timeout_releases_request() {
    let temp = TempDir::new().expect("tempdir");
    let paths = paths(&temp);
    let server = start_server(paths.clone()).await.expect("server");
    let mut stream = UnixStream::connect(&paths.socket_path)
        .await
        .expect("adapter");
    let adapter_instance_id = Uuid::new_v4();
    let hello = RequestEnvelope {
        protocol_version: CURRENT_PROTOCOL_VERSION,
        request_id: Uuid::new_v4(),
        body: RequestBody::AdapterHello(AdapterHello {
            adapter_id: "dev.fubun.linux".to_owned(),
            adapter_version: "test".to_owned(),
            instance_id: adapter_instance_id,
            action_capabilities: vec!["desktop.notification.show.v1".to_owned()],
            event_capabilities: Vec::new(),
            status: AdapterStatusSnapshot {
                tools: vec![AdapterToolStatus {
                    name: "notify-send".to_owned(),
                    available: true,
                }],
                desktop_entry_ids: vec![],
                permitted_resource_ids: Vec::new(),
            },
        }),
    };
    write_json_frame(&mut stream, &hello).await.expect("hello");
    let _: ResponseEnvelope = read_json_frame(&mut stream)
        .await
        .expect("ack")
        .expect("ack value");
    let mut client = FubunClient::connect(&paths.socket_path, "integration", "test")
        .await
        .expect("client");
    let definition = RitualDefinition {
        schema_version: RITUAL_SCHEMA_VERSION.to_owned(),
        name: "Timeout".to_owned(),
        actions: vec![
            ActionSpec::DesktopNotificationShow {
                title: "x".to_owned(),
                body: "y".to_owned(),
            },
            ActionSpec::DesktopNotificationShow {
                title: "later".to_owned(),
                body: "later".to_owned(),
            },
        ],
        execution: RitualExecutionConfig {
            mode: ExecutionMode::Sequential,
            on_failure: FailureMode::Stop,
            timeout_seconds: 1,
        },
    };
    let record = match client
        .request(RequestBody::RitualCreate(RitualCreateRequest {
            definition,
        }))
        .await
        .expect("create")
    {
        ResponsePayload::RitualCreated(record) => record,
        _ => panic!("ritual"),
    };
    client
        .request(RequestBody::RitualActivate(RitualActivateRequest {
            ritual_id: record.ritual.id,
            approve: true,
        }))
        .await
        .expect("activate");
    let started = tokio::time::Instant::now();
    let result = client
        .request(RequestBody::RitualRun(RitualIdRequest {
            ritual_id: record.ritual.id,
        }))
        .await
        .expect("timeout history");
    assert!(started.elapsed() < Duration::from_secs(12));
    let ResponsePayload::RitualRun(result) = result else {
        panic!("execution payload")
    };
    assert_eq!(
        result.execution.failure_code.as_deref(),
        Some("ritual_timeout")
    );
    assert_eq!(result.steps[0].status, ExecutionStepStatus::Aborted);
    assert_eq!(
        result.steps[0].adapter_id.as_deref(),
        Some("dev.fubun.linux")
    );
    assert_eq!(
        result.steps[0].adapter_instance_id,
        Some(adapter_instance_id)
    );
    assert_eq!(result.steps[1].status, ExecutionStepStatus::Aborted);
    assert_eq!(
        result.steps[1].result_code.as_deref(),
        Some("ritual_timeout")
    );
    assert_terminal_steps(&result.steps);
    drop(client);
    drop(stream);
    server.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn action_timeout_records_adapter_identity_and_aborts_following_steps() {
    let temp = TempDir::new().expect("tempdir");
    let paths = paths(&temp);
    let server = start_server(paths.clone()).await.expect("server");
    let adapter = silent_adapter(&paths).await;
    let mut client = FubunClient::connect(&paths.socket_path, "integration", "test")
        .await
        .expect("client");
    let definition = RitualDefinition {
        schema_version: RITUAL_SCHEMA_VERSION.to_owned(),
        name: "Action timeout".to_owned(),
        actions: vec![
            ActionSpec::DesktopNotificationShow {
                title: "first".to_owned(),
                body: "first".to_owned(),
            },
            ActionSpec::DesktopNotificationShow {
                title: "second".to_owned(),
                body: "second".to_owned(),
            },
        ],
        execution: RitualExecutionConfig {
            mode: ExecutionMode::Sequential,
            on_failure: FailureMode::Stop,
            timeout_seconds: 30,
        },
    };
    let record = match client
        .request(RequestBody::RitualCreate(RitualCreateRequest {
            definition,
        }))
        .await
        .expect("create")
    {
        ResponsePayload::RitualCreated(record) => record,
        _ => panic!("ritual"),
    };
    client
        .request(RequestBody::RitualActivate(RitualActivateRequest {
            ritual_id: record.ritual.id,
            approve: true,
        }))
        .await
        .expect("activate");
    let result = client
        .request(RequestBody::RitualRun(RitualIdRequest {
            ritual_id: record.ritual.id,
        }))
        .await
        .expect("timeout history");
    let ResponsePayload::RitualRun(result) = result else {
        panic!("execution")
    };
    assert_eq!(result.execution.status, ExecutionStatus::Failed);
    assert_eq!(
        result.execution.failure_code.as_deref(),
        Some("action_timeout")
    );
    assert_eq!(result.steps[0].status, ExecutionStepStatus::Failed);
    assert_eq!(
        result.steps[0].adapter_id.as_deref(),
        Some("dev.fubun.linux")
    );
    assert_eq!(
        result.steps[0].adapter_instance_id,
        Some(adapter.instance_id)
    );
    assert_eq!(result.steps[1].status, ExecutionStepStatus::Aborted);
    assert_eq!(
        result.steps[1].result_code.as_deref(),
        Some("stopped_after_failure")
    );
    assert_terminal_steps(&result.steps);
    drop(client);
    adapter.abort();
    server.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn adapter_disconnect_fails_without_permanent_wait() {
    let temp = TempDir::new().expect("tempdir");
    let paths = paths(&temp);
    let server = start_server(paths.clone()).await.expect("server");
    let mut stream = UnixStream::connect(&paths.socket_path)
        .await
        .expect("adapter");
    let hello = RequestEnvelope {
        protocol_version: CURRENT_PROTOCOL_VERSION,
        request_id: Uuid::new_v4(),
        body: RequestBody::AdapterHello(AdapterHello {
            adapter_id: "dev.fubun.linux".to_owned(),
            adapter_version: "test".to_owned(),
            instance_id: Uuid::new_v4(),
            action_capabilities: vec!["desktop.notification.show.v1".to_owned()],
            event_capabilities: Vec::new(),
            status: AdapterStatusSnapshot {
                tools: vec![AdapterToolStatus {
                    name: "notify-send".to_owned(),
                    available: true,
                }],
                desktop_entry_ids: vec![],
                permitted_resource_ids: Vec::new(),
            },
        }),
    };
    write_json_frame(&mut stream, &hello).await.expect("hello");
    let _: ResponseEnvelope = read_json_frame(&mut stream)
        .await
        .expect("ack")
        .expect("ack value");
    let mut client = FubunClient::connect(&paths.socket_path, "integration", "test")
        .await
        .expect("client");
    let definition = RitualDefinition {
        schema_version: RITUAL_SCHEMA_VERSION.to_owned(),
        name: "Disconnect".to_owned(),
        actions: vec![ActionSpec::DesktopNotificationShow {
            title: "x".to_owned(),
            body: "y".to_owned(),
        }],
        execution: RitualExecutionConfig {
            mode: ExecutionMode::Sequential,
            on_failure: FailureMode::Stop,
            timeout_seconds: 30,
        },
    };
    let record = match client
        .request(RequestBody::RitualCreate(RitualCreateRequest {
            definition,
        }))
        .await
        .expect("create")
    {
        ResponsePayload::RitualCreated(record) => record,
        _ => panic!("ritual"),
    };
    client
        .request(RequestBody::RitualActivate(RitualActivateRequest {
            ritual_id: record.ritual.id,
            approve: true,
        }))
        .await
        .expect("activate");
    drop(stream);
    tokio::time::sleep(Duration::from_millis(50)).await;
    let started = tokio::time::Instant::now();
    let result = client
        .request(RequestBody::RitualRun(RitualIdRequest {
            ritual_id: record.ritual.id,
        }))
        .await
        .expect_err("disconnect failure");
    assert!(started.elapsed() < Duration::from_secs(2));
    assert!(
        matches!(result, ClientError::Rejected { ref code, .. } if code == "capability_unavailable" || code == "adapter_unavailable")
    );
    drop(client);
    server.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn adapter_disconnect_during_action_releases_lock_and_pending_request() {
    let temp = TempDir::new().expect("tempdir");
    let paths = paths(&temp);
    let server = start_server(paths.clone()).await.expect("server");
    let disconnect = disconnecting_adapter(&paths).await;
    let mut client = FubunClient::connect(&paths.socket_path, "integration", "test")
        .await
        .expect("client");
    let definition = RitualDefinition {
        schema_version: RITUAL_SCHEMA_VERSION.to_owned(),
        name: "Disconnect during action".to_owned(),
        actions: vec![
            ActionSpec::DesktopNotificationShow {
                title: "x".to_owned(),
                body: "y".to_owned(),
            },
            ActionSpec::DesktopNotificationShow {
                title: "later".to_owned(),
                body: "later".to_owned(),
            },
        ],
        execution: RitualExecutionConfig {
            mode: ExecutionMode::Sequential,
            on_failure: FailureMode::Stop,
            timeout_seconds: 30,
        },
    };
    let record = match client
        .request(RequestBody::RitualCreate(RitualCreateRequest {
            definition,
        }))
        .await
        .expect("create")
    {
        ResponsePayload::RitualCreated(record) => record,
        _ => panic!("ritual"),
    };
    client
        .request(RequestBody::RitualActivate(RitualActivateRequest {
            ritual_id: record.ritual.id,
            approve: true,
        }))
        .await
        .expect("activate");
    let run = client
        .request(RequestBody::RitualRun(RitualIdRequest {
            ritual_id: record.ritual.id,
        }))
        .await
        .expect("execution history");
    let ResponsePayload::RitualRun(result) = run else {
        panic!("execution")
    };
    assert_eq!(result.execution.status, ExecutionStatus::Failed);
    assert_eq!(
        result.execution.failure_code.as_deref(),
        Some("adapter_disconnected")
    );
    assert_eq!(
        result.steps[0].result_code.as_deref(),
        Some("adapter_disconnected")
    );
    assert_eq!(
        result.steps[0].adapter_id.as_deref(),
        Some("dev.fubun.linux")
    );
    assert_eq!(
        result.steps[0].adapter_instance_id,
        Some(disconnect.instance_id)
    );
    assert_eq!(result.steps[1].status, ExecutionStepStatus::Aborted);
    assert_eq!(
        result.steps[1].result_code.as_deref(),
        Some("stopped_after_failure")
    );
    assert_terminal_steps(&result.steps);
    disconnect.task.await.expect("disconnect task");

    let retry_adapter = fake_adapter(&paths, Arc::new(AtomicUsize::new(0)), None).await;
    let retry = client
        .request(RequestBody::RitualRun(RitualIdRequest {
            ritual_id: record.ritual.id,
        }))
        .await
        .expect("retry after disconnect");
    let ResponsePayload::RitualRun(retry) = retry else {
        panic!("retry execution")
    };
    assert_eq!(retry.execution.status, ExecutionStatus::Succeeded);
    retry_adapter.abort();
    drop(client);
    server.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn protocol_error_records_adapter_identity_and_aborts_following_steps() {
    let temp = TempDir::new().expect("tempdir");
    let paths = paths(&temp);
    let server = start_server(paths.clone()).await.expect("server");
    let adapter = protocol_error_adapter(&paths).await;
    let mut client = FubunClient::connect(&paths.socket_path, "integration", "test")
        .await
        .expect("client");
    let definition = RitualDefinition {
        schema_version: RITUAL_SCHEMA_VERSION.to_owned(),
        name: "Protocol error".to_owned(),
        actions: vec![
            ActionSpec::DesktopNotificationShow {
                title: "first".to_owned(),
                body: "first".to_owned(),
            },
            ActionSpec::DesktopNotificationShow {
                title: "second".to_owned(),
                body: "second".to_owned(),
            },
        ],
        execution: RitualExecutionConfig {
            mode: ExecutionMode::Sequential,
            on_failure: FailureMode::Stop,
            timeout_seconds: 30,
        },
    };
    let record = match client
        .request(RequestBody::RitualCreate(RitualCreateRequest {
            definition,
        }))
        .await
        .expect("create")
    {
        ResponsePayload::RitualCreated(record) => record,
        _ => panic!("ritual"),
    };
    client
        .request(RequestBody::RitualActivate(RitualActivateRequest {
            ritual_id: record.ritual.id,
            approve: true,
        }))
        .await
        .expect("activate");
    let result = client
        .request(RequestBody::RitualRun(RitualIdRequest {
            ritual_id: record.ritual.id,
        }))
        .await
        .expect("protocol failure history");
    let ResponsePayload::RitualRun(result) = result else {
        panic!("execution")
    };
    assert_eq!(result.execution.status, ExecutionStatus::Failed);
    assert_eq!(
        result.execution.failure_code.as_deref(),
        Some("protocol_error")
    );
    assert_eq!(result.steps[0].status, ExecutionStepStatus::Failed);
    assert_eq!(
        result.steps[0].adapter_id.as_deref(),
        Some("dev.fubun.linux")
    );
    assert_eq!(
        result.steps[0].adapter_instance_id,
        Some(adapter.instance_id)
    );
    assert_eq!(result.steps[1].status, ExecutionStepStatus::Aborted);
    assert_eq!(
        result.steps[1].result_code.as_deref(),
        Some("stopped_after_failure")
    );
    assert_terminal_steps(&result.steps);
    adapter.task.await.expect("protocol adapter task");
    drop(client);
    server.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn notification_dispatch_uses_the_same_eligible_instance_as_preview() {
    let temp = TempDir::new().expect("tempdir");
    let paths = paths(&temp);
    let server = start_server(paths.clone()).await.expect("server");
    let missing_tool_requests = Arc::new(AtomicUsize::new(0));
    let selected_requests = Arc::new(AtomicUsize::new(0));
    let missing_tool = status_adapter(
        &paths,
        "notification-tool-missing",
        vec!["desktop.notification.show.v1"],
        vec![("notify-send", false)],
        Vec::new(),
        missing_tool_requests.clone(),
    )
    .await;
    let selected = status_adapter(
        &paths,
        "notification-selected",
        vec!["desktop.notification.show.v1"],
        vec![("notify-send", true)],
        Vec::new(),
        selected_requests.clone(),
    )
    .await;
    let mut client = FubunClient::connect(&paths.socket_path, "integration", "test")
        .await
        .expect("client");
    let definition = RitualDefinition {
        schema_version: RITUAL_SCHEMA_VERSION.to_owned(),
        name: "Select notification adapter".to_owned(),
        actions: vec![ActionSpec::DesktopNotificationShow {
            title: "title".to_owned(),
            body: "body".to_owned(),
        }],
        execution: RitualExecutionConfig {
            mode: ExecutionMode::Sequential,
            on_failure: FailureMode::Stop,
            timeout_seconds: 10,
        },
    };
    let record = match client
        .request(RequestBody::RitualCreate(RitualCreateRequest {
            definition,
        }))
        .await
        .expect("create")
    {
        ResponsePayload::RitualCreated(record) => record,
        _ => panic!("ritual"),
    };
    let preview = match client
        .request(RequestBody::RitualPreview(RitualIdRequest {
            ritual_id: record.ritual.id,
        }))
        .await
        .expect("preview")
    {
        ResponsePayload::RitualPreview(preview) => preview,
        _ => panic!("preview"),
    };
    assert!(preview.executable);
    client
        .request(RequestBody::RitualActivate(RitualActivateRequest {
            ritual_id: record.ritual.id,
            approve: true,
        }))
        .await
        .expect("activate");
    let result = client
        .request(RequestBody::RitualRun(RitualIdRequest {
            ritual_id: record.ritual.id,
        }))
        .await
        .expect("run");
    let ResponsePayload::RitualRun(result) = result else {
        panic!("execution")
    };
    assert_eq!(missing_tool_requests.load(Ordering::SeqCst), 0);
    assert_eq!(selected_requests.load(Ordering::SeqCst), 1);
    assert_eq!(
        result.steps[0].adapter_instance_id,
        Some(selected.instance_id)
    );
    assert_eq!(
        result.steps[0].adapter_id.as_deref(),
        Some("dev.fubun.linux")
    );
    assert_terminal_steps(&result.steps);
    drop(client);
    missing_tool.abort();
    selected.abort();
    server.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn desktop_entry_dispatch_requires_the_same_adapter_instance() {
    let temp = TempDir::new().expect("tempdir");
    let paths = paths(&temp);
    let server = start_server(paths.clone()).await.expect("server");
    let missing_entry_requests = Arc::new(AtomicUsize::new(0));
    let selected_requests = Arc::new(AtomicUsize::new(0));
    let missing_entry = status_adapter(
        &paths,
        "desktop-entry-missing",
        vec!["linux.app.ensure_running.v1"],
        vec![("gtk-launch", true)],
        Vec::new(),
        missing_entry_requests.clone(),
    )
    .await;
    let selected = status_adapter(
        &paths,
        "desktop-entry-selected",
        vec!["linux.app.ensure_running.v1"],
        vec![("gtk-launch", true)],
        vec!["code"],
        selected_requests.clone(),
    )
    .await;
    let mut client = FubunClient::connect(&paths.socket_path, "integration", "test")
        .await
        .expect("client");
    let definition = RitualDefinition {
        schema_version: RITUAL_SCHEMA_VERSION.to_owned(),
        name: "Select desktop adapter".to_owned(),
        actions: vec![ActionSpec::LinuxAppEnsureRunning {
            app_id: "code".to_owned(),
        }],
        execution: RitualExecutionConfig {
            mode: ExecutionMode::Sequential,
            on_failure: FailureMode::Stop,
            timeout_seconds: 10,
        },
    };
    let record = match client
        .request(RequestBody::RitualCreate(RitualCreateRequest {
            definition,
        }))
        .await
        .expect("create")
    {
        ResponsePayload::RitualCreated(record) => record,
        _ => panic!("ritual"),
    };
    let preview = match client
        .request(RequestBody::RitualPreview(RitualIdRequest {
            ritual_id: record.ritual.id,
        }))
        .await
        .expect("preview")
    {
        ResponsePayload::RitualPreview(preview) => preview,
        _ => panic!("preview"),
    };
    assert!(preview.executable);
    client
        .request(RequestBody::RitualActivate(RitualActivateRequest {
            ritual_id: record.ritual.id,
            approve: true,
        }))
        .await
        .expect("activate");
    let result = client
        .request(RequestBody::RitualRun(RitualIdRequest {
            ritual_id: record.ritual.id,
        }))
        .await
        .expect("run");
    let ResponsePayload::RitualRun(result) = result else {
        panic!("execution")
    };
    assert_eq!(missing_entry_requests.load(Ordering::SeqCst), 0);
    assert_eq!(selected_requests.load(Ordering::SeqCst), 1);
    assert_eq!(
        result.steps[0].adapter_instance_id,
        Some(selected.instance_id)
    );
    assert_terminal_steps(&result.steps);
    drop(client);
    missing_entry.abort();
    selected.abort();
    server.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn split_adapter_requirements_are_not_composed_for_activation_or_dispatch() {
    let temp = TempDir::new().expect("tempdir");
    let paths = paths(&temp);
    let server = start_server(paths.clone()).await.expect("server");
    let tool_only_requests = Arc::new(AtomicUsize::new(0));
    let entry_only_requests = Arc::new(AtomicUsize::new(0));
    let tool_only = status_adapter(
        &paths,
        "tool-only",
        vec!["linux.app.ensure_running.v1"],
        vec![("gtk-launch", true)],
        Vec::new(),
        tool_only_requests.clone(),
    )
    .await;
    let entry_only = status_adapter(
        &paths,
        "entry-only",
        vec!["linux.app.ensure_running.v1"],
        vec![("gtk-launch", false)],
        vec!["code"],
        entry_only_requests.clone(),
    )
    .await;
    let mut client = FubunClient::connect(&paths.socket_path, "integration", "test")
        .await
        .expect("client");
    let definition = RitualDefinition {
        schema_version: RITUAL_SCHEMA_VERSION.to_owned(),
        name: "Reject split requirements".to_owned(),
        actions: vec![ActionSpec::LinuxAppEnsureRunning {
            app_id: "code".to_owned(),
        }],
        execution: RitualExecutionConfig {
            mode: ExecutionMode::Sequential,
            on_failure: FailureMode::Stop,
            timeout_seconds: 10,
        },
    };
    let record = match client
        .request(RequestBody::RitualCreate(RitualCreateRequest {
            definition,
        }))
        .await
        .expect("create")
    {
        ResponsePayload::RitualCreated(record) => record,
        _ => panic!("ritual"),
    };
    let preview = match client
        .request(RequestBody::RitualPreview(RitualIdRequest {
            ritual_id: record.ritual.id,
        }))
        .await
        .expect("preview")
    {
        ResponsePayload::RitualPreview(preview) => preview,
        _ => panic!("preview"),
    };
    assert!(!preview.executable);
    let activation = client
        .request(RequestBody::RitualActivate(RitualActivateRequest {
            ritual_id: record.ritual.id,
            approve: true,
        }))
        .await
        .expect_err("split requirements block activation");
    assert!(
        matches!(activation, ClientError::Rejected { ref code, .. } if code == "preflight_failed")
    );
    assert_eq!(tool_only_requests.load(Ordering::SeqCst), 0);
    assert_eq!(entry_only_requests.load(Ordering::SeqCst), 0);
    drop(client);
    tool_only.abort();
    entry_only.abort();
    server.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn preview_rejects_missing_tool_and_resource_kind_changes() {
    let temp = TempDir::new().expect("tempdir");
    let paths = paths(&temp);
    let server = start_server(paths.clone()).await.expect("server");
    let limited =
        limited_adapter(&paths, vec!["desktop.notification.show.v1"], "notify-send").await;
    let mut client = FubunClient::connect(&paths.socket_path, "integration", "test")
        .await
        .expect("client");
    let notification = RitualDefinition {
        schema_version: RITUAL_SCHEMA_VERSION.to_owned(),
        name: "Missing tool".to_owned(),
        actions: vec![ActionSpec::DesktopNotificationShow {
            title: "x".to_owned(),
            body: "y".to_owned(),
        }],
        execution: RitualExecutionConfig {
            mode: ExecutionMode::Sequential,
            on_failure: FailureMode::Stop,
            timeout_seconds: 10,
        },
    };
    let notification_record = match client
        .request(RequestBody::RitualCreate(RitualCreateRequest {
            definition: notification,
        }))
        .await
        .expect("create")
    {
        ResponsePayload::RitualCreated(record) => record,
        _ => panic!("ritual"),
    };
    let preview = match client
        .request(RequestBody::RitualPreview(RitualIdRequest {
            ritual_id: notification_record.ritual.id,
        }))
        .await
        .expect("preview")
    {
        ResponsePayload::RitualPreview(preview) => preview,
        _ => panic!("preview"),
    };
    assert!(!preview.executable);
    assert_eq!(preview.actions[0].required_tool_available, Some(false));
    let activation = client
        .request(RequestBody::RitualActivate(RitualActivateRequest {
            ritual_id: notification_record.ritual.id,
            approve: true,
        }))
        .await
        .expect_err("missing tool must block activation");
    assert!(
        matches!(activation, ClientError::Rejected { ref code, .. } if code == "preflight_failed")
    );

    limited.abort();
    let path_adapter = limited_adapter(&paths, vec!["linux.path.open.v1"], "gtk-launch").await;
    let file_path = temp.path().join("kind-file");
    std::fs::write(&file_path, b"fixture").expect("file");
    let file_resource = match client
        .request(RequestBody::ResourceCreate(ResourceCreateRequest {
            label: "kind-file".to_owned(),
            path: file_path.to_string_lossy().into_owned(),
            sensitivity: Sensitivity::Normal,
        }))
        .await
        .expect("file resource")
    {
        ResponsePayload::ResourceCreated(resource) => resource,
        _ => panic!("resource"),
    };
    let file_ritual = match client
        .request(RequestBody::RitualCreate(RitualCreateRequest {
            definition: RitualDefinition {
                schema_version: RITUAL_SCHEMA_VERSION.to_owned(),
                name: "File kind".to_owned(),
                actions: vec![ActionSpec::LinuxPathOpen {
                    resource_id: file_resource.id,
                }],
                execution: RitualExecutionConfig {
                    mode: ExecutionMode::Sequential,
                    on_failure: FailureMode::Stop,
                    timeout_seconds: 10,
                },
            },
        }))
        .await
        .expect("ritual")
    {
        ResponsePayload::RitualCreated(record) => record,
        _ => panic!("ritual"),
    };
    std::fs::remove_file(&file_path).expect("remove file");
    std::fs::create_dir(&file_path).expect("replace with directory");
    let file_preview = match client
        .request(RequestBody::RitualPreview(RitualIdRequest {
            ritual_id: file_ritual.ritual.id,
        }))
        .await
        .expect("preview")
    {
        ResponsePayload::RitualPreview(preview) => preview,
        _ => panic!("preview"),
    };
    assert!(!file_preview.executable);
    assert_eq!(file_preview.actions[0].resource_kind_matches, Some(false));

    let directory_path = temp.path().join("kind-directory");
    std::fs::create_dir(&directory_path).expect("directory");
    let directory_resource = match client
        .request(RequestBody::ResourceCreate(ResourceCreateRequest {
            label: "kind-directory".to_owned(),
            path: directory_path.to_string_lossy().into_owned(),
            sensitivity: Sensitivity::Normal,
        }))
        .await
        .expect("directory resource")
    {
        ResponsePayload::ResourceCreated(resource) => resource,
        _ => panic!("resource"),
    };
    let directory_ritual = match client
        .request(RequestBody::RitualCreate(RitualCreateRequest {
            definition: RitualDefinition {
                schema_version: RITUAL_SCHEMA_VERSION.to_owned(),
                name: "Directory kind".to_owned(),
                actions: vec![ActionSpec::LinuxPathOpen {
                    resource_id: directory_resource.id,
                }],
                execution: RitualExecutionConfig {
                    mode: ExecutionMode::Sequential,
                    on_failure: FailureMode::Stop,
                    timeout_seconds: 10,
                },
            },
        }))
        .await
        .expect("ritual")
    {
        ResponsePayload::RitualCreated(record) => record,
        _ => panic!("ritual"),
    };
    std::fs::remove_dir(&directory_path).expect("remove directory");
    std::fs::write(&directory_path, b"replacement").expect("replace with file");
    let directory_preview = match client
        .request(RequestBody::RitualPreview(RitualIdRequest {
            ritual_id: directory_ritual.ritual.id,
        }))
        .await
        .expect("preview")
    {
        ResponsePayload::RitualPreview(preview) => preview,
        _ => panic!("preview"),
    };
    assert!(!directory_preview.executable);
    assert_eq!(
        directory_preview.actions[0].resource_kind_matches,
        Some(false)
    );

    let symlink_path = temp.path().join("symlink-resource");
    let replacement_target = temp.path().join("symlink-replacement");
    std::fs::write(&symlink_path, b"original").expect("symlink source");
    std::fs::write(&replacement_target, b"replacement").expect("replacement");
    let symlink_resource = match client
        .request(RequestBody::ResourceCreate(ResourceCreateRequest {
            label: "symlink".to_owned(),
            path: symlink_path.to_string_lossy().into_owned(),
            sensitivity: Sensitivity::Normal,
        }))
        .await
        .expect("symlink resource")
    {
        ResponsePayload::ResourceCreated(resource) => resource,
        _ => panic!("resource"),
    };
    let symlink_ritual = match client
        .request(RequestBody::RitualCreate(RitualCreateRequest {
            definition: RitualDefinition {
                schema_version: RITUAL_SCHEMA_VERSION.to_owned(),
                name: "Symlink".to_owned(),
                actions: vec![ActionSpec::LinuxPathOpen {
                    resource_id: symlink_resource.id,
                }],
                execution: RitualExecutionConfig {
                    mode: ExecutionMode::Sequential,
                    on_failure: FailureMode::Stop,
                    timeout_seconds: 10,
                },
            },
        }))
        .await
        .expect("symlink ritual")
    {
        ResponsePayload::RitualCreated(record) => record,
        _ => panic!("ritual"),
    };
    std::fs::remove_file(&symlink_path).expect("remove original");
    std::os::unix::fs::symlink(&replacement_target, &symlink_path).expect("swap symlink");
    let symlink_preview = match client
        .request(RequestBody::RitualPreview(RitualIdRequest {
            ritual_id: symlink_ritual.ritual.id,
        }))
        .await
        .expect("preview")
    {
        ResponsePayload::RitualPreview(preview) => preview,
        _ => panic!("preview"),
    };
    assert!(!symlink_preview.executable);
    assert_eq!(
        symlink_preview.actions[0].resource_path_matches,
        Some(false)
    );
    path_adapter.abort();
    drop(client);
    server.shutdown().await.expect("shutdown");
}
