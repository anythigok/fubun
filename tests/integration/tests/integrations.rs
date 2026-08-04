use std::time::Duration;

use fubun_core::{paths::FubunPaths, start_server, FubunClient};
use fubun_domain::{
    Actor, AdapterIdentity, Event, EventData, EventType, ObservationSource, ObservationStatus,
    PrivacyClass, EVENT_SPEC_VERSION,
};
use fubun_protocol::{
    read_json_frame, write_json_frame, AdapterEventAck, AdapterEventEmitRequest, AdapterHello,
    AdapterRequestBody, AdapterRequestEnvelope, AdapterResponseBody, AdapterResponseEnvelope,
    AdapterStatusSnapshot, BrowserObservationEnableRequest, ClientHelloAck, EventIngestRequest,
    RequestBody, ResponseBody, ResponseEnvelope, ResponsePayload, CURRENT_PROTOCOL_VERSION,
};
use tempfile::TempDir;
use time::OffsetDateTime;
use tokio::net::UnixStream;
use uuid::Uuid;

fn paths(temp: &TempDir) -> FubunPaths {
    FubunPaths::from_xdg_roots(temp.path().join("runtime"), temp.path().join("data"))
        .expect("paths")
}

async fn browser_adapter(paths: &FubunPaths, resource_id: Uuid) -> (UnixStream, Uuid) {
    let mut stream = UnixStream::connect(&paths.socket_path)
        .await
        .expect("socket");
    let instance_id = Uuid::new_v4();
    let request = fubun_protocol::RequestEnvelope {
        protocol_version: CURRENT_PROTOCOL_VERSION,
        request_id: Uuid::new_v4(),
        body: RequestBody::AdapterHello(AdapterHello {
            adapter_id: "dev.fubun.browser.chromium".to_owned(),
            adapter_version: "test".to_owned(),
            instance_id,
            action_capabilities: vec!["browser.tab.ensure_open.v1".to_owned()],
            event_capabilities: vec!["dev.fubun.browser.resource.opened.v1".to_owned()],
            status: AdapterStatusSnapshot {
                tools: Vec::new(),
                desktop_entry_ids: Vec::new(),
                permitted_resource_ids: vec![resource_id],
            },
        }),
    };
    let id = request.request_id;
    write_json_frame(&mut stream, &request)
        .await
        .expect("hello");
    let response: ResponseEnvelope = read_json_frame(&mut stream)
        .await
        .expect("ack")
        .expect("ack");
    assert_eq!(response.request_id, id);
    assert!(matches!(
        response.body,
        ResponseBody::Ok(ResponsePayload::AdapterHelloAck(ClientHelloAck { .. }))
    ));
    (stream, instance_id)
}

fn client_semantic_event(event_type: EventType, data: EventData) -> Event {
    Event {
        spec_version: EVENT_SPEC_VERSION.to_owned(),
        id: Uuid::new_v4(),
        event_type,
        source: "client-controlled".to_owned(),
        occurred_at: OffsetDateTime::now_utc(),
        received_at: OffsetDateTime::now_utc(),
        actor: Actor::User,
        adapter: AdapterIdentity {
            id: "client-controlled".to_owned(),
            version: "test".to_owned(),
            instance_id: Uuid::new_v4(),
            sequence_no: 1,
        },
        context: None,
        privacy: PrivacyClass::Normal,
        data,
    }
}

#[tokio::test]
async fn client_event_ingest_rejects_semantic_event_spoofing_but_keeps_synthetic_fixture_support() {
    let temp = TempDir::new().expect("temp");
    let paths = paths(&temp);
    let server = start_server(paths.clone()).await.expect("server");
    let mut client = FubunClient::connect(&paths.socket_path, "integration", "test")
        .await
        .expect("client");
    for (event_type, data) in [
        (
            EventType::BrowserResourceOpenedV1,
            EventData::BrowserResourceOpened {
                resource_id: Uuid::new_v4(),
            },
        ),
        (
            EventType::VscodeWorkspaceOpenedV1,
            EventData::VscodeWorkspaceOpened {
                resource_id: Uuid::new_v4(),
            },
        ),
    ] {
        let error = client
            .request(RequestBody::EventIngest(EventIngestRequest {
                event: client_semantic_event(event_type, data),
            }))
            .await
            .expect_err("client semantic event must be rejected");
        assert!(
            matches!(error, fubun_core::ClientError::Rejected { code, .. } if code == "invalid_event")
        );
    }
    let synthetic = Event {
        spec_version: EVENT_SPEC_VERSION.to_owned(),
        id: Uuid::new_v4(),
        event_type: EventType::SyntheticV1,
        source: "dev.fixture".to_owned(),
        occurred_at: OffsetDateTime::now_utc(),
        received_at: OffsetDateTime::now_utc(),
        actor: Actor::User,
        adapter: AdapterIdentity {
            id: "dev.fixture".to_owned(),
            version: "test".to_owned(),
            instance_id: Uuid::new_v4(),
            sequence_no: 1,
        },
        context: None,
        privacy: PrivacyClass::Normal,
        data: EventData::Synthetic {
            label: "fixture".to_owned(),
            counter: 1,
        },
    };
    let response = client
        .request(RequestBody::EventIngest(EventIngestRequest {
            event: synthetic,
        }))
        .await
        .expect("synthetic fixture remains supported");
    assert!(matches!(response, ResponsePayload::EventIngested(_)));
    server.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn browser_scope_event_is_core_built_and_pause_blocks_events() {
    let temp = TempDir::new().expect("temp");
    let paths = paths(&temp);
    let server = start_server(paths.clone()).await.expect("server");
    let mut client = FubunClient::connect(&paths.socket_path, "integration", "test")
        .await
        .expect("client");
    let enabled = client
        .request(RequestBody::BrowserObservationEnable(
            BrowserObservationEnableRequest {
                label: "Research".to_owned(),
                url: "https://Example.COM:443/research?id=1#section".to_owned(),
            },
        ))
        .await
        .expect("enable");
    let ResponsePayload::BrowserObservationEnabled(enabled) = enabled else {
        panic!("enabled")
    };
    assert_eq!(enabled.resource.kind, fubun_domain::ResourceKind::WebPage);
    assert_eq!(
        enabled.resource.canonical_locator,
        "https://example.com/research"
    );
    let (mut adapter, instance_id) = browser_adapter(&paths, enabled.resource.id).await;
    let event_request_id = Uuid::new_v4();
    write_json_frame(
        &mut adapter,
        &AdapterResponseEnvelope {
            protocol_version: CURRENT_PROTOCOL_VERSION,
            request_id: event_request_id,
            action_execution_id: Uuid::nil(),
            body: AdapterResponseBody::EventEmit(AdapterEventEmitRequest {
                sequence_no: 1,
                occurred_at: OffsetDateTime::now_utc(),
                event_type: EventType::BrowserResourceOpenedV1,
                resource_id: enabled.resource.id,
            }),
        },
    )
    .await
    .expect("event");
    let ack: AdapterRequestEnvelope =
        tokio::time::timeout(Duration::from_secs(1), read_json_frame(&mut adapter))
            .await
            .expect("ack timeout")
            .expect("ack read")
            .expect("ack value");
    assert_eq!(ack.request_id, event_request_id);
    let AdapterRequestBody::EventAck(AdapterEventAck {
        stored, duplicate, ..
    }) = ack.body
    else {
        panic!("event ack")
    };
    assert!(stored && !duplicate);

    tokio::time::sleep(Duration::from_millis(20)).await;
    let events = client
        .request(RequestBody::EventsList(fubun_protocol::EventsListRequest {
            since: None,
            limit: 10,
        }))
        .await
        .expect("events");
    let ResponsePayload::EventList(events) = events else {
        panic!("events list")
    };
    assert_eq!(events.events.len(), 1);
    assert_eq!(events.events[0].actor, fubun_domain::Actor::User);
    assert_eq!(events.events[0].source, "browser.chromium");
    assert_eq!(events.events[0].adapter.instance_id, instance_id);
    assert!(matches!(
        events.events[0].data,
        EventData::BrowserResourceOpened { .. }
    ));

    client
        .request(RequestBody::ObservationPause(
            fubun_protocol::ObservationPauseRequest {
                scope_id: enabled.scope.id,
            },
        ))
        .await
        .expect("pause");
    let scopes = client
        .request(RequestBody::ObservationsList(
            fubun_protocol::ObservationListRequest {
                source: Some(ObservationSource::BrowserChromium),
            },
        ))
        .await
        .expect("scopes");
    let ResponsePayload::ObservationList(scopes) = scopes else {
        panic!("scopes")
    };
    assert_eq!(scopes.scopes[0].status, ObservationStatus::Paused);
    server.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn browser_action_dispatches_to_permissioned_adapter() {
    let temp = TempDir::new().expect("temp");
    let paths = paths(&temp);
    let server = start_server(paths.clone()).await.expect("server");
    let mut client = FubunClient::connect(&paths.socket_path, "integration", "test")
        .await
        .expect("client");
    let enabled = client
        .request(RequestBody::BrowserObservationEnable(
            BrowserObservationEnableRequest {
                label: "Action".to_owned(),
                url: "https://example.com/action".to_owned(),
            },
        ))
        .await
        .expect("enable");
    let ResponsePayload::BrowserObservationEnabled(enabled) = enabled else {
        panic!("enabled")
    };
    let (mut adapter, instance_id) = browser_adapter(&paths, enabled.resource.id).await;
    let definition = fubun_domain::RitualDefinition {
        schema_version: fubun_domain::RITUAL_SCHEMA_VERSION.to_owned(),
        name: "Open registered page".to_owned(),
        actions: vec![fubun_domain::ActionSpec::BrowserTabEnsureOpen {
            resource_id: enabled.resource.id,
        }],
        execution: fubun_domain::RitualExecutionConfig {
            mode: fubun_domain::ExecutionMode::Sequential,
            on_failure: fubun_domain::FailureMode::Stop,
            timeout_seconds: 10,
        },
    };
    let created = client
        .request(RequestBody::RitualCreate(
            fubun_protocol::RitualCreateRequest { definition },
        ))
        .await
        .expect("create");
    let ResponsePayload::RitualCreated(created) = created else {
        panic!("ritual")
    };
    let preview = client
        .request(RequestBody::RitualPreview(
            fubun_protocol::RitualIdRequest {
                ritual_id: created.ritual.id,
            },
        ))
        .await
        .expect("preview");
    let ResponsePayload::RitualPreview(preview) = preview else {
        panic!("preview")
    };
    assert!(preview.executable);
    client
        .request(RequestBody::RitualActivate(
            fubun_protocol::RitualActivateRequest {
                ritual_id: created.ritual.id,
                approve: true,
            },
        ))
        .await
        .expect("activate");
    let run_client = tokio::spawn(async move {
        let mut client = FubunClient::connect(&paths.socket_path, "runner", "test")
            .await
            .expect("runner");
        client
            .request(RequestBody::RitualRun(fubun_protocol::RitualIdRequest {
                ritual_id: created.ritual.id,
            }))
            .await
            .expect("run")
    });
    let request: AdapterRequestEnvelope =
        tokio::time::timeout(Duration::from_secs(1), read_json_frame(&mut adapter))
            .await
            .expect("dispatch timeout")
            .expect("dispatch read")
            .expect("dispatch");
    let action_execution_id = request.action_execution_id;
    assert!(matches!(request.body, AdapterRequestBody::ActionExecute(_)));
    write_json_frame(
        &mut adapter,
        &AdapterResponseEnvelope {
            protocol_version: CURRENT_PROTOCOL_VERSION,
            request_id: request.request_id,
            action_execution_id,
            body: AdapterResponseBody::ActionResult(fubun_protocol::ActionResult {
                status: fubun_protocol::AdapterActionStatus::Skipped,
                result_code: "already_open".to_owned(),
                redacted_message: "registered page already open".to_owned(),
            }),
        },
    )
    .await
    .expect("result");
    let result = run_client.await.expect("runner task");
    let ResponsePayload::RitualRun(record) = result else {
        panic!("run result")
    };
    assert_eq!(
        record.execution.status,
        fubun_domain::ExecutionStatus::Succeeded
    );
    assert_eq!(
        record.steps[0].adapter_id.as_deref(),
        Some("dev.fubun.browser.chromium")
    );
    assert_eq!(record.steps[0].adapter_instance_id, Some(instance_id));
    server.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn vscode_event_only_adapter_can_store_semantic_event() {
    let temp = TempDir::new().expect("temp");
    let paths = paths(&temp);
    let server = start_server(paths.clone()).await.expect("server");
    let workspace = temp.path().join("workspace");
    std::fs::create_dir_all(&workspace).expect("workspace");
    let mut client = FubunClient::connect(&paths.socket_path, "integration", "test")
        .await
        .expect("client");
    let enabled = client
        .request(RequestBody::VscodeObservationEnable(
            fubun_protocol::VscodeObservationEnableRequest {
                label: "Workspace".to_owned(),
                absolute_path: workspace.to_string_lossy().into_owned(),
            },
        ))
        .await
        .expect("enable");
    let ResponsePayload::VscodeObservationEnabled(enabled) = enabled else {
        panic!("enabled")
    };
    let mut adapter = UnixStream::connect(&paths.socket_path)
        .await
        .expect("adapter");
    let request = fubun_protocol::RequestEnvelope {
        protocol_version: CURRENT_PROTOCOL_VERSION,
        request_id: Uuid::new_v4(),
        body: RequestBody::AdapterHello(AdapterHello {
            adapter_id: "dev.fubun.vscode".to_owned(),
            adapter_version: "test".to_owned(),
            instance_id: Uuid::new_v4(),
            action_capabilities: Vec::new(),
            event_capabilities: vec!["dev.fubun.vscode.workspace.opened.v1".to_owned()],
            status: AdapterStatusSnapshot {
                tools: Vec::new(),
                desktop_entry_ids: Vec::new(),
                permitted_resource_ids: Vec::new(),
            },
        }),
    };
    write_json_frame(&mut adapter, &request)
        .await
        .expect("hello");
    let _: ResponseEnvelope = read_json_frame(&mut adapter)
        .await
        .expect("ack")
        .expect("ack");
    let event_request_id = Uuid::new_v4();
    write_json_frame(
        &mut adapter,
        &AdapterResponseEnvelope {
            protocol_version: CURRENT_PROTOCOL_VERSION,
            request_id: event_request_id,
            action_execution_id: Uuid::nil(),
            body: AdapterResponseBody::EventEmit(AdapterEventEmitRequest {
                sequence_no: 1,
                occurred_at: OffsetDateTime::now_utc(),
                event_type: EventType::VscodeWorkspaceOpenedV1,
                resource_id: enabled.resource.id,
            }),
        },
    )
    .await
    .expect("event");
    let ack: AdapterRequestEnvelope =
        tokio::time::timeout(Duration::from_secs(1), read_json_frame(&mut adapter))
            .await
            .expect("ack timeout")
            .expect("ack read")
            .expect("ack value");
    assert!(matches!(
        ack.body,
        AdapterRequestBody::EventAck(AdapterEventAck { stored: true, .. })
    ));
    let events = client
        .request(RequestBody::EventsList(fubun_protocol::EventsListRequest {
            since: None,
            limit: 10,
        }))
        .await
        .expect("events");
    let ResponsePayload::EventList(events) = events else {
        panic!("events")
    };
    assert_eq!(events.events.len(), 1);
    assert_eq!(events.events[0].source, "vscode.workspace");
    assert!(matches!(
        events.events[0].data,
        EventData::VscodeWorkspaceOpened { .. }
    ));
    server.shutdown().await.expect("shutdown");
}
