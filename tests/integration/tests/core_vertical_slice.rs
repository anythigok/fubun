use std::{fs, os::unix::fs::PermissionsExt};

use fubun_core::{paths::FubunPaths, start_server, ClientError, FubunClient};
use fubun_domain::Event;
use fubun_protocol::{
    read_json_frame, write_json_frame, ClientHello, EmptyRequest, EventIngestRequest,
    EventsListRequest, ProtocolVersion, RequestBody, RequestEnvelope, ResponseBody,
    ResponseEnvelope, ResponsePayload, CURRENT_PROTOCOL_VERSION, MAX_MESSAGE_SIZE,
};
use tempfile::TempDir;
use tokio::{io::AsyncWriteExt, net::UnixStream};
use uuid::Uuid;

const FIXTURE: &str = include_str!("../../../fixtures/synthetic-event.json");

fn paths(temp: &TempDir) -> FubunPaths {
    FubunPaths::from_xdg_roots(temp.path().join("runtime"), temp.path().join("data"))
        .expect("absolute temp paths")
}

fn fixture() -> Event {
    serde_json::from_str(FIXTURE).expect("valid fixture")
}

async fn client(paths: &FubunPaths) -> FubunClient {
    FubunClient::connect(&paths.socket_path, "integration-test", "0.1.0")
        .await
        .expect("connect client")
}

#[tokio::test]
async fn event_survives_restart_and_duplicate_is_explicit() {
    let temp = TempDir::new().expect("tempdir");
    let paths = paths(&temp);

    let server = start_server(paths.clone()).await.expect("start server");
    let mut first_client = client(&paths).await;
    let ingested = first_client
        .request(RequestBody::EventIngest(EventIngestRequest {
            event: fixture(),
        }))
        .await
        .expect("ingest event");
    assert!(matches!(ingested, ResponsePayload::EventIngested(_)));
    drop(first_client);
    server.shutdown().await.expect("first shutdown");

    let restarted = start_server(paths.clone()).await.expect("restart server");
    let mut second_client = client(&paths).await;
    let listed = second_client
        .request(RequestBody::EventsList(EventsListRequest {
            since: None,
            limit: 100,
        }))
        .await
        .expect("list events");
    let ResponsePayload::EventList(listed) = listed else {
        panic!("unexpected list response");
    };
    assert_eq!(listed.events.len(), 1);

    let duplicate = second_client
        .request(RequestBody::EventIngest(EventIngestRequest {
            event: fixture(),
        }))
        .await
        .expect_err("duplicate must fail");
    assert!(matches!(
        duplicate,
        ClientError::Rejected { ref code, .. } if code == "duplicate_event"
    ));
    drop(second_client);
    restarted.shutdown().await.expect("second shutdown");
}

#[tokio::test]
async fn malformed_and_oversized_messages_do_not_crash_daemon() {
    let temp = TempDir::new().expect("tempdir");
    let paths = paths(&temp);
    let server = start_server(paths.clone()).await.expect("start server");

    let mut malformed = UnixStream::connect(&paths.socket_path)
        .await
        .expect("connect malformed client");
    malformed
        .write_all(&1_u32.to_le_bytes())
        .await
        .expect("write malformed length");
    malformed
        .write_all(b"{")
        .await
        .expect("write malformed payload");
    drop(malformed);

    let mut oversized = UnixStream::connect(&paths.socket_path)
        .await
        .expect("connect oversized client");
    let oversized_length = u32::try_from(MAX_MESSAGE_SIZE + 1).expect("fits u32");
    oversized
        .write_all(&oversized_length.to_le_bytes())
        .await
        .expect("write oversized length");
    drop(oversized);

    let mut valid = client(&paths).await;
    let status = valid
        .request(RequestBody::SystemStatus(EmptyRequest::default()))
        .await
        .expect("daemon remains available");
    assert!(matches!(status, ResponsePayload::Status(_)));
    drop(valid);
    server.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn protocol_major_mismatch_is_rejected() {
    let temp = TempDir::new().expect("tempdir");
    let paths = paths(&temp);
    let server = start_server(paths.clone()).await.expect("start server");
    let mut stream = UnixStream::connect(&paths.socket_path)
        .await
        .expect("connect raw client");
    let incompatible = ProtocolVersion { major: 2, minor: 0 };
    let request_id = Uuid::new_v4();
    let request = RequestEnvelope {
        protocol_version: incompatible,
        request_id,
        body: RequestBody::ClientHello(ClientHello {
            client_name: "future-client".to_owned(),
            client_version: "2.0.0".to_owned(),
            protocol_version: incompatible,
        }),
    };
    write_json_frame(&mut stream, &request)
        .await
        .expect("write incompatible hello");
    let response: ResponseEnvelope = read_json_frame(&mut stream)
        .await
        .expect("read response")
        .expect("response exists");
    assert_eq!(response.request_id, request_id);
    assert!(matches!(
        response.body,
        ResponseBody::Error(ref error) if error.code == "protocol_major_mismatch"
    ));
    assert_eq!(response.protocol_version, CURRENT_PROTOCOL_VERSION);
    drop(stream);
    server.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn socket_and_data_permissions_are_private() {
    let temp = TempDir::new().expect("tempdir");
    let paths = paths(&temp);
    let server = start_server(paths.clone()).await.expect("start server");

    let runtime_mode = fs::metadata(&paths.runtime_directory)
        .expect("runtime metadata")
        .permissions()
        .mode()
        & 0o777;
    let socket_mode = fs::metadata(&paths.socket_path)
        .expect("socket metadata")
        .permissions()
        .mode()
        & 0o777;
    let data_mode = fs::metadata(&paths.data_directory)
        .expect("data metadata")
        .permissions()
        .mode()
        & 0o777;
    let database_mode = fs::metadata(&paths.database_path)
        .expect("database metadata")
        .permissions()
        .mode()
        & 0o777;

    assert_eq!(runtime_mode, 0o700);
    assert_eq!(socket_mode, 0o600);
    assert_eq!(data_mode, 0o700);
    assert_eq!(database_mode, 0o600);
    server.shutdown().await.expect("shutdown");
}
