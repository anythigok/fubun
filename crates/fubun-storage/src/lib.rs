//! SQLite persistence behind a single dedicated writer thread.

use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    sync::{mpsc, Arc},
    thread,
    time::Duration,
};

use fubun_domain::{Actor, Event, EventType, PrivacyClass, ValidationError};
use rusqlite::{params, Connection, ErrorCode};
use thiserror::Error;
use time::{format_description::well_known::Rfc3339, OffsetDateTime};
use tokio::sync::oneshot;

pub const SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("SQLite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("timestamp formatting error: {0}")]
    Time(#[from] time::error::Format),
    #[error("filesystem error: {0}")]
    Io(#[from] std::io::Error),
    #[error("event validation failed: {0}")]
    Validation(#[from] ValidationError),
    #[error("duplicate event for adapter_instance_id and sequence_no")]
    DuplicateEvent,
    #[error("storage queue is closed")]
    QueueClosed,
    #[error("storage response channel was dropped")]
    ResponseDropped,
    #[error("unsupported database schema version {0}")]
    UnsupportedSchema(u32),
    #[error("storage writer thread failed during startup: {0}")]
    Startup(String),
    #[error("storage writer thread panicked")]
    WriterPanicked,
}

enum Command {
    Insert(Box<Event>, oneshot::Sender<Result<(), StorageError>>),
    List {
        since: Option<OffsetDateTime>,
        limit: u32,
        response: oneshot::Sender<Result<Vec<Event>, StorageError>>,
    },
    SchemaVersion(oneshot::Sender<Result<u32, StorageError>>),
    Shutdown(oneshot::Sender<()>),
}

#[derive(Clone)]
pub struct StorageHandle {
    sender: mpsc::Sender<Command>,
    database_path: Arc<PathBuf>,
}

pub struct Storage {
    handle: StorageHandle,
    writer: Option<thread::JoinHandle<()>>,
}

impl Storage {
    pub fn open(database_path: impl AsRef<Path>) -> Result<Self, StorageError> {
        let database_path = database_path.as_ref().to_path_buf();
        prepare_database_path(&database_path)?;

        let (sender, receiver) = mpsc::channel();
        let (startup_sender, startup_receiver) = mpsc::sync_channel(1);
        let writer_path = database_path.clone();
        let writer = thread::Builder::new()
            .name("fubun-sqlite-writer".to_owned())
            .spawn(move || writer_main(&writer_path, receiver, startup_sender))?;

        startup_receiver
            .recv()
            .map_err(|_| StorageError::QueueClosed)?
            .map_err(StorageError::Startup)?;

        Ok(Self {
            handle: StorageHandle {
                sender,
                database_path: Arc::new(database_path),
            },
            writer: Some(writer),
        })
    }

    #[must_use]
    pub fn handle(&self) -> StorageHandle {
        self.handle.clone()
    }

    pub async fn shutdown(mut self) -> Result<(), StorageError> {
        let (response_sender, response_receiver) = oneshot::channel();
        self.handle
            .sender
            .send(Command::Shutdown(response_sender))
            .map_err(|_| StorageError::QueueClosed)?;
        response_receiver
            .await
            .map_err(|_| StorageError::ResponseDropped)?;

        if let Some(writer) = self.writer.take() {
            tokio::task::spawn_blocking(move || writer.join())
                .await
                .map_err(|_| StorageError::WriterPanicked)?
                .map_err(|_| StorageError::WriterPanicked)?;
        }
        Ok(())
    }
}

impl StorageHandle {
    pub async fn insert_event(&self, event: Event) -> Result<(), StorageError> {
        event.validate()?;
        let (response_sender, response_receiver) = oneshot::channel();
        self.sender
            .send(Command::Insert(Box::new(event), response_sender))
            .map_err(|_| StorageError::QueueClosed)?;
        response_receiver
            .await
            .map_err(|_| StorageError::ResponseDropped)?
    }

    pub async fn list_events(
        &self,
        since: Option<OffsetDateTime>,
        limit: u32,
    ) -> Result<Vec<Event>, StorageError> {
        let (response_sender, response_receiver) = oneshot::channel();
        self.sender
            .send(Command::List {
                since,
                limit,
                response: response_sender,
            })
            .map_err(|_| StorageError::QueueClosed)?;
        response_receiver
            .await
            .map_err(|_| StorageError::ResponseDropped)?
    }

    pub async fn schema_version(&self) -> Result<u32, StorageError> {
        let (response_sender, response_receiver) = oneshot::channel();
        self.sender
            .send(Command::SchemaVersion(response_sender))
            .map_err(|_| StorageError::QueueClosed)?;
        response_receiver
            .await
            .map_err(|_| StorageError::ResponseDropped)?
    }

    #[must_use]
    pub fn database_path(&self) -> &Path {
        self.database_path.as_path()
    }
}

fn prepare_database_path(path: &Path) -> Result<(), StorageError> {
    let parent = path.parent().ok_or_else(|| {
        StorageError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "database path has no parent",
        ))
    })?;
    fs::create_dir_all(parent)?;
    fs::set_permissions(parent, fs::Permissions::from_mode(0o700))?;
    Ok(())
}

fn writer_main(
    database_path: &Path,
    receiver: mpsc::Receiver<Command>,
    startup: mpsc::SyncSender<Result<(), String>>,
) {
    let mut connection = match initialize_connection(database_path) {
        Ok(connection) => {
            let _ = startup.send(Ok(()));
            connection
        }
        Err(error) => {
            let _ = startup.send(Err(error.to_string()));
            return;
        }
    };

    for command in receiver {
        match command {
            Command::Insert(event, response) => {
                let _ = response.send(insert_event(&mut connection, &event));
            }
            Command::List {
                since,
                limit,
                response,
            } => {
                let _ = response.send(list_events(&connection, since, limit));
            }
            Command::SchemaVersion(response) => {
                let _ = response.send(current_schema_version(&connection));
            }
            Command::Shutdown(response) => {
                let _ = response.send(());
                break;
            }
        }
    }
}

fn initialize_connection(path: &Path) -> Result<Connection, StorageError> {
    let mut connection = Connection::open(path)?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    connection.busy_timeout(Duration::from_secs(5))?;
    connection.pragma_update(None, "foreign_keys", true)?;
    connection.pragma_update(None, "journal_mode", "WAL")?;
    migrate(&mut connection)?;
    Ok(connection)
}

fn migrate(connection: &mut Connection) -> Result<(), StorageError> {
    connection.execute_batch(
        "CREATE TABLE IF NOT EXISTS schema_migrations (
            version INTEGER PRIMARY KEY,
            applied_at TEXT NOT NULL
        );",
    )?;
    let current = current_schema_version(connection)?;
    if current > SCHEMA_VERSION {
        return Err(StorageError::UnsupportedSchema(current));
    }
    if current == 0 {
        let transaction = connection.transaction()?;
        transaction.execute_batch(
            "CREATE TABLE adapters (
                instance_id TEXT PRIMARY KEY,
                adapter_id TEXT NOT NULL,
                adapter_version TEXT NOT NULL,
                last_sequence_no INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE settings (
                key TEXT PRIMARY KEY,
                value_json TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE events (
                id TEXT PRIMARY KEY,
                spec_version TEXT NOT NULL,
                event_type TEXT NOT NULL,
                source TEXT NOT NULL,
                occurred_at TEXT NOT NULL,
                received_at TEXT NOT NULL,
                actor TEXT NOT NULL,
                adapter_id TEXT NOT NULL,
                adapter_version TEXT NOT NULL,
                adapter_instance_id TEXT NOT NULL,
                sequence_no INTEGER NOT NULL CHECK(sequence_no >= 0),
                context_json TEXT,
                privacy TEXT NOT NULL,
                data_json TEXT NOT NULL,
                canonical_json TEXT NOT NULL,
                FOREIGN KEY(adapter_instance_id) REFERENCES adapters(instance_id),
                UNIQUE(adapter_instance_id, sequence_no)
            );
            CREATE INDEX events_received_at_idx ON events(received_at);",
        )?;
        let now = OffsetDateTime::now_utc().format(&Rfc3339)?;
        transaction.execute(
            "INSERT INTO schema_migrations(version, applied_at) VALUES (?1, ?2)",
            params![SCHEMA_VERSION, now],
        )?;
        transaction.commit()?;
    }
    Ok(())
}

fn current_schema_version(connection: &Connection) -> Result<u32, StorageError> {
    let version = connection
        .query_row("SELECT MAX(version) FROM schema_migrations", [], |row| {
            row.get::<_, Option<u32>>(0)
        })?
        .unwrap_or(0);
    Ok(version)
}

fn insert_event(connection: &mut Connection, event: &Event) -> Result<(), StorageError> {
    let occurred_at = event.occurred_at.format(&Rfc3339)?;
    let received_at = event.received_at.format(&Rfc3339)?;
    let context_json = event
        .context
        .as_ref()
        .map(serde_json::to_string)
        .transpose()?;
    let data_json = serde_json::to_string(&event.data)?;
    let canonical_json = serde_json::to_string(event)?;
    let sequence_no = i64::try_from(event.adapter.sequence_no)
        .map_err(|_| StorageError::Validation(ValidationError::SequenceOutOfRange))?;

    let transaction = connection.transaction()?;
    transaction.execute(
        "INSERT INTO adapters(
            instance_id, adapter_id, adapter_version, last_sequence_no, created_at, updated_at
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?5)
        ON CONFLICT(instance_id) DO UPDATE SET
            adapter_id=excluded.adapter_id,
            adapter_version=excluded.adapter_version,
            last_sequence_no=MAX(adapters.last_sequence_no, excluded.last_sequence_no),
            updated_at=excluded.updated_at",
        params![
            event.adapter.instance_id.to_string(),
            event.adapter.id,
            event.adapter.version,
            sequence_no,
            received_at,
        ],
    )?;

    let result = transaction.execute(
        "INSERT INTO events(
            id, spec_version, event_type, source, occurred_at, received_at, actor,
            adapter_id, adapter_version, adapter_instance_id, sequence_no,
            context_json, privacy, data_json, canonical_json
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)",
        params![
            event.id.to_string(),
            event.spec_version,
            event_type_name(event.event_type),
            event.source,
            occurred_at,
            received_at,
            actor_name(event.actor),
            event.adapter.id,
            event.adapter.version,
            event.adapter.instance_id.to_string(),
            sequence_no,
            context_json,
            privacy_name(event.privacy),
            data_json,
            canonical_json,
        ],
    );

    match result {
        Ok(_) => transaction.commit().map_err(StorageError::from),
        Err(error) if is_duplicate_constraint(&error) => Err(StorageError::DuplicateEvent),
        Err(error) => Err(StorageError::Sqlite(error)),
    }
}

fn list_events(
    connection: &Connection,
    since: Option<OffsetDateTime>,
    limit: u32,
) -> Result<Vec<Event>, StorageError> {
    let since = since.map(|value| value.format(&Rfc3339)).transpose()?;
    let mut statement = connection.prepare(
        "SELECT canonical_json FROM events
         WHERE (?1 IS NULL OR received_at >= ?1)
         ORDER BY received_at ASC, id ASC
         LIMIT ?2",
    )?;
    let rows = statement.query_map(params![since, i64::from(limit.min(1000))], |row| {
        row.get::<_, String>(0)
    })?;
    let serialized = rows.collect::<Result<Vec<_>, _>>()?;
    serialized
        .into_iter()
        .map(|json| serde_json::from_str(&json).map_err(StorageError::from))
        .collect()
}

fn is_duplicate_constraint(error: &rusqlite::Error) -> bool {
    matches!(
        error,
        rusqlite::Error::SqliteFailure(inner, _)
            if inner.code == ErrorCode::ConstraintViolation
    )
}

const fn actor_name(actor: Actor) -> &'static str {
    match actor {
        Actor::User => "user",
        Actor::Fubun => "fubun",
        Actor::System => "system",
        Actor::Imported => "imported",
    }
}

const fn privacy_name(privacy: PrivacyClass) -> &'static str {
    match privacy {
        PrivacyClass::Normal => "normal",
        PrivacyClass::Sensitive => "sensitive",
    }
}

const fn event_type_name(event_type: EventType) -> &'static str {
    match event_type {
        EventType::SyntheticV1 => "dev.fubun.dev.synthetic.v1",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fubun_domain::{AdapterIdentity, SyntheticEventData, EVENT_SPEC_VERSION};
    use tempfile::TempDir;
    use uuid::Uuid;

    fn event(instance_id: Uuid) -> Event {
        Event {
            spec_version: EVENT_SPEC_VERSION.to_owned(),
            id: Uuid::new_v4(),
            event_type: EventType::SyntheticV1,
            source: "storage-test".to_owned(),
            occurred_at: OffsetDateTime::now_utc(),
            received_at: OffsetDateTime::now_utc(),
            actor: Actor::User,
            adapter: AdapterIdentity {
                id: "dev.test".to_owned(),
                version: "0.1.0".to_owned(),
                instance_id,
                sequence_no: 1,
            },
            context: None,
            privacy: PrivacyClass::Normal,
            data: SyntheticEventData {
                label: "storage-smoke".to_owned(),
                counter: 1,
            },
        }
    }

    #[tokio::test]
    async fn persists_across_restart_and_rejects_duplicate() {
        let temp = TempDir::new().expect("tempdir");
        let path = temp.path().join("fubun/fubun.db");
        let instance_id = Uuid::new_v4();
        let storage = Storage::open(&path).expect("open storage");
        let handle = storage.handle();
        handle
            .insert_event(event(instance_id))
            .await
            .expect("insert event");
        let duplicate = event(instance_id);
        let error = handle
            .insert_event(duplicate)
            .await
            .expect_err("duplicate must fail");
        assert!(matches!(error, StorageError::DuplicateEvent));
        storage.shutdown().await.expect("shutdown");

        let reopened = Storage::open(&path).expect("reopen storage");
        let events = reopened
            .handle()
            .list_events(None, 100)
            .await
            .expect("list events");
        assert_eq!(events.len(), 1);
        reopened.shutdown().await.expect("shutdown reopened");
    }

    #[test]
    fn data_directory_and_database_permissions_are_private() {
        let temp = TempDir::new().expect("tempdir");
        let path = temp.path().join("fubun/fubun.db");
        let storage = Storage::open(&path).expect("open storage");
        let directory_mode = fs::metadata(path.parent().expect("parent"))
            .expect("directory metadata")
            .permissions()
            .mode()
            & 0o777;
        let database_mode = fs::metadata(&path)
            .expect("database metadata")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(directory_mode, 0o700);
        assert_eq!(database_mode, 0o600);
        drop(storage);
    }
}
