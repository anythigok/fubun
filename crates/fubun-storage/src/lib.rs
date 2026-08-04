//! SQLite persistence behind a single dedicated writer thread.

use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    sync::{mpsc, Arc},
    thread,
    time::Duration,
};

use fubun_domain::{
    Actor, Approval, Event, EventType, Execution, ExecutionStatus, ExecutionStep,
    ExecutionStepStatus, PrivacyClass, Resource, ResourceKind, ResourceScope, Ritual,
    RitualDefinition, RitualStatus, RitualVersion, Sensitivity, TriggerKind, ValidationError,
};
use rusqlite::{params, Connection, ErrorCode, OptionalExtension};
use thiserror::Error;
use time::{format_description::well_known::Rfc3339, OffsetDateTime};
use tokio::sync::oneshot;
use uuid::Uuid;

pub const SCHEMA_VERSION: u32 = 2;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("SQLite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("timestamp formatting error: {0}")]
    Time(#[from] time::error::Format),
    #[error("timestamp parsing error: {0}")]
    TimeParse(#[from] time::error::Parse),
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
    #[error("requested record was not found")]
    NotFound,
    #[error("resource already exists")]
    DuplicateResource,
    #[error("ritual is already running")]
    RitualAlreadyRunning,
    #[error("stored data is invalid: {0}")]
    InvalidData(String),
}

enum Operation {
    InsertEvent(Event),
    ListEvents {
        since: Option<OffsetDateTime>,
        limit: u32,
    },
    SchemaVersion,
    CreateResource(Resource),
    ListResources,
    GetResource(Uuid),
    CreateRitual {
        ritual: Ritual,
        version: RitualVersion,
        definition: RitualDefinition,
    },
    UpdateRitual {
        ritual: Ritual,
        version: RitualVersion,
        definition: RitualDefinition,
    },
    ListRituals,
    GetRitual(Uuid),
    SetRitualStatus {
        ritual_id: Uuid,
        status: RitualStatus,
    },
    CreateApprovals(Vec<Approval>),
    ListApprovals(Uuid),
    StartExecution {
        execution: Execution,
        steps: Vec<ExecutionStep>,
    },
    UpdateExecution(Execution),
    UpdateStep(ExecutionStep),
    ListExecutions,
    GetExecution(Uuid),
    AbortRunningExecutions,
    Counts,
}

enum StorageResponse {
    Unit,
    Events(Vec<Event>),
    SchemaVersion(u32),
    Resource(Resource),
    Resources(Vec<Resource>),
    RitualRecord {
        ritual: Ritual,
        version: RitualVersion,
        definition: RitualDefinition,
    },
    Rituals(Vec<Ritual>),
    Approvals(Vec<Approval>),
    ExecutionRecord {
        execution: Execution,
        steps: Vec<ExecutionStep>,
    },
    Executions(Vec<Execution>),
    Counts {
        running: usize,
        draft: usize,
        active: usize,
    },
}

#[allow(clippy::large_enum_variant)]
enum Command {
    Operation(
        Operation,
        oneshot::Sender<Result<StorageResponse, StorageError>>,
    ),
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
        match self.request(Operation::InsertEvent(event)).await? {
            StorageResponse::Unit => Ok(()),
            _ => Err(StorageError::InvalidData(
                "unexpected insert response".to_owned(),
            )),
        }
    }

    pub async fn list_events(
        &self,
        since: Option<OffsetDateTime>,
        limit: u32,
    ) -> Result<Vec<Event>, StorageError> {
        match self.request(Operation::ListEvents { since, limit }).await? {
            StorageResponse::Events(events) => Ok(events),
            _ => Err(StorageError::InvalidData(
                "unexpected event list response".to_owned(),
            )),
        }
    }

    pub async fn schema_version(&self) -> Result<u32, StorageError> {
        match self.request(Operation::SchemaVersion).await? {
            StorageResponse::SchemaVersion(version) => Ok(version),
            _ => Err(StorageError::InvalidData(
                "unexpected schema response".to_owned(),
            )),
        }
    }

    async fn request(&self, operation: Operation) -> Result<StorageResponse, StorageError> {
        let (response_sender, response_receiver) = oneshot::channel();
        self.sender
            .send(Command::Operation(operation, response_sender))
            .map_err(|_| StorageError::QueueClosed)?;
        response_receiver
            .await
            .map_err(|_| StorageError::ResponseDropped)?
    }

    pub async fn create_resource(&self, resource: Resource) -> Result<Resource, StorageError> {
        match self.request(Operation::CreateResource(resource)).await? {
            StorageResponse::Resource(resource) => Ok(resource),
            _ => Err(StorageError::InvalidData(
                "unexpected resource response".to_owned(),
            )),
        }
    }

    pub async fn list_resources(&self) -> Result<Vec<Resource>, StorageError> {
        match self.request(Operation::ListResources).await? {
            StorageResponse::Resources(resources) => Ok(resources),
            _ => Err(StorageError::InvalidData(
                "unexpected resource list response".to_owned(),
            )),
        }
    }

    pub async fn get_resource(&self, resource_id: Uuid) -> Result<Resource, StorageError> {
        match self.request(Operation::GetResource(resource_id)).await? {
            StorageResponse::Resource(resource) => Ok(resource),
            _ => Err(StorageError::InvalidData(
                "unexpected resource response".to_owned(),
            )),
        }
    }

    pub async fn create_ritual(
        &self,
        ritual: Ritual,
        version: RitualVersion,
        definition: RitualDefinition,
    ) -> Result<(Ritual, RitualVersion, RitualDefinition), StorageError> {
        match self
            .request(Operation::CreateRitual {
                ritual,
                version,
                definition,
            })
            .await?
        {
            StorageResponse::RitualRecord {
                ritual,
                version,
                definition,
            } => Ok((ritual, version, definition)),
            _ => Err(StorageError::InvalidData(
                "unexpected ritual response".to_owned(),
            )),
        }
    }

    pub async fn update_ritual(
        &self,
        ritual: Ritual,
        version: RitualVersion,
        definition: RitualDefinition,
    ) -> Result<(Ritual, RitualVersion, RitualDefinition), StorageError> {
        match self
            .request(Operation::UpdateRitual {
                ritual,
                version,
                definition,
            })
            .await?
        {
            StorageResponse::RitualRecord {
                ritual,
                version,
                definition,
            } => Ok((ritual, version, definition)),
            _ => Err(StorageError::InvalidData(
                "unexpected ritual response".to_owned(),
            )),
        }
    }

    pub async fn list_rituals(&self) -> Result<Vec<Ritual>, StorageError> {
        match self.request(Operation::ListRituals).await? {
            StorageResponse::Rituals(rituals) => Ok(rituals),
            _ => Err(StorageError::InvalidData(
                "unexpected ritual list response".to_owned(),
            )),
        }
    }

    pub async fn get_ritual(
        &self,
        ritual_id: Uuid,
    ) -> Result<(Ritual, RitualVersion, RitualDefinition), StorageError> {
        match self.request(Operation::GetRitual(ritual_id)).await? {
            StorageResponse::RitualRecord {
                ritual,
                version,
                definition,
            } => Ok((ritual, version, definition)),
            _ => Err(StorageError::InvalidData(
                "unexpected ritual response".to_owned(),
            )),
        }
    }

    pub async fn set_ritual_status(
        &self,
        ritual_id: Uuid,
        status: RitualStatus,
    ) -> Result<(), StorageError> {
        match self
            .request(Operation::SetRitualStatus { ritual_id, status })
            .await?
        {
            StorageResponse::Unit => Ok(()),
            _ => Err(StorageError::InvalidData(
                "unexpected status response".to_owned(),
            )),
        }
    }

    pub async fn create_approvals(&self, approvals: Vec<Approval>) -> Result<(), StorageError> {
        match self.request(Operation::CreateApprovals(approvals)).await? {
            StorageResponse::Unit => Ok(()),
            _ => Err(StorageError::InvalidData(
                "unexpected approval response".to_owned(),
            )),
        }
    }

    pub async fn list_approvals(&self, version_id: Uuid) -> Result<Vec<Approval>, StorageError> {
        match self.request(Operation::ListApprovals(version_id)).await? {
            StorageResponse::Approvals(approvals) => Ok(approvals),
            _ => Err(StorageError::InvalidData(
                "unexpected approval list response".to_owned(),
            )),
        }
    }

    pub async fn start_execution(
        &self,
        execution: Execution,
        steps: Vec<ExecutionStep>,
    ) -> Result<(), StorageError> {
        match self
            .request(Operation::StartExecution { execution, steps })
            .await?
        {
            StorageResponse::Unit => Ok(()),
            _ => Err(StorageError::InvalidData(
                "unexpected execution response".to_owned(),
            )),
        }
    }

    pub async fn update_execution(&self, execution: Execution) -> Result<(), StorageError> {
        match self.request(Operation::UpdateExecution(execution)).await? {
            StorageResponse::Unit => Ok(()),
            _ => Err(StorageError::InvalidData(
                "unexpected execution response".to_owned(),
            )),
        }
    }

    pub async fn update_step(&self, step: ExecutionStep) -> Result<(), StorageError> {
        match self.request(Operation::UpdateStep(step)).await? {
            StorageResponse::Unit => Ok(()),
            _ => Err(StorageError::InvalidData(
                "unexpected step response".to_owned(),
            )),
        }
    }

    pub async fn list_executions(&self) -> Result<Vec<Execution>, StorageError> {
        match self.request(Operation::ListExecutions).await? {
            StorageResponse::Executions(executions) => Ok(executions),
            _ => Err(StorageError::InvalidData(
                "unexpected execution list response".to_owned(),
            )),
        }
    }

    pub async fn get_execution(
        &self,
        execution_id: Uuid,
    ) -> Result<(Execution, Vec<ExecutionStep>), StorageError> {
        match self.request(Operation::GetExecution(execution_id)).await? {
            StorageResponse::ExecutionRecord { execution, steps } => Ok((execution, steps)),
            _ => Err(StorageError::InvalidData(
                "unexpected execution response".to_owned(),
            )),
        }
    }

    pub async fn abort_running_executions(&self) -> Result<(), StorageError> {
        match self.request(Operation::AbortRunningExecutions).await? {
            StorageResponse::Unit => Ok(()),
            _ => Err(StorageError::InvalidData(
                "unexpected abort response".to_owned(),
            )),
        }
    }

    pub async fn counts(&self) -> Result<(usize, usize, usize), StorageError> {
        match self.request(Operation::Counts).await? {
            StorageResponse::Counts {
                running,
                draft,
                active,
            } => Ok((running, draft, active)),
            _ => Err(StorageError::InvalidData(
                "unexpected counts response".to_owned(),
            )),
        }
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
            Command::Operation(operation, response) => {
                let result = execute_operation(&mut connection, operation);
                let _ = response.send(result);
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
            params![1_u32, now],
        )?;
        transaction.commit()?;
    }
    let current = current_schema_version(connection)?;
    if current == 1 {
        let transaction = connection.transaction()?;
        transaction.execute_batch(
            "CREATE TABLE resources (
                id TEXT PRIMARY KEY,
                kind TEXT NOT NULL,
                label TEXT NOT NULL,
                locator TEXT NOT NULL,
                canonical_locator TEXT NOT NULL,
                sensitivity TEXT NOT NULL,
                scope TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE(canonical_locator)
            );
            CREATE TABLE rituals (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                status TEXT NOT NULL,
                current_version_id TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
                ,FOREIGN KEY(current_version_id) REFERENCES ritual_versions(id) DEFERRABLE INITIALLY DEFERRED
            );
            CREATE TABLE ritual_versions (
                id TEXT PRIMARY KEY,
                ritual_id TEXT NOT NULL,
                version INTEGER NOT NULL,
                schema_version TEXT NOT NULL,
                canonical_json TEXT NOT NULL,
                content_hash TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY(ritual_id) REFERENCES rituals(id),
                UNIQUE(ritual_id, version)
            );
            CREATE TABLE ritual_approvals (
                id TEXT PRIMARY KEY,
                ritual_version_id TEXT NOT NULL,
                action_type TEXT NOT NULL,
                capability TEXT NOT NULL,
                resource_id TEXT,
                app_id TEXT,
                content_hash TEXT NOT NULL,
                approved_at TEXT NOT NULL,
                FOREIGN KEY(ritual_version_id) REFERENCES ritual_versions(id)
            );
            CREATE TABLE executions (
                id TEXT PRIMARY KEY,
                ritual_id TEXT NOT NULL,
                ritual_version_id TEXT NOT NULL,
                status TEXT NOT NULL,
                trigger_kind TEXT NOT NULL,
                started_at TEXT NOT NULL,
                finished_at TEXT,
                failure_code TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY(ritual_id) REFERENCES rituals(id),
                FOREIGN KEY(ritual_version_id) REFERENCES ritual_versions(id)
            );
            CREATE TABLE execution_steps (
                id TEXT PRIMARY KEY,
                execution_id TEXT NOT NULL,
                step_index INTEGER NOT NULL,
                action_type TEXT NOT NULL,
                status TEXT NOT NULL,
                adapter_id TEXT,
                started_at TEXT NOT NULL,
                finished_at TEXT,
                result_code TEXT,
                redacted_message TEXT,
                FOREIGN KEY(execution_id) REFERENCES executions(id),
                UNIQUE(execution_id, step_index)
            );
            CREATE TABLE ritual_execution_locks (
                ritual_id TEXT PRIMARY KEY,
                execution_id TEXT NOT NULL,
                FOREIGN KEY(ritual_id) REFERENCES rituals(id),
                FOREIGN KEY(execution_id) REFERENCES executions(id)
            );
            CREATE INDEX ritual_versions_ritual_idx ON ritual_versions(ritual_id, version);
            CREATE INDEX executions_created_idx ON executions(created_at);
            CREATE INDEX execution_steps_execution_idx ON execution_steps(execution_id, step_index);",
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

fn execute_operation(
    connection: &mut Connection,
    operation: Operation,
) -> Result<StorageResponse, StorageError> {
    match operation {
        Operation::InsertEvent(event) => {
            insert_event(connection, &event)?;
            Ok(StorageResponse::Unit)
        }
        Operation::ListEvents { since, limit } => Ok(StorageResponse::Events(list_events(
            connection, since, limit,
        )?)),
        Operation::SchemaVersion => Ok(StorageResponse::SchemaVersion(current_schema_version(
            connection,
        )?)),
        Operation::CreateResource(resource) => {
            let result = connection.execute(
                "INSERT INTO resources(
                    id, kind, label, locator, canonical_locator, sensitivity, scope, created_at, updated_at
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?8)",
                params![
                    resource.id.to_string(),
                    resource_kind_name(resource.kind),
                    resource.label,
                    resource.locator,
                    resource.canonical_locator,
                    sensitivity_name(resource.sensitivity),
                    resource_scope_name(resource.scope),
                    format_ts(resource.created_at)?,
                ],
            );
            match result {
                Ok(_) => Ok(StorageResponse::Resource(resource)),
                Err(error) if is_duplicate_constraint(&error) => {
                    Err(StorageError::DuplicateResource)
                }
                Err(error) => Err(StorageError::Sqlite(error)),
            }
        }
        Operation::ListResources => Ok(StorageResponse::Resources(list_resources(connection)?)),
        Operation::GetResource(resource_id) => Ok(StorageResponse::Resource(get_resource(
            connection,
            resource_id,
        )?)),
        Operation::CreateRitual {
            ritual,
            version,
            definition,
        } => create_ritual(connection, ritual, version, definition),
        Operation::UpdateRitual {
            ritual,
            version,
            definition,
        } => update_ritual(connection, ritual, version, definition),
        Operation::ListRituals => Ok(StorageResponse::Rituals(list_rituals(connection)?)),
        Operation::GetRitual(ritual_id) => {
            let (ritual, version, definition) = get_ritual(connection, ritual_id)?;
            Ok(StorageResponse::RitualRecord {
                ritual,
                version,
                definition,
            })
        }
        Operation::SetRitualStatus { ritual_id, status } => {
            let changed = connection.execute(
                "UPDATE rituals SET status = ?1, updated_at = ?2 WHERE id = ?3",
                params![
                    ritual_status_name(status),
                    format_ts(OffsetDateTime::now_utc())?,
                    ritual_id.to_string()
                ],
            )?;
            if changed == 0 {
                return Err(StorageError::NotFound);
            }
            Ok(StorageResponse::Unit)
        }
        Operation::CreateApprovals(approvals) => {
            let transaction = connection.transaction()?;
            for approval in approvals {
                transaction.execute(
                    "INSERT INTO ritual_approvals(
                        id, ritual_version_id, action_type, capability, resource_id, app_id, content_hash, approved_at
                    ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                    params![
                        approval.id.to_string(),
                        approval.ritual_version_id.to_string(),
                        approval.action_type,
                        approval.capability,
                        approval.resource_id.map(|id| id.to_string()),
                        approval.app_id,
                        approval.content_hash,
                        format_ts(approval.approved_at)?,
                    ],
                )?;
            }
            transaction.commit()?;
            Ok(StorageResponse::Unit)
        }
        Operation::ListApprovals(version_id) => Ok(StorageResponse::Approvals(list_approvals(
            connection, version_id,
        )?)),
        Operation::StartExecution { execution, steps } => {
            start_execution(connection, execution, steps)
        }
        Operation::UpdateExecution(execution) => {
            update_execution(connection, execution)?;
            Ok(StorageResponse::Unit)
        }
        Operation::UpdateStep(step) => {
            update_step(connection, step)?;
            Ok(StorageResponse::Unit)
        }
        Operation::ListExecutions => Ok(StorageResponse::Executions(list_executions(connection)?)),
        Operation::GetExecution(execution_id) => {
            let (execution, steps) = get_execution(connection, execution_id)?;
            Ok(StorageResponse::ExecutionRecord { execution, steps })
        }
        Operation::AbortRunningExecutions => {
            abort_running_executions(connection)?;
            Ok(StorageResponse::Unit)
        }
        Operation::Counts => {
            let running = count_where(connection, "executions", "status = 'running'")?;
            let draft = count_where(connection, "rituals", "status = 'draft'")?;
            let active = count_where(connection, "rituals", "status = 'active'")?;
            Ok(StorageResponse::Counts {
                running,
                draft,
                active,
            })
        }
    }
}

fn list_resources(connection: &Connection) -> Result<Vec<Resource>, StorageError> {
    let mut statement = connection.prepare(
        "SELECT id, kind, label, locator, canonical_locator, sensitivity, scope, created_at, updated_at
         FROM resources ORDER BY created_at ASC, id ASC",
    )?;
    let rows = statement.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, String>(3)?,
            row.get::<_, String>(4)?,
            row.get::<_, String>(5)?,
            row.get::<_, String>(6)?,
            row.get::<_, String>(7)?,
            row.get::<_, String>(8)?,
        ))
    })?;
    rows.map(|row| row.map_err(StorageError::from).and_then(resource_from_row))
        .collect()
}

fn get_resource(connection: &Connection, resource_id: Uuid) -> Result<Resource, StorageError> {
    let mut statement = connection.prepare(
        "SELECT id, kind, label, locator, canonical_locator, sensitivity, scope, created_at, updated_at
         FROM resources WHERE id = ?1",
    )?;
    let row = statement
        .query_row(params![resource_id.to_string()], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, String>(6)?,
                row.get::<_, String>(7)?,
                row.get::<_, String>(8)?,
            ))
        })
        .optional()
        .map_err(StorageError::from)?
        .ok_or(StorageError::NotFound)?;
    resource_from_row(row)
}

fn resource_from_row(
    row: (
        String,
        String,
        String,
        String,
        String,
        String,
        String,
        String,
        String,
    ),
) -> Result<Resource, StorageError> {
    Ok(Resource {
        id: parse_uuid(&row.0)?,
        kind: parse_resource_kind(&row.1)?,
        label: row.2,
        locator: row.3,
        canonical_locator: row.4,
        sensitivity: parse_sensitivity(&row.5)?,
        scope: parse_resource_scope(&row.6)?,
        created_at: parse_ts(&row.7)?,
        updated_at: parse_ts(&row.8)?,
    })
}

fn create_ritual(
    connection: &mut Connection,
    ritual: Ritual,
    version: RitualVersion,
    definition: RitualDefinition,
) -> Result<StorageResponse, StorageError> {
    let transaction = connection.transaction()?;
    transaction.execute(
        "INSERT INTO rituals(id, name, status, current_version_id, created_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![
            ritual.id.to_string(),
            ritual.name,
            ritual_status_name(ritual.status),
            version.id.to_string(),
            format_ts(ritual.created_at)?,
            format_ts(ritual.updated_at)?,
        ],
    )?;
    insert_ritual_version(&transaction, &version)?;
    transaction.commit()?;
    Ok(StorageResponse::RitualRecord {
        ritual,
        version,
        definition,
    })
}

fn update_ritual(
    connection: &mut Connection,
    ritual: Ritual,
    version: RitualVersion,
    definition: RitualDefinition,
) -> Result<StorageResponse, StorageError> {
    let transaction = connection.transaction()?;
    let changed = transaction.execute(
        "UPDATE rituals SET name = ?1, status = ?2, current_version_id = ?3, updated_at = ?4 WHERE id = ?5",
        params![
            ritual.name,
            ritual_status_name(ritual.status),
            version.id.to_string(),
            format_ts(ritual.updated_at)?,
            ritual.id.to_string(),
        ],
    )?;
    if changed == 0 {
        return Err(StorageError::NotFound);
    }
    insert_ritual_version(&transaction, &version)?;
    transaction.commit()?;
    Ok(StorageResponse::RitualRecord {
        ritual,
        version,
        definition,
    })
}

fn insert_ritual_version(
    transaction: &rusqlite::Transaction<'_>,
    version: &RitualVersion,
) -> Result<(), StorageError> {
    transaction.execute(
        "INSERT INTO ritual_versions(
            id, ritual_id, version, schema_version, canonical_json, content_hash, created_at
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            version.id.to_string(),
            version.ritual_id.to_string(),
            i64::from(version.version),
            version.schema_version,
            version.canonical_json,
            version.content_hash,
            format_ts(version.created_at)?,
        ],
    )?;
    Ok(())
}

fn list_rituals(connection: &Connection) -> Result<Vec<Ritual>, StorageError> {
    let mut statement = connection.prepare(
        "SELECT id, name, status, current_version_id, created_at, updated_at
         FROM rituals ORDER BY created_at ASC, id ASC",
    )?;
    let rows = statement.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, String>(3)?,
            row.get::<_, String>(4)?,
            row.get::<_, String>(5)?,
        ))
    })?;
    rows.map(|row| row.map_err(StorageError::from).and_then(ritual_from_row))
        .collect()
}

fn ritual_from_row(
    row: (String, String, String, String, String, String),
) -> Result<Ritual, StorageError> {
    Ok(Ritual {
        id: parse_uuid(&row.0)?,
        name: row.1,
        status: parse_ritual_status(&row.2)?,
        current_version_id: parse_uuid(&row.3)?,
        created_at: parse_ts(&row.4)?,
        updated_at: parse_ts(&row.5)?,
    })
}

fn get_ritual(
    connection: &Connection,
    ritual_id: Uuid,
) -> Result<(Ritual, RitualVersion, RitualDefinition), StorageError> {
    let ritual = {
        let mut statement = connection.prepare(
            "SELECT id, name, status, current_version_id, created_at, updated_at
             FROM rituals WHERE id = ?1",
        )?;
        let row = statement
            .query_row(params![ritual_id.to_string()], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                ))
            })
            .optional()
            .map_err(StorageError::from)?
            .ok_or(StorageError::NotFound)?;
        ritual_from_row(row)?
    };
    let version = {
        let mut statement = connection.prepare(
            "SELECT id, ritual_id, version, schema_version, canonical_json, content_hash, created_at
             FROM ritual_versions WHERE id = ?1",
        )?;
        let row = statement.query_row(params![ritual.current_version_id.to_string()], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, String>(6)?,
            ))
        })?;
        RitualVersion {
            id: parse_uuid(&row.0)?,
            ritual_id: parse_uuid(&row.1)?,
            version: u32::try_from(row.2)
                .map_err(|_| StorageError::InvalidData("version".to_owned()))?,
            schema_version: row.3,
            canonical_json: row.4,
            content_hash: row.5,
            created_at: parse_ts(&row.6)?,
        }
    };
    let definition = serde_json::from_str(&version.canonical_json)?;
    Ok((ritual, version, definition))
}

fn list_approvals(
    connection: &Connection,
    version_id: Uuid,
) -> Result<Vec<Approval>, StorageError> {
    let mut statement = connection.prepare(
        "SELECT id, ritual_version_id, action_type, capability, resource_id, app_id, content_hash, approved_at
         FROM ritual_approvals WHERE ritual_version_id = ?1 ORDER BY id ASC",
    )?;
    let rows = statement.query_map(params![version_id.to_string()], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, String>(3)?,
            row.get::<_, Option<String>>(4)?,
            row.get::<_, Option<String>>(5)?,
            row.get::<_, String>(6)?,
            row.get::<_, String>(7)?,
        ))
    })?;
    rows.map(|row| {
        let row = row.map_err(StorageError::from)?;
        Ok(Approval {
            id: parse_uuid(&row.0)?,
            ritual_version_id: parse_uuid(&row.1)?,
            action_type: row.2,
            capability: row.3,
            resource_id: row.4.map(|value| parse_uuid(&value)).transpose()?,
            app_id: row.5,
            content_hash: row.6,
            approved_at: parse_ts(&row.7)?,
        })
    })
    .collect()
}

fn start_execution(
    connection: &mut Connection,
    execution: Execution,
    steps: Vec<ExecutionStep>,
) -> Result<StorageResponse, StorageError> {
    let transaction = connection.transaction()?;
    transaction.execute(
        "INSERT INTO executions(
            id, ritual_id, ritual_version_id, status, trigger_kind, started_at, finished_at, failure_code, created_at
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        params![
            execution.id.to_string(),
            execution.ritual_id.to_string(),
            execution.ritual_version_id.to_string(),
            execution_status_name(execution.status),
            trigger_kind_name(execution.trigger_kind),
            format_ts(execution.started_at)?,
            execution.finished_at.map(format_ts).transpose()?,
            execution.failure_code,
            format_ts(execution.created_at)?,
        ],
    )?;
    let lock = transaction.execute(
        "INSERT INTO ritual_execution_locks(ritual_id, execution_id) VALUES (?1, ?2)",
        params![execution.ritual_id.to_string(), execution.id.to_string()],
    );
    if let Err(error) = lock {
        if is_duplicate_constraint(&error) {
            return Err(StorageError::RitualAlreadyRunning);
        }
        return Err(StorageError::Sqlite(error));
    }
    for step in steps {
        insert_step(&transaction, &step)?;
    }
    transaction.commit()?;
    Ok(StorageResponse::Unit)
}

fn insert_step(
    transaction: &rusqlite::Transaction<'_>,
    step: &ExecutionStep,
) -> Result<(), StorageError> {
    transaction.execute(
        "INSERT INTO execution_steps(
            id, execution_id, step_index, action_type, status, adapter_id, started_at, finished_at, result_code, redacted_message
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
        params![
            step.id.to_string(),
            step.execution_id.to_string(),
            i64::from(step.step_index),
            step.action_type,
            execution_step_status_name(step.status),
            step.adapter_id,
            format_ts(step.started_at)?,
            step.finished_at.map(format_ts).transpose()?,
            step.result_code,
            step.redacted_message,
        ],
    )?;
    Ok(())
}

fn update_execution(connection: &Connection, execution: Execution) -> Result<(), StorageError> {
    let changed = connection.execute(
        "UPDATE executions SET status = ?1, started_at = ?2, finished_at = ?3, failure_code = ?4 WHERE id = ?5",
        params![
            execution_status_name(execution.status),
            format_ts(execution.started_at)?,
            execution.finished_at.map(format_ts).transpose()?,
            execution.failure_code,
            execution.id.to_string(),
        ],
    )?;
    if changed == 0 {
        return Err(StorageError::NotFound);
    }
    if !matches!(execution.status, ExecutionStatus::Running) {
        connection.execute(
            "DELETE FROM ritual_execution_locks WHERE execution_id = ?1",
            params![execution.id.to_string()],
        )?;
    }
    Ok(())
}

fn update_step(connection: &Connection, step: ExecutionStep) -> Result<(), StorageError> {
    let changed = connection.execute(
        "UPDATE execution_steps SET status = ?1, adapter_id = ?2, started_at = ?3, finished_at = ?4,
             result_code = ?5, redacted_message = ?6 WHERE id = ?7",
        params![
            execution_step_status_name(step.status),
            step.adapter_id,
            format_ts(step.started_at)?,
            step.finished_at.map(format_ts).transpose()?,
            step.result_code,
            step.redacted_message,
            step.id.to_string(),
        ],
    )?;
    if changed == 0 {
        return Err(StorageError::NotFound);
    }
    Ok(())
}

fn list_executions(connection: &Connection) -> Result<Vec<Execution>, StorageError> {
    let mut statement = connection.prepare(
        "SELECT id, ritual_id, ritual_version_id, status, trigger_kind, started_at, finished_at, failure_code, created_at
         FROM executions ORDER BY created_at DESC, id DESC",
    )?;
    let rows = statement.query_map([], execution_row)?;
    rows.map(|row| row.map_err(StorageError::from).and_then(execution_from_row))
        .collect()
}

fn get_execution(
    connection: &Connection,
    execution_id: Uuid,
) -> Result<(Execution, Vec<ExecutionStep>), StorageError> {
    let execution = connection
        .query_row(
            "SELECT id, ritual_id, ritual_version_id, status, trigger_kind, started_at, finished_at, failure_code, created_at
             FROM executions WHERE id = ?1",
            params![execution_id.to_string()],
            execution_row,
        )
        .optional()
        .map_err(StorageError::from)?
        .ok_or(StorageError::NotFound)
        .and_then(execution_from_row)?;
    let mut statement = connection.prepare(
        "SELECT id, execution_id, step_index, action_type, status, adapter_id, started_at, finished_at, result_code, redacted_message
         FROM execution_steps WHERE execution_id = ?1 ORDER BY step_index ASC",
    )?;
    let rows = statement.query_map(params![execution_id.to_string()], execution_step_row)?;
    let steps = rows
        .map(|row| {
            row.map_err(StorageError::from)
                .and_then(execution_step_from_row)
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok((execution, steps))
}

#[allow(clippy::type_complexity)]
fn execution_row(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<(
    String,
    String,
    String,
    String,
    String,
    String,
    Option<String>,
    Option<String>,
    String,
)> {
    Ok((
        row.get(0)?,
        row.get(1)?,
        row.get(2)?,
        row.get(3)?,
        row.get(4)?,
        row.get(5)?,
        row.get(6)?,
        row.get(7)?,
        row.get(8)?,
    ))
}

#[allow(clippy::type_complexity)]
fn execution_from_row(
    row: (
        String,
        String,
        String,
        String,
        String,
        String,
        Option<String>,
        Option<String>,
        String,
    ),
) -> Result<Execution, StorageError> {
    Ok(Execution {
        id: parse_uuid(&row.0)?,
        ritual_id: parse_uuid(&row.1)?,
        ritual_version_id: parse_uuid(&row.2)?,
        status: parse_execution_status(&row.3)?,
        trigger_kind: parse_trigger_kind(&row.4)?,
        started_at: parse_ts(&row.5)?,
        finished_at: row.6.map(|value| parse_ts(&value)).transpose()?,
        failure_code: row.7,
        created_at: parse_ts(&row.8)?,
    })
}

#[allow(clippy::type_complexity)]
fn execution_step_row(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<(
    String,
    String,
    i64,
    String,
    String,
    Option<String>,
    String,
    Option<String>,
    Option<String>,
    Option<String>,
)> {
    Ok((
        row.get(0)?,
        row.get(1)?,
        row.get(2)?,
        row.get(3)?,
        row.get(4)?,
        row.get(5)?,
        row.get(6)?,
        row.get(7)?,
        row.get(8)?,
        row.get(9)?,
    ))
}

#[allow(clippy::type_complexity)]
fn execution_step_from_row(
    row: (
        String,
        String,
        i64,
        String,
        String,
        Option<String>,
        String,
        Option<String>,
        Option<String>,
        Option<String>,
    ),
) -> Result<ExecutionStep, StorageError> {
    Ok(ExecutionStep {
        id: parse_uuid(&row.0)?,
        execution_id: parse_uuid(&row.1)?,
        step_index: u32::try_from(row.2)
            .map_err(|_| StorageError::InvalidData("step index".to_owned()))?,
        action_type: row.3,
        status: parse_execution_step_status(&row.4)?,
        adapter_id: row.5,
        started_at: parse_ts(&row.6)?,
        finished_at: row.7.map(|value| parse_ts(&value)).transpose()?,
        result_code: row.8,
        redacted_message: row.9,
    })
}

fn abort_running_executions(connection: &Connection) -> Result<(), StorageError> {
    let now = format_ts(OffsetDateTime::now_utc())?;
    connection.execute(
        "UPDATE executions SET status = 'aborted', finished_at = ?1, failure_code = 'daemon_restarted'
         WHERE status = 'running'",
        params![now],
    )?;
    connection.execute(
        "UPDATE execution_steps
         SET status = 'aborted', finished_at = ?1, result_code = 'daemon_restarted',
             redacted_message = 'execution aborted because daemon restarted'
         WHERE status IN ('running', 'pending')
           AND execution_id IN (
               SELECT id FROM executions
               WHERE status = 'aborted' AND failure_code = 'daemon_restarted'
           )",
        params![now],
    )?;
    connection.execute("DELETE FROM ritual_execution_locks", [])?;
    Ok(())
}

fn count_where(
    connection: &Connection,
    table: &str,
    predicate: &str,
) -> Result<usize, StorageError> {
    let sql = format!("SELECT COUNT(*) FROM {table} WHERE {predicate}");
    let count = connection.query_row(&sql, [], |row| row.get::<_, i64>(0))?;
    usize::try_from(count).map_err(|_| StorageError::InvalidData("negative count".to_owned()))
}

fn format_ts(value: OffsetDateTime) -> Result<String, StorageError> {
    Ok(value.format(&Rfc3339)?)
}

fn parse_ts(value: &str) -> Result<OffsetDateTime, StorageError> {
    Ok(OffsetDateTime::parse(value, &Rfc3339)?)
}

fn parse_uuid(value: &str) -> Result<Uuid, StorageError> {
    Uuid::parse_str(value).map_err(|_| StorageError::InvalidData("invalid UUID".to_owned()))
}

const fn resource_kind_name(value: ResourceKind) -> &'static str {
    match value {
        ResourceKind::File => "filesystem.file",
        ResourceKind::Directory => "filesystem.directory",
    }
}

fn parse_resource_kind(value: &str) -> Result<ResourceKind, StorageError> {
    match value {
        "filesystem.file" => Ok(ResourceKind::File),
        "filesystem.directory" => Ok(ResourceKind::Directory),
        _ => Err(StorageError::InvalidData("resource kind".to_owned())),
    }
}

const fn sensitivity_name(value: Sensitivity) -> &'static str {
    match value {
        Sensitivity::Normal => "normal",
        Sensitivity::Sensitive => "sensitive",
    }
}

fn parse_sensitivity(value: &str) -> Result<Sensitivity, StorageError> {
    match value {
        "normal" => Ok(Sensitivity::Normal),
        "sensitive" => Ok(Sensitivity::Sensitive),
        _ => Err(StorageError::InvalidData("sensitivity".to_owned())),
    }
}

const fn resource_scope_name(value: ResourceScope) -> &'static str {
    match value {
        ResourceScope::Exact => "exact",
    }
}
fn parse_resource_scope(value: &str) -> Result<ResourceScope, StorageError> {
    match value {
        "exact" => Ok(ResourceScope::Exact),
        _ => Err(StorageError::InvalidData("resource scope".to_owned())),
    }
}

const fn ritual_status_name(value: RitualStatus) -> &'static str {
    match value {
        RitualStatus::Draft => "draft",
        RitualStatus::Active => "active",
        RitualStatus::Paused => "paused",
        RitualStatus::Archived => "archived",
    }
}
fn parse_ritual_status(value: &str) -> Result<RitualStatus, StorageError> {
    match value {
        "draft" => Ok(RitualStatus::Draft),
        "active" => Ok(RitualStatus::Active),
        "paused" => Ok(RitualStatus::Paused),
        "archived" => Ok(RitualStatus::Archived),
        _ => Err(StorageError::InvalidData("ritual status".to_owned())),
    }
}

const fn trigger_kind_name(value: TriggerKind) -> &'static str {
    match value {
        TriggerKind::Manual => "manual",
    }
}
fn parse_trigger_kind(value: &str) -> Result<TriggerKind, StorageError> {
    match value {
        "manual" => Ok(TriggerKind::Manual),
        _ => Err(StorageError::InvalidData("trigger kind".to_owned())),
    }
}

const fn execution_status_name(value: ExecutionStatus) -> &'static str {
    match value {
        ExecutionStatus::Planned => "planned",
        ExecutionStatus::Running => "running",
        ExecutionStatus::Succeeded => "succeeded",
        ExecutionStatus::Failed => "failed",
        ExecutionStatus::Partial => "partial",
        ExecutionStatus::Aborted => "aborted",
    }
}
fn parse_execution_status(value: &str) -> Result<ExecutionStatus, StorageError> {
    match value {
        "planned" => Ok(ExecutionStatus::Planned),
        "running" => Ok(ExecutionStatus::Running),
        "succeeded" => Ok(ExecutionStatus::Succeeded),
        "failed" => Ok(ExecutionStatus::Failed),
        "partial" => Ok(ExecutionStatus::Partial),
        "aborted" => Ok(ExecutionStatus::Aborted),
        _ => Err(StorageError::InvalidData("execution status".to_owned())),
    }
}

const fn execution_step_status_name(value: ExecutionStepStatus) -> &'static str {
    match value {
        ExecutionStepStatus::Pending => "pending",
        ExecutionStepStatus::Running => "running",
        ExecutionStepStatus::Succeeded => "succeeded",
        ExecutionStepStatus::Skipped => "skipped",
        ExecutionStepStatus::Failed => "failed",
        ExecutionStepStatus::Aborted => "aborted",
    }
}
fn parse_execution_step_status(value: &str) -> Result<ExecutionStepStatus, StorageError> {
    match value {
        "pending" => Ok(ExecutionStepStatus::Pending),
        "running" => Ok(ExecutionStepStatus::Running),
        "succeeded" => Ok(ExecutionStepStatus::Succeeded),
        "skipped" => Ok(ExecutionStepStatus::Skipped),
        "failed" => Ok(ExecutionStepStatus::Failed),
        "aborted" => Ok(ExecutionStepStatus::Aborted),
        _ => Err(StorageError::InvalidData(
            "execution step status".to_owned(),
        )),
    }
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
    use fubun_domain::{
        ActionSpec, AdapterIdentity, ExecutionMode, FailureMode, RitualExecutionConfig,
        SyntheticEventData, EVENT_SPEC_VERSION, RITUAL_SCHEMA_VERSION,
    };
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

    #[tokio::test]
    async fn migrates_phase_one_database_to_phase_two() {
        let temp = TempDir::new().expect("tempdir");
        let path = temp.path().join("fubun/fubun.db");
        fs::create_dir_all(path.parent().expect("parent")).expect("parent");
        let connection = Connection::open(&path).expect("phase one database");
        connection
            .execute_batch(
                "CREATE TABLE schema_migrations(version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL);
                 INSERT INTO schema_migrations(version, applied_at) VALUES (1, '2026-08-03T00:00:00Z');
                 CREATE TABLE adapters(instance_id TEXT PRIMARY KEY, adapter_id TEXT NOT NULL, adapter_version TEXT NOT NULL, last_sequence_no INTEGER NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
                 CREATE TABLE settings(key TEXT PRIMARY KEY, value_json TEXT NOT NULL, updated_at TEXT NOT NULL);
                 CREATE TABLE events(id TEXT PRIMARY KEY, spec_version TEXT NOT NULL, event_type TEXT NOT NULL, source TEXT NOT NULL, occurred_at TEXT NOT NULL, received_at TEXT NOT NULL, actor TEXT NOT NULL, adapter_id TEXT NOT NULL, adapter_version TEXT NOT NULL, adapter_instance_id TEXT NOT NULL, sequence_no INTEGER NOT NULL, context_json TEXT, privacy TEXT NOT NULL, data_json TEXT NOT NULL, canonical_json TEXT NOT NULL, UNIQUE(adapter_instance_id, sequence_no));",
            )
            .expect("phase one schema");
        drop(connection);
        let storage = Storage::open(&path).expect("migrate");
        let version = storage
            .handle()
            .schema_version()
            .await
            .expect("schema version");
        assert_eq!(version, 2);
        drop(storage);
    }

    #[tokio::test]
    async fn restart_aborts_running_execution_steps_and_preserves_completed_steps() {
        let temp = TempDir::new().expect("tempdir");
        let path = temp.path().join("fubun/fubun.db");
        let storage = Storage::open(&path).expect("open storage");
        let handle = storage.handle();
        let now = OffsetDateTime::now_utc();
        let ritual_id = Uuid::new_v4();
        let version_id = Uuid::new_v4();
        let definition = fubun_domain::RitualDefinition {
            schema_version: RITUAL_SCHEMA_VERSION.to_owned(),
            name: "restart".to_owned(),
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
        let canonical = definition.canonical_json().expect("canonical");
        let hash = definition.content_hash().expect("hash");
        handle
            .create_ritual(
                Ritual {
                    id: ritual_id,
                    name: definition.name.clone(),
                    status: RitualStatus::Active,
                    current_version_id: version_id,
                    created_at: now,
                    updated_at: now,
                },
                RitualVersion {
                    id: version_id,
                    ritual_id,
                    version: 1,
                    schema_version: RITUAL_SCHEMA_VERSION.to_owned(),
                    canonical_json: canonical,
                    content_hash: hash,
                    created_at: now,
                },
                definition,
            )
            .await
            .expect("ritual");
        let execution_id = Uuid::new_v4();
        let steps = vec![
            ExecutionStep {
                id: Uuid::new_v4(),
                execution_id,
                step_index: 0,
                action_type: "desktop.notification.show.v1".to_owned(),
                status: ExecutionStepStatus::Pending,
                adapter_id: None,
                started_at: now,
                finished_at: None,
                result_code: None,
                redacted_message: None,
            },
            ExecutionStep {
                id: Uuid::new_v4(),
                execution_id,
                step_index: 1,
                action_type: "desktop.notification.show.v1".to_owned(),
                status: ExecutionStepStatus::Running,
                adapter_id: Some("dev.fubun.linux".to_owned()),
                started_at: now,
                finished_at: None,
                result_code: None,
                redacted_message: None,
            },
            ExecutionStep {
                id: Uuid::new_v4(),
                execution_id,
                step_index: 2,
                action_type: "desktop.notification.show.v1".to_owned(),
                status: ExecutionStepStatus::Succeeded,
                adapter_id: Some("dev.fubun.linux".to_owned()),
                started_at: now,
                finished_at: Some(now),
                result_code: Some("sent".to_owned()),
                redacted_message: Some("notification sent".to_owned()),
            },
        ];
        handle
            .start_execution(
                Execution {
                    id: execution_id,
                    ritual_id,
                    ritual_version_id: version_id,
                    status: ExecutionStatus::Running,
                    trigger_kind: TriggerKind::Manual,
                    started_at: now,
                    finished_at: None,
                    failure_code: None,
                    created_at: now,
                },
                steps,
            )
            .await
            .expect("execution");
        handle
            .abort_running_executions()
            .await
            .expect("abort on restart");
        let (execution, steps) = handle.get_execution(execution_id).await.expect("history");
        assert_eq!(execution.status, ExecutionStatus::Aborted);
        assert_eq!(execution.failure_code.as_deref(), Some("daemon_restarted"));
        assert_eq!(steps[0].status, ExecutionStepStatus::Aborted);
        assert_eq!(steps[0].result_code.as_deref(), Some("daemon_restarted"));
        assert_eq!(
            steps[0].redacted_message.as_deref(),
            Some("execution aborted because daemon restarted")
        );
        assert_eq!(steps[1].status, ExecutionStepStatus::Aborted);
        assert_eq!(steps[1].result_code.as_deref(), Some("daemon_restarted"));
        assert_eq!(steps[2].status, ExecutionStepStatus::Succeeded);
        storage.shutdown().await.expect("shutdown");
    }
}
