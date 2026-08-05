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
    ExecutionStepStatus, ObservationScope, ObservationSource, ObservationStatus, PrivacyClass,
    Resource, ResourceKind, ResourceScope, Ritual, RitualDefinition, RitualStatus, RitualVersion,
    Sensitivity, TriggerKind, ValidationError,
};
use fubun_mining::{
    DiscoveredSession, DiscoveredSuggestion, DiscoveryRun, DiscoveryRunStatus, SessionEvent,
    SuggestionStatus,
};
use rusqlite::{params, Connection, ErrorCode, OptionalExtension};
use serde_json::Value;
use thiserror::Error;
use time::{format_description::well_known::Rfc3339, OffsetDateTime};
use tokio::sync::oneshot;
use uuid::Uuid;

pub const SCHEMA_VERSION: u32 = 5;
// Keep the public response aligned with the protocol's default and well
// below the 256 KiB IPC frame ceiling. Discovery uses its private scan API.
pub const PUBLIC_EVENT_LIST_LIMIT: u32 = 100;
pub const DISCOVERY_EVENT_SCAN_LIMIT: u32 = 100_001;

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
    #[error("discovery run is already running")]
    DiscoveryAlreadyRunning,
    #[error("discovery input is too large")]
    DiscoveryInputTooLarge,
    #[error("suggestion is in an invalid state")]
    SuggestionInvalidState,
    #[error("suggestion evidence is stale")]
    SuggestionStale,
    #[error("invalid snooze duration")]
    InvalidSnoozeDuration,
    #[error("discovery run was not found")]
    DiscoveryNotFound,
    #[error("session was not found")]
    SessionNotFound,
    #[error("suggestion was not found")]
    SuggestionNotFound,
    #[error("stored data is invalid: {0}")]
    InvalidData(String),
}

enum Operation {
    InsertEvent(Event),
    ListEvents {
        since: Option<OffsetDateTime>,
        limit: u32,
    },
    ListEventsForDiscovery {
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
    EnsureObservationScope(ObservationScope),
    ListObservationScopes(Option<ObservationSource>),
    PauseObservationScope(Uuid),
    StartDiscoveryRun(DiscoveryRun),
    FinishDiscoveryRunFailed {
        run_id: Uuid,
        failure_code: String,
        finished_at: OffsetDateTime,
    },
    AbortRunningDiscoveryRuns,
    PersistDiscovery {
        run: DiscoveryRun,
        sessions: Vec<DiscoveredSession>,
        suggestions: Vec<DiscoveredSuggestion>,
    },
    ListDiscoveryRuns,
    ListSessions(Option<Uuid>),
    GetSession(Uuid),
    ListSuggestions {
        status: Option<SuggestionStatus>,
        workspace_resource_id: Option<Uuid>,
    },
    GetSuggestion(Uuid),
    SetSuggestionStatus {
        suggestion_id: Uuid,
        status: SuggestionStatus,
        until: Option<OffsetDateTime>,
    },
    AcceptSuggestion {
        suggestion_id: Uuid,
        ritual: Ritual,
        version: RitualVersion,
        definition: RitualDefinition,
    },
}

#[allow(clippy::large_enum_variant)]
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
    ObservationScope(ObservationScope),
    ObservationScopes(Vec<ObservationScope>),
    DiscoveryRun(DiscoveryRun),
    DiscoveryRuns(Vec<DiscoveryRun>),
    Sessions(Vec<DiscoveredSession>),
    Session(DiscoveredSession),
    Suggestions(Vec<DiscoveredSuggestion>),
    Suggestion {
        suggestion: DiscoveredSuggestion,
        status: SuggestionStatus,
        snoozed_until: Option<OffsetDateTime>,
        accepted_ritual_id: Option<Uuid>,
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
        match self
            .request(Operation::ListEvents {
                since,
                limit: limit.min(PUBLIC_EVENT_LIST_LIMIT),
            })
            .await?
        {
            StorageResponse::Events(events) => Ok(events),
            _ => Err(StorageError::InvalidData(
                "unexpected event list response".to_owned(),
            )),
        }
    }

    /// Returns the bounded event stream reserved for the discovery pipeline.
    /// This operation is intentionally not exposed through the IPC request enum.
    pub async fn list_events_for_discovery(
        &self,
        since: Option<OffsetDateTime>,
        limit: u32,
    ) -> Result<Vec<Event>, StorageError> {
        match self
            .request(Operation::ListEventsForDiscovery {
                since,
                limit: limit.min(DISCOVERY_EVENT_SCAN_LIMIT),
            })
            .await?
        {
            StorageResponse::Events(events) => Ok(events),
            _ => Err(StorageError::InvalidData(
                "unexpected discovery event list response".to_owned(),
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

    pub async fn ensure_observation_scope(
        &self,
        scope: ObservationScope,
    ) -> Result<ObservationScope, StorageError> {
        match self
            .request(Operation::EnsureObservationScope(scope))
            .await?
        {
            StorageResponse::ObservationScope(scope) => Ok(scope),
            _ => Err(StorageError::InvalidData(
                "unexpected observation scope response".to_owned(),
            )),
        }
    }

    pub async fn list_observation_scopes(
        &self,
        source: Option<ObservationSource>,
    ) -> Result<Vec<ObservationScope>, StorageError> {
        match self
            .request(Operation::ListObservationScopes(source))
            .await?
        {
            StorageResponse::ObservationScopes(scopes) => Ok(scopes),
            _ => Err(StorageError::InvalidData(
                "unexpected observation scope list response".to_owned(),
            )),
        }
    }

    pub async fn pause_observation_scope(
        &self,
        scope_id: Uuid,
    ) -> Result<ObservationScope, StorageError> {
        match self
            .request(Operation::PauseObservationScope(scope_id))
            .await?
        {
            StorageResponse::ObservationScope(scope) => Ok(scope),
            _ => Err(StorageError::InvalidData(
                "unexpected observation pause response".to_owned(),
            )),
        }
    }

    pub async fn start_discovery_run(&self, run: DiscoveryRun) -> Result<(), StorageError> {
        match self.request(Operation::StartDiscoveryRun(run)).await? {
            StorageResponse::Unit => Ok(()),
            _ => Err(StorageError::InvalidData(
                "unexpected discovery start response".to_owned(),
            )),
        }
    }

    pub async fn finish_discovery_run_failed(
        &self,
        run_id: Uuid,
        failure_code: &str,
        finished_at: OffsetDateTime,
    ) -> Result<(), StorageError> {
        match self
            .request(Operation::FinishDiscoveryRunFailed {
                run_id,
                failure_code: failure_code.to_owned(),
                finished_at,
            })
            .await?
        {
            StorageResponse::Unit => Ok(()),
            _ => Err(StorageError::InvalidData(
                "unexpected discovery failure response".to_owned(),
            )),
        }
    }

    pub async fn persist_discovery(
        &self,
        run: DiscoveryRun,
        sessions: Vec<DiscoveredSession>,
        suggestions: Vec<DiscoveredSuggestion>,
    ) -> Result<DiscoveryRun, StorageError> {
        match self
            .request(Operation::PersistDiscovery {
                run,
                sessions,
                suggestions,
            })
            .await?
        {
            StorageResponse::DiscoveryRun(run) => Ok(run),
            _ => Err(StorageError::InvalidData(
                "unexpected discovery response".to_owned(),
            )),
        }
    }

    pub async fn abort_running_discovery_runs(&self) -> Result<(), StorageError> {
        match self.request(Operation::AbortRunningDiscoveryRuns).await? {
            StorageResponse::Unit => Ok(()),
            _ => Err(StorageError::InvalidData(
                "unexpected discovery abort response".to_owned(),
            )),
        }
    }

    pub async fn list_discovery_runs(&self) -> Result<Vec<DiscoveryRun>, StorageError> {
        match self.request(Operation::ListDiscoveryRuns).await? {
            StorageResponse::DiscoveryRuns(runs) => Ok(runs),
            _ => Err(StorageError::InvalidData(
                "unexpected discovery list response".to_owned(),
            )),
        }
    }

    pub async fn list_sessions(
        &self,
        workspace: Option<Uuid>,
    ) -> Result<Vec<DiscoveredSession>, StorageError> {
        match self.request(Operation::ListSessions(workspace)).await? {
            StorageResponse::Sessions(sessions) => Ok(sessions),
            _ => Err(StorageError::InvalidData(
                "unexpected session list response".to_owned(),
            )),
        }
    }

    pub async fn get_session(&self, id: Uuid) -> Result<DiscoveredSession, StorageError> {
        match self.request(Operation::GetSession(id)).await? {
            StorageResponse::Session(session) => Ok(session),
            _ => Err(StorageError::InvalidData(
                "unexpected session response".to_owned(),
            )),
        }
    }

    pub async fn list_suggestions(
        &self,
        status: Option<SuggestionStatus>,
        workspace: Option<Uuid>,
    ) -> Result<Vec<DiscoveredSuggestion>, StorageError> {
        match self
            .request(Operation::ListSuggestions {
                status,
                workspace_resource_id: workspace,
            })
            .await?
        {
            StorageResponse::Suggestions(suggestions) => Ok(suggestions),
            _ => Err(StorageError::InvalidData(
                "unexpected suggestion list response".to_owned(),
            )),
        }
    }

    pub async fn get_suggestion(
        &self,
        id: Uuid,
    ) -> Result<
        (
            DiscoveredSuggestion,
            SuggestionStatus,
            Option<OffsetDateTime>,
            Option<Uuid>,
        ),
        StorageError,
    > {
        match self.request(Operation::GetSuggestion(id)).await? {
            StorageResponse::Suggestion {
                suggestion,
                status,
                snoozed_until,
                accepted_ritual_id,
            } => Ok((suggestion, status, snoozed_until, accepted_ritual_id)),
            _ => Err(StorageError::InvalidData(
                "unexpected suggestion response".to_owned(),
            )),
        }
    }

    pub async fn set_suggestion_status(
        &self,
        id: Uuid,
        status: SuggestionStatus,
        until: Option<OffsetDateTime>,
    ) -> Result<
        (
            DiscoveredSuggestion,
            SuggestionStatus,
            Option<OffsetDateTime>,
            Option<Uuid>,
        ),
        StorageError,
    > {
        match self
            .request(Operation::SetSuggestionStatus {
                suggestion_id: id,
                status,
                until,
            })
            .await?
        {
            StorageResponse::Suggestion {
                suggestion,
                status,
                snoozed_until,
                accepted_ritual_id,
            } => Ok((suggestion, status, snoozed_until, accepted_ritual_id)),
            _ => Err(StorageError::InvalidData(
                "unexpected suggestion status response".to_owned(),
            )),
        }
    }

    pub async fn accept_suggestion(
        &self,
        suggestion_id: Uuid,
        ritual: Ritual,
        version: RitualVersion,
        definition: RitualDefinition,
    ) -> Result<(Ritual, RitualVersion, RitualDefinition), StorageError> {
        match self
            .request(Operation::AcceptSuggestion {
                suggestion_id,
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
                "unexpected suggestion accept response".to_owned(),
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
            params![2_u32, now],
        )?;
        transaction.commit()?;
    }
    let current = current_schema_version(connection)?;
    if current == 2 {
        let transaction = connection.transaction()?;
        transaction
            .execute_batch("ALTER TABLE execution_steps ADD COLUMN adapter_instance_id TEXT;")?;
        let now = OffsetDateTime::now_utc().format(&Rfc3339)?;
        transaction.execute(
            "INSERT INTO schema_migrations(version, applied_at) VALUES (?1, ?2)",
            params![3_u32, now],
        )?;
        transaction.commit()?;
    }
    let current = current_schema_version(connection)?;
    if current == 3 {
        let transaction = connection.transaction()?;
        transaction.execute_batch(
            "CREATE TABLE observation_scopes (
                id TEXT PRIMARY KEY,
                source TEXT NOT NULL CHECK(source IN ('browser.chromium', 'vscode.workspace')),
                resource_id TEXT NOT NULL,
                status TEXT NOT NULL CHECK(status IN ('active', 'paused')),
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY(resource_id) REFERENCES resources(id),
                UNIQUE(source, resource_id)
            );
            CREATE INDEX observation_scopes_source_idx ON observation_scopes(source);",
        )?;
        migrate_legacy_event_payloads(&transaction)?;
        let now = OffsetDateTime::now_utc().format(&Rfc3339)?;
        transaction.execute(
            "INSERT INTO schema_migrations(version, applied_at) VALUES (?1, ?2)",
            params![4_u32, now],
        )?;
        transaction.commit()?;
    }
    let current = current_schema_version(connection)?;
    if current == 4 {
        let transaction = connection.transaction()?;
        transaction.execute_batch(
            "CREATE TABLE discovery_runs (
                id TEXT PRIMARY KEY,
                algorithm_version TEXT NOT NULL,
                status TEXT NOT NULL CHECK(status IN ('running','succeeded','failed','aborted')),
                started_at TEXT NOT NULL,
                finished_at TEXT,
                input_event_count INTEGER NOT NULL,
                sessions_upserted INTEGER NOT NULL,
                candidates_evaluated INTEGER NOT NULL,
                suggestions_created INTEGER NOT NULL,
                failure_code TEXT
            );
            CREATE TABLE sessions (
                id TEXT PRIMARY KEY,
                algorithm_version TEXT NOT NULL,
                kind TEXT NOT NULL CHECK(kind = 'workspace_start'),
                workspace_resource_id TEXT NOT NULL,
                anchor_event_id TEXT NOT NULL UNIQUE,
                started_at TEXT NOT NULL,
                finished_at TEXT,
                event_count INTEGER NOT NULL,
                eligible INTEGER NOT NULL,
                FOREIGN KEY(workspace_resource_id) REFERENCES resources(id)
            );
            CREATE TABLE session_events (
                session_id TEXT NOT NULL,
                event_id TEXT NOT NULL,
                ordinal INTEGER NOT NULL,
                received_at TEXT NOT NULL,
                resource_id TEXT NOT NULL,
                is_anchor INTEGER NOT NULL,
                FOREIGN KEY(session_id) REFERENCES sessions(id) ON DELETE CASCADE,
                FOREIGN KEY(event_id) REFERENCES events(id) ON DELETE CASCADE,
                FOREIGN KEY(resource_id) REFERENCES resources(id),
                UNIQUE(session_id, ordinal),
                UNIQUE(session_id, event_id)
            );
            CREATE TABLE suggestions (
                id TEXT PRIMARY KEY,
                algorithm_version TEXT NOT NULL,
                workspace_resource_id TEXT NOT NULL,
                pattern_fingerprint TEXT NOT NULL UNIQUE,
                support_sessions INTEGER NOT NULL,
                eligible_sessions INTEGER NOT NULL,
                confidence_basis_points INTEGER NOT NULL,
                first_seen_at TEXT NOT NULL,
                last_seen_at TEXT NOT NULL,
                observation_span_seconds INTEGER NOT NULL,
                median_completion_ms INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                status TEXT NOT NULL CHECK(status IN ('pending','snoozed','dismissed','accepted','blocked')),
                snoozed_until TEXT,
                accepted_ritual_id TEXT,
                FOREIGN KEY(workspace_resource_id) REFERENCES resources(id),
                FOREIGN KEY(accepted_ritual_id) REFERENCES rituals(id)
            );
            CREATE TABLE suggestion_actions (
                suggestion_id TEXT NOT NULL,
                ordinal INTEGER NOT NULL,
                resource_id TEXT NOT NULL,
                FOREIGN KEY(suggestion_id) REFERENCES suggestions(id) ON DELETE CASCADE,
                FOREIGN KEY(resource_id) REFERENCES resources(id),
                UNIQUE(suggestion_id, ordinal)
            );
            CREATE TABLE suggestion_support_sessions (
                suggestion_id TEXT NOT NULL,
                session_id TEXT NOT NULL,
                FOREIGN KEY(suggestion_id) REFERENCES suggestions(id) ON DELETE CASCADE,
                FOREIGN KEY(session_id) REFERENCES sessions(id),
                UNIQUE(suggestion_id, session_id)
            );
            CREATE INDEX sessions_workspace_idx ON sessions(workspace_resource_id);
            CREATE INDEX suggestions_status_idx ON suggestions(status, updated_at);",
        )?;
        let now = OffsetDateTime::now_utc().format(&Rfc3339)?;
        transaction.execute(
            "INSERT INTO schema_migrations(version, applied_at) VALUES (?1, ?2)",
            params![5_u32, now],
        )?;
        transaction.commit()?;
    }
    Ok(())
}

/// Phase 1 stored synthetic data as `{label, counter}`. Phase 2 introduced a
/// strict tagged `EventData`; normalize legacy rows during the v4 transaction
/// so existing event history remains readable without weakening the current
/// canonical schema.
fn migrate_legacy_event_payloads(
    transaction: &rusqlite::Transaction<'_>,
) -> Result<(), StorageError> {
    let events_table_exists = transaction.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'events')",
        [],
        |row| row.get::<_, i64>(0),
    )?;
    if events_table_exists == 0 {
        return Ok(());
    }
    let mut statement = transaction.prepare(
        "SELECT id, data_json, canonical_json FROM events
         WHERE event_type = 'dev.fubun.dev.synthetic.v1'",
    )?;
    let rows = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    drop(statement);

    for (id, data_json, canonical_json) in rows {
        let mut data: Value = serde_json::from_str(&data_json)?;
        let mut canonical: Value = serde_json::from_str(&canonical_json)?;
        let is_legacy = data
            .as_object()
            .is_some_and(|object| !object.contains_key("kind"));
        if !is_legacy {
            continue;
        }
        let Some(data_object) = data.as_object_mut() else {
            continue;
        };
        if !data_object.contains_key("label") || !data_object.contains_key("counter") {
            continue;
        }
        let Some(canonical_data) = canonical.get_mut("data").and_then(Value::as_object_mut) else {
            continue;
        };
        if canonical_data.contains_key("kind")
            || !canonical_data.contains_key("label")
            || !canonical_data.contains_key("counter")
        {
            continue;
        }
        data_object.insert("kind".to_owned(), Value::String("synthetic".to_owned()));
        canonical_data.insert("kind".to_owned(), Value::String("synthetic".to_owned()));
        transaction.execute(
            "UPDATE events SET data_json = ?1, canonical_json = ?2 WHERE id = ?3",
            params![
                serde_json::to_string(&data)?,
                serde_json::to_string(&canonical)?,
                id
            ],
        )?;
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
        Operation::ListEventsForDiscovery { since, limit } => Ok(StorageResponse::Events(
            list_events(connection, since, limit.min(DISCOVERY_EVENT_SCAN_LIMIT))?,
        )),
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
        Operation::EnsureObservationScope(scope) => {
            let existing = connection
                .query_row(
                    "SELECT id, source, resource_id, status, created_at, updated_at
                     FROM observation_scopes WHERE source = ?1 AND resource_id = ?2",
                    params![
                        observation_source_name(scope.source),
                        scope.resource_id.to_string()
                    ],
                    observation_scope_from_sql_row,
                )
                .optional()?;
            if let Some(mut existing) = existing {
                existing.status = ObservationStatus::Active;
                existing.updated_at = scope.updated_at;
                connection.execute(
                    "UPDATE observation_scopes SET status = 'active', updated_at = ?1 WHERE id = ?2",
                    params![format_ts(existing.updated_at)?, existing.id.to_string()],
                )?;
                Ok(StorageResponse::ObservationScope(existing))
            } else {
                connection.execute(
                    "INSERT INTO observation_scopes(id, source, resource_id, status, created_at, updated_at)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                    params![
                        scope.id.to_string(),
                        observation_source_name(scope.source),
                        scope.resource_id.to_string(),
                        observation_status_name(scope.status),
                        format_ts(scope.created_at)?,
                        format_ts(scope.updated_at)?,
                    ],
                )?;
                Ok(StorageResponse::ObservationScope(scope))
            }
        }
        Operation::ListObservationScopes(source) => {
            let mut statement = connection.prepare(
                "SELECT id, source, resource_id, status, created_at, updated_at
                 FROM observation_scopes
                 WHERE (?1 IS NULL OR source = ?1)
                 ORDER BY created_at ASC, id ASC",
            )?;
            let rows = statement.query_map(
                params![source.map(observation_source_name)],
                observation_scope_from_sql_row,
            )?;
            let scopes = rows.collect::<Result<Vec<_>, _>>()?;
            Ok(StorageResponse::ObservationScopes(scopes))
        }
        Operation::PauseObservationScope(scope_id) => {
            let mut scope = connection
                .query_row(
                    "SELECT id, source, resource_id, status, created_at, updated_at
                     FROM observation_scopes WHERE id = ?1",
                    params![scope_id.to_string()],
                    observation_scope_from_sql_row,
                )
                .optional()?
                .ok_or(StorageError::NotFound)?;
            if scope.status == ObservationStatus::Active {
                let now = OffsetDateTime::now_utc();
                connection.execute(
                    "UPDATE observation_scopes SET status = 'paused', updated_at = ?1
                     WHERE id = ?2 AND status = 'active'",
                    params![format_ts(now)?, scope_id.to_string()],
                )?;
                scope.status = ObservationStatus::Paused;
                scope.updated_at = now;
            }
            Ok(StorageResponse::ObservationScope(scope))
        }
        Operation::StartDiscoveryRun(run) => {
            let running: i64 = connection.query_row(
                "SELECT COUNT(*) FROM discovery_runs WHERE status = 'running'",
                [],
                |row| row.get(0),
            )?;
            if running > 0 {
                return Err(StorageError::DiscoveryAlreadyRunning);
            }
            connection.execute(
                "INSERT INTO discovery_runs(id, algorithm_version, status, started_at, finished_at,
                    input_event_count, sessions_upserted, candidates_evaluated, suggestions_created, failure_code)
                 VALUES (?1, ?2, ?3, ?4, NULL, ?5, 0, 0, 0, NULL)",
                params![run.id.to_string(), run.algorithm_version, discovery_status_name(run.status), format_ts(run.started_at)?, run.input_event_count],
            )?;
            Ok(StorageResponse::Unit)
        }
        Operation::FinishDiscoveryRunFailed {
            run_id,
            failure_code,
            finished_at,
        } => {
            connection.execute(
                "UPDATE discovery_runs
                 SET status='failed', finished_at=?1, failure_code=?2
                 WHERE id=?3 AND status='running'",
                params![format_ts(finished_at)?, failure_code, run_id.to_string()],
            )?;
            Ok(StorageResponse::Unit)
        }
        Operation::AbortRunningDiscoveryRuns => {
            let now = format_ts(OffsetDateTime::now_utc())?;
            connection.execute(
                "UPDATE discovery_runs SET status = 'aborted', finished_at = ?1, failure_code = 'daemon_restarted' WHERE status = 'running'",
                params![now],
            )?;
            Ok(StorageResponse::Unit)
        }
        Operation::PersistDiscovery {
            run,
            sessions,
            suggestions,
        } => {
            let transaction = connection.transaction()?;
            for session in &sessions {
                transaction.execute(
                    "INSERT INTO sessions(id, algorithm_version, kind, workspace_resource_id, anchor_event_id,
                        started_at, finished_at, event_count, eligible)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
                     ON CONFLICT(id) DO UPDATE SET finished_at=excluded.finished_at,
                        event_count=excluded.event_count, eligible=excluded.eligible",
                    params![session.id.to_string(), session.algorithm_version, session.kind, session.workspace_resource_id.to_string(), session.anchor_event_id.to_string(), format_ts(session.started_at)?, session.finished_at.map(format_ts).transpose()?, session.event_count, i64::from(session.eligible)],
                )?;
                transaction.execute(
                    "DELETE FROM session_events WHERE session_id = ?1",
                    params![session.id.to_string()],
                )?;
                for event in &session.events {
                    transaction.execute(
                        "INSERT INTO session_events(session_id,event_id,ordinal,received_at,resource_id,is_anchor)
                         VALUES (?1,?2,?3,?4,?5,?6)",
                        params![session.id.to_string(), event.event_id.to_string(), event.ordinal, format_ts(event.received_at)?, event.resource_id.to_string(), i64::from(event.is_anchor)],
                    )?;
                }
            }
            let now = OffsetDateTime::now_utc();
            let now_timestamp = format_ts(now)?;
            transaction.execute(
                "UPDATE suggestions SET status='pending', snoozed_until=NULL
                 WHERE status IN ('snoozed','dismissed') AND snoozed_until IS NOT NULL AND snoozed_until <= ?1",
                params![now_timestamp],
            )?;
            let recent_cutoff = format_ts(now - time::Duration::hours(24))?;
            let mut pending_like_count: i64 = transaction.query_row(
                "SELECT COUNT(*) FROM suggestions WHERE status = 'pending'",
                [],
                |row| row.get(0),
            )?;
            let mut has_recent_new_suggestion: bool = transaction.query_row(
                "SELECT EXISTS(SELECT 1 FROM suggestions WHERE created_at >= ?1)",
                params![recent_cutoff],
                |row| row.get(0),
            )?;
            let mut suggestions_created = 0usize;
            for suggestion in &suggestions {
                let existing: Option<(String, String, Option<String>, Option<String>)> = transaction.query_row(
                    "SELECT id, status, snoozed_until, accepted_ritual_id FROM suggestions WHERE pattern_fingerprint = ?1",
                    params![suggestion.pattern_fingerprint],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                ).optional()?;
                if existing.is_none() && (has_recent_new_suggestion || pending_like_count >= 5) {
                    continue;
                }
                let status = existing
                    .as_ref()
                    .map(|(_, status, _, _)| status.as_str())
                    .unwrap_or("pending");
                transaction.execute(
                    "INSERT INTO suggestions(id, algorithm_version, workspace_resource_id, pattern_fingerprint,
                        support_sessions, eligible_sessions, confidence_basis_points, first_seen_at, last_seen_at,
                        observation_span_seconds, median_completion_ms, created_at, updated_at, status, snoozed_until, accepted_ritual_id)
                     VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?12,?13,NULL,NULL)
                     ON CONFLICT(pattern_fingerprint) DO UPDATE SET support_sessions=excluded.support_sessions,
                        eligible_sessions=excluded.eligible_sessions, confidence_basis_points=excluded.confidence_basis_points,
                        last_seen_at=excluded.last_seen_at, observation_span_seconds=excluded.observation_span_seconds,
                        median_completion_ms=excluded.median_completion_ms, updated_at=excluded.updated_at",
                    params![suggestion.id.to_string(), suggestion.algorithm_version, suggestion.workspace_resource_id.to_string(), suggestion.pattern_fingerprint, suggestion.support_sessions, suggestion.eligible_sessions, suggestion.confidence_basis_points, format_ts(suggestion.first_seen_at)?, format_ts(suggestion.last_seen_at)?, suggestion.observation_span_seconds, suggestion.median_completion_ms, format_ts(now)?, status],
                )?;
                if existing.is_none() {
                    suggestions_created += 1;
                    pending_like_count += 1;
                    has_recent_new_suggestion = true;
                }
                transaction.execute(
                    "DELETE FROM suggestion_actions WHERE suggestion_id = ?1",
                    params![suggestion.id.to_string()],
                )?;
                for (ordinal, resource_id) in suggestion.action_resource_ids.iter().enumerate() {
                    transaction.execute("INSERT INTO suggestion_actions(suggestion_id,ordinal,resource_id) VALUES (?1,?2,?3)", params![suggestion.id.to_string(), ordinal as u32, resource_id.to_string()])?;
                }
                transaction.execute(
                    "DELETE FROM suggestion_support_sessions WHERE suggestion_id = ?1",
                    params![suggestion.id.to_string()],
                )?;
                for session_id in &suggestion.supporting_session_ids {
                    transaction.execute("INSERT INTO suggestion_support_sessions(suggestion_id,session_id) VALUES (?1,?2)", params![suggestion.id.to_string(), session_id.to_string()])?;
                }
            }
            let finished = OffsetDateTime::now_utc();
            transaction.execute(
                "UPDATE discovery_runs SET status='succeeded', finished_at=?1, input_event_count=?2,
                    sessions_upserted=?3, candidates_evaluated=?4, suggestions_created=?5
                 WHERE id=?6",
                params![format_ts(finished)?, run.input_event_count, sessions.len(), run.candidates_evaluated, suggestions_created, run.id.to_string()],
            )?;
            transaction.commit()?;
            let mut completed = run;
            completed.status = DiscoveryRunStatus::Succeeded;
            completed.finished_at = Some(finished);
            completed.sessions_upserted = sessions.len() as u32;
            completed.suggestions_created = suggestions_created as u32;
            Ok(StorageResponse::DiscoveryRun(completed))
        }
        Operation::ListDiscoveryRuns => Ok(StorageResponse::DiscoveryRuns(list_discovery_runs(
            connection,
        )?)),
        Operation::ListSessions(workspace) => Ok(StorageResponse::Sessions(list_sessions(
            connection, workspace,
        )?)),
        Operation::GetSession(id) => Ok(StorageResponse::Session(get_session(connection, id)?)),
        Operation::ListSuggestions {
            status,
            workspace_resource_id,
        } => Ok(StorageResponse::Suggestions(list_suggestions(
            connection,
            status,
            workspace_resource_id,
        )?)),
        Operation::GetSuggestion(id) => {
            let (suggestion, status, snoozed_until, accepted_ritual_id) =
                get_suggestion(connection, id)?;
            Ok(StorageResponse::Suggestion {
                suggestion,
                status,
                snoozed_until,
                accepted_ritual_id,
            })
        }
        Operation::SetSuggestionStatus {
            suggestion_id,
            status,
            until,
        } => {
            let existing = get_suggestion(connection, suggestion_id)?;
            let current = existing.1;
            if current == SuggestionStatus::Accepted || current == SuggestionStatus::Blocked {
                return Err(StorageError::SuggestionInvalidState);
            }
            if current == SuggestionStatus::Snoozed && status == SuggestionStatus::Pending {
                // expiry reconciliation is intentionally allowed.
            } else if !valid_suggestion_transition(current, status) {
                return Err(StorageError::SuggestionInvalidState);
            }
            connection.execute(
                "UPDATE suggestions SET status=?1, snoozed_until=?2, updated_at=?3 WHERE id=?4",
                params![
                    suggestion_status_name(status),
                    until.map(format_ts).transpose()?,
                    format_ts(OffsetDateTime::now_utc())?,
                    suggestion_id.to_string()
                ],
            )?;
            let (suggestion, status, snoozed_until, accepted_ritual_id) =
                get_suggestion(connection, suggestion_id)?;
            Ok(StorageResponse::Suggestion {
                suggestion,
                status,
                snoozed_until,
                accepted_ritual_id,
            })
        }
        Operation::AcceptSuggestion {
            suggestion_id,
            ritual,
            version,
            definition,
        } => {
            let transaction = connection.transaction()?;
            let (suggestion, status, _, accepted) = get_suggestion_tx(&transaction, suggestion_id)?;
            if status == SuggestionStatus::Accepted {
                let ritual_id = accepted.ok_or_else(|| {
                    StorageError::InvalidData("accepted suggestion lacks ritual".to_owned())
                })?;
                let (ritual, version, definition) = get_ritual_tx(&transaction, ritual_id)?;
                transaction.commit()?;
                return Ok(StorageResponse::RitualRecord {
                    ritual,
                    version,
                    definition,
                });
            }
            if status == SuggestionStatus::Blocked || status == SuggestionStatus::Dismissed {
                return Err(StorageError::SuggestionInvalidState);
            }
            let workspace = get_resource_tx(&transaction, suggestion.workspace_resource_id)?;
            if workspace.kind != ResourceKind::Directory
                || !scope_active_tx(
                    &transaction,
                    workspace.id,
                    ObservationSource::VscodeWorkspace,
                )?
            {
                return Err(StorageError::SuggestionStale);
            }
            for resource_id in &suggestion.action_resource_ids {
                let resource = get_resource_tx(&transaction, *resource_id)?;
                if resource.kind != ResourceKind::WebPage
                    || !scope_active_tx(
                        &transaction,
                        *resource_id,
                        ObservationSource::BrowserChromium,
                    )?
                {
                    return Err(StorageError::SuggestionStale);
                }
            }
            insert_ritual_tx(&transaction, &ritual, &version)?;
            transaction.execute("UPDATE suggestions SET status='accepted', accepted_ritual_id=?1, updated_at=?2 WHERE id=?3", params![ritual.id.to_string(), format_ts(OffsetDateTime::now_utc())?, suggestion_id.to_string()])?;
            transaction.commit()?;
            Ok(StorageResponse::RitualRecord {
                ritual,
                version,
                definition,
            })
        }
    }
}

fn discovery_status_name(status: DiscoveryRunStatus) -> &'static str {
    match status {
        DiscoveryRunStatus::Running => "running",
        DiscoveryRunStatus::Succeeded => "succeeded",
        DiscoveryRunStatus::Failed => "failed",
        DiscoveryRunStatus::Aborted => "aborted",
    }
}

fn parse_discovery_status(value: &str) -> Result<DiscoveryRunStatus, StorageError> {
    match value {
        "running" => Ok(DiscoveryRunStatus::Running),
        "succeeded" => Ok(DiscoveryRunStatus::Succeeded),
        "failed" => Ok(DiscoveryRunStatus::Failed),
        "aborted" => Ok(DiscoveryRunStatus::Aborted),
        _ => Err(StorageError::InvalidData(
            "invalid discovery status".to_owned(),
        )),
    }
}

fn suggestion_status_name(status: SuggestionStatus) -> &'static str {
    match status {
        SuggestionStatus::Pending => "pending",
        SuggestionStatus::Snoozed => "snoozed",
        SuggestionStatus::Dismissed => "dismissed",
        SuggestionStatus::Accepted => "accepted",
        SuggestionStatus::Blocked => "blocked",
    }
}

fn parse_suggestion_status(value: &str) -> Result<SuggestionStatus, StorageError> {
    match value {
        "pending" => Ok(SuggestionStatus::Pending),
        "snoozed" => Ok(SuggestionStatus::Snoozed),
        "dismissed" => Ok(SuggestionStatus::Dismissed),
        "accepted" => Ok(SuggestionStatus::Accepted),
        "blocked" => Ok(SuggestionStatus::Blocked),
        _ => Err(StorageError::InvalidData(
            "invalid suggestion status".to_owned(),
        )),
    }
}

fn valid_suggestion_transition(from: SuggestionStatus, to: SuggestionStatus) -> bool {
    matches!(
        (from, to),
        (SuggestionStatus::Pending, SuggestionStatus::Snoozed)
            | (SuggestionStatus::Pending, SuggestionStatus::Dismissed)
            | (SuggestionStatus::Pending, SuggestionStatus::Accepted)
            | (SuggestionStatus::Pending, SuggestionStatus::Blocked)
            | (SuggestionStatus::Snoozed, SuggestionStatus::Accepted)
            | (SuggestionStatus::Snoozed, SuggestionStatus::Dismissed)
            | (SuggestionStatus::Snoozed, SuggestionStatus::Blocked)
    )
}

fn list_discovery_runs(connection: &Connection) -> Result<Vec<DiscoveryRun>, StorageError> {
    let mut statement = connection.prepare("SELECT id, algorithm_version, status, started_at, finished_at, input_event_count, sessions_upserted, candidates_evaluated, suggestions_created, failure_code FROM discovery_runs ORDER BY started_at DESC, id ASC")?;
    let rows = statement.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, String>(3)?,
            row.get::<_, Option<String>>(4)?,
            row.get::<_, u32>(5)?,
            row.get::<_, u32>(6)?,
            row.get::<_, u32>(7)?,
            row.get::<_, u32>(8)?,
            row.get::<_, Option<String>>(9)?,
        ))
    })?;
    rows.map(|row| {
        row.map_err(StorageError::from)
            .and_then(discovery_run_from_row)
    })
    .collect()
}

#[allow(clippy::type_complexity)]
fn discovery_run_from_row(
    row: (
        String,
        String,
        String,
        String,
        Option<String>,
        u32,
        u32,
        u32,
        u32,
        Option<String>,
    ),
) -> Result<DiscoveryRun, StorageError> {
    Ok(DiscoveryRun {
        id: parse_uuid(&row.0)?,
        algorithm_version: row.1,
        status: parse_discovery_status(&row.2)?,
        started_at: parse_ts(&row.3)?,
        finished_at: row.4.as_deref().map(parse_ts).transpose()?,
        input_event_count: row.5,
        sessions_upserted: row.6,
        candidates_evaluated: row.7,
        suggestions_created: row.8,
        failure_code: row.9,
    })
}

#[allow(clippy::type_complexity)]
fn session_from_connection(
    connection: &Connection,
    row: (
        String,
        String,
        String,
        String,
        String,
        String,
        Option<String>,
        u32,
        bool,
    ),
) -> Result<DiscoveredSession, StorageError> {
    let id = parse_uuid(&row.0)?;
    let mut statement = connection.prepare("SELECT event_id, resource_id, ordinal, received_at, is_anchor FROM session_events WHERE session_id=?1 ORDER BY ordinal ASC")?;
    let events = statement
        .query_map(params![row.0.clone()], |event| {
            Ok((
                event.get::<_, String>(0)?,
                event.get::<_, String>(1)?,
                event.get::<_, u32>(2)?,
                event.get::<_, String>(3)?,
                event.get::<_, i64>(4)?,
            ))
        })?
        .map(|event| {
            event.map_err(StorageError::from).and_then(|value| {
                Ok(SessionEvent {
                    event_id: parse_uuid(&value.0)?,
                    resource_id: parse_uuid(&value.1)?,
                    ordinal: value.2,
                    received_at: parse_ts(&value.3)?,
                    is_anchor: value.4 != 0,
                })
            })
        })
        .collect::<Result<Vec<_>, StorageError>>()?;
    Ok(DiscoveredSession {
        id,
        algorithm_version: row.1,
        kind: row.2,
        workspace_resource_id: parse_uuid(&row.3)?,
        anchor_event_id: parse_uuid(&row.4)?,
        started_at: parse_ts(&row.5)?,
        finished_at: row.6.as_deref().map(parse_ts).transpose()?,
        event_count: row.7,
        eligible: row.8,
        events,
    })
}

fn list_sessions(
    connection: &Connection,
    workspace: Option<Uuid>,
) -> Result<Vec<DiscoveredSession>, StorageError> {
    let mut statement = connection.prepare("SELECT id, algorithm_version, kind, workspace_resource_id, anchor_event_id, started_at, finished_at, event_count, eligible FROM sessions WHERE (?1 IS NULL OR workspace_resource_id=?1) ORDER BY started_at ASC, id ASC")?;
    let rows = statement.query_map(params![workspace.map(|id| id.to_string())], |row| {
        Ok((
            row.get(0)?,
            row.get(1)?,
            row.get(2)?,
            row.get(3)?,
            row.get(4)?,
            row.get(5)?,
            row.get(6)?,
            row.get(7)?,
            row.get::<_, i64>(8)? != 0,
        ))
    })?;
    let records = rows
        .map(|row| {
            row.map_err(StorageError::from)
                .and_then(|value| session_from_connection(connection, value))
        })
        .collect::<Result<Vec<_>, StorageError>>()?;
    Ok(records)
}

fn get_session(connection: &Connection, id: Uuid) -> Result<DiscoveredSession, StorageError> {
    let row = connection.query_row("SELECT id, algorithm_version, kind, workspace_resource_id, anchor_event_id, started_at, finished_at, event_count, eligible FROM sessions WHERE id=?1", params![id.to_string()], |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?,row.get(4)?,row.get(5)?,row.get(6)?,row.get(7)?,row.get::<_,i64>(8)? != 0))).optional()?.ok_or(StorageError::SessionNotFound)?;
    session_from_connection(connection, row)
}

#[allow(clippy::type_complexity)]
fn suggestion_row(
    connection: &Connection,
    id: Uuid,
) -> Result<
    (
        String,
        String,
        String,
        String,
        u32,
        u32,
        u32,
        String,
        String,
        i64,
        i64,
        String,
        String,
        String,
        Option<String>,
        Option<String>,
    ),
    StorageError,
> {
    connection.query_row("SELECT id, algorithm_version, workspace_resource_id, pattern_fingerprint, support_sessions, eligible_sessions, confidence_basis_points, first_seen_at, last_seen_at, observation_span_seconds, median_completion_ms, created_at, updated_at, status, snoozed_until, accepted_ritual_id FROM suggestions WHERE id=?1", params![id.to_string()], |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?,row.get(4)?,row.get(5)?,row.get(6)?,row.get(7)?,row.get(8)?,row.get(9)?,row.get(10)?,row.get(11)?,row.get(12)?,row.get(13)?,row.get(14)?,row.get(15)?))).optional()?.ok_or(StorageError::SuggestionNotFound)
}

fn get_suggestion(
    connection: &Connection,
    id: Uuid,
) -> Result<
    (
        DiscoveredSuggestion,
        SuggestionStatus,
        Option<OffsetDateTime>,
        Option<Uuid>,
    ),
    StorageError,
> {
    let row = suggestion_row(connection, id)?;
    let mut actions_statement = connection.prepare(
        "SELECT resource_id FROM suggestion_actions WHERE suggestion_id=?1 ORDER BY ordinal ASC",
    )?;
    let actions = actions_statement
        .query_map(params![id.to_string()], |action| action.get::<_, String>(0))?
        .map(|item| {
            item.map_err(StorageError::from)
                .and_then(|value| parse_uuid(&value))
        })
        .collect::<Result<Vec<_>, StorageError>>()?;
    let mut supports_statement = connection.prepare("SELECT session_id FROM suggestion_support_sessions WHERE suggestion_id=?1 ORDER BY session_id ASC")?;
    let supports = supports_statement
        .query_map(params![id.to_string()], |support| {
            support.get::<_, String>(0)
        })?
        .map(|item| {
            item.map_err(StorageError::from)
                .and_then(|value| parse_uuid(&value))
        })
        .collect::<Result<Vec<_>, StorageError>>()?;
    let status = parse_suggestion_status(&row.13)?;
    Ok((
        DiscoveredSuggestion {
            id: parse_uuid(&row.0)?,
            algorithm_version: row.1,
            workspace_resource_id: parse_uuid(&row.2)?,
            pattern_fingerprint: row.3,
            action_resource_ids: actions,
            supporting_session_ids: supports,
            support_sessions: row.4,
            eligible_sessions: row.5,
            confidence_basis_points: row.6,
            first_seen_at: parse_ts(&row.7)?,
            last_seen_at: parse_ts(&row.8)?,
            observation_span_seconds: row.9,
            median_completion_ms: row.10,
        },
        status,
        row.14.as_deref().map(parse_ts).transpose()?,
        row.15.as_deref().map(parse_uuid).transpose()?,
    ))
}

fn list_suggestions(
    connection: &Connection,
    status: Option<SuggestionStatus>,
    workspace: Option<Uuid>,
) -> Result<Vec<DiscoveredSuggestion>, StorageError> {
    let now = format_ts(OffsetDateTime::now_utc())?;
    connection.execute("UPDATE suggestions SET status='pending', snoozed_until=NULL WHERE status IN ('snoozed','dismissed') AND snoozed_until IS NOT NULL AND snoozed_until <= ?1", params![now])?;
    let mut statement = connection.prepare("SELECT id FROM suggestions WHERE (?1 IS NULL OR status=?1) AND (?2 IS NULL OR workspace_resource_id=?2) ORDER BY updated_at DESC, id ASC")?;
    let rows = statement.query_map(
        params![
            status.map(suggestion_status_name),
            workspace.map(|id| id.to_string())
        ],
        |row| row.get::<_, String>(0),
    )?;
    rows.map(|row| {
        row.map_err(StorageError::from)
            .and_then(|id| get_suggestion(connection, parse_uuid(&id)?).map(|value| value.0))
    })
    .collect()
}

fn get_suggestion_tx(
    transaction: &rusqlite::Transaction<'_>,
    id: Uuid,
) -> Result<
    (
        DiscoveredSuggestion,
        SuggestionStatus,
        Option<OffsetDateTime>,
        Option<Uuid>,
    ),
    StorageError,
> {
    get_suggestion(transaction, id)
}

fn get_ritual_tx(
    transaction: &rusqlite::Transaction<'_>,
    id: Uuid,
) -> Result<(Ritual, RitualVersion, RitualDefinition), StorageError> {
    get_ritual(transaction, id)
}

fn get_resource_tx(
    transaction: &rusqlite::Transaction<'_>,
    id: Uuid,
) -> Result<Resource, StorageError> {
    get_resource(transaction, id)
}

fn scope_active_tx(
    transaction: &rusqlite::Transaction<'_>,
    id: Uuid,
    source: ObservationSource,
) -> Result<bool, StorageError> {
    Ok(transaction.query_row("SELECT EXISTS(SELECT 1 FROM observation_scopes WHERE resource_id=?1 AND source=?2 AND status='active')", params![id.to_string(), observation_source_name(source)], |row| row.get::<_,i64>(0))? != 0)
}

fn insert_ritual_tx(
    transaction: &rusqlite::Transaction<'_>,
    ritual: &Ritual,
    version: &RitualVersion,
) -> Result<(), StorageError> {
    transaction.execute("INSERT INTO rituals(id,name,status,current_version_id,created_at,updated_at) VALUES (?1,?2,?3,?4,?5,?6)", params![ritual.id.to_string(), ritual.name, ritual_status_name(ritual.status), version.id.to_string(), format_ts(ritual.created_at)?, format_ts(ritual.updated_at)?])?;
    insert_ritual_version(transaction, version)
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
            id, execution_id, step_index, action_type, status, adapter_id, adapter_instance_id, started_at, finished_at, result_code, redacted_message
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
        params![
            step.id.to_string(),
            step.execution_id.to_string(),
            i64::from(step.step_index),
            step.action_type,
            execution_step_status_name(step.status),
            step.adapter_id,
            step.adapter_instance_id.map(|value| value.to_string()),
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
        "UPDATE execution_steps SET status = ?1, adapter_id = ?2, adapter_instance_id = ?3,
             started_at = ?4, finished_at = ?5, result_code = ?6, redacted_message = ?7 WHERE id = ?8",
        params![
            execution_step_status_name(step.status),
            step.adapter_id,
            step.adapter_instance_id.map(|value| value.to_string()),
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
        "SELECT id, execution_id, step_index, action_type, status, adapter_id, adapter_instance_id, started_at, finished_at, result_code, redacted_message
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
        row.get(10)?,
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
        adapter_instance_id: row.6.map(|value| parse_uuid(&value)).transpose()?,
        started_at: parse_ts(&row.7)?,
        finished_at: row.8.map(|value| parse_ts(&value)).transpose()?,
        result_code: row.9,
        redacted_message: row.10,
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
        ResourceKind::WebPage => "web.page",
    }
}

fn parse_resource_kind(value: &str) -> Result<ResourceKind, StorageError> {
    match value {
        "filesystem.file" => Ok(ResourceKind::File),
        "filesystem.directory" => Ok(ResourceKind::Directory),
        "web.page" => Ok(ResourceKind::WebPage),
        _ => Err(StorageError::InvalidData("resource kind".to_owned())),
    }
}

const fn observation_source_name(value: ObservationSource) -> &'static str {
    match value {
        ObservationSource::BrowserChromium => "browser.chromium",
        ObservationSource::VscodeWorkspace => "vscode.workspace",
    }
}

fn parse_observation_source(value: &str) -> Result<ObservationSource, StorageError> {
    match value {
        "browser.chromium" => Ok(ObservationSource::BrowserChromium),
        "vscode.workspace" => Ok(ObservationSource::VscodeWorkspace),
        _ => Err(StorageError::InvalidData("observation source".to_owned())),
    }
}

const fn observation_status_name(value: ObservationStatus) -> &'static str {
    match value {
        ObservationStatus::Active => "active",
        ObservationStatus::Paused => "paused",
    }
}

fn parse_observation_status(value: &str) -> Result<ObservationStatus, StorageError> {
    match value {
        "active" => Ok(ObservationStatus::Active),
        "paused" => Ok(ObservationStatus::Paused),
        _ => Err(StorageError::InvalidData("observation status".to_owned())),
    }
}

fn observation_scope_from_sql_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<ObservationScope> {
    let id: String = row.get(0)?;
    let source: String = row.get(1)?;
    let resource_id: String = row.get(2)?;
    let status: String = row.get(3)?;
    let created_at: String = row.get(4)?;
    let updated_at: String = row.get(5)?;
    // rusqlite row mappers cannot return StorageError, so invalid persisted
    // values are surfaced as a conversion error and never silently accepted.
    Ok(ObservationScope {
        id: sql_parse(parse_uuid(&id))?,
        source: sql_parse(parse_observation_source(&source))?,
        resource_id: sql_parse(parse_uuid(&resource_id))?,
        status: sql_parse(parse_observation_status(&status))?,
        created_at: sql_parse(parse_ts(&created_at))?,
        updated_at: sql_parse(parse_ts(&updated_at))?,
    })
}

fn sql_parse<T>(value: Result<T, StorageError>) -> rusqlite::Result<T> {
    value.map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(error))
    })
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
    let rows = statement.query_map(params![since, i64::from(limit)], |row| {
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
        EventType::BrowserResourceOpenedV1 => "dev.fubun.browser.resource.opened.v1",
        EventType::VscodeWorkspaceOpenedV1 => "dev.fubun.vscode.workspace.opened.v1",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fubun_domain::{
        ActionSpec, AdapterIdentity, EventData, ExecutionMode, FailureMode, ObservationScope,
        Resource, ResourceKind, ResourceScope, RitualExecutionConfig, Sensitivity,
        EVENT_SPEC_VERSION, RITUAL_SCHEMA_VERSION,
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
            data: EventData::Synthetic {
                label: "storage-smoke".to_owned(),
                counter: 1,
            },
        }
    }

    fn insert_event_scan_fixture(path: &Path, count: u32) {
        let connection = Connection::open(path).expect("open scan fixture");
        connection
            .pragma_update(None, "foreign_keys", true)
            .expect("foreign keys");
        let transaction = connection
            .unchecked_transaction()
            .expect("begin scan fixture");
        let instance_id = Uuid::from_u128(0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa);
        let timestamp = OffsetDateTime::now_utc();
        let timestamp_text = format_ts(timestamp).expect("timestamp");
        transaction
            .execute(
                "INSERT INTO adapters(instance_id, adapter_id, adapter_version, last_sequence_no, created_at, updated_at)
                 VALUES (?1, 'dev.scan', '0.1.0', ?2, ?3, ?3)",
                params![instance_id.to_string(), i64::from(count), timestamp_text],
            )
            .expect("adapter fixture");
        for sequence in 1..=count {
            let mut fixture = event(instance_id);
            fixture.id = Uuid::from_u128(u128::from(sequence));
            fixture.received_at = timestamp + time::Duration::seconds(i64::from(sequence));
            fixture.occurred_at = fixture.received_at;
            fixture.adapter.id = "dev.scan".to_owned();
            fixture.adapter.sequence_no = u64::from(sequence);
            let occurred_at = format_ts(fixture.occurred_at).expect("occurred_at");
            let received_at = format_ts(fixture.received_at).expect("received_at");
            let data_json = serde_json::to_string(&fixture.data).expect("data");
            let canonical_json = serde_json::to_string(&fixture).expect("canonical");
            transaction
                .execute(
                    "INSERT INTO events(
                        id, spec_version, event_type, source, occurred_at, received_at, actor,
                        adapter_id, adapter_version, adapter_instance_id, sequence_no,
                        context_json, privacy, data_json, canonical_json
                    ) VALUES (?1, ?2, 'dev.fubun.dev.synthetic.v1', ?3, ?4, ?5, 'user',
                        'dev.scan', '0.1.0', ?6, ?7, NULL, 'normal', ?8, ?9)",
                    params![
                        fixture.id.to_string(),
                        EVENT_SPEC_VERSION,
                        fixture.source,
                        occurred_at,
                        received_at,
                        instance_id.to_string(),
                        i64::from(sequence),
                        data_json,
                        canonical_json,
                    ],
                )
                .expect("event fixture");
        }
        transaction.commit().expect("commit scan fixture");
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

    #[tokio::test]
    async fn public_event_list_is_bounded_and_discovery_scan_is_private_and_large() {
        let temp = TempDir::new().expect("tempdir");
        let path = temp.path().join("fubun/fubun.db");
        let storage = Storage::open(&path).expect("open storage");
        storage.shutdown().await.expect("shutdown before fixture");
        insert_event_scan_fixture(&path, DISCOVERY_EVENT_SCAN_LIMIT);

        let reopened = Storage::open(&path).expect("reopen storage");
        let handle = reopened.handle();
        let public = handle
            .list_events(None, DISCOVERY_EVENT_SCAN_LIMIT)
            .await
            .expect("public event list");
        assert_eq!(
            public.len(),
            usize::try_from(PUBLIC_EVENT_LIST_LIMIT).unwrap()
        );
        let discovery = handle
            .list_events_for_discovery(None, DISCOVERY_EVENT_SCAN_LIMIT)
            .await
            .expect("discovery event list");
        assert_eq!(
            discovery.len(),
            usize::try_from(DISCOVERY_EVENT_SCAN_LIMIT).unwrap()
        );
        let discovery_below_limit = handle
            .list_events_for_discovery(None, DISCOVERY_EVENT_SCAN_LIMIT - 1)
            .await
            .expect("bounded discovery event list");
        assert_eq!(
            discovery_below_limit.len(),
            usize::try_from(DISCOVERY_EVENT_SCAN_LIMIT - 1).unwrap()
        );
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
    async fn migrates_phase_one_database_to_current_schema() {
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
        assert_eq!(version, SCHEMA_VERSION);
        storage.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn v4_migration_normalizes_legacy_synthetic_event_data() {
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
        let event_id = Uuid::new_v4();
        let instance_id = Uuid::new_v4();
        let timestamp = "2026-08-03T00:00:00Z";
        let legacy_data = r#"{"label":"legacy","counter":1}"#;
        let legacy_event = format!(
            r#"{{"spec_version":"1.0","id":"{event_id}","type":"dev.fubun.dev.synthetic.v1","source":"legacy","occurred_at":"{timestamp}","received_at":"{timestamp}","actor":"user","adapter":{{"id":"dev.test","version":"0.1.0","instance_id":"{instance_id}","sequence_no":1}},"context":null,"privacy":"normal","data":{legacy_data}}}"#
        );
        connection
            .execute(
                "INSERT INTO adapters(instance_id, adapter_id, adapter_version, last_sequence_no, created_at, updated_at) VALUES (?1, 'dev.test', '0.1.0', 1, ?2, ?2)",
                params![instance_id.to_string(), timestamp],
            )
            .expect("adapter");
        connection
            .execute(
                "INSERT INTO events(id, spec_version, event_type, source, occurred_at, received_at, actor, adapter_id, adapter_version, adapter_instance_id, sequence_no, context_json, privacy, data_json, canonical_json) VALUES (?1, '1.0', 'dev.fubun.dev.synthetic.v1', 'legacy', ?2, ?2, 'user', 'dev.test', '0.1.0', ?3, 1, NULL, 'normal', ?4, ?5)",
                params![event_id.to_string(), timestamp, instance_id.to_string(), legacy_data, legacy_event],
            )
            .expect("legacy event");
        drop(connection);

        let storage = Storage::open(&path).expect("migrate");
        let events = storage
            .handle()
            .list_events(None, 10)
            .await
            .expect("legacy events remain readable");
        assert!(matches!(
            events.first().map(|event| &event.data),
            Some(EventData::Synthetic { label, counter }) if label == "legacy" && *counter == 1
        ));
        storage.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn raw_anchor_event_can_be_deleted_while_derived_evidence_remains() {
        let temp = TempDir::new().expect("tempdir");
        let path = temp.path().join("fubun/fubun.db");
        let storage = Storage::open(&path).expect("open storage");
        let handle = storage.handle();
        let now = OffsetDateTime::now_utc();
        let workspace_id = Uuid::new_v4();
        handle
            .create_resource(Resource {
                id: workspace_id,
                kind: ResourceKind::Directory,
                label: "workspace evidence".to_owned(),
                locator: "/tmp/fubun-workspace-evidence".to_owned(),
                canonical_locator: "/tmp/fubun-workspace-evidence".to_owned(),
                sensitivity: Sensitivity::Normal,
                scope: ResourceScope::Exact,
                created_at: now,
                updated_at: now,
            })
            .await
            .expect("workspace");
        let anchor = event(Uuid::new_v4());
        let anchor_id = anchor.id;
        handle.insert_event(anchor).await.expect("anchor event");
        storage.shutdown().await.expect("shutdown");

        let connection = Connection::open(&path).expect("reopen database");
        connection
            .pragma_update(None, "foreign_keys", true)
            .expect("foreign keys");
        let session_id = Uuid::new_v4();
        let suggestion_id = Uuid::new_v4();
        let ts = format_ts(now).expect("timestamp");
        connection
            .execute(
                "INSERT INTO sessions(id, algorithm_version, kind, workspace_resource_id, anchor_event_id,
                    started_at, finished_at, event_count, eligible)
                 VALUES (?1, 'workspace-browser-start/v1', 'workspace_start', ?2, ?3, ?4, ?4, 1, 1)",
                params![session_id.to_string(), workspace_id.to_string(), anchor_id.to_string(), ts],
            )
            .expect("derived session");
        connection
            .execute(
                "INSERT INTO suggestions(id, algorithm_version, workspace_resource_id, pattern_fingerprint,
                    support_sessions, eligible_sessions, confidence_basis_points, first_seen_at, last_seen_at,
                    observation_span_seconds, median_completion_ms, created_at, updated_at, status,
                    snoozed_until, accepted_ritual_id)
                 VALUES (?1, 'workspace-browser-start/v1', ?2, 'retention-fingerprint', 1, 1, 10000,
                    ?3, ?3, 1, 1, ?3, ?3, 'pending', NULL, NULL)",
                params![suggestion_id.to_string(), workspace_id.to_string(), ts],
            )
            .expect("suggestion evidence");
        connection
            .execute(
                "INSERT INTO suggestion_support_sessions(suggestion_id, session_id) VALUES (?1, ?2)",
                params![suggestion_id.to_string(), session_id.to_string()],
            )
            .expect("supporting session");
        connection
            .execute(
                "DELETE FROM events WHERE id = ?1",
                params![anchor_id.to_string()],
            )
            .expect("raw event retention delete");
        let session_count: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM sessions WHERE id = ?1",
                params![session_id.to_string()],
                |row| row.get(0),
            )
            .expect("session summary");
        let suggestion_count: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM suggestions WHERE id = ?1",
                params![suggestion_id.to_string()],
                |row| row.get(0),
            )
            .expect("suggestion evidence");
        assert_eq!(session_count, 1);
        assert_eq!(suggestion_count, 1);
    }

    #[tokio::test]
    async fn migrates_phase_two_execution_steps_with_adapter_instance_identity() {
        let temp = TempDir::new().expect("tempdir");
        let path = temp.path().join("fubun/fubun.db");
        fs::create_dir_all(path.parent().expect("parent")).expect("parent");
        let connection = Connection::open(&path).expect("phase two database");
        connection
            .execute_batch(
                "CREATE TABLE schema_migrations(version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL);
                 INSERT INTO schema_migrations(version, applied_at) VALUES (2, '2026-08-04T00:00:00Z');
                 CREATE TABLE execution_steps(
                    id TEXT PRIMARY KEY,
                    execution_id TEXT NOT NULL,
                    step_index INTEGER NOT NULL,
                    action_type TEXT NOT NULL,
                    status TEXT NOT NULL,
                    adapter_id TEXT,
                    started_at TEXT NOT NULL,
                    finished_at TEXT,
                    result_code TEXT,
                    redacted_message TEXT
                 );",
            )
            .expect("phase two schema");
        drop(connection);

        let storage = Storage::open(&path).expect("migrate");
        assert_eq!(
            storage
                .handle()
                .schema_version()
                .await
                .expect("schema version"),
            SCHEMA_VERSION
        );
        storage.shutdown().await.expect("shutdown");

        let connection = Connection::open(&path).expect("reopen migrated database");
        let columns = connection
            .prepare("PRAGMA table_info(execution_steps)")
            .expect("table info")
            .query_map([], |row| row.get::<_, String>(1))
            .expect("columns")
            .collect::<Result<Vec<_>, _>>()
            .expect("column names");
        assert!(columns.iter().any(|column| column == "adapter_instance_id"));
    }

    #[tokio::test]
    async fn pausing_an_observation_scope_is_idempotent() {
        let temp = TempDir::new().expect("tempdir");
        let path = temp.path().join("fubun/fubun.db");
        let storage = Storage::open(&path).expect("open storage");
        let handle = storage.handle();
        let now = OffsetDateTime::now_utc();
        let resource_id = Uuid::new_v4();
        handle
            .create_resource(Resource {
                id: resource_id,
                kind: ResourceKind::WebPage,
                label: "pause test".to_owned(),
                locator: "https://example.com/pause".to_owned(),
                canonical_locator: "https://example.com/pause".to_owned(),
                sensitivity: Sensitivity::Normal,
                scope: ResourceScope::Exact,
                created_at: now,
                updated_at: now,
            })
            .await
            .expect("resource");
        let scope_id = Uuid::new_v4();
        let active = handle
            .ensure_observation_scope(ObservationScope {
                id: scope_id,
                source: ObservationSource::BrowserChromium,
                resource_id,
                status: ObservationStatus::Active,
                created_at: now,
                updated_at: now,
            })
            .await
            .expect("scope");
        let paused = handle
            .pause_observation_scope(scope_id)
            .await
            .expect("active pause");
        let paused_again = handle
            .pause_observation_scope(scope_id)
            .await
            .expect("paused pause");
        assert_eq!(active.status, ObservationStatus::Active);
        assert_eq!(paused.status, ObservationStatus::Paused);
        assert_eq!(paused_again, paused);
        assert!(matches!(
            handle.pause_observation_scope(Uuid::new_v4()).await,
            Err(StorageError::NotFound)
        ));
        storage.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn failed_discovery_run_is_terminal_and_releases_single_flight() {
        let temp = TempDir::new().expect("tempdir");
        let path = temp.path().join("fubun/fubun.db");
        let storage = Storage::open(&path).expect("open storage");
        let handle = storage.handle();
        let started_at = OffsetDateTime::now_utc();
        let run = DiscoveryRun {
            id: Uuid::new_v4(),
            algorithm_version: fubun_mining::ALGORITHM_VERSION.to_owned(),
            status: DiscoveryRunStatus::Running,
            started_at,
            finished_at: None,
            input_event_count: 0,
            sessions_upserted: 0,
            candidates_evaluated: 0,
            suggestions_created: 0,
            failure_code: None,
        };
        handle
            .start_discovery_run(run.clone())
            .await
            .expect("start discovery");
        let finished_at = started_at + time::Duration::seconds(1);
        handle
            .finish_discovery_run_failed(run.id, "resource_load_failed", finished_at)
            .await
            .expect("finish failure");
        let stored = handle
            .list_discovery_runs()
            .await
            .expect("list runs")
            .into_iter()
            .next()
            .expect("stored run");
        assert_eq!(stored.status, DiscoveryRunStatus::Failed);
        assert_eq!(stored.failure_code.as_deref(), Some("resource_load_failed"));
        assert_eq!(stored.finished_at, Some(finished_at));

        let next = DiscoveryRun {
            id: Uuid::new_v4(),
            started_at: finished_at,
            ..run
        };
        handle
            .start_discovery_run(next)
            .await
            .expect("failed run must release single flight");
        storage.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn suggestion_cap_counts_pending_only() {
        let temp = TempDir::new().expect("tempdir");
        let path = temp.path().join("fubun/fubun.db");
        let storage = Storage::open(&path).expect("open storage");
        let handle = storage.handle();
        let now = OffsetDateTime::now_utc();
        let workspace_id = Uuid::new_v4();
        handle
            .create_resource(Resource {
                id: workspace_id,
                kind: ResourceKind::Directory,
                label: "pending cap".to_owned(),
                locator: "/tmp/fubun-pending-cap".to_owned(),
                canonical_locator: "/tmp/fubun-pending-cap".to_owned(),
                sensitivity: Sensitivity::Normal,
                scope: ResourceScope::Exact,
                created_at: now,
                updated_at: now,
            })
            .await
            .expect("workspace");
        storage.shutdown().await.expect("shutdown");

        let connection = Connection::open(&path).expect("database");
        let old = format_ts(now - time::Duration::days(2)).expect("old timestamp");
        let future = format_ts(now + time::Duration::days(7)).expect("future timestamp");
        for index in 0..14 {
            let status = if index < 4 { "pending" } else { "snoozed" };
            let snoozed_until = if status == "snoozed" {
                Some(future.as_str())
            } else {
                None
            };
            connection
                .execute(
                    "INSERT INTO suggestions(id, algorithm_version, workspace_resource_id, pattern_fingerprint,
                        support_sessions, eligible_sessions, confidence_basis_points, first_seen_at, last_seen_at,
                        observation_span_seconds, median_completion_ms, created_at, updated_at, status,
                        snoozed_until, accepted_ritual_id)
                     VALUES (?1, 'workspace-browser-start/v1', ?2, ?3, 3, 3, 10000, ?4, ?4, 64800, 1000, ?4, ?4, ?5, ?6, NULL)",
                    params![Uuid::new_v4().to_string(), workspace_id.to_string(), format!("old-{index}"), old, status, snoozed_until],
                )
                .expect("seed suggestion state");
        }
        drop(connection);

        let storage = Storage::open(&path).expect("reopen storage");
        let handle = storage.handle();
        let run = DiscoveryRun {
            id: Uuid::new_v4(),
            algorithm_version: fubun_mining::ALGORITHM_VERSION.to_owned(),
            status: DiscoveryRunStatus::Running,
            started_at: now,
            finished_at: None,
            input_event_count: 0,
            sessions_upserted: 0,
            candidates_evaluated: 1,
            suggestions_created: 0,
            failure_code: None,
        };
        handle.start_discovery_run(run.clone()).await.expect("run");
        let suggestion = DiscoveredSuggestion {
            id: Uuid::new_v4(),
            algorithm_version: fubun_mining::ALGORITHM_VERSION.to_owned(),
            workspace_resource_id: workspace_id,
            pattern_fingerprint: "new-pending".to_owned(),
            action_resource_ids: vec![workspace_id],
            supporting_session_ids: Vec::new(),
            support_sessions: 3,
            eligible_sessions: 3,
            confidence_basis_points: 10_000,
            first_seen_at: now,
            last_seen_at: now,
            observation_span_seconds: 64800,
            median_completion_ms: 1000,
        };
        handle
            .persist_discovery(run, Vec::new(), vec![suggestion])
            .await
            .expect("new pending suggestion");
        let pending = handle
            .list_suggestions(Some(SuggestionStatus::Pending), None)
            .await
            .expect("pending list");
        assert_eq!(pending.len(), 5);
        storage.shutdown().await.expect("shutdown");
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
                adapter_instance_id: None,
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
                adapter_instance_id: Some(Uuid::new_v4()),
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
                adapter_instance_id: Some(Uuid::new_v4()),
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
