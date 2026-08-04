use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
};

use clap::{Args, Parser, Subcommand};
use fubun_core::{paths::FubunPaths, ClientError, FubunClient};
use fubun_domain::{Event, ObservationSource, RitualDefinition};
use fubun_protocol::{
    DoctorReport, EmptyRequest, EventIngestRequest, EventsListRequest, ExecutionIdRequest,
    ObservationListRequest, ObservationPauseRequest, RequestBody, ResourceCreateRequest,
    ResourceIdRequest, ResponsePayload, RitualActivateRequest, RitualCreateRequest,
    RitualIdRequest, RitualUpdateRequest,
};
use thiserror::Error;
use time::{Duration, OffsetDateTime};
use uuid::Uuid;

const FIXTURE: &str = include_str!("../../../fixtures/synthetic-event.json");

#[derive(Debug, Parser)]
#[command(name = "fubun", version, about = "Fubun local core CLI")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Status,
    Dev {
        #[command(subcommand)]
        command: DevCommand,
    },
    Events {
        #[command(subcommand)]
        command: EventsCommand,
    },
    Doctor,
    Resource {
        #[command(subcommand)]
        command: ResourceCommand,
    },
    Ritual {
        #[command(subcommand)]
        command: RitualCommand,
    },
    Adapter {
        #[command(subcommand)]
        command: AdapterCommand,
    },
    Execution {
        #[command(subcommand)]
        command: ExecutionCommand,
    },
    Observations {
        #[command(subcommand)]
        command: ObservationsCommand,
    },
    Observation {
        #[command(subcommand)]
        command: ObservationCommand,
    },
    Integrations {
        #[command(subcommand)]
        command: IntegrationsCommand,
    },
    Browser {
        #[command(subcommand)]
        command: BrowserCommand,
    },
}

#[derive(Debug, Subcommand)]
enum DevCommand {
    EmitFixture,
}

#[derive(Debug, Subcommand)]
enum EventsCommand {
    List {
        #[arg(long, value_name = "DURATION")]
        since: Option<String>,
    },
}

#[derive(Debug, Subcommand)]
enum ResourceCommand {
    AddPath(ResourceAddPath),
    List,
    Show { resource_id: Uuid },
}

#[derive(Debug, Args)]
struct ResourceAddPath {
    #[arg(long)]
    label: String,
    #[arg(long)]
    path: PathBuf,
    #[arg(long, default_value = "normal")]
    sensitivity: String,
}

#[derive(Debug, Subcommand)]
enum RitualCommand {
    Validate {
        file: Option<PathBuf>,
        #[arg(long = "json-file", alias = "file")]
        json_file: Option<PathBuf>,
    },
    Create {
        file: Option<PathBuf>,
        #[arg(long = "json-file", alias = "file")]
        json_file: Option<PathBuf>,
    },
    Update {
        ritual_id: Uuid,
        file: Option<PathBuf>,
        #[arg(long = "json-file", alias = "file")]
        json_file: Option<PathBuf>,
    },
    List,
    Show {
        ritual_id: Uuid,
    },
    Preview {
        ritual_id: Uuid,
    },
    Activate {
        ritual_id: Uuid,
        #[arg(long)]
        approve: bool,
    },
    Pause {
        ritual_id: Uuid,
    },
    Run {
        ritual_id: Uuid,
    },
}

#[derive(Debug, Subcommand)]
enum AdapterCommand {
    List,
    Status,
}

#[derive(Debug, Subcommand)]
enum ExecutionCommand {
    List,
    Show { execution_id: Uuid },
}

#[derive(Debug, Subcommand)]
enum ObservationsCommand {
    List {
        #[arg(long)]
        source: Option<String>,
    },
}

#[derive(Debug, Subcommand)]
enum ObservationCommand {
    Pause { scope_id: Uuid },
}

#[derive(Debug, Subcommand)]
enum IntegrationsCommand {
    Status,
}

#[derive(Debug, Subcommand)]
enum BrowserCommand {
    Host {
        #[command(subcommand)]
        command: BrowserHostCommand,
    },
}

#[derive(Debug, Subcommand)]
enum BrowserHostCommand {
    Install(BrowserHostInstall),
    Status,
    Uninstall {
        #[arg(long)]
        browser: String,
    },
}

#[derive(Debug, Args)]
struct BrowserHostInstall {
    #[arg(long)]
    browser: String,
    #[arg(long = "extension-id")]
    extension_id: String,
    #[arg(long = "host-path")]
    host_path: PathBuf,
}

#[derive(Debug, Error)]
enum CliError {
    #[error("invalid duration: {0}; expected a positive integer followed by s, m, h, or d")]
    InvalidDuration(String),
    #[error("unexpected daemon response")]
    UnexpectedResponse,
    #[error("JSON file error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("file error: {0}")]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Client(#[from] ClientError),
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    if let Command::Ritual {
        command: RitualCommand::Validate { file, json_file },
    } = &cli.command
    {
        let definition = read_definition_arg(file.as_ref(), json_file.as_ref())?;
        definition
            .validate()
            .map_err(|_| CliError::UnexpectedResponse)?;
        println!("valid");
        return Ok(());
    }
    let paths = FubunPaths::discover()?;
    match cli.command {
        Command::Status => status(&paths.socket_path).await?,
        Command::Dev {
            command: DevCommand::EmitFixture,
        } => emit_fixture(&paths.socket_path).await?,
        Command::Events {
            command: EventsCommand::List { since },
        } => list_events(&paths.socket_path, since.as_deref()).await?,
        Command::Doctor => doctor(&paths).await?,
        Command::Resource { command } => resource_command(&paths.socket_path, command).await?,
        Command::Ritual { command } => ritual_command(&paths.socket_path, command).await?,
        Command::Adapter { command } => adapter_command(&paths.socket_path, command).await?,
        Command::Execution { command } => execution_command(&paths.socket_path, command).await?,
        Command::Observations { command } => {
            observations_command(&paths.socket_path, command).await?
        }
        Command::Observation { command } => {
            observation_command(&paths.socket_path, command).await?
        }
        Command::Integrations { command } => {
            integrations_command(&paths.socket_path, command).await?
        }
        Command::Browser { command } => browser_command(command)?,
    }
    Ok(())
}

async fn connect(socket_path: &Path) -> Result<FubunClient, ClientError> {
    FubunClient::connect(socket_path, "fubun-cli", env!("CARGO_PKG_VERSION")).await
}

async fn status(socket_path: &Path) -> Result<(), CliError> {
    let mut client = connect(socket_path).await?;
    let report = client.status().await?;
    println!(
        "daemon: {}\nprotocol: {}.{}\ndatabase: {}\nschema: {}",
        report.daemon,
        report.protocol_version.major,
        report.protocol_version.minor,
        report.database_status,
        report.schema_version
    );
    Ok(())
}

async fn emit_fixture(socket_path: &Path) -> Result<(), CliError> {
    let event: Event = serde_json::from_str(FIXTURE)?;
    event.validate().map_err(|_| CliError::UnexpectedResponse)?;
    let mut client = connect(socket_path).await?;
    match client
        .request(RequestBody::EventIngest(EventIngestRequest { event }))
        .await?
    {
        ResponsePayload::EventIngested(result) => {
            println!("stored event: {}", result.event_id);
            Ok(())
        }
        _ => Err(CliError::UnexpectedResponse),
    }
}

async fn list_events(socket_path: &Path, since: Option<&str>) -> Result<(), CliError> {
    let since = since.map(parse_since).transpose()?;
    let mut client = connect(socket_path).await?;
    match client
        .request(RequestBody::EventsList(EventsListRequest {
            since,
            limit: 100,
        }))
        .await?
    {
        ResponsePayload::EventList(result) => {
            println!("{}", serde_json::to_string_pretty(&result.events)?);
            Ok(())
        }
        _ => Err(CliError::UnexpectedResponse),
    }
}

async fn doctor(paths: &FubunPaths) -> Result<(), CliError> {
    println!("runtime directory: {}", paths.runtime_directory.display());
    println!("data directory: {}", paths.data_directory.display());
    println!("socket exists: {}", paths.socket_path.exists());
    let mut client = connect(&paths.socket_path).await?;
    let payload = client
        .request(RequestBody::SystemDoctor(EmptyRequest::default()))
        .await?;
    let ResponsePayload::Doctor(DoctorReport {
        database_path,
        database_status,
        schema_version,
        gtk_launch_available,
        xdg_open_available,
        notify_send_available,
        connected_adapters,
        action_registry_version,
        ritual_schema_version,
        running_executions,
        draft_rituals,
        active_rituals,
        browser_adapters,
        vscode_adapters,
        active_browser_scopes,
        active_vscode_scopes,
    }) = payload
    else {
        return Err(CliError::UnexpectedResponse);
    };
    println!("daemon connection: ok\nprotocol: 1.0\ndatabase path: {database_path}\ndatabase status: {database_status}\nschema version: {schema_version}\nadapters: {connected_adapters}\nbrowser adapters: {browser_adapters}\nvscode adapters: {vscode_adapters}\nactive browser scopes: {active_browser_scopes}\nactive vscode scopes: {active_vscode_scopes}\ngtk-launch: {gtk_launch_available}\nxdg-open: {xdg_open_available}\nnotify-send: {notify_send_available}\naction registry: {action_registry_version}\nritual schema: {ritual_schema_version}\nrunning executions: {running_executions}\ndraft rituals: {draft_rituals}\nactive rituals: {active_rituals}");
    Ok(())
}

async fn resource_command(socket_path: &Path, command: ResourceCommand) -> Result<(), CliError> {
    let mut client = connect(socket_path).await?;
    let payload = match command {
        ResourceCommand::AddPath(args) => {
            let sensitivity = match args.sensitivity.as_str() {
                "normal" => fubun_domain::Sensitivity::Normal,
                "sensitive" => fubun_domain::Sensitivity::Sensitive,
                _ => return Err(CliError::UnexpectedResponse),
            };
            client
                .request(RequestBody::ResourceCreate(ResourceCreateRequest {
                    label: args.label,
                    path: args.path.to_string_lossy().into_owned(),
                    sensitivity,
                }))
                .await?
        }
        ResourceCommand::List => {
            client
                .request(RequestBody::ResourceList(EmptyRequest::default()))
                .await?
        }
        ResourceCommand::Show { resource_id } => {
            client
                .request(RequestBody::ResourceShow(ResourceIdRequest { resource_id }))
                .await?
        }
    };
    println!("{}", serde_json::to_string_pretty(&payload)?);
    Ok(())
}

async fn ritual_command(socket_path: &Path, command: RitualCommand) -> Result<(), CliError> {
    let mut client = connect(socket_path).await?;
    let payload = match command {
        RitualCommand::Validate { .. } => unreachable!(),
        RitualCommand::Create { file, json_file } => {
            client
                .request(RequestBody::RitualCreate(RitualCreateRequest {
                    definition: read_definition_arg(file.as_ref(), json_file.as_ref())?,
                }))
                .await?
        }
        RitualCommand::Update {
            ritual_id,
            file,
            json_file,
        } => {
            client
                .request(RequestBody::RitualUpdate(RitualUpdateRequest {
                    ritual_id,
                    definition: read_definition_arg(file.as_ref(), json_file.as_ref())?,
                }))
                .await?
        }
        RitualCommand::List => {
            client
                .request(RequestBody::RitualList(EmptyRequest::default()))
                .await?
        }
        RitualCommand::Show { ritual_id } => {
            client
                .request(RequestBody::RitualShow(RitualIdRequest { ritual_id }))
                .await?
        }
        RitualCommand::Preview { ritual_id } => {
            client
                .request(RequestBody::RitualPreview(RitualIdRequest { ritual_id }))
                .await?
        }
        RitualCommand::Activate { ritual_id, approve } => {
            client
                .request(RequestBody::RitualActivate(RitualActivateRequest {
                    ritual_id,
                    approve,
                }))
                .await?
        }
        RitualCommand::Pause { ritual_id } => {
            client
                .request(RequestBody::RitualPause(RitualIdRequest { ritual_id }))
                .await?
        }
        RitualCommand::Run { ritual_id } => {
            client
                .request(RequestBody::RitualRun(RitualIdRequest { ritual_id }))
                .await?
        }
    };
    println!("{}", serde_json::to_string_pretty(&payload)?);
    Ok(())
}

fn read_definition(path: &Path) -> Result<RitualDefinition, CliError> {
    Ok(serde_json::from_slice(&fs::read(path)?)?)
}

fn read_definition_arg(
    positional: Option<&PathBuf>,
    option: Option<&PathBuf>,
) -> Result<RitualDefinition, CliError> {
    let path = option.or(positional).ok_or(CliError::UnexpectedResponse)?;
    read_definition(path)
}

async fn adapter_command(socket_path: &Path, command: AdapterCommand) -> Result<(), CliError> {
    let mut client = connect(socket_path).await?;
    let body = match command {
        AdapterCommand::List => RequestBody::AdapterList(EmptyRequest::default()),
        AdapterCommand::Status => RequestBody::AdapterStatus(EmptyRequest::default()),
    };
    println!(
        "{}",
        serde_json::to_string_pretty(&client.request(body).await?)?
    );
    Ok(())
}

async fn execution_command(socket_path: &Path, command: ExecutionCommand) -> Result<(), CliError> {
    let mut client = connect(socket_path).await?;
    let body = match command {
        ExecutionCommand::List => RequestBody::ExecutionList(EmptyRequest::default()),
        ExecutionCommand::Show { execution_id } => {
            RequestBody::ExecutionShow(ExecutionIdRequest { execution_id })
        }
    };
    println!(
        "{}",
        serde_json::to_string_pretty(&client.request(body).await?)?
    );
    Ok(())
}

async fn observations_command(
    socket_path: &Path,
    command: ObservationsCommand,
) -> Result<(), CliError> {
    let ObservationsCommand::List { source } = command;
    let source = source
        .map(|value| match value.as_str() {
            "browser.chromium" => Ok(ObservationSource::BrowserChromium),
            "vscode.workspace" => Ok(ObservationSource::VscodeWorkspace),
            _ => Err(CliError::UnexpectedResponse),
        })
        .transpose()?;
    let mut client = connect(socket_path).await?;
    let payload = client
        .request(RequestBody::ObservationsList(ObservationListRequest {
            source,
        }))
        .await?;
    println!("{}", serde_json::to_string_pretty(&payload)?);
    Ok(())
}

async fn observation_command(
    socket_path: &Path,
    command: ObservationCommand,
) -> Result<(), CliError> {
    let ObservationCommand::Pause { scope_id } = command;
    let mut client = connect(socket_path).await?;
    let payload = client
        .request(RequestBody::ObservationPause(ObservationPauseRequest {
            scope_id,
        }))
        .await?;
    println!("{}", serde_json::to_string_pretty(&payload)?);
    Ok(())
}

async fn integrations_command(
    socket_path: &Path,
    command: IntegrationsCommand,
) -> Result<(), CliError> {
    let IntegrationsCommand::Status = command;
    let mut client = connect(socket_path).await?;
    let payload = client
        .request(RequestBody::IntegrationsStatus(EmptyRequest::default()))
        .await?;
    println!("{}", serde_json::to_string_pretty(&payload)?);
    Ok(())
}

fn browser_command(command: BrowserCommand) -> Result<(), CliError> {
    let BrowserCommand::Host { command } = command;
    match command {
        BrowserHostCommand::Install(args) => install_native_host(args),
        BrowserHostCommand::Status => {
            let (config, _) = native_host_paths("chrome")?;
            println!(
                "config: {}\nchrome: {}\nchromium: {}",
                config.display(),
                native_host_paths("chrome")?.1.exists(),
                native_host_paths("chromium")?.1.exists()
            );
            Ok(())
        }
        BrowserHostCommand::Uninstall { browser } => uninstall_native_host(&browser),
    }
}

fn native_host_paths(browser: &str) -> Result<(PathBuf, PathBuf), CliError> {
    let config_home = std::env::var_os("XDG_CONFIG_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".config")))
        .ok_or(CliError::UnexpectedResponse)?;
    let manifest_dir = match browser {
        "chrome" => config_home.join("google-chrome/NativeMessagingHosts"),
        "chromium" => config_home.join("chromium/NativeMessagingHosts"),
        _ => return Err(CliError::UnexpectedResponse),
    };
    Ok((
        config_home.join("fubun/browser-native-host.json"),
        manifest_dir.join("dev.fubun.browser.json"),
    ))
}

fn validate_extension_id(value: &str) -> Result<(), CliError> {
    if value.len() != 32 || !value.bytes().all(|byte| (b'a'..=b'p').contains(&byte)) {
        return Err(CliError::UnexpectedResponse);
    }
    Ok(())
}

fn install_native_host(args: BrowserHostInstall) -> Result<(), CliError> {
    validate_extension_id(&args.extension_id)?;
    let host_path = fs::canonicalize(&args.host_path)?;
    let metadata = fs::metadata(&host_path)?;
    if !host_path.is_absolute() || !metadata.is_file() || metadata.permissions().mode() & 0o111 == 0
    {
        return Err(CliError::UnexpectedResponse);
    }
    let (config_path, manifest_path) = native_host_paths(&args.browser)?;
    let config_dir = config_path.parent().ok_or(CliError::UnexpectedResponse)?;
    fs::create_dir_all(config_dir)?;
    fs::set_permissions(config_dir, fs::Permissions::from_mode(0o700))?;
    let mut ids = if config_path.exists() {
        serde_json::from_slice::<serde_json::Value>(&fs::read(&config_path)?)?
            .get("allowed_extension_ids")
            .and_then(serde_json::Value::as_array)
            .map(|values| {
                values
                    .iter()
                    .filter_map(serde_json::Value::as_str)
                    .map(ToOwned::to_owned)
                    .collect()
            })
            .unwrap_or_default()
    } else {
        Vec::new()
    };
    if !ids.contains(&args.extension_id) {
        ids.push(args.extension_id.clone());
    }
    ids.sort();
    let config = serde_json::json!({"allowed_extension_ids": ids});
    atomic_private_write(&config_path, serde_json::to_vec_pretty(&config)?)?;
    let manifest_dir = manifest_path.parent().ok_or(CliError::UnexpectedResponse)?;
    fs::create_dir_all(manifest_dir)?;
    fs::set_permissions(manifest_dir, fs::Permissions::from_mode(0o700))?;
    let manifest = serde_json::json!({
        "name": "dev.fubun.browser",
        "description": "Fubun browser integration",
        "path": host_path,
        "type": "stdio",
        "allowed_origins": [format!("chrome-extension://{}/", args.extension_id)]
    });
    atomic_private_write(&manifest_path, serde_json::to_vec_pretty(&manifest)?)?;
    println!("installed {}", manifest_path.display());
    Ok(())
}

fn uninstall_native_host(browser: &str) -> Result<(), CliError> {
    let (config_path, manifest_path) = native_host_paths(browser)?;
    if manifest_path.exists() {
        fs::remove_file(manifest_path)?;
    }
    let other_browser = if browser == "chrome" {
        "chromium"
    } else {
        "chrome"
    };
    let other_manifest_exists = native_host_paths(other_browser)?.1.exists();
    if config_path.exists() && !other_manifest_exists {
        fs::remove_file(config_path)?;
    }
    Ok(())
}

fn atomic_private_write(path: &Path, bytes: Vec<u8>) -> Result<(), CliError> {
    let parent = path.parent().ok_or(CliError::UnexpectedResponse)?;
    fs::create_dir_all(parent)?;
    let temporary = path.with_extension("tmp");
    fs::write(&temporary, bytes)?;
    fs::set_permissions(&temporary, fs::Permissions::from_mode(0o600))?;
    fs::rename(temporary, path)?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    Ok(())
}

fn parse_since(input: &str) -> Result<OffsetDateTime, CliError> {
    if input.len() < 2 {
        return Err(CliError::InvalidDuration(input.to_owned()));
    }
    let (number, unit) = input.split_at(input.len() - 1);
    let value = number
        .parse::<i64>()
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| CliError::InvalidDuration(input.to_owned()))?;
    let duration = match unit {
        "s" => Duration::seconds(value),
        "m" => Duration::minutes(value),
        "h" => Duration::hours(value),
        "d" => Duration::days(value),
        _ => return Err(CliError::InvalidDuration(input.to_owned())),
    };
    Ok(OffsetDateTime::now_utc() - duration)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn parses_hour_duration() {
        let before = OffsetDateTime::now_utc() - Duration::minutes(59);
        assert!(parse_since("1h").expect("valid duration") < before);
    }
    #[test]
    fn rejects_invalid_duration() {
        assert!(matches!(
            parse_since("zero"),
            Err(CliError::InvalidDuration(_))
        ));
    }
}
