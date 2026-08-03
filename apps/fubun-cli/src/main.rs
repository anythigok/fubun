use std::path::Path;

use clap::{Parser, Subcommand};
use fubun_core::{paths::FubunPaths, ClientError, FubunClient};
use fubun_domain::Event;
use fubun_protocol::{
    DoctorReport, EmptyRequest, EventIngestRequest, EventsListRequest, RequestBody, ResponsePayload,
};
use thiserror::Error;
use time::{Duration, OffsetDateTime};

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
}

#[derive(Debug, Subcommand)]
enum DevCommand {
    EmitFixture,
}

#[derive(Debug, Subcommand)]
enum EventsCommand {
    List {
        #[arg(long, value_name = "DURATION", help = "Examples: 30m, 1h, 2d")]
        since: Option<String>,
    },
}

#[derive(Debug, Error)]
enum CliError {
    #[error("invalid duration: {0}; expected a positive integer followed by s, m, h, or d")]
    InvalidDuration(String),
    #[error("unexpected daemon response")]
    UnexpectedResponse,
    #[error(transparent)]
    Client(#[from] ClientError),
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
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
    }
    Ok(())
}

async fn connect(socket_path: &Path) -> Result<FubunClient, ClientError> {
    FubunClient::connect(socket_path, "fubun-cli", env!("CARGO_PKG_VERSION")).await
}

async fn status(socket_path: &Path) -> Result<(), CliError> {
    let mut client = connect(socket_path).await?;
    let report = client.status().await?;
    println!("daemon: {}", report.daemon);
    println!(
        "protocol: {}.{}",
        report.protocol_version.major, report.protocol_version.minor
    );
    println!("database: {}", report.database_status);
    println!("schema: {}", report.schema_version);
    Ok(())
}

async fn emit_fixture(socket_path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let event: Event = serde_json::from_str(FIXTURE)?;
    event.validate()?;
    let mut client = connect(socket_path).await?;
    match client
        .request(RequestBody::EventIngest(EventIngestRequest { event }))
        .await?
    {
        ResponsePayload::EventIngested(result) => {
            println!("stored event: {}", result.event_id);
            Ok(())
        }
        _ => Err(CliError::UnexpectedResponse.into()),
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
            println!(
                "{}",
                serde_json::to_string_pretty(&result.events)
                    .map_err(|_| CliError::UnexpectedResponse)?
            );
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
    }) = payload
    else {
        return Err(CliError::UnexpectedResponse);
    };
    println!("daemon connection: ok");
    println!("protocol: 1.0");
    println!("database path: {database_path}");
    println!("database status: {database_status}");
    println!("schema version: {schema_version}");
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
        let parsed = parse_since("1h").expect("valid duration");
        assert!(parsed < before);
    }

    #[test]
    fn rejects_invalid_duration() {
        assert!(matches!(
            parse_since("zero"),
            Err(CliError::InvalidDuration(_))
        ));
    }
}
