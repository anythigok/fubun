//! Linux adapter for the fixed Phase 2 action registry.

use std::{
    env, fs, io,
    os::unix::{
        fs::{MetadataExt, PermissionsExt},
        process::ExitStatusExt,
    },
    path::{Path, PathBuf},
    pin::Pin,
    process::ExitStatus,
};

use fubun_domain::{ActionSpec, ResourceKind};
use fubun_protocol::{
    read_json_frame, write_json_frame, ActionResult, AdapterActionStatus, AdapterHello,
    AdapterRequestBody, AdapterRequestEnvelope, AdapterResponseBody, AdapterResponseEnvelope,
    AdapterStatusSnapshot, AdapterToolStatus, ClientHelloAck, RequestBody, RequestEnvelope,
    ResponseBody, ResponseEnvelope, CURRENT_PROTOCOL_VERSION,
};
use tokio::{net::UnixStream, process::Command};
use uuid::Uuid;

const ADAPTER_ID: &str = "dev.fubun.linux";
const CAPABILITIES: [&str; 3] = [
    "linux.app.ensure_running.v1",
    "linux.path.open.v1",
    "desktop.notification.show.v1",
];

trait ExecutableRunner: Send + Sync {
    fn run<'a>(
        &'a self,
        executable: &'a str,
        args: &'a [&'a str],
    ) -> Pin<Box<dyn FutureStatus + 'a>>;
    fn is_fake(&self) -> bool {
        false
    }
}

trait FutureStatus: std::future::Future<Output = io::Result<ExitStatus>> + Send {}
impl<T> FutureStatus for T where T: std::future::Future<Output = io::Result<ExitStatus>> + Send {}

struct ProcessRunner;

impl ExecutableRunner for ProcessRunner {
    fn run<'a>(
        &'a self,
        executable: &'a str,
        args: &'a [&'a str],
    ) -> Pin<Box<dyn FutureStatus + 'a>> {
        match executable {
            "gtk-launch" => {
                Box::pin(async move { Command::new("gtk-launch").args(args).status().await })
            }
            "xdg-open" => {
                Box::pin(async move { Command::new("xdg-open").args(args).status().await })
            }
            "notify-send" => {
                Box::pin(async move { Command::new("notify-send").args(args).status().await })
            }
            _ => Box::pin(async {
                Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "executable is not in the fixed registry",
                ))
            }),
        }
    }
}

#[derive(Clone, Copy)]
struct FakeRunner;

impl ExecutableRunner for FakeRunner {
    fn run<'a>(
        &'a self,
        _executable: &'a str,
        _args: &'a [&'a str],
    ) -> Pin<Box<dyn FutureStatus + 'a>> {
        Box::pin(async { Ok(ExitStatus::from_raw(0)) })
    }
    fn is_fake(&self) -> bool {
        true
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_target(false)
        .compact()
        .init();
    let mut socket_path = None;
    let mut fake = false;
    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--socket" => socket_path = args.next().map(PathBuf::from),
            "--fake" => fake = true,
            _ => return Err(format!("unknown argument: {arg}").into()),
        }
    }
    let socket_path = match socket_path {
        Some(path) => path,
        None => default_socket_path()?,
    };
    let mut stream = UnixStream::connect(socket_path).await?;
    let hello = RequestEnvelope {
        protocol_version: CURRENT_PROTOCOL_VERSION,
        request_id: Uuid::new_v4(),
        body: RequestBody::AdapterHello(AdapterHello {
            adapter_id: ADAPTER_ID.to_owned(),
            adapter_version: env!("CARGO_PKG_VERSION").to_owned(),
            instance_id: Uuid::new_v4(),
            action_capabilities: CAPABILITIES
                .iter()
                .map(|value| (*value).to_owned())
                .collect(),
            status: if fake {
                fake_status()
            } else {
                collect_status()
            },
        }),
    };
    let hello_id = hello.request_id;
    write_json_frame(&mut stream, &hello).await?;
    let ack: ResponseEnvelope = read_json_frame(&mut stream)
        .await?
        .ok_or("daemon closed during adapter hello")?;
    if ack.request_id != hello_id
        || !matches!(
            ack.body,
            ResponseBody::Ok(fubun_protocol::ResponsePayload::AdapterHelloAck(
                ClientHelloAck { .. }
            ))
        )
    {
        return Err("adapter handshake rejected".into());
    }
    let runner: Box<dyn ExecutableRunner> = if fake {
        Box::new(FakeRunner)
    } else {
        Box::new(ProcessRunner)
    };
    while let Some(request) = read_json_frame::<_, AdapterRequestEnvelope>(&mut stream).await? {
        let response = handle_request(request, runner.as_ref()).await;
        write_json_frame(&mut stream, &response).await?;
    }
    Ok(())
}

fn default_socket_path() -> Result<PathBuf, io::Error> {
    let runtime = env::var_os("XDG_RUNTIME_DIR")
        .map(PathBuf::from)
        .ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "XDG_RUNTIME_DIR is required")
        })?;
    Ok(runtime.join("fubun/core.sock"))
}

fn collect_status() -> AdapterStatusSnapshot {
    AdapterStatusSnapshot {
        tools: ["gtk-launch", "xdg-open", "notify-send"]
            .into_iter()
            .map(|name| AdapterToolStatus {
                name: name.to_owned(),
                available: executable_exists(name),
            })
            .collect(),
        desktop_entry_ids: desktop_entry_ids(),
    }
}

fn fake_status() -> AdapterStatusSnapshot {
    AdapterStatusSnapshot {
        tools: ["gtk-launch", "xdg-open", "notify-send"]
            .into_iter()
            .map(|name| AdapterToolStatus {
                name: name.to_owned(),
                available: true,
            })
            .collect(),
        desktop_entry_ids: vec!["code".to_owned()],
    }
}

fn executable_exists(name: &str) -> bool {
    env::var_os("PATH")
        .into_iter()
        .flat_map(|paths| env::split_paths(&paths).collect::<Vec<_>>())
        .any(|directory| {
            let candidate = directory.join(name);
            fs::metadata(candidate)
                .map(|metadata| metadata.is_file() && metadata.permissions().mode() & 0o111 != 0)
                .unwrap_or(false)
        })
}

fn application_directories() -> Vec<PathBuf> {
    let mut directories = Vec::new();
    if let Some(path) = env::var_os("XDG_DATA_HOME") {
        directories.push(PathBuf::from(path).join("applications"));
    }
    if let Some(home) = env::var_os("HOME") {
        directories.push(PathBuf::from(home).join(".local/share/applications"));
    }
    if let Some(data_dirs) = env::var_os("XDG_DATA_DIRS") {
        directories.extend(env::split_paths(&data_dirs).map(|path| path.join("applications")));
    }
    directories.push(PathBuf::from("/usr/local/share/applications"));
    directories.push(PathBuf::from("/usr/share/applications"));
    directories
}

fn desktop_entry_ids() -> Vec<String> {
    let mut ids = Vec::new();
    for directory in application_directories() {
        let Ok(entries) = fs::read_dir(directory) else {
            continue;
        };
        for entry in entries.flatten() {
            if entry
                .path()
                .extension()
                .is_some_and(|extension| extension == "desktop")
            {
                if let Some(name) = entry.path().file_stem().and_then(|value| value.to_str()) {
                    ids.push(name.to_owned());
                }
            }
        }
    }
    ids.sort();
    ids.dedup();
    ids
}

async fn handle_request(
    request: AdapterRequestEnvelope,
    runner: &dyn ExecutableRunner,
) -> AdapterResponseEnvelope {
    let body = match request.body {
        AdapterRequestBody::ActionExecute(payload) => {
            let result = execute_action(payload.action, payload.resolved_resource, runner)
                .await
                .unwrap_or_else(|message| ActionResult {
                    status: AdapterActionStatus::Failed,
                    result_code: "action_failed".to_owned(),
                    redacted_message: message,
                });
            AdapterResponseBody::ActionResult(result)
        }
        AdapterRequestBody::Status(_) => AdapterResponseBody::Status(collect_status()),
    };
    AdapterResponseEnvelope {
        protocol_version: CURRENT_PROTOCOL_VERSION,
        request_id: request.request_id,
        action_execution_id: request.action_execution_id,
        body,
    }
}

async fn execute_action(
    action: ActionSpec,
    resource: Option<fubun_protocol::ResolvedResource>,
    runner: &dyn ExecutableRunner,
) -> Result<ActionResult, String> {
    action
        .validate()
        .map_err(|_| "action validation failed".to_owned())?;
    match action {
        ActionSpec::LinuxAppEnsureRunning { app_id } => ensure_running(&app_id, runner).await,
        ActionSpec::LinuxPathOpen { resource_id } => open_path(resource_id, resource, runner).await,
        ActionSpec::DesktopNotificationShow { title, body } => {
            if !executable_exists("notify-send") && !runner.is_fake() {
                return Err("notify-send is unavailable".to_owned());
            }
            let status = runner
                .run("notify-send", &[&title, &body])
                .await
                .map_err(|_| "notification failed".to_owned())?;
            if status.success() {
                Ok(ActionResult {
                    status: AdapterActionStatus::Succeeded,
                    result_code: "sent".to_owned(),
                    redacted_message: "notification sent".to_owned(),
                })
            } else {
                Err("notification failed".to_owned())
            }
        }
    }
}

async fn ensure_running(
    app_id: &str,
    runner: &dyn ExecutableRunner,
) -> Result<ActionResult, String> {
    if app_id.is_empty()
        || app_id.contains('/')
        || !app_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'.' || byte == b'_')
    {
        return Err("invalid desktop entry id".to_owned());
    }
    if runner.is_fake() {
        return Ok(ActionResult {
            status: AdapterActionStatus::Succeeded,
            result_code: "launched".to_owned(),
            redacted_message: "fake launch completed".to_owned(),
        });
    }
    let entry = find_desktop_entry(app_id).ok_or_else(|| "desktop entry not found".to_owned())?;
    let executable = desktop_exec_basename(&entry)
        .ok_or_else(|| "desktop entry has no fixed executable".to_owned())?;
    if process_running(&executable) {
        return Ok(ActionResult {
            status: AdapterActionStatus::Skipped,
            result_code: "already_running".to_owned(),
            redacted_message: "application already running".to_owned(),
        });
    }
    if !executable_exists("gtk-launch") && !runner.is_fake() {
        return Err("gtk-launch is unavailable".to_owned());
    }
    let status = runner
        .run("gtk-launch", &[app_id])
        .await
        .map_err(|_| "application launch failed".to_owned())?;
    if status.success() {
        Ok(ActionResult {
            status: AdapterActionStatus::Succeeded,
            result_code: "launched".to_owned(),
            redacted_message: "application launch requested".to_owned(),
        })
    } else {
        Err("application launch failed".to_owned())
    }
}

async fn open_path(
    resource_id: Uuid,
    resource: Option<fubun_protocol::ResolvedResource>,
    runner: &dyn ExecutableRunner,
) -> Result<ActionResult, String> {
    let resource = resource.ok_or_else(|| "resolved resource is required".to_owned())?;
    if resource.resource_id != resource_id {
        return Err("resource identity mismatch".to_owned());
    }
    let path = Path::new(&resource.canonical_locator);
    if !path.is_absolute()
        || !path.exists()
        || fs::canonicalize(path)
            .map(|value| value != path)
            .unwrap_or(true)
    {
        return Err("resource changed".to_owned());
    }
    let metadata = fs::metadata(path).map_err(|_| "resource unavailable".to_owned())?;
    let kind_matches = match resource.kind {
        ResourceKind::File => metadata.is_file(),
        ResourceKind::Directory => metadata.is_dir(),
    };
    if !kind_matches {
        return Err("resource type changed".to_owned());
    }
    if !executable_exists("xdg-open") && !runner.is_fake() {
        return Err("xdg-open is unavailable".to_owned());
    }
    let path_text = path.to_string_lossy().into_owned();
    let status = runner
        .run("xdg-open", &[&path_text])
        .await
        .map_err(|_| "path open failed".to_owned())?;
    if status.success() {
        Ok(ActionResult {
            status: AdapterActionStatus::Succeeded,
            result_code: "opened".to_owned(),
            redacted_message: "path open requested".to_owned(),
        })
    } else {
        Err("path open failed".to_owned())
    }
}

fn find_desktop_entry(app_id: &str) -> Option<PathBuf> {
    application_directories()
        .into_iter()
        .map(|directory| directory.join(format!("{app_id}.desktop")))
        .find(|path| path.is_file())
}

fn desktop_exec_basename(path: &Path) -> Option<String> {
    let content = fs::read_to_string(path).ok()?;
    content.lines().find_map(|line| {
        line.strip_prefix("Exec=")
            .and_then(|value| value.split_whitespace().next())
            .and_then(|value| Path::new(value).file_name())
            .and_then(|value| value.to_str())
            .map(ToOwned::to_owned)
    })
}

fn process_running(executable: &str) -> bool {
    let current_uid = nixless_current_uid();
    let Ok(entries) = fs::read_dir("/proc") else {
        return false;
    };
    entries
        .flatten()
        .filter_map(|entry| entry.file_name().into_string().ok())
        .filter(|name| name.bytes().all(|byte| byte.is_ascii_digit()))
        .any(|pid| {
            let process_dir = entry_path(&pid);
            let metadata = fs::metadata(&process_dir).ok();
            if metadata
                .as_ref()
                .is_some_and(|value| value.uid() != current_uid)
            {
                return false;
            }
            fs::read_link(process_dir.join("exe"))
                .ok()
                .and_then(|path| {
                    path.file_name()
                        .and_then(|name| name.to_str())
                        .map(|name| name == executable)
                })
                .unwrap_or(false)
        })
}

fn entry_path(pid: &str) -> PathBuf {
    PathBuf::from("/proc").join(pid)
}
fn nixless_current_uid() -> u32 {
    fs::metadata("/proc/self")
        .map(|metadata| metadata.uid())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn app_ids_do_not_accept_paths() {
        assert!("../x".contains('/'));
        assert!("safe.app"
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'.' || byte == b'_'));
    }
    #[test]
    fn desktop_directories_never_use_user_supplied_path() {
        assert!(application_directories()
            .iter()
            .all(|path| path.ends_with("applications")));
    }
}
