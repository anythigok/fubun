use fubun_core::{paths::FubunPaths, start_server};
use tracing::info;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_target(false)
        .compact()
        .init();

    let paths = FubunPaths::discover()?;
    let server = start_server(paths).await?;
    info!(socket = %server.paths().socket_path.display(), "fubund is ready");
    tokio::signal::ctrl_c().await?;
    info!("shutdown signal received");
    server.shutdown().await?;
    Ok(())
}
