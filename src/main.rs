use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use buddy3d_proxy::config::Config;
use buddy3d_proxy::init_tracing;
use buddy3d_proxy::prusa::api::{list_cameras, list_printers};
use buddy3d_proxy::prusa::auth::{AuthEndpoints, AuthOrchestrator};
use buddy3d_proxy::prusa::client::PrusaClient;
use buddy3d_proxy::rate_limit::RateLimiter;
use buddy3d_proxy::token_store::TokenStore;
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "buddy3d-proxy")]
struct Cli {
    #[command(subcommand)]
    command: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Log in and print every printer + camera visible to the configured account.
    /// Persists tokens to TOKEN_STORE_PATH so subsequent invocations skip login.
    ListCameras,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();
    let cli = Cli::parse();
    let cfg = Config::from_env().context("load config from environment")?;

    let limiter = Arc::new(RateLimiter::new(3, Duration::from_secs(60)));
    let http = reqwest::Client::builder()
        .cookie_store(true)
        .build()
        .context("build reqwest client")?;
    let prusa = PrusaClient::new(http, limiter);
    let endpoints = AuthEndpoints::default();
    let store = TokenStore::new(&cfg.token_store_path);
    let orch = Arc::new(AuthOrchestrator::new(
        prusa.clone(),
        endpoints.clone(),
        store,
        cfg.prusa_email.clone(),
        cfg.prusa_password.clone(),
    ));

    match cli.command {
        Cmd::ListCameras => {
            let token = orch.access_token().await.context("acquire access token")?;
            let printers = list_printers(&prusa, &endpoints.connect_base, &token)
                .await
                .context("list printers")?;
            for p in &printers {
                tracing::info!(printer.uuid = %p.uuid, printer.name = %p.name, "discovered printer");
                let cams = list_cameras(&prusa, &endpoints.connect_base, &token, &p.uuid)
                    .await
                    .with_context(|| format!("list cameras for printer {}", p.uuid))?;
                for c in cams {
                    tracing::info!(
                        printer.uuid = %p.uuid,
                        camera.id = c.id,
                        camera.name = c.name.as_deref().unwrap_or("(unnamed)"),
                        "discovered camera",
                    );
                }
            }
        }
    }
    Ok(())
}
