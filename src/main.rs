use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use buddy3d_proxy::config::Config;
use buddy3d_proxy::init_tracing;
use buddy3d_proxy::prusa::api::{fetch_webrtc_config, list_cameras, list_printers};
use buddy3d_proxy::prusa::auth::{AuthEndpoints, AuthOrchestrator};
use buddy3d_proxy::prusa::client::PrusaClient;
use buddy3d_proxy::prusa::signaling::PrusaSignaling;
use buddy3d_proxy::rate_limit::RateLimiter;
use buddy3d_proxy::token_store::TokenStore;
use buddy3d_proxy::webrtc_session::{run_session, WebRtcSession};
use clap::{Parser, Subcommand};
use tokio::sync::mpsc;

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
    /// Connect to Prusa signaling, negotiate WebRTC, and log RTP packet stats.
    WatchStream {
        /// Run for this many seconds, then exit cleanly.
        #[arg(long, default_value_t = 30)]
        duration_seconds: u64,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();
    let cli = Cli::parse();
    let cfg = Config::from_env().context("load config from environment")?;

    let limiter = Arc::new(RateLimiter::new(3, Duration::from_secs(60)));
    let http = reqwest::Client::builder()
        .cookie_store(true)
        .user_agent(
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 \
             (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",
        )
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
        Cmd::WatchStream { duration_seconds } => {
            let token = orch.access_token().await.context("acquire access token")?;

            // Discover the printer + camera so we can grab the camera's
            // persistent token (the signaling server uses it as a permission key).
            let printers = list_printers(&prusa, &endpoints.connect_base, &token)
                .await
                .context("list printers")?;
            let printer = printers
                .first()
                .context("no printers visible to this account")?;
            let cams = list_cameras(&prusa, &endpoints.connect_base, &token, &printer.uuid)
                .await
                .with_context(|| format!("list cameras for printer {}", printer.uuid))?;
            let camera = cams
                .first()
                .context("no cameras visible on this printer")?;
            tracing::info!(
                camera.id = camera.id,
                camera.name = camera.name.as_deref().unwrap_or("(unnamed)"),
                "selected camera",
            );

            let webrtc_cfg = fetch_webrtc_config(&prusa, &token)
                .await
                .context("fetch webrtc config")?;
            tracing::info!(
                ice_server_count = webrtc_cfg.ice_servers.len(),
                "fetched webrtc config"
            );

            let signaling =
                PrusaSignaling::connect(camera.token.clone(), token.clone(), webrtc_cfg.clone())
                    .await
                    .context("connect signaling")?;

            let (signal_tx, signal_rx) = mpsc::channel(32);
            let (rtp_tx, mut rtp_rx) = mpsc::channel(1024);
            let session_id = signaling.session_id.clone();
            let session = Arc::new(
                WebRtcSession::new(
                    &webrtc_cfg,
                    camera.token.clone(),
                    session_id,
                    signal_tx.clone(),
                    rtp_tx,
                )
                .await
                .context("build webrtc session")?,
            );

            // RTP packet counter: logs every 5 seconds.
            let counter = tokio::spawn(async move {
                let mut packets: u64 = 0;
                let mut bytes: u64 = 0;
                let mut tick = tokio::time::interval(Duration::from_secs(5));
                tick.tick().await; // skip immediate first tick
                loop {
                    tokio::select! {
                        pkt = rtp_rx.recv() => {
                            match pkt {
                                Some(p) => {
                                    packets += 1;
                                    bytes += p.payload.len() as u64;
                                }
                                None => break,
                            }
                        }
                        _ = tick.tick() => {
                            tracing::info!(packets, bytes, "rtp stats");
                        }
                    }
                }
                tracing::info!(packets, bytes, "rtp final");
            });

            let driver = {
                let s = session.clone();
                tokio::spawn(async move {
                    run_session(signaling, &*s, signal_tx, signal_rx).await;
                })
            };

            // Run for the requested duration or until Ctrl+C / driver exit.
            let timeout = tokio::time::sleep(Duration::from_secs(duration_seconds));
            tokio::pin!(timeout);
            tokio::select! {
                _ = &mut timeout => {
                    tracing::info!("duration reached, shutting down");
                }
                _ = tokio::signal::ctrl_c() => {
                    tracing::info!("ctrl+c, shutting down");
                }
                _ = driver => {
                    tracing::info!("session driver finished early");
                }
            }
            let _ = session.close().await;
            counter.abort();
        }
    }
    Ok(())
}
