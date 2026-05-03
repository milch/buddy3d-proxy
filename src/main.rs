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
    /// Run the RTSP proxy. Listens on RTSP_PORT (default 8554) and serves the
    /// camera at rtsp://host:RTSP_PORT/RTSP_PATH. WebRTC stays idle until the
    /// first viewer connects, and is torn down IDLE_TIMEOUT_SECONDS after the
    /// last viewer disconnects.
    Serve,
    /// Tell the camera to reboot. Useful when the camera has degraded its
    /// stream quality (e.g. dropped to 640x480@10fps after many reconnects).
    /// Sends a CameraTrigger with `start_device_reboot=1` (proto field 9,
    /// confirmed against a captured browser-initiated reboot in
    /// api/2025-05-03 14-05.har).
    RestartCamera {
        /// CameraTrigger protobuf field number to set to 1. The default (9)
        /// is `start_device_reboot`. Other observed field names from the JS
        /// bundle: get_snapshot, set_timelapse_enable, snapshot_interval,
        /// send_camera_name — their field numbers are still unknown.
        #[arg(long, default_value_t = 9)]
        field: u32,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // rustls 0.23 (pulled in by webrtc 0.17 + reqwest 0.13) requires an
    // explicit process-level CryptoProvider before any TLS handshake. Install
    // aws-lc-rs once at startup; ignore the result if it's already installed.
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

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
        Cmd::Serve => {
            use buddy3d_proxy::rtsp::Server;
            use buddy3d_proxy::supervisor::webrtc_factory::WebRtcFactory;
            use buddy3d_proxy::supervisor::Supervisor;

            let token = orch.access_token().await.context("acquire access token")?;
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
                .context("no cameras visible on this printer")?
                .clone();

            let camera_name = camera.name.clone().unwrap_or_else(|| "buddy3d".into());
            let rtsp_path = cfg.rtsp_path.clone().unwrap_or_else(|| slugify(&camera_name));
            tracing::info!(
                camera.id = camera.id,
                camera.name = %camera_name,
                rtsp_path = %rtsp_path,
                "selected camera"
            );

            let factory = Arc::new(WebRtcFactory {
                orch: orch.clone(),
                prusa: prusa.clone(),
                camera,
            });
            let supervisor = Supervisor::new(
                factory,
                camera_name,
                rtsp_path.clone(),
                cfg.idle_timeout,
            );

            let _handle = Server::start(&cfg.rtsp_bind_addr, cfg.rtsp_port, supervisor)
                .await
                .context("rtsp server start")?;

            tracing::info!(
                "rtsp ready at rtsp://{}:{}/{}; waiting for clients",
                cfg.rtsp_bind_addr,
                cfg.rtsp_port,
                rtsp_path
            );

            // Run until Ctrl+C.
            tokio::signal::ctrl_c()
                .await
                .context("install ctrl+c handler")?;
            tracing::info!("ctrl+c received; shutting down");
        }
        Cmd::RestartCamera { field } => {
            let token = orch.access_token().await.context("acquire access token")?;
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
                .context("no cameras visible on this printer")?
                .clone();
            tracing::info!(
                camera.id = camera.id,
                camera.name = camera.name.as_deref().unwrap_or("(unnamed)"),
                field,
                "sending restart trigger to camera"
            );

            let webrtc_cfg =
                buddy3d_proxy::prusa::api::fetch_webrtc_config(&prusa, &token)
                    .await
                    .context("fetch webrtc config")?;
            let signaling = PrusaSignaling::connect(
                camera.token.clone(),
                token.clone(),
                webrtc_cfg.clone(),
            )
            .await
            .context("connect signaling")?;

            // Hand-encode a CameraTrigger: { camera_token: <token>, [field]: 1 }
            // Field 11 (token) is known from observed wire frames. The action
            // field number is what we're probing.
            let payload = encode_camera_trigger(field, &camera.token);
            tracing::info!(payload_len = payload.len(), "sending trigger");
            signaling
                .send_trigger(payload)
                .await
                .context("send trigger")?;

            // Give the server a moment to deliver and the camera a moment to act.
            tokio::time::sleep(Duration::from_secs(2)).await;
            tracing::info!("restart trigger sent; camera should reboot in a few seconds");
        }
    }
    Ok(())
}

/// Encode a `CameraTrigger` protobuf with `field_num: 1` and `token` at field 11.
/// Wire format: each `<tag><value>` pair, where tag = (field_num << 3) | wire_type.
/// uint32 is wire type 0 (varint), string is wire type 2 (LEN).
fn encode_camera_trigger(field_num: u32, token: &str) -> Vec<u8> {
    let mut buf = Vec::with_capacity(token.len() + 8);
    // <field_num> uint32 = 1
    let tag = (field_num << 3) | 0;
    encode_varint(&mut buf, tag as u64);
    encode_varint(&mut buf, 1); // value
    // field 11 string = token
    encode_varint(&mut buf, ((11u64) << 3) | 2);
    encode_varint(&mut buf, token.len() as u64);
    buf.extend_from_slice(token.as_bytes());
    buf
}

fn encode_varint(buf: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        buf.push((value as u8) | 0x80);
        value >>= 7;
    }
    buf.push(value as u8);
}

/// Lowercase, replace whitespace + non-alphanumerics with `-`, collapse runs.
fn slugify(name: &str) -> String {
    let mut out = String::with_capacity(name.len());
    let mut last_dash = true;
    for c in name.chars() {
        if c.is_ascii_alphanumeric() {
            out.push(c.to_ascii_lowercase());
            last_dash = false;
        } else if !last_dash {
            out.push('-');
            last_dash = true;
        }
    }
    out.trim_matches('-').to_string()
}
