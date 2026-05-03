pub mod config;
pub mod jwt;
pub mod pkce;
pub mod prusa;
pub mod proto;
pub mod rate_limit;
pub mod rtsp;
pub mod token_store;
pub mod tracing_redact;
pub mod webrtc_session;

pub fn init_tracing() {
    use tracing_subscriber::{EnvFilter, fmt};
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,buddy3d_proxy=debug"));
    fmt()
        .json()
        .with_env_filter(filter)
        .with_target(true)
        .init();
}
