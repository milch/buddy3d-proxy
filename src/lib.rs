pub mod backoff;
pub mod config;
pub mod jwt;
pub mod pkce;
pub mod prusa;
pub mod proto;
pub mod rate_limit;
pub mod rtsp;
pub mod supervisor;
pub mod token_store;
pub mod tracing_redact;
pub mod webrtc_session;

/// Initialize the tracing subscriber.
///
/// Auto-picks a format based on whether stdout is a TTY:
/// - **TTY (interactive)**: pretty/compact human-readable lines with colors.
/// - **Non-TTY (piped, redirected, Docker, systemd)**: structured JSON.
///
/// Override the level filter via `RUST_LOG`. Default is
/// `info,buddy3d_proxy=debug,webrtc_srtp::session=warn` — that last entry
/// silences the per-packet `srtp ssrc=N index=M: duplicated` INFO spam
/// emitted while a session sits in the warm-idle window after the last
/// viewer disconnects.
pub fn init_tracing() {
    use std::io::IsTerminal;
    use tracing_subscriber::{fmt, EnvFilter};
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        EnvFilter::new("info,buddy3d_proxy=debug,webrtc_srtp::session=warn")
    });
    if std::io::stdout().is_terminal() {
        fmt()
            .with_env_filter(filter)
            .with_target(true)
            .with_ansi(true)
            .compact()
            .init();
    } else {
        fmt()
            .json()
            .with_env_filter(filter)
            .with_target(true)
            .init();
    }
}
