//! 60-second periodic metrics line. Single `tracing::info!` per tick;
//! field names match the contract in the design doc.

use crate::rate_limit::RateLimiter;
use crate::supervisor::Supervisor;
use std::sync::Arc;
use std::time::Duration;

pub async fn run(
    camera_name: String,
    supervisor: Arc<Supervisor>,
    limiter: Arc<RateLimiter>,
    interval: Duration,
) {
    let mut tick = tokio::time::interval(interval);
    tick.tick().await; // discard immediate first tick
    loop {
        tick.tick().await;
        let snap = supervisor.snapshot().await;
        let bucket_now = limiter.tokens().await;
        let bucket_cap = limiter.cap();
        tracing::info!(
            camera = %camera_name,
            state = ?snap.state,
            viewers = snap.viewers,
            wss_reconnects_total = snap.wss_reconnects_total,
            webrtc_renegotiations_total = snap.webrtc_renegotiations_total,
            prusa_5xx_bucket_now = bucket_now,
            prusa_5xx_bucket_cap = bucket_cap,
            last_error_age_s = snap.last_error_age_secs,
            session_uptime_s = snap.session_uptime_secs,
            "metrics"
        );
    }
}
