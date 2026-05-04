//! Supervisor state → MQTT state string.
//!
//! Pure function + a small watcher task that publishes only on transitions
//! (no spammy steady-state republishes).

use crate::supervisor::{State, Supervisor};
use std::sync::Arc;
use tokio::sync::watch;

/// MQTT state values published to the `state` topic. `auth_failed` takes
/// precedence over the supervisor's own state when the auth orchestrator
/// has latched its failure sentinel.
pub fn translate(state: State, failed: bool) -> &'static str {
    if failed {
        return "auth_failed";
    }
    match state {
        State::Idle => "idle",
        State::Connecting => "connecting",
        State::Streaming => "streaming",
    }
}

/// Spawned task: watches supervisor + failed_watch and calls `publish` with
/// the MQTT state string on every transition. The first call publishes the
/// initial state. Returns when either watch closes.
pub async fn run_watcher(
    supervisor: Arc<Supervisor>,
    failed_watch: Option<watch::Receiver<bool>>,
    mut publish: impl FnMut(&'static str) + Send + 'static,
) {
    let mut state_rx = supervisor.state_changes();
    let mut failed_rx = failed_watch;

    let initial = {
        let snap = supervisor.snapshot().await;
        let failed = failed_rx.as_ref().map(|r| *r.borrow()).unwrap_or(false);
        translate(snap.state, failed)
    };
    let mut last = initial;
    publish(initial);

    loop {
        tokio::select! {
            res = state_rx.changed() => {
                if res.is_err() { return; }
                let snap = supervisor.snapshot().await;
                let failed = failed_rx.as_ref().map(|r| *r.borrow()).unwrap_or(false);
                let now = translate(snap.state, failed);
                if now != last {
                    publish(now);
                    last = now;
                }
            }
            res = async {
                match failed_rx.as_mut() {
                    Some(rx) => rx.changed().await,
                    None => std::future::pending().await,
                }
            } => {
                if res.is_err() { return; }
                let snap = supervisor.snapshot().await;
                let failed = failed_rx.as_ref().map(|r| *r.borrow()).unwrap_or(false);
                let now = translate(snap.state, failed);
                if now != last {
                    publish(now);
                    last = now;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auth_failed_takes_precedence() {
        assert_eq!(translate(State::Streaming, true), "auth_failed");
        assert_eq!(translate(State::Idle, true), "auth_failed");
        assert_eq!(translate(State::Connecting, true), "auth_failed");
    }

    #[test]
    fn supervisor_state_maps_directly_when_not_failed() {
        assert_eq!(translate(State::Idle, false), "idle");
        assert_eq!(translate(State::Connecting, false), "connecting");
        assert_eq!(translate(State::Streaming, false), "streaming");
    }
}
