//! Tiny HTTP server exposing `GET /healthz`.
//!
//! Returns 200 unless the auth orchestrator has latched its `failed` watch,
//! in which case 503. Transient streaming failures and rate-limit waits do
//! NOT produce 503 — the caller must restart the process to recover from
//! a hard auth failure (wrong password, account locked).

use axum::{extract::State, http::StatusCode, response::IntoResponse, routing::get, Router};
use std::net::SocketAddr;
use tokio::sync::watch;

#[derive(Clone)]
struct AppState {
    failed_rx: watch::Receiver<bool>,
}

async fn healthz(State(state): State<AppState>) -> impl IntoResponse {
    if *state.failed_rx.borrow() {
        (StatusCode::SERVICE_UNAVAILABLE, "auth_failed\n")
    } else {
        (StatusCode::OK, "ok\n")
    }
}

/// Bind on `addr` and serve `/healthz`. Runs forever; spawn with `tokio::spawn`.
pub async fn serve(addr: SocketAddr, failed_rx: watch::Receiver<bool>) -> std::io::Result<()> {
    let app = Router::new()
        .route("/healthz", get(healthz))
        .with_state(AppState { failed_rx });
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(%addr, "healthz server listening");
    axum::serve(listener, app)
        .await
        .map_err(|e| std::io::Error::other(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn healthz_returns_ok_when_not_failed() {
        let (_tx, rx) = watch::channel(false);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = Router::new()
            .route("/healthz", get(healthz))
            .with_state(AppState { failed_rx: rx });
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let resp = reqwest::get(format!("http://{addr}/healthz")).await.unwrap();
        assert_eq!(resp.status(), 200);
        assert_eq!(resp.text().await.unwrap(), "ok\n");
    }

    #[tokio::test]
    async fn healthz_returns_503_when_failed() {
        let (tx, rx) = watch::channel(false);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = Router::new()
            .route("/healthz", get(healthz))
            .with_state(AppState { failed_rx: rx });
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        tx.send(true).unwrap();
        let resp = reqwest::get(format!("http://{addr}/healthz")).await.unwrap();
        assert_eq!(resp.status(), 503);
    }
}
