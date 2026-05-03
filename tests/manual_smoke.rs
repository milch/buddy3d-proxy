//! Hits the *real* Prusa Connect API. Run with:
//!
//!     PRUSA_EMAIL=... PRUSA_PASSWORD=... TOKEN_STORE_PATH=/tmp/tokens.json \
//!         cargo test --test manual_smoke -- --ignored --nocapture
//!
//! Or via the justfile recipe:
//!
//!     PRUSA_EMAIL=... PRUSA_PASSWORD=... just smoke
//!
//! Verifies: bootstrap, refresh-on-second-run, list_printers/list_cameras.

use std::sync::Arc;
use std::time::Duration;

use buddy3d_proxy::prusa::api::{list_cameras, list_printers};
use buddy3d_proxy::prusa::auth::{AuthEndpoints, AuthOrchestrator};
use buddy3d_proxy::prusa::client::PrusaClient;
use buddy3d_proxy::rate_limit::RateLimiter;
use buddy3d_proxy::token_store::TokenStore;

#[tokio::test]
#[ignore]
async fn real_prusa_account_smoke() {
    let email = std::env::var("PRUSA_EMAIL").expect("PRUSA_EMAIL");
    let password = std::env::var("PRUSA_PASSWORD").expect("PRUSA_PASSWORD");
    let token_path = std::env::var("TOKEN_STORE_PATH")
        .unwrap_or_else(|_| "/tmp/buddy3d-tokens.json".into());

    let limiter = Arc::new(RateLimiter::new(3, Duration::from_secs(60)));
    let http = reqwest::Client::builder().cookie_store(true).build().unwrap();
    let prusa = PrusaClient::new(http, limiter);
    let endpoints = AuthEndpoints::default();
    let store = TokenStore::new(&token_path);
    let orch = Arc::new(AuthOrchestrator::new(
        prusa.clone(),
        endpoints.clone(),
        store,
        email,
        password,
    ));

    let token = orch.access_token().await.expect("auth token");
    let printers = list_printers(&prusa, &endpoints.connect_base, &token)
        .await
        .unwrap();
    assert!(!printers.is_empty(), "expected at least one printer on this account");
    for p in &printers {
        let cams = list_cameras(&prusa, &endpoints.connect_base, &token, &p.uuid)
            .await
            .unwrap();
        eprintln!("printer {} ({}) has {} cameras", p.uuid, p.name, cams.len());
        for c in cams {
            eprintln!("  camera {} {:?}", c.id, c.name);
        }
    }
}
