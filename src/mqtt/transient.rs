//! Brings up a one-shot signaling connection to push a single command, then
//! tears down. Used when no live session is active. Factored out of the
//! `send_camera_configuration` helper that previously lived in `main.rs`.

use crate::prusa::api::{fetch_webrtc_config, list_cameras, list_printers, Camera};
use crate::prusa::auth::{AuthEndpoints, AuthOrchestrator};
use crate::prusa::client::PrusaClient;
use crate::prusa::commands::encode_camera_trigger;
use crate::prusa::signaling::PrusaSignaling;
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, thiserror::Error)]
pub enum TransientError {
    #[error("auth: {0}")]
    Auth(String),
    #[error("api: {0}")]
    Api(String),
    #[error("signaling: {0}")]
    Signaling(String),
}

pub struct TransientSignaler {
    pub orch: Arc<AuthOrchestrator>,
    pub prusa: PrusaClient,
    pub endpoints: AuthEndpoints,
    pub camera: Camera,
}

impl TransientSignaler {
    /// Send one configuration or trigger payload via a fresh signaling
    /// connection. `kind` must be `"configuration"` or `"trigger"`.
    pub async fn send(&self, kind: &str, payload: Vec<u8>) -> Result<(), TransientError> {
        let token = self
            .orch
            .access_token()
            .await
            .map_err(|e| TransientError::Auth(e.to_string()))?;
        let webrtc_cfg = fetch_webrtc_config(&self.prusa, &token)
            .await
            .map_err(|e| TransientError::Api(e.to_string()))?;
        let signaling = PrusaSignaling::connect(
            self.camera.token.clone(),
            token.clone(),
            webrtc_cfg.clone(),
        )
        .await
        .map_err(|e| TransientError::Signaling(e.to_string()))?;

        // Trigger-3 ("subscribe to settings") is required by the server before
        // it accepts any configuration event from a non-WebRTC client.
        let trigger3 = encode_camera_trigger(3, &self.camera.token);
        signaling
            .send_trigger(trigger3)
            .await
            .map_err(|e| TransientError::Signaling(e.to_string()))?;
        tokio::time::sleep(Duration::from_millis(200)).await;

        match kind {
            "configuration" => signaling
                .send_configuration(payload)
                .await
                .map_err(|e| TransientError::Signaling(e.to_string()))?,
            "trigger" => signaling
                .send_trigger(payload)
                .await
                .map_err(|e| TransientError::Signaling(e.to_string()))?,
            other => return Err(TransientError::Signaling(format!("unknown kind {other}"))),
        }

        tokio::time::sleep(Duration::from_secs(1)).await;
        Ok(())
    }
}

/// Refresh the camera reference from the API.
pub async fn lookup_camera(
    orch: &Arc<AuthOrchestrator>,
    prusa: &PrusaClient,
    endpoints: &AuthEndpoints,
) -> Result<Camera, TransientError> {
    let token = orch
        .access_token()
        .await
        .map_err(|e| TransientError::Auth(e.to_string()))?;
    let printers = list_printers(prusa, &endpoints.connect_base, &token)
        .await
        .map_err(|e| TransientError::Api(e.to_string()))?;
    let printer = printers
        .first()
        .ok_or_else(|| TransientError::Api("no printers".into()))?;
    let cams = list_cameras(prusa, &endpoints.connect_base, &token, &printer.uuid)
        .await
        .map_err(|e| TransientError::Api(e.to_string()))?;
    cams.first()
        .cloned()
        .ok_or_else(|| TransientError::Api("no cameras".into()))
}
