//! Real `StreamFactory` impl that wires WebRTC + signaling + auth.
//!
//! Each `connect()` brings up a fresh end-to-end session and spawns a
//! background task that keeps the broadcast channel populated until the
//! returned `StopHandle` is dropped.

use crate::prusa::api::{fetch_webrtc_config, Camera};
use crate::prusa::auth::AuthOrchestrator;
use crate::prusa::client::PrusaClient;
use crate::prusa::signaling::PrusaSignaling;
use crate::rtsp::sdp::{extract_h264_params, H264Params};
use crate::rtsp::server::SourceError;
use crate::supervisor::{StopHandle, StreamFactory};
use crate::webrtc_session::{run_session, WebRtcSession};
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc, oneshot};
use webrtc::rtp::packet::Packet as RtpPacket;

/// Upper bound on `RTCPeerConnection::close()` so a wedged close can't pin
/// the driver task forever.
const PC_CLOSE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

pub struct WebRtcFactory {
    pub orch: Arc<AuthOrchestrator>,
    pub prusa: PrusaClient,
    pub camera: Camera,
    /// Shared registry that the MQTT command dispatcher reads to send commands
    /// over the live signaling channel without bringing up a transient connection.
    /// Populated on every successful connect; cleared on session tear-down.
    pub live_outbound: crate::live_outbound::LiveOutbound,
    /// Long-lived watch carrying the camera's current `Status` event. Updated
    /// on every session bring-up so the MQTT subsystem can publish the
    /// camera's actual current mode + quality (vs. assuming defaults).
    pub camera_status: Arc<crate::live_outbound::CameraStatusWatch>,
}

#[async_trait::async_trait]
impl StreamFactory for WebRtcFactory {
    async fn connect(
        &self,
        rtp_tx: broadcast::Sender<RtpPacket>,
    ) -> Result<(H264Params, StopHandle, crate::supervisor::SessionEnded), SourceError> {
        let token = self
            .orch
            .access_token()
            .await
            .map_err(|e| SourceError::Unavailable(format!("auth: {e}")))?;

        let webrtc_cfg = fetch_webrtc_config(&self.prusa, &token)
            .await
            .map_err(|e| SourceError::Unavailable(format!("webrtc-config: {e}")))?;

        let signaling = PrusaSignaling::connect_with_status_sink(
            self.camera.token.clone(),
            token.clone(),
            webrtc_cfg.clone(),
            Some(self.camera_status.clone()),
        )
        .await
        .map_err(|e| SourceError::Unavailable(format!("signaling: {e}")))?;

        let sid = signaling.session_id.clone();
        let (signal_tx, signal_rx) = mpsc::channel(32);
        let (rtp_internal_tx, mut rtp_internal_rx) = mpsc::channel::<RtpPacket>(1024);

        let (session, mut pc_terminated_rx) = WebRtcSession::new(
            &webrtc_cfg,
            self.camera.token.clone(),
            sid,
            signal_tx.clone(),
            rtp_internal_tx,
        )
        .await
        .map_err(|e| SourceError::Unavailable(format!("session: {e}")))?;
        let session = Arc::new(session);

        let pc = session.peer_connection();
        let driver_session = session.clone();
        let (kill_tx, mut kill_rx) = oneshot::channel::<()>();

        // Stash the live outbound for the MQTT command dispatcher. Cleared
        // on session tear-down via Joiner::drop.
        {
            let mut guard = self.live_outbound.lock().await;
            *guard = Some(signaling.outbound.clone());
        }

        let (ended_tx, ended_rx) = tokio::sync::oneshot::channel::<()>();

        // Spawn the run_session driver. Stop when kill_rx fires, the
        // signaling channel closes, or the peer connection reaches a
        // terminal state (ICE failure that did NOT close signaling — without
        // this arm the supervisor stays at Streaming indefinitely after a
        // mid-stream ICE drop). Every exit closes the peer connection: its
        // state callback keeps it alive, so an unclosed PC leaks its ICE/DTLS
        // tasks forever.
        tokio::spawn(async move {
            let ended_on_its_own = tokio::select! {
                _ = run_session(signaling, &driver_session, signal_tx, signal_rx) => true,
                _ = &mut pc_terminated_rx => true,
                _ = &mut kill_rx => false,
            };
            match tokio::time::timeout(PC_CLOSE_TIMEOUT, driver_session.close()).await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => tracing::warn!(error = %e, "peer connection close failed"),
                Err(_) => tracing::warn!("peer connection close timed out"),
            }
            if ended_on_its_own {
                let _ = ended_tx.send(());
            }
        });

        // Forward RTP from the per-session mpsc into the broadcast channel.
        let forwarder_handle = tokio::spawn(async move {
            let mut received: u64 = 0;
            let mut delivered: u64 = 0;
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(5));
            tick.tick().await;
            loop {
                tokio::select! {
                    pkt = rtp_internal_rx.recv() => {
                        match pkt {
                            Some(pkt) => {
                                received += 1;
                                if rtp_tx.send(pkt).is_ok() {
                                    delivered += 1;
                                }
                            }
                            None => break,
                        }
                    }
                    _ = tick.tick() => {
                        tracing::debug!(
                            received,
                            delivered,
                            subscribers = rtp_tx.receiver_count(),
                            "rtp forwarder stats"
                        );
                    }
                }
            }
            tracing::info!(received, delivered, "rtp forwarder ended");
        });

        // Teardown for the StopHandle. Built before the SDP wait so an early
        // return below tears the session down too.
        struct Joiner {
            kill: Option<oneshot::Sender<()>>,
            forwarder: tokio::task::JoinHandle<()>,
            live_outbound: crate::live_outbound::LiveOutbound,
        }
        impl Drop for Joiner {
            fn drop(&mut self) {
                // Don't abort the driver: kill lets it close the peer
                // connection on its way out.
                if let Some(tx) = self.kill.take() {
                    let _ = tx.send(());
                }
                self.forwarder.abort();
                // Clear the live outbound registry. The Drop runs synchronously
                // on whatever thread held the StopHandle, so we hand the
                // clear-op to a tokio task to avoid blocking on the Mutex.
                let live = self.live_outbound.clone();
                tokio::spawn(async move {
                    let mut guard = live.lock().await;
                    *guard = None;
                });
            }
        }
        let joiner = Joiner {
            kill: Some(kill_tx),
            forwarder: forwarder_handle,
            live_outbound: self.live_outbound.clone(),
        };

        // Poll for the negotiated remote SDP — `handle_signal` calls
        // `set_remote_description` on the SDP-offer event, so we wait up to
        // 15s for it to appear.
        let mut h264 = None;
        for _ in 0..150 {
            if let Some(remote) = pc.remote_description().await {
                if let Some(p) = extract_h264_params(&remote.sdp) {
                    h264 = Some(p);
                    break;
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        let h264 = h264.ok_or_else(|| {
            SourceError::Unavailable("no H.264 params in remote SDP after 15s".into())
        })?;

        Ok((
            h264,
            StopHandle {
                kill: Box::new(joiner),
            },
            ended_rx,
        ))
    }
}
