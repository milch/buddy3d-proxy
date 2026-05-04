//! Real `StreamFactory` impl that wires WebRTC + signaling + auth.
//!
//! Each `connect()` brings up a fresh end-to-end session and spawns a
//! background task that keeps the broadcast channel populated until the
//! returned `StopHandle` is dropped.

use crate::prusa::api::{fetch_webrtc_config, Camera};
use crate::prusa::auth::AuthOrchestrator;
use crate::prusa::client::PrusaClient;
use crate::prusa::commands::encode_set_quality;
use crate::prusa::signaling::client::Outbound;
use crate::prusa::signaling::PrusaSignaling;
use crate::rtsp::sdp::{extract_h264_params, H264Params};
use crate::rtsp::server::SourceError;
use crate::supervisor::{StopHandle, StreamFactory};
use crate::webrtc_session::{run_session, WebRtcSession};
use bytes::Bytes;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, mpsc, oneshot};
use webrtc::peer_connection::peer_connection_state::RTCPeerConnectionState;
use webrtc::rtp::packet::Packet as RtpPacket;

pub struct WebRtcFactory {
    pub orch: Arc<AuthOrchestrator>,
    pub prusa: PrusaClient,
    pub camera: Camera,
    /// Shared registry that the MQTT command dispatcher reads to send commands
    /// over the live signaling channel without bringing up a transient connection.
    /// Populated on every successful connect; cleared on session tear-down.
    pub live_outbound: crate::live_outbound::LiveOutbound,
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

        let signaling = PrusaSignaling::connect(
            self.camera.token.clone(),
            token.clone(),
            webrtc_cfg.clone(),
        )
        .await
        .map_err(|e| SourceError::Unavailable(format!("signaling: {e}")))?;

        let sid = signaling.session_id.clone();
        let (signal_tx, signal_rx) = mpsc::channel(32);
        let (rtp_internal_tx, mut rtp_internal_rx) = mpsc::channel::<RtpPacket>(1024);

        let session = Arc::new(
            WebRtcSession::new(
                &webrtc_cfg,
                self.camera.token.clone(),
                sid,
                signal_tx.clone(),
                rtp_internal_tx,
            )
            .await
            .map_err(|e| SourceError::Unavailable(format!("session: {e}")))?,
        );

        let pc = session.peer_connection();
        let driver_session = session.clone();
        let (kill_tx, mut kill_rx) = oneshot::channel::<()>();

        // Clone the signaling outbound channel BEFORE moving signaling into
        // the driver task. We use it later to push the auto-FHD configuration
        // once WebRTC is fully up.
        let outbound_for_post_connect = signaling.outbound.clone();

        // Stash the live outbound for the MQTT command dispatcher. Cleared
        // on session tear-down via Joiner::drop.
        {
            let mut guard = self.live_outbound.lock().await;
            *guard = Some(signaling.outbound.clone());
        }

        let camera_token_for_post_connect = self.camera.token.clone();
        let pc_for_post_connect = pc.clone();

        let (ended_tx, ended_rx) = tokio::sync::oneshot::channel::<()>();

        // Spawn the run_session driver. Stop when kill_rx fires or the
        // signaling channel closes.
        let driver_handle = tokio::spawn(async move {
            tokio::select! {
                _ = run_session(signaling, &driver_session, signal_tx, signal_rx) => {
                    let _ = ended_tx.send(());
                }
                _ = &mut kill_rx => {
                    let _ = driver_session.close().await;
                }
            }
        });

        // Once the peer connection reaches Connected, send the camera-side
        // commands that restore full 1080p quality. After many WebRTC
        // reconnects the camera silently degrades to 640x480@10fps until
        // told otherwise.
        tokio::spawn(async move {
            let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
            loop {
                if pc_for_post_connect.connection_state()
                    == RTCPeerConnectionState::Connected
                {
                    break;
                }
                if tokio::time::Instant::now() > deadline {
                    tracing::warn!(
                        "skipping auto set-quality FHD: peer never reached Connected"
                    );
                    return;
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            // Small grace period so the camera has finished setting up.
            tokio::time::sleep(Duration::from_millis(500)).await;

            let payload = encode_set_quality(3, &camera_token_for_post_connect);
            if outbound_for_post_connect
                .send(Outbound::BinaryEvent {
                    name: "configuration".into(),
                    payload: Bytes::from(payload),
                    expect_ack: false,
                })
                .await
                .is_ok()
            {
                tracing::info!("auto-applied set-quality=FHD after WebRTC connect");
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

        // Bundle both task handles into the StopHandle. Drop = abort = teardown.
        struct Joiner {
            kill: Option<oneshot::Sender<()>>,
            forwarder: tokio::task::JoinHandle<()>,
            driver: tokio::task::JoinHandle<()>,
            live_outbound: crate::live_outbound::LiveOutbound,
        }
        impl Drop for Joiner {
            fn drop(&mut self) {
                if let Some(tx) = self.kill.take() {
                    let _ = tx.send(());
                }
                self.forwarder.abort();
                self.driver.abort();
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
            driver: driver_handle,
            live_outbound: self.live_outbound.clone(),
        };

        Ok((
            h264,
            StopHandle {
                kill: Box::new(joiner),
            },
            ended_rx,
        ))
    }
}
