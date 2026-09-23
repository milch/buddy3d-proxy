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

        let (ended_tx, mut ended_rx) = tokio::sync::oneshot::channel::<()>();

        // Spawn the run_session driver. Stop when kill_rx fires, the
        // signaling channel closes, or the peer connection reaches a
        // terminal state (ICE failure that did NOT close signaling — without
        // this arm the supervisor stays at Streaming indefinitely after a
        // mid-stream ICE drop). Every exit closes the peer connection so its
        // internal ICE/DTLS tasks are stopped too.
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

        let h264 = wait_for_ready(&pc, &mut ended_rx).await?;

        Ok((
            h264,
            StopHandle {
                kill: Box::new(joiner),
            },
            ended_rx,
        ))
    }
}

async fn wait_for_ready(
    pc: &webrtc::peer_connection::RTCPeerConnection,
    ended: &mut crate::supervisor::SessionEnded,
) -> Result<H264Params, SourceError> {
    use webrtc::peer_connection::peer_connection_state::RTCPeerConnectionState;

    let ready = async {
        // A remote offer survives close() and can arrive before ICE/DTLS fails.
        // Only a connected peer with negotiated H.264 earns a successful attempt.
        loop {
            let remote = pc.remote_description().await;
            match pc.connection_state() {
                RTCPeerConnectionState::Failed | RTCPeerConnectionState::Closed => {
                    return Err(SourceError::Unavailable(
                        "peer connection failed before WebRTC was ready".into(),
                    ));
                }
                RTCPeerConnectionState::Connected => {
                    if let Some(remote) = remote {
                        if let Some(params) = extract_h264_params(&remote.sdp) {
                            return Ok(params);
                        }
                    }
                }
                _ => {}
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    };
    tokio::select! {
        biased;
        _ = ended => Err(SourceError::Unavailable("session ended before WebRTC was ready".into())),
        result = tokio::time::timeout(std::time::Duration::from_secs(15), ready) => {
            result.unwrap_or_else(|_| Err(SourceError::Unavailable(
                "WebRTC did not connect with H.264 within 15s".into(),
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rtsp::server::StreamSource;
    use webrtc::api::{media_engine::MediaEngine, APIBuilder};
    use webrtc::peer_connection::{
        configuration::RTCConfiguration, sdp::session_description::RTCSessionDescription,
        RTCPeerConnection,
    };

    const H264_OFFER: &str = concat!(
        "v=0\r\no=- 123 1 IN IP4 127.0.0.1\r\ns=-\r\nt=0 0\r\n",
        "a=group:BUNDLE 0\r\nm=video 9 UDP/TLS/RTP/SAVPF 96\r\nc=IN IP4 0.0.0.0\r\n",
        "a=mid:0\r\na=sendonly\r\na=rtcp-mux\r\na=ice-ufrag:abcd\r\n",
        "a=ice-pwd:abcdefghijklmnopqrstuvwxyz\r\n",
        "a=fingerprint:sha-256 00:11:22:33:44:55:66:77:88:99:AA:BB:CC:DD:EE:FF:",
        "00:11:22:33:44:55:66:77:88:99:AA:BB:CC:DD:EE:FF\r\na=setup:actpass\r\n",
        "a=rtpmap:96 H264/90000\r\na=fmtp:96 packetization-mode=1;profile-level-id=42e01f\r\n",
    );

    async fn peer_with_offer() -> RTCPeerConnection {
        let mut media = MediaEngine::default();
        media.register_default_codecs().unwrap();
        let pc = APIBuilder::new()
            .with_media_engine(media)
            .build()
            .new_peer_connection(RTCConfiguration::default())
            .await
            .unwrap();
        pc.set_remote_description(RTCSessionDescription::offer(H264_OFFER.into()).unwrap())
            .await
            .unwrap();
        pc
    }

    struct ReadinessFactory(Arc<RTCPeerConnection>);

    #[async_trait::async_trait]
    impl StreamFactory for ReadinessFactory {
        async fn connect(
            &self,
            _rtp: broadcast::Sender<RtpPacket>,
        ) -> Result<(H264Params, StopHandle, crate::supervisor::SessionEnded), SourceError>
        {
            let (tx, mut ended) = oneshot::channel();
            let params = wait_for_ready(&self.0, &mut ended).await?;
            Ok((params, StopHandle { kill: Box::new(tx) }, ended))
        }
    }

    #[tokio::test]
    async fn closed_peer_with_cached_sdp_is_not_a_successful_connect() {
        let pc = peer_with_offer().await;
        pc.close().await.unwrap();
        assert!(pc.remote_description().await.is_some());
        let (_tx, mut ended) = oneshot::channel();
        assert!(
            wait_for_ready(&pc, &mut ended).await.is_err(),
            "closed peer earned a successful connect"
        );
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn sdp_without_a_connected_peer_times_out() {
        let pc = Arc::new(peer_with_offer().await);
        let sup = crate::supervisor::Supervisor::new(
            Arc::new(ReadinessFactory(pc.clone())),
            "cam".into(),
            "cam".into(),
            std::time::Duration::from_secs(60),
            None,
        );
        let result = sup.subscribe().await;
        pc.close().await.unwrap();
        assert!(result.is_err(), "SDP alone earned a successful connect");
        assert_eq!(
            sup.snapshot().await.reconnect_tokens,
            9,
            "unconnected peer did not spend a retry token"
        );
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn ended_session_interrupts_readiness_wait() {
        let pc = peer_with_offer().await;
        let (tx, mut ended) = oneshot::channel();
        tx.send(()).unwrap();
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            wait_for_ready(&pc, &mut ended),
        )
        .await;
        pc.close().await.unwrap();
        assert!(result.expect("readiness ignored session end").is_err());
    }

    #[tokio::test]
    async fn connected_h264_peer_is_ready_and_earns_a_token() {
        use webrtc::api::setting_engine::SettingEngine;
        use webrtc::rtp_transceiver::rtp_codec::RTPCodecType;
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let mut media = MediaEngine::default();
        media.register_default_codecs().unwrap();
        let mut settings = SettingEngine::default();
        settings.set_include_loopback_candidate(true);
        settings.set_ip_filter(Box::new(|ip| ip.is_loopback()));
        let api = APIBuilder::new()
            .with_media_engine(media)
            .with_setting_engine(settings)
            .build();
        let offerer = api
            .new_peer_connection(RTCConfiguration::default())
            .await
            .unwrap();
        let answerer = Arc::new(
            api.new_peer_connection(RTCConfiguration::default())
                .await
                .unwrap(),
        );
        let connect = async {
            offerer
                .add_transceiver_from_kind(RTPCodecType::Video, None)
                .await
                .unwrap();
            let offer = offerer.create_offer(None).await.unwrap();
            let mut gathered = offerer.gathering_complete_promise().await;
            offerer.set_local_description(offer).await.unwrap();
            gathered.recv().await;
            answerer
                .set_remote_description(offerer.local_description().await.unwrap())
                .await
                .unwrap();
            let answer = answerer.create_answer(None).await.unwrap();
            let mut gathered = answerer.gathering_complete_promise().await;
            answerer.set_local_description(answer).await.unwrap();
            gathered.recv().await;
            offerer
                .set_remote_description(answerer.local_description().await.unwrap())
                .await
                .unwrap();
            let sup = crate::supervisor::Supervisor::new(
                Arc::new(ReadinessFactory(answerer.clone())),
                "cam".into(),
                "cam".into(),
                std::time::Duration::from_secs(60),
                None,
            );
            sup.inner.reconnect_budget.lock().unwrap().record_failure();
            let sub = sup.subscribe().await.unwrap();
            assert_eq!(sup.snapshot().await.reconnect_tokens, 10);
            assert!(!sub.h264.profile_level_id.is_empty());
            // Even connected peers must fail readiness if signaling already ended.
            let (tx, mut ended) = oneshot::channel();
            tx.send(()).unwrap();
            assert!(wait_for_ready(&answerer, &mut ended).await.is_err());
        };
        let result = tokio::time::timeout(std::time::Duration::from_secs(20), connect).await;
        answerer.close().await.unwrap();
        offerer.close().await.unwrap();
        result.expect("local peers did not connect");
    }
}
