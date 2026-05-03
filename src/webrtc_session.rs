//! WebRTC PeerConnection lifecycle, configured as the answerer.
//!
//! Receives SDP/ICE from the signaling channel via `handle_signal` (Task 8),
//! emits outgoing SDP/ICE via the `outbound_signal_tx` channel, and pushes
//! received RTP packets onto `rtp_tx` for downstream consumption.

use crate::prusa::api::WebRtcConfig;
use crate::proto;
use std::sync::Arc;
use tokio::sync::mpsc;
use webrtc::api::interceptor_registry::register_default_interceptors;
use webrtc::api::media_engine::MediaEngine;
use webrtc::api::APIBuilder;
use webrtc::ice_transport::ice_server::RTCIceServer;
use webrtc::interceptor::registry::Registry;
use webrtc::peer_connection::configuration::RTCConfiguration;
use webrtc::peer_connection::RTCPeerConnection;
use webrtc::rtp::packet::Packet as RtpPacket;

#[derive(Debug, thiserror::Error)]
pub enum SessionError {
    #[error("webrtc error: {0}")]
    WebRtc(#[from] webrtc::Error),
    #[error("send channel closed")]
    SendClosed,
}

pub struct WebRtcSession {
    pc: Arc<RTCPeerConnection>,
}

impl WebRtcSession {
    /// Build the PeerConnection with ICE servers from the Prusa config.
    /// `outbound_signal_tx` receives SDP/ICE messages we want to send back to Prusa.
    /// `rtp_tx` receives every inbound RTP packet on the negotiated video track.
    pub async fn new(
        cfg: &WebRtcConfig,
        outbound_signal_tx: mpsc::Sender<proto::WebRtcSignal>,
        rtp_tx: mpsc::Sender<RtpPacket>,
    ) -> Result<Self, SessionError> {
        let mut media_engine = MediaEngine::default();
        media_engine.register_default_codecs()?;

        let mut registry = Registry::new();
        registry = register_default_interceptors(registry, &mut media_engine)?;

        let api = APIBuilder::new()
            .with_media_engine(media_engine)
            .with_interceptor_registry(registry)
            .build();

        let ice_servers: Vec<RTCIceServer> = cfg
            .ice_servers
            .iter()
            .map(|s| RTCIceServer {
                urls: s.url_list(),
                username: s.username.clone().unwrap_or_default(),
                credential: s.credential.clone().unwrap_or_default(),
                ..Default::default()
            })
            .collect();

        let configuration = RTCConfiguration {
            ice_servers,
            ..Default::default()
        };

        let pc = Arc::new(api.new_peer_connection(configuration).await?);

        // Register ICE candidate callback → forward to outbound channel.
        let candidate_tx = outbound_signal_tx.clone();
        pc.on_ice_candidate(Box::new(move |c| {
            let candidate_tx = candidate_tx.clone();
            Box::pin(async move {
                if let Some(c) = c {
                    if let Ok(init) = c.to_json() {
                        let signal = proto::WebRtcSignal {
                            msg_type: 4, // ICE candidate from client
                            direction: 2,
                            body: encode_ice_candidate(
                                &init.candidate,
                                &init.sdp_mid.unwrap_or_default(),
                            ),
                            ..Default::default()
                        };
                        let _ = candidate_tx.send(signal).await;
                    }
                }
            })
        }));

        // OnTrack: every RTP packet arriving on the negotiated track gets pushed.
        let rtp_tx_clone = rtp_tx.clone();
        pc.on_track(Box::new(move |track, _receiver, _transceiver| {
            let rtp_tx = rtp_tx_clone.clone();
            Box::pin(async move {
                tracing::info!(
                    codec = %track.codec().capability.mime_type,
                    track_id = %track.id(),
                    "received track from peer",
                );
                tokio::spawn(async move {
                    loop {
                        match track.read_rtp().await {
                            Ok((pkt, _attr)) => {
                                if rtp_tx.send(pkt).await.is_err() {
                                    return;
                                }
                            }
                            Err(e) => {
                                tracing::debug!(error = %e, "track read_rtp ended");
                                return;
                            }
                        }
                    }
                });
            })
        }));

        Ok(Self { pc })
    }

    /// Returns a clone of the underlying PC for tasks that need direct access
    /// (SDP and ICE handlers in subsequent tasks).
    pub fn peer_connection(&self) -> Arc<RTCPeerConnection> {
        self.pc.clone()
    }

    pub async fn close(&self) -> Result<(), SessionError> {
        self.pc.close().await?;
        Ok(())
    }
}

fn encode_ice_candidate(candidate_line: &str, mid: &str) -> Vec<u8> {
    use prost::Message;
    let body = proto::IceCandidateBody {
        candidate: format!("a={candidate_line}"),
        stream_id: mid.to_string(),
    };
    let mut buf = Vec::new();
    body.encode(&mut buf).expect("encode never fails");
    buf
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_ice_candidate_prepends_attribute_marker() {
        let buf = encode_ice_candidate(
            "candidate:1 1 UDP 2122317823 10.0.0.1 36803 typ host",
            "0",
        );
        use prost::Message;
        let body = proto::IceCandidateBody::decode(buf.as_slice()).unwrap();
        assert!(body.candidate.starts_with("a=candidate:"));
        assert_eq!(body.stream_id, "0");
    }
}
