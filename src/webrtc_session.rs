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
    #[error("protobuf decode error: {0}")]
    Decode(String),
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

    /// Process an inbound `WebRtcSignal` from the signaling server. Handles
    /// SDP offers (responding with an answer) and ICE candidates.
    pub async fn handle_signal(
        &self,
        signal: &proto::WebRtcSignal,
        outbound_signal_tx: &mpsc::Sender<proto::WebRtcSignal>,
    ) -> Result<(), SessionError> {
        use prost::Message;
        match signal.msg_type {
            // 1 = SDP offer from server.
            1 => {
                let body = proto::SdpBody::decode(signal.body.as_slice()).map_err(|e| {
                    SessionError::Decode(format!("decode sdp: {e}"))
                })?;
                tracing::debug!(sdp_len = body.sdp.len(), mid = %body.mid, "received SDP offer");

                let offer =
                    webrtc::peer_connection::sdp::session_description::RTCSessionDescription::offer(
                        body.sdp,
                    )?;
                self.pc.set_remote_description(offer).await?;

                let answer = self.pc.create_answer(None).await?;
                self.pc.set_local_description(answer.clone()).await?;

                let mut answer_buf = Vec::new();
                proto::SdpBody {
                    sdp: answer.sdp,
                    mid: body.mid.clone(),
                }
                .encode(&mut answer_buf)
                .expect("encode never fails");

                let reply = proto::WebRtcSignal {
                    token: signal.token.clone(),
                    session_id: signal.session_id.clone(),
                    peer_id: signal.peer_id.clone(),
                    msg_type: 2, // SDP answer from client
                    direction: 2,
                    body: answer_buf,
                    ..Default::default()
                };
                outbound_signal_tx
                    .send(reply)
                    .await
                    .map_err(|_| SessionError::SendClosed)?;
            }
            // 4 = ICE candidate from server.
            4 => {
                let body = proto::IceCandidateBody::decode(signal.body.as_slice()).map_err(|e| {
                    SessionError::Decode(format!("decode ice: {e}"))
                })?;
                let candidate_line = body
                    .candidate
                    .strip_prefix("a=")
                    .unwrap_or(&body.candidate)
                    .to_string();
                let init = webrtc::ice_transport::ice_candidate::RTCIceCandidateInit {
                    candidate: candidate_line,
                    sdp_mid: Some(body.stream_id.clone()),
                    sdp_mline_index: Some(0),
                    username_fragment: None,
                };
                self.pc.add_ice_candidate(init).await?;
            }
            other => {
                tracing::debug!(msg_type = other, "ignoring webrtc signal");
            }
        }
        Ok(())
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
