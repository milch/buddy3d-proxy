//! WebRTC PeerConnection lifecycle, configured as the answerer.
//!
//! Receives SDP/ICE from the signaling channel via `handle_signal` (Task 8),
//! emits outgoing SDP/ICE via the `outbound_signal_tx` channel, and pushes
//! received RTP packets onto `rtp_tx` for downstream consumption.

use crate::prusa::api::WebRtcConfig;
use crate::prusa::signaling::{PrusaSignaling, SignalingEvent};
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
    camera_token: String,
    /// Our Socket.IO sid, supplied by the server in `40{"sid":"..."}` and
    /// echoed back in every outbound WebRtcSignal as `session_id` (the JS
    /// client calls it `clientSocketId`).
    session_id: String,
}

impl WebRtcSession {
    /// Build the PeerConnection with ICE servers from the Prusa config.
    /// `outbound_signal_tx` receives SDP/ICE messages we want to send back to Prusa.
    /// `rtp_tx` receives every inbound RTP packet on the negotiated video track.
    pub async fn new(
        cfg: &WebRtcConfig,
        camera_token: String,
        session_id: String,
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

        // webrtc-rs 0.8 validates each ICE server entry: TURN entries must have
        // non-empty username AND credential, STUN entries must have empty creds,
        // and `?transport=...` query strings on TURN URLs are not parsed correctly
        // by some webrtc-rs versions. Split mixed entries and strip query strings
        // defensively.
        let ice_servers: Vec<RTCIceServer> = cfg
            .ice_servers
            .iter()
            .flat_map(|s| {
                let username = s.username.clone().unwrap_or_default();
                let credential = s.credential.clone().unwrap_or_default();
                s.url_list()
                    .into_iter()
                    .map(|url| {
                        let is_turn = url.starts_with("turn:") || url.starts_with("turns:");
                        let clean_url = url.split('?').next().unwrap_or(&url).to_string();
                        RTCIceServer {
                            urls: vec![clean_url],
                            username: if is_turn { username.clone() } else { String::new() },
                            credential: if is_turn {
                                credential.clone()
                            } else {
                                String::new()
                            },
                            ..Default::default()
                        }
                    })
                    .collect::<Vec<_>>()
            })
            .collect();
        for s in &ice_servers {
            tracing::debug!(
                urls = ?s.urls,
                has_creds = !s.username.is_empty(),
                "ice server"
            );
        }

        let configuration = RTCConfiguration {
            ice_servers,
            ..Default::default()
        };

        let pc = Arc::new(api.new_peer_connection(configuration).await?);

        // Register ICE candidate callback → forward to outbound channel.
        let candidate_tx = outbound_signal_tx.clone();
        let candidate_token = camera_token.clone();
        let candidate_sid = session_id.clone();
        pc.on_ice_candidate(Box::new(move |c| {
            let candidate_tx = candidate_tx.clone();
            let candidate_token = candidate_token.clone();
            let candidate_sid = candidate_sid.clone();
            Box::pin(async move {
                if let Some(c) = c {
                    if let Ok(init) = c.to_json() {
                        let signal = proto::WebRtcSignal {
                            token: candidate_token,
                            session_id: candidate_sid,
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

        // When the peer connection reaches Connected, log the selected ICE
        // candidate pair so we can tell whether the media is going LAN-direct
        // (host), via STUN-discovered public IPs (srflx), or relayed through
        // a TURN server (relay). For Prusa Connect over the internet, we
        // expect srflx in the common case and relay when symmetric NAT/CGNAT
        // forces it. host means we're on the same LAN as the camera.
        let pc_for_state = pc.clone();
        pc.on_peer_connection_state_change(Box::new(move |state| {
            let pc = pc_for_state.clone();
            Box::pin(async move {
                use webrtc::peer_connection::peer_connection_state::RTCPeerConnectionState;
                if state != RTCPeerConnectionState::Connected {
                    return;
                }
                let pair = pc.sctp().transport().ice_transport().get_selected_candidate_pair().await;
                if let Some(pair) = pair {
                    let local_kind = describe_candidate_type(pair.local.typ);
                    let remote_kind = describe_candidate_type(pair.remote.typ);
                    let path = match (local_kind, remote_kind) {
                        ("host", "host") => "LAN direct",
                        ("relay", _) | (_, "relay") => "TURN relay",
                        ("srflx", _) | (_, "srflx") => "STUN-aided direct (P2P over NAT)",
                        ("prflx", _) | (_, "prflx") => "peer-reflexive direct",
                        _ => "unknown",
                    };
                    tracing::info!(
                        path = %path,
                        local.kind = %local_kind,
                        local.address = %pair.local.address,
                        local.port = pair.local.port,
                        remote.kind = %remote_kind,
                        remote.address = %pair.remote.address,
                        remote.port = pair.remote.port,
                        "ice connected via {path}",
                    );
                } else {
                    tracing::warn!("peer connected but no selected ice candidate pair");
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

        Ok(Self {
            pc,
            camera_token,
            session_id,
        })
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
        tracing::debug!(
            msg_type = signal.msg_type,
            direction = signal.direction,
            body_len = signal.body.len(),
            has_ice_config = signal.ice_config.is_some(),
            "handle_signal"
        );
        match signal.msg_type {
            // Observed in real traffic:
            //   1 = client → server kick-off (we send, never receive)
            //   2 = client → server SDP answer (we send, never receive)
            //   3 = server → client SDP offer
            //   4 = ICE candidate (bidirectional)
            // We respond to incoming SDP offers (3) and ICE (4).
            3 => {
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
                    // Always populate from our known camera_token + session_id;
                    // inbound may omit them. Prusa rejects without.
                    token: self.camera_token.clone(),
                    session_id: self.session_id.clone(),
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

/// Top-level session driver: takes an authenticated PrusaSignaling and a
/// WebRtcSession, and pumps signaling events into the WebRTC session and
/// outbound signals back out, until something closes.
pub async fn run_session(
    mut signaling: PrusaSignaling,
    session: &WebRtcSession,
    outbound_signal_tx: mpsc::Sender<proto::WebRtcSignal>,
    mut outbound_signal_rx: mpsc::Receiver<proto::WebRtcSignal>,
) {
    loop {
        tokio::select! {
            ev = signaling.events.recv() => {
                match ev {
                    Some(SignalingEvent::WebRtc(signal)) => {
                        if let Err(e) = session.handle_signal(&signal, &outbound_signal_tx).await {
                            tracing::warn!(error = %e, "handle_signal failed");
                        }
                    }
                    Some(SignalingEvent::Status(_)) | Some(SignalingEvent::Features(_)) => {
                        // Informational; we don't need to act on these for streaming.
                    }
                    Some(SignalingEvent::Unknown { name, .. }) => {
                        tracing::debug!(event = %name, "unhandled signaling event");
                    }
                    Some(SignalingEvent::Closed(reason)) => {
                        tracing::info!(reason = %reason, "signaling closed");
                        return;
                    }
                    None => return,
                }
            }
            out = outbound_signal_rx.recv() => {
                let Some(out) = out else { return };
                if let Err(e) = signaling.send_webrtc(out).await {
                    tracing::warn!(error = %e, "send_webrtc failed");
                    return;
                }
            }
        }
    }
}

/// Map a webrtc-rs candidate type to the short ICE name (`host`, `srflx`,
/// `prflx`, `relay`) that's well-known in WebRTC vocabulary.
fn describe_candidate_type(t: webrtc::ice_transport::ice_candidate_type::RTCIceCandidateType) -> &'static str {
    use webrtc::ice_transport::ice_candidate_type::RTCIceCandidateType;
    match t {
        RTCIceCandidateType::Host => "host",
        RTCIceCandidateType::Srflx => "srflx",
        RTCIceCandidateType::Prflx => "prflx",
        RTCIceCandidateType::Relay => "relay",
        RTCIceCandidateType::Unspecified => "unknown",
    }
}
