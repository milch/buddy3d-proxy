//! High-level Prusa signaling client. Wraps the raw Socket.IO client with prost
//! encoding/decoding so callers see typed events.

pub mod client;
pub mod engineio;
pub mod socketio;

use crate::prusa::signaling::client::{Inbound, Outbound};
use bytes::Bytes;
use prost::Message;
use tokio::sync::mpsc;

const SIGNALING_URL: &str =
    "wss://camera-signaling.prusa3d.com/socket.io/?EIO=4&transport=websocket";

/// Typed inbound event from the signaling server.
#[derive(Debug, Clone)]
pub enum SignalingEvent {
    Status(crate::proto::Status),
    Features(crate::proto::Features),
    WebRtc(crate::proto::WebRtcSignal),
    /// An event we don't handle (logged, surfaced as raw bytes).
    Unknown { name: String, payload: Bytes },
    Closed(String),
}

#[derive(Debug, thiserror::Error)]
pub enum SignalingError {
    #[error("client error: {0}")]
    Client(#[from] client::ClientError),
    #[error("send channel closed")]
    SendClosed,
}

/// Connected signaling session. Use `send_webrtc()` to push outbound signaling
/// to the server, and the receiver to pull inbound events.
pub struct PrusaSignaling {
    outbound: mpsc::Sender<Outbound>,
    pub events: mpsc::Receiver<SignalingEvent>,
}

impl PrusaSignaling {
    /// Connect, authenticate via `client_authentication`, and start dispatching events.
    /// `webrtc_token` is the short-lived token from `fetch_webrtc_config`.
    /// `access_jwt` is the Prusa Connect access token.
    pub async fn connect(
        webrtc_token: String,
        access_jwt: String,
    ) -> Result<Self, SignalingError> {
        let (outbound, mut raw_events) = client::connect(SIGNALING_URL).await?;

        // Send Engine.IO MESSAGE → Socket.IO CONNECT (`40{...}`) with token auth.
        outbound
            .send(Outbound::Connect(
                serde_json::json!({"token": webrtc_token.clone()}),
            ))
            .await
            .map_err(|_| SignalingError::SendClosed)?;

        // Spawn a translator task: raw Inbound -> SignalingEvent.
        let (typed_tx, typed_rx) = mpsc::channel::<SignalingEvent>(64);
        let outbound_for_auth = outbound.clone();
        tokio::spawn(async move {
            // Wait for the Connected event, then send `client_authentication`.
            while let Some(ev) = raw_events.recv().await {
                match ev {
                    Inbound::Connected => {
                        let auth = crate::proto::ClientAuthentication {
                            token: webrtc_token.clone(),
                            client_kind: "client".to_string(),
                            access_jwt: access_jwt.clone(),
                        };
                        let mut buf = Vec::new();
                        if auth.encode(&mut buf).is_err() {
                            let _ = typed_tx
                                .send(SignalingEvent::Closed("encode auth".into()))
                                .await;
                            return;
                        }
                        let _ = outbound_for_auth
                            .send(Outbound::BinaryEvent {
                                name: "client_authentication".into(),
                                payload: Bytes::from(buf),
                            })
                            .await;
                        // After sending auth, fall through to event loop.
                        break;
                    }
                    Inbound::TransportClosed(reason) => {
                        let _ = typed_tx.send(SignalingEvent::Closed(reason)).await;
                        return;
                    }
                    _ => {}
                }
            }

            // Main event-translation loop.
            while let Some(ev) = raw_events.recv().await {
                let translated = match ev {
                    Inbound::BinaryEvent { name, payload } => translate_binary(&name, payload),
                    Inbound::TransportClosed(reason) => SignalingEvent::Closed(reason),
                    Inbound::Disconnected => SignalingEvent::Closed("disconnect".into()),
                    Inbound::Connected => continue, // unexpected duplicate, ignore
                };
                if typed_tx.send(translated).await.is_err() {
                    return;
                }
            }
        });

        Ok(Self {
            outbound,
            events: typed_rx,
        })
    }

    /// Send a `webrtc` event with a `WebRtcSignal` protobuf payload.
    pub async fn send_webrtc(
        &self,
        signal: crate::proto::WebRtcSignal,
    ) -> Result<(), SignalingError> {
        let mut buf = Vec::new();
        signal
            .encode(&mut buf)
            .expect("WebRtcSignal encode never fails for valid struct");
        self.outbound
            .send(Outbound::BinaryEvent {
                name: "webrtc".into(),
                payload: Bytes::from(buf),
            })
            .await
            .map_err(|_| SignalingError::SendClosed)
    }
}

fn translate_binary(name: &str, payload: Bytes) -> SignalingEvent {
    match name {
        "status" => crate::proto::Status::decode(payload.as_ref())
            .map(SignalingEvent::Status)
            .unwrap_or_else(|_| SignalingEvent::Unknown {
                name: name.to_string(),
                payload,
            }),
        "features" => crate::proto::Features::decode(payload.as_ref())
            .map(SignalingEvent::Features)
            .unwrap_or_else(|_| SignalingEvent::Unknown {
                name: name.to_string(),
                payload,
            }),
        "webrtc" => crate::proto::WebRtcSignal::decode(payload.as_ref())
            .map(SignalingEvent::WebRtc)
            .unwrap_or_else(|_| SignalingEvent::Unknown {
                name: name.to_string(),
                payload,
            }),
        _ => SignalingEvent::Unknown {
            name: name.to_string(),
            payload,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn translate_binary_decodes_status() {
        let mut buf = Vec::new();
        let msg = crate::proto::Status {
            token: "tok".into(),
            camera_id: "cam".into(),
            ..Default::default()
        };
        msg.encode(&mut buf).unwrap();
        let ev = translate_binary("status", Bytes::from(buf));
        match ev {
            SignalingEvent::Status(s) => assert_eq!(s.token, "tok"),
            other => panic!("expected Status, got {other:?}"),
        }
    }

    #[test]
    fn translate_binary_unknown_event_falls_through() {
        let ev = translate_binary("mystery", Bytes::from_static(b"\x00"));
        match ev {
            SignalingEvent::Unknown { name, .. } => assert_eq!(name, "mystery"),
            other => panic!("expected Unknown, got {other:?}"),
        }
    }
}
