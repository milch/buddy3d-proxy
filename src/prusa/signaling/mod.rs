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
    pub(crate) outbound: mpsc::Sender<Outbound>,
    pub events: mpsc::Receiver<SignalingEvent>,
    /// The Socket.IO sid the server assigned when we connected. Callers need
    /// it to populate `WebRtcSignal.session_id` on outbound webrtc events
    /// (`clientSocketId` in the JS terminology).
    pub session_id: String,
}

impl PrusaSignaling {
    /// Connect, authenticate via `client_authentication`, send the two `trigger`
    /// events that activate the camera, then send the `webrtc` kick-off
    /// (msg_type=1) carrying the ICE configuration. After that, the server
    /// drives the SDP offer/answer dance.
    ///
    /// `camera_token` is the per-camera persistent token from
    /// `Camera.token` on the `/app/printers/<uuid>/cameras` REST endpoint.
    /// `access_jwt` is the Prusa Connect access token.
    /// `webrtc_cfg` is the ICE configuration from `fetch_webrtc_config` —
    /// echoed back to the server in the kick-off so it knows which TURN
    /// servers we're prepared to use.
    pub async fn connect(
        camera_token: String,
        access_jwt: String,
        webrtc_cfg: crate::prusa::api::WebRtcConfig,
    ) -> Result<Self, SignalingError> {
        let (outbound, mut raw_events) = client::connect(SIGNALING_URL).await?;

        // Send Engine.IO MESSAGE → Socket.IO CONNECT (`40{...}`) with the
        // camera's token. The signaling server validates this against the
        // tokens it knows about.
        outbound
            .send(Outbound::Connect(
                serde_json::json!({"token": camera_token.clone()}),
            ))
            .await
            .map_err(|_| SignalingError::SendClosed)?;

        // Synchronously await the Socket.IO CONNECT reply so we can return
        // the assigned sid to the caller (it goes into every outbound
        // WebRtcSignal as `session_id`).
        let socket_sid = loop {
            match raw_events.recv().await {
                Some(Inbound::Connected { sid }) => break sid.unwrap_or_default(),
                Some(Inbound::TransportClosed(reason)) => {
                    return Err(SignalingError::Client(client::ClientError::Closed(reason)))
                }
                Some(_) => continue,
                None => {
                    return Err(SignalingError::Client(client::ClientError::Closed(
                        "stream end".into(),
                    )))
                }
            }
        };
        let connect_sid = socket_sid.clone();

        // Spawn a translator task: raw Inbound -> SignalingEvent.
        let (typed_tx, typed_rx) = mpsc::channel::<SignalingEvent>(64);
        let outbound_for_auth = outbound.clone();
        tokio::spawn(async move {
            // We're already past the Socket.IO CONNECT ack (synchronously
            // captured above); send `client_authentication` straight away.
            let auth = crate::proto::ClientAuthentication {
                token: camera_token.clone(),
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
            tracing::debug!(len = buf.len(), "outbound client_authentication");
            let _ = outbound_for_auth
                .send(Outbound::BinaryEvent {
                    name: "client_authentication".into(),
                    payload: Bytes::from(buf),
                    expect_ack: true,
                })
                .await;

            // Wait for the auth ACK, then send triggers + webrtc kick-off.
            let mut auth_acked = false;

            // Main event-translation loop.
            while let Some(ev) = raw_events.recv().await {
                let translated = match ev {
                    Inbound::BinaryEvent { name, payload } => translate_binary(&name, payload),
                    Inbound::Ack { id, payload } => {
                        if !auth_acked && id == 0 {
                            // payload is `[0]` for success in this protocol.
                            let success = matches!(
                                payload.first().and_then(|v| v.as_u64()),
                                Some(0)
                            );
                            if !success {
                                let _ = typed_tx
                                    .send(SignalingEvent::Closed(format!(
                                        "auth ack rejected: {payload:?}"
                                    )))
                                    .await;
                                return;
                            }
                            auth_acked = true;
                            for (f1, f2) in [(1u32, 0u32), (0u32, 1u32)] {
                                let trigger = crate::proto::Trigger {
                                    field1: f1,
                                    field2: f2,
                                    token: camera_token.clone(),
                                };
                                let mut tbuf = Vec::new();
                                if trigger.encode(&mut tbuf).is_ok() {
                                    let _ = outbound_for_auth
                                        .send(Outbound::BinaryEvent {
                                            name: "trigger".into(),
                                            payload: Bytes::from(tbuf),
                                            expect_ack: false,
                                        })
                                        .await;
                                }
                            }
                            // Send the WebRTC kick-off so the server emits
                            // its SDP offer.
                            let kickoff = crate::proto::WebRtcSignal {
                                token: camera_token.clone(),
                                session_id: connect_sid.clone(),
                                peer_id: connect_sid.clone(),
                                msg_type: 1,
                                direction: 2,
                                ice_config: Some(build_ice_config(&webrtc_cfg)),
                                ..Default::default()
                            };
                            let mut kbuf = Vec::new();
                            if kickoff.encode(&mut kbuf).is_ok() {
                                tracing::debug!(
                                    len = kbuf.len(),
                                    "outbound webrtc kick-off"
                                );
                                let _ = outbound_for_auth
                                    .send(Outbound::BinaryEvent {
                                        name: "webrtc".into(),
                                        payload: Bytes::from(kbuf),
                                        expect_ack: false,
                                    })
                                    .await;
                            }
                        }
                        continue;
                    }
                    Inbound::TransportClosed(reason) => SignalingEvent::Closed(reason),
                    Inbound::Disconnected => SignalingEvent::Closed("disconnect".into()),
                    Inbound::Connected { .. } => continue, // unexpected duplicate, ignore
                };
                if typed_tx.send(translated).await.is_err() {
                    return;
                }
            }
        });

        Ok(Self {
            outbound,
            events: typed_rx,
            session_id: socket_sid,
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
                expect_ack: false,
            })
            .await
            .map_err(|_| SignalingError::SendClosed)
    }

    /// Send a `trigger` event with a raw pre-encoded CameraTrigger payload.
    /// Used by `restart-camera` (and any future ad-hoc trigger commands)
    /// where the caller hand-encodes the proto.
    pub async fn send_trigger(&self, payload: Vec<u8>) -> Result<(), SignalingError> {
        self.outbound
            .send(Outbound::BinaryEvent {
                name: "trigger".into(),
                payload: Bytes::from(payload),
                expect_ack: false,
            })
            .await
            .map_err(|_| SignalingError::SendClosed)
    }

    /// Send a `configuration` event with a raw pre-encoded Configuration payload.
    /// Used by `set-quality` and other settings-mutation commands.
    pub async fn send_configuration(&self, payload: Vec<u8>) -> Result<(), SignalingError> {
        self.outbound
            .send(Outbound::BinaryEvent {
                name: "configuration".into(),
                payload: Bytes::from(payload),
                expect_ack: false,
            })
            .await
            .map_err(|_| SignalingError::SendClosed)
    }
}

/// Convert a fetched `WebRtcConfig` (from REST) into the protobuf `IceConfig`
/// the signaling server expects in the kick-off. Each REST entry typically has
/// 1 username/credential pair plus N URLs; we group them as one IceServerGroup.
fn build_ice_config(cfg: &crate::prusa::api::WebRtcConfig) -> crate::proto::IceConfig {
    use crate::proto::{IceConfig, IceServer, IceServerGroup};
    let server_groups = cfg
        .ice_servers
        .iter()
        .map(|entry| {
            let servers = entry
                .url_list()
                .into_iter()
                .map(|url| parse_ice_url(&url))
                .collect();
            IceServerGroup {
                servers,
                username: entry.username.clone().unwrap_or_default(),
                credential: entry.credential.clone().unwrap_or_default(),
            }
        })
        .collect();
    IceConfig {
        server_groups,
        field2: 1,
        ttl_seconds: cfg.ttl_seconds.try_into().unwrap_or(300),
    }
}

/// Parse e.g. `turns:coturn.prusa3d.com:5349` or
/// `turn:coturn.prusa3d.com:3478?transport=udp` into IceServer fields.
fn parse_ice_url(url: &str) -> crate::proto::IceServer {
    let (scheme, rest) = url.split_once(':').unwrap_or(("stun", url));
    let server_type = match scheme {
        "stun" => 1,
        "turn" => 2,
        "turns" => 3,
        _ => 0,
    };
    let (host_port, query) = rest.split_once('?').unwrap_or((rest, ""));
    let (host, port) = host_port
        .rsplit_once(':')
        .map(|(h, p)| (h.to_string(), p.parse().unwrap_or(0)))
        .unwrap_or_else(|| (host_port.to_string(), 0));
    let transport = match query
        .split_once('=')
        .map(|(_, v)| v)
        .unwrap_or("")
        .to_ascii_lowercase()
        .as_str()
    {
        "udp" => 1,
        "tcp" => 2,
        "tls" => 3,
        _ => 0,
    };
    crate::proto::IceServer {
        server_type,
        host,
        port,
        transport,
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
