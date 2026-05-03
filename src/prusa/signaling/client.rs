//! WebSocket-backed Socket.IO transport. Drives the Engine.IO heartbeat and
//! routes events through tokio mpsc channels.

use crate::prusa::signaling::{engineio, socketio};
use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

/// Outbound events the caller can submit. The driver encodes them into Socket.IO
/// + Engine.IO frames.
#[derive(Debug, Clone)]
pub enum Outbound {
    /// CONNECT with auth payload.
    Connect(serde_json::Value),
    /// EVENT with one binary attachment (the protobuf-encoded payload).
    /// `expect_ack` controls whether the Socket.IO frame carries an ack_id —
    /// only `client_authentication` does in Prusa's protocol; other events
    /// (trigger, webrtc) get rejected with a session-member error if we
    /// include one.
    BinaryEvent {
        name: String,
        payload: Bytes,
        expect_ack: bool,
    },
}

/// Inbound events from the server, after Correlator assembly.
#[derive(Debug, Clone)]
pub enum Inbound {
    /// The Socket.IO server acknowledged our CONNECT and assigned a `sid`.
    /// We need this sid to populate `WebRtcSignal.session_id` on the kick-off.
    Connected { sid: Option<String> },
    Disconnected,
    /// Server sent a binary event with attachments. We deliver a single attachment
    /// because every observed Prusa event uses exactly one.
    BinaryEvent { name: String, payload: Bytes },
    /// Server ACK'd one of our `emitWithAck` events. Used to gate follow-up
    /// emits until the auth flow is complete (otherwise the server returns
    /// `ClientIsNotSessionMemberError` for any event that races the auth ACK).
    Ack { id: u64, payload: Vec<serde_json::Value> },
    /// Underlying transport closed unexpectedly.
    TransportClosed(String),
}

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("websocket connect failed: {0}")]
    Connect(#[source] tokio_tungstenite::tungstenite::Error),
    #[error("websocket io error: {0}")]
    Io(#[source] tokio_tungstenite::tungstenite::Error),
    #[error("engine.io parse error: {0}")]
    EngineIo(#[from] engineio::ParseError),
    #[error("socket.io parse error: {0}")]
    SocketIo(#[from] socketio::CorrelatorError),
    #[error("server did not send Engine.IO OPEN as first frame")]
    MissingOpen,
    #[error("server closed: {0}")]
    Closed(String),
}

/// Connect to a Socket.IO endpoint over WebSocket. Spawns a background task that
/// drives the connection and returns channels for sending/receiving events.
///
/// `endpoint_url` should be the full WSS URL including the
/// `/socket.io/?EIO=4&transport=websocket` path.
pub async fn connect(
    endpoint_url: &str,
) -> Result<(mpsc::Sender<Outbound>, mpsc::Receiver<Inbound>), ClientError> {
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    use tokio_tungstenite::tungstenite::http::header::{ORIGIN, USER_AGENT};

    // The Prusa signaling server appears to gate Socket.IO `client_authentication`
    // on the WS upgrade Origin. The captured browser handshake uses
    // `Origin: https://connect.prusa3d.com` and a browser User-Agent; without
    // them the server returns "Missing client permissions" after auth.
    let mut req = endpoint_url
        .into_client_request()
        .map_err(ClientError::Connect)?;
    req.headers_mut().insert(
        ORIGIN,
        "https://connect.prusa3d.com".parse().expect("static value"),
    );
    req.headers_mut().insert(
        USER_AGENT,
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 \
         (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36"
            .parse()
            .expect("static value"),
    );

    let (ws, _) = tokio_tungstenite::connect_async(req)
        .await
        .map_err(ClientError::Connect)?;

    let (mut sink, mut stream) = ws.split();

    // First frame must be Engine.IO OPEN.
    let first = stream
        .next()
        .await
        .ok_or(ClientError::MissingOpen)?
        .map_err(ClientError::Io)?;
    let open_text = match first {
        Message::Text(t) => t,
        _ => return Err(ClientError::MissingOpen),
    };
    let open = match engineio::parse_text(open_text.as_str())? {
        engineio::TextPacket::Open(o) => o,
        _ => return Err(ClientError::MissingOpen),
    };
    let ping_interval = Duration::from_millis(open.ping_interval);

    let (outbound_tx, mut outbound_rx) = mpsc::channel::<Outbound>(64);
    let (inbound_tx, inbound_rx) = mpsc::channel::<Inbound>(64);

    tokio::spawn(async move {
        let mut correlator = socketio::Correlator::new();
        let mut ping_timer = tokio::time::interval(ping_interval + Duration::from_secs(5));
        // First tick fires immediately; skip it.
        ping_timer.tick().await;
        let mut next_ack_id: u64 = 0;

        loop {
            tokio::select! {
                msg = stream.next() => {
                    match msg {
                        None => {
                            let _ = inbound_tx.send(Inbound::TransportClosed("stream end".into())).await;
                            break;
                        }
                        Some(Err(e)) => {
                            let _ = inbound_tx.send(Inbound::TransportClosed(format!("{e}"))).await;
                            break;
                        }
                        Some(Ok(Message::Text(t))) => {
                            // We received traffic — reset the watchdog.
                            ping_timer.reset();
                            tracing::debug!(frame = %t.as_str(), "ws recv text");
                            match engineio::parse_text(t.as_str()) {
                                Ok(engineio::TextPacket::Ping) => {
                                    if sink.send(Message::Text(engineio::encode_pong().into())).await.is_err() {
                                        let _ = inbound_tx.send(Inbound::TransportClosed("write ping reply failed".into())).await;
                                        break;
                                    }
                                }
                                Ok(engineio::TextPacket::Message(payload)) => {
                                    match correlator.feed_text(&payload) {
                                        Ok(Some(socketio::Event::Connected { auth })) => {
                                            let sid = auth
                                                .as_ref()
                                                .and_then(|v| v.get("sid"))
                                                .and_then(|v| v.as_str())
                                                .map(|s| s.to_string());
                                            let _ = inbound_tx.send(Inbound::Connected { sid }).await;
                                        }
                                        Ok(Some(socketio::Event::Disconnected)) => {
                                            let _ = inbound_tx.send(Inbound::Disconnected).await;
                                        }
                                        Ok(Some(socketio::Event::Ack { id, args })) => {
                                            let _ = inbound_tx.send(Inbound::Ack { id, payload: args }).await;
                                        }
                                        Ok(Some(socketio::Event::Binary { name, attachments })) => {
                                            let payload = attachments.into_iter().next().unwrap_or_default();
                                            let _ = inbound_tx.send(Inbound::BinaryEvent { name, payload }).await;
                                        }
                                        Ok(Some(socketio::Event::Text { name, args })) => {
                                            tracing::warn!(event = %name, args = ?args, "received text-only socket.io event");
                                        }
                                        Ok(None) => {}
                                        Err(e) => {
                                            tracing::warn!(error = %e, "socket.io parse error");
                                        }
                                    }
                                }
                                Ok(engineio::TextPacket::Close)
                                | Ok(engineio::TextPacket::Pong)
                                | Ok(engineio::TextPacket::Open(_))
                                | Ok(engineio::TextPacket::Noop) => {}
                                Err(e) => {
                                    tracing::warn!(error = %e, "engine.io parse error");
                                }
                            }
                        }
                        Some(Ok(Message::Binary(b))) => {
                            ping_timer.reset();
                            match correlator.feed_binary(Bytes::from(b)) {
                                Ok(Some(socketio::Event::Binary { name, attachments })) => {
                                    let payload = attachments.into_iter().next().unwrap_or_default();
                                    let _ = inbound_tx.send(Inbound::BinaryEvent { name, payload }).await;
                                }
                                Ok(_) => {}
                                Err(e) => {
                                    tracing::warn!(error = %e, "binary correlator error");
                                }
                            }
                        }
                        Some(Ok(Message::Close(_))) => {
                            let _ = inbound_tx.send(Inbound::TransportClosed("close frame".into())).await;
                            break;
                        }
                        Some(Ok(_)) => {}
                    }
                }
                out = outbound_rx.recv() => {
                    let Some(out) = out else { break };
                    let frames = match out {
                        Outbound::Connect(auth) => {
                            let body = socketio::encode_connect(&auth);
                            let text = engineio::encode_message(&body);
                            tracing::debug!(frame = %text, "ws send text (connect)");
                            vec![Message::Text(text.into())]
                        }
                        Outbound::BinaryEvent {
                            name,
                            payload,
                            expect_ack,
                        } => {
                            let ack_id = if expect_ack {
                                let id = next_ack_id;
                                next_ack_id += 1;
                                Some(id)
                            } else {
                                None
                            };
                            let header = socketio::encode_binary_event(&name, 1, ack_id);
                            let text = engineio::encode_message(&header);
                            tracing::debug!(
                                frame = %text,
                                bin_len = payload.len(),
                                "ws send binary event"
                            );
                            vec![
                                Message::Text(text.into()),
                                Message::Binary(payload.into()),
                            ]
                        }
                    };
                    for f in frames {
                        if sink.send(f).await.is_err() {
                            let _ = inbound_tx.send(Inbound::TransportClosed("write failed".into())).await;
                            return;
                        }
                    }
                }
                _ = ping_timer.tick() => {
                    // Server is supposed to ping us; if it hasn't within
                    // ping_interval + 5s, treat it as a stall.
                    let _ = inbound_tx.send(Inbound::TransportClosed("server ping timeout".into())).await;
                    break;
                }
            }
        }
    });

    Ok((outbound_tx, inbound_rx))
}
