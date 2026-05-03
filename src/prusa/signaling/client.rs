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
    BinaryEvent { name: String, payload: Bytes },
}

/// Inbound events from the server, after Correlator assembly.
#[derive(Debug, Clone)]
pub enum Inbound {
    Connected,
    Disconnected,
    /// Server sent a binary event with attachments. We deliver a single attachment
    /// because every observed Prusa event uses exactly one.
    BinaryEvent { name: String, payload: Bytes },
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
    let (ws, _) = tokio_tungstenite::connect_async(endpoint_url)
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
                            match engineio::parse_text(t.as_str()) {
                                Ok(engineio::TextPacket::Ping) => {
                                    if sink.send(Message::Text(engineio::encode_pong().into())).await.is_err() {
                                        let _ = inbound_tx.send(Inbound::TransportClosed("write ping reply failed".into())).await;
                                        break;
                                    }
                                }
                                Ok(engineio::TextPacket::Message(payload)) => {
                                    match correlator.feed_text(&payload) {
                                        Ok(Some(socketio::Event::Connected { .. })) => {
                                            let _ = inbound_tx.send(Inbound::Connected).await;
                                        }
                                        Ok(Some(socketio::Event::Disconnected)) => {
                                            let _ = inbound_tx.send(Inbound::Disconnected).await;
                                        }
                                        Ok(Some(socketio::Event::Binary { name, attachments })) => {
                                            let payload = attachments.into_iter().next().unwrap_or_default();
                                            let _ = inbound_tx.send(Inbound::BinaryEvent { name, payload }).await;
                                        }
                                        Ok(Some(socketio::Event::Text { name, .. })) => {
                                            tracing::debug!(event = %name, "received text-only socket.io event");
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
                            vec![Message::Text(engineio::encode_message(&body).into())]
                        }
                        Outbound::BinaryEvent { name, payload } => {
                            let header = socketio::encode_binary_event(&name, 1);
                            vec![
                                Message::Text(engineio::encode_message(&header).into()),
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
