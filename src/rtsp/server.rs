//! RTSP TCP server. Accepts connections, drives per-session logic, forwards
//! RTP packets as RFC 2326 §10.12 interleaved frames.

use crate::rtsp::interleaved;
use crate::rtsp::message::{parse_request, ParseError};
use crate::rtsp::sdp::H264Params;
use crate::rtsp::session::{Session, SessionAction, SessionContext};
use bytes::BytesMut;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;
use webrtc::rtp::packet::Packet as RtpPacket;

/// Events the server emits about viewer connect/disconnect. The supervisor
/// counts these to know when to bring up / tear down WebRTC.
#[derive(Debug, Clone)]
pub enum ViewerEvent {
    Attached,
    Detached,
}

/// What a server needs from a supervisor to serve a connection. Decoupled via
/// trait so server.rs has no compile-time dep on `supervisor.rs`.
#[async_trait::async_trait]
pub trait StreamSource: Send + Sync + 'static {
    /// Block until WebRTC is up and the SDP is ready, then return both the SDP
    /// params and a fresh broadcast receiver of the RTP stream. The viewer
    /// count is incremented for the duration of the returned `Subscription`.
    async fn subscribe(&self) -> Result<Subscription, SourceError>;
    /// Camera display name (for the SDP `s=` line).
    fn camera_name(&self) -> &str;
    /// Path component of the RTSP URI we accept (e.g. "buddy3d-camera").
    fn rtsp_path(&self) -> &str;
}

/// Held by the connection task while a viewer is attached. Drop = decrement.
pub struct Subscription {
    pub h264: H264Params,
    pub rtp: broadcast::Receiver<RtpPacket>,
    /// Sent on Drop so the supervisor knows the viewer left.
    pub on_drop: tokio::sync::mpsc::UnboundedSender<ViewerEvent>,
}

impl Drop for Subscription {
    fn drop(&mut self) {
        // Unbounded so a sync Drop can always deliver. A bounded try_send
        // could fail under heavy reconnect churn, leaking the viewer count.
        let _ = self.on_drop.send(ViewerEvent::Detached);
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SourceError {
    #[error("stream source unavailable: {0}")]
    Unavailable(String),
}

#[derive(Debug, thiserror::Error)]
pub enum ServeError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

/// Returned by `Server::start`. Drop = stop the listener.
pub struct ServerHandle {
    pub bound_addr: std::net::SocketAddr,
    _shutdown: tokio::sync::oneshot::Sender<()>,
}

pub struct Server;

impl Server {
    /// Bind `bind_addr:port` and start accepting RTSP connections in a background
    /// task. Returns immediately with the bound address (useful when port=0).
    pub async fn start(
        bind_addr: &str,
        port: u16,
        source: Arc<dyn StreamSource>,
    ) -> Result<ServerHandle, ServeError> {
        let listener = TcpListener::bind((bind_addr, port)).await?;
        let bound_addr = listener.local_addr()?;
        tracing::info!(%bound_addr, "rtsp server listening");
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel();

        tokio::spawn(async move {
            let session_counter = Arc::new(AtomicU64::new(0));
            loop {
                tokio::select! {
                    accept = listener.accept() => {
                        match accept {
                            Ok((stream, peer)) => {
                                let source = source.clone();
                                let counter = session_counter.clone();
                                tokio::spawn(async move {
                                    if let Err(e) = handle_connection(stream, source, counter).await {
                                        tracing::warn!(%peer, error = %e, "rtsp connection ended");
                                    }
                                });
                            }
                            Err(e) => {
                                tracing::warn!(error = %e, "rtsp accept failed");
                            }
                        }
                    }
                    _ = &mut shutdown_rx => {
                        tracing::info!("rtsp server shutting down");
                        return;
                    }
                }
            }
        });

        Ok(ServerHandle {
            bound_addr,
            _shutdown: shutdown_tx,
        })
    }
}

async fn handle_connection(
    mut stream: TcpStream,
    source: Arc<dyn StreamSource>,
    session_counter: Arc<AtomicU64>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let session_id = format!(
        "BUDDY3D-{}",
        session_counter.fetch_add(1, Ordering::Relaxed)
    );
    let mut session = Session::new();
    let mut subscription: Option<Subscription> = None;
    let mut buf = BytesMut::with_capacity(8 * 1024);
    let mut read_buf = [0u8; 4096];

    loop {
        let n = stream.read(&mut read_buf).await?;
        if n == 0 {
            // EOF — client closed connection. Subscription drops, viewer count
            // decrements automatically.
            return Ok(());
        }
        buf.extend_from_slice(&read_buf[..n]);

        // Parse as many requests as the buffer can yield.
        loop {
            let (request, consumed) = match parse_request(&buf) {
                Ok(pair) => pair,
                Err(ParseError::Incomplete) => break,
                Err(e) => {
                    tracing::warn!(error = %e, "rtsp parse failure; closing connection");
                    return Ok(());
                }
            };
            // Trim consumed prefix.
            let _ = buf.split_to(consumed);

            // Lazily acquire the subscription on the first request that needs
            // codec params (DESCRIBE). For OPTIONS we can answer without one.
            if subscription.is_none() && needs_subscription(request.method) {
                subscription = Some(source.subscribe().await?);
            }

            let h264 = subscription
                .as_ref()
                .map(|s| s.h264.clone())
                .unwrap_or(H264Params {
                    profile_level_id: "42c01e".into(),
                    sprop_parameter_sets: String::new(),
                    packetization_mode: 1,
                    payload_type: 96,
                });

            let ctx = SessionContext {
                camera_name: source.camera_name(),
                h264_params: &h264,
                expected_path: source.rtsp_path(),
                new_session_id: &session_id,
            };
            let (response, action) = session.handle(&request, &ctx);
            stream.write_all(response.format().as_bytes()).await?;

            match action {
                SessionAction::None => {}
                SessionAction::StartStreaming { rtp_channel } => {
                    let mut rtp_rx = subscription
                        .as_mut()
                        .map(|s| s.rtp.resubscribe())
                        .ok_or("StartStreaming without subscription")?;
                    forward_rtp(&mut stream, &mut rtp_rx, rtp_channel).await?;
                    return Ok(());
                }
                SessionAction::Stop => {
                    return Ok(());
                }
            }
        }
    }
}

fn needs_subscription(method: crate::rtsp::message::Method) -> bool {
    use crate::rtsp::message::Method;
    matches!(method, Method::Describe | Method::Setup | Method::Play)
}

async fn forward_rtp(
    stream: &mut TcpStream,
    rtp_rx: &mut broadcast::Receiver<RtpPacket>,
    channel: u8,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut frame_buf = BytesMut::with_capacity(2048);
    let mut serialize_buf = Vec::with_capacity(2048);
    let mut discard = [0u8; 4096];
    loop {
        // Watch the socket too: when the camera is down no RTP arrives, so no
        // write ever fails, and without a read we'd never see the client hang
        // up. Inbound bytes (RTCP, keepalives) are discarded.
        let recv = tokio::select! {
            recv = rtp_rx.recv() => recv,
            read = stream.read(&mut discard) => match read {
                Ok(0) | Err(_) => return Ok(()),
                Ok(_) => continue,
            },
        };
        match recv {
            Ok(pkt) => {
                serialize_buf.clear();
                let header = pkt.header.clone();
                let header_bytes = header_to_bytes(&header);
                serialize_buf.extend_from_slice(&header_bytes);
                serialize_buf.extend_from_slice(&pkt.payload);

                frame_buf.clear();
                interleaved::encode_frame(&mut frame_buf, channel, &serialize_buf);
                stream.write_all(&frame_buf).await?;
            }
            Err(broadcast::error::RecvError::Lagged(n)) => {
                // Slow consumer fell behind; log and continue.
                tracing::warn!(dropped = n, "rtsp viewer lagged, dropping packets");
            }
            Err(broadcast::error::RecvError::Closed) => {
                return Ok(());
            }
        }
    }
}

/// Serialize a webrtc-rs RTP header to the on-wire 12-byte (or longer with CSRCs)
/// RTP header format.
fn header_to_bytes(h: &webrtc::rtp::header::Header) -> Vec<u8> {
    let mut buf = Vec::with_capacity(12 + h.csrc.len() * 4);
    let mut first = (h.version & 0x3) << 6;
    if h.padding {
        first |= 0x20;
    }
    if h.extension {
        first |= 0x10;
    }
    first |= (h.csrc.len() as u8) & 0x0F;
    buf.push(first);

    let mut second = h.payload_type & 0x7F;
    if h.marker {
        second |= 0x80;
    }
    buf.push(second);

    buf.extend_from_slice(&h.sequence_number.to_be_bytes());
    buf.extend_from_slice(&h.timestamp.to_be_bytes());
    buf.extend_from_slice(&h.ssrc.to_be_bytes());
    for csrc in &h.csrc {
        buf.extend_from_slice(&csrc.to_be_bytes());
    }
    if h.extension {
        buf.extend_from_slice(&h.extension_profile.to_be_bytes());
        let payload_len_words = h
            .extensions
            .iter()
            .map(|e| (e.payload.len() + 1 + 3) / 4)
            .sum::<usize>() as u16;
        buf.extend_from_slice(&payload_len_words.to_be_bytes());
        for ext in &h.extensions {
            buf.push(ext.id);
            buf.push(ext.payload.len() as u8);
            buf.extend_from_slice(&ext.payload);
            // Pad to 4-byte boundary
            while buf.len() % 4 != 0 {
                buf.push(0);
            }
        }
    }
    buf
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    /// Source whose RTP stream never produces a packet, like a camera that's
    /// down while the supervisor still reports Streaming.
    struct SilentSource {
        on_drop: tokio::sync::mpsc::UnboundedSender<ViewerEvent>,
        rtp_tx: broadcast::Sender<RtpPacket>,
    }

    #[async_trait::async_trait]
    impl StreamSource for SilentSource {
        async fn subscribe(&self) -> Result<Subscription, SourceError> {
            Ok(Subscription {
                h264: H264Params {
                    profile_level_id: "42c01e".into(),
                    sprop_parameter_sets: "Z0L,aM4".into(),
                    packetization_mode: 1,
                    payload_type: 96,
                },
                rtp: self.rtp_tx.subscribe(),
                on_drop: self.on_drop.clone(),
            })
        }
        fn camera_name(&self) -> &str {
            "Cam"
        }
        fn rtsp_path(&self) -> &str {
            "cam"
        }
    }

    #[tokio::test]
    async fn client_disconnect_while_streaming_without_rtp_releases_subscription() {
        let (on_drop, mut events) = tokio::sync::mpsc::unbounded_channel();
        let (rtp_tx, _) = broadcast::channel(16);
        let source = Arc::new(SilentSource { on_drop, rtp_tx });
        let handle = Server::start("127.0.0.1", 0, source.clone()).await.unwrap();

        let mut client = TcpStream::connect(handle.bound_addr).await.unwrap();
        client
            .write_all(
                b"DESCRIBE rtsp://h/cam RTSP/1.0\r\nCSeq: 1\r\n\r\n\
                  SETUP rtsp://h/cam/streamid=0 RTSP/1.0\r\nCSeq: 2\r\n\
                  Transport: RTP/AVP/TCP;unicast;interleaved=0-1\r\n\r\n\
                  PLAY rtsp://h/cam RTSP/1.0\r\nCSeq: 3\r\nSession: BUDDY3D-0\r\n\r\n",
            )
            .await
            .unwrap();

        // Wait for the PLAY response so the server is inside forward_rtp.
        let mut got = Vec::new();
        let mut buf = [0u8; 4096];
        while !String::from_utf8_lossy(&got).contains("Range: npt") {
            let n = tokio::time::timeout(Duration::from_secs(5), client.read(&mut buf))
                .await
                .expect("timed out waiting for PLAY response")
                .unwrap();
            assert_ne!(n, 0, "server closed before PLAY response");
            got.extend_from_slice(&buf[..n]);
        }

        drop(client);

        let ev = tokio::time::timeout(Duration::from_secs(5), events.recv())
            .await
            .expect("subscription not released after client disconnect");
        assert!(matches!(ev, Some(ViewerEvent::Detached)));
        // Keep the RTP sender alive until here so Closed can't end the stream.
        drop(source);
    }

    #[test]
    fn header_serializer_minimal_round_trip_shape() {
        use super::header_to_bytes;
        use webrtc::rtp::header::Header;
        let h = Header {
            version: 2,
            padding: false,
            extension: false,
            marker: true,
            payload_type: 96,
            sequence_number: 12345,
            timestamp: 67890,
            ssrc: 0xDEADBEEF,
            csrc: vec![],
            extension_profile: 0,
            extensions: vec![],
            extensions_padding: 0,
        };
        let bytes = header_to_bytes(&h);
        assert_eq!(bytes.len(), 12);
        // V=2, no padding, no ext, no CSRCs => 0b10_0_0_0000 = 0x80
        assert_eq!(bytes[0], 0x80);
        // marker + PT=96 => 0x80 | 0x60 = 0xE0
        assert_eq!(bytes[1], 0xE0);
        assert_eq!(&bytes[2..4], &12345u16.to_be_bytes());
        assert_eq!(&bytes[4..8], &67890u32.to_be_bytes());
        assert_eq!(&bytes[8..12], &0xDEADBEEFu32.to_be_bytes());
    }
}
