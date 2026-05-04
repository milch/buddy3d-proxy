//! Snapshot subsystem: reassembles H.264 NAL units from the supervisor's RTP
//! broadcast channel, decodes the latest IDR via openh264, encodes JPEG, and
//! hands the bytes to the MQTT hub.

pub mod encode;
pub mod h264;

use crate::supervisor::{State, Supervisor};
use bytes::Bytes;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

/// Latest decoder-relevant NAL units captured from the live RTP stream.
/// Prusa's camera doesn't ship SPS/PPS via SDP `sprop-parameter-sets` —
/// it sends them in-band as NAL units (type 7 and 8) right before each
/// IDR keyframe (typically packaged in a STAP-A). The snapshot worker
/// consumes them from here.
#[derive(Default, Debug)]
struct LatestParams {
    sps: Option<Bytes>,
    pps: Option<Bytes>,
    idr: Option<Bytes>,
}

/// Spawn the snapshot worker. It subscribes to the supervisor's RTP
/// broadcast on every state-transition into `Streaming`, runs the
/// reassembler to capture SPS/PPS/IDR NAL units, and ticks every
/// `interval` to publish the latest IDR (decoded to JPEG) via `publish`.
///
/// `interval` of `Duration::ZERO` disables snapshot publishing entirely.
pub async fn run(
    supervisor: Arc<Supervisor>,
    interval: Duration,
    publish: impl Fn(Bytes) + Send + Sync + 'static,
) {
    if interval.is_zero() {
        tracing::info!("snapshot disabled (interval=0)");
        return;
    }

    let publish = Arc::new(publish);
    let mut state_rx = supervisor.state_changes();
    let latest: Arc<Mutex<LatestParams>> = Arc::new(Mutex::new(LatestParams::default()));
    let mut current_session: Option<tokio::task::JoinHandle<()>> = None;

    loop {
        // Re-evaluate on every state transition.
        let snap = supervisor.snapshot().await;
        if snap.state == State::Streaming {
            if current_session.as_ref().map_or(true, |h| h.is_finished()) {
                if let Some(rx) = supervisor.subscribe_rtp().await {
                    let latest_for_consumer = latest.clone();
                    current_session = Some(tokio::spawn(consume_rtp(rx, latest_for_consumer)));
                }
            }
        } else if let Some(h) = current_session.take() {
            h.abort();
            *latest.lock().await = LatestParams::default();
        }

        tokio::select! {
            res = state_rx.changed() => {
                if res.is_err() { return; }
            }
            _ = tokio::time::sleep(interval) => {
                if matches!(supervisor.snapshot().await.state, State::Streaming) {
                    let g = latest.lock().await;
                    let triple = match (g.sps.clone(), g.pps.clone(), g.idr.clone()) {
                        (Some(s), Some(p), Some(i)) => Some((s, p, i)),
                        _ => {
                            tracing::debug!(
                                have_sps = g.sps.is_some(),
                                have_pps = g.pps.is_some(),
                                have_idr = g.idr.is_some(),
                                "snapshot tick: missing NAL units; will retry next tick"
                            );
                            None
                        }
                    };
                    drop(g);
                    if let Some((sps, pps, idr)) = triple {
                        let publish = publish.clone();
                        tokio::task::spawn_blocking(move || {
                            let annex_b = encode::build_annex_b(&sps, &pps, &idr);
                            match encode::decode_to_jpeg(&annex_b) {
                                Ok(jpeg) => {
                                    tracing::debug!(bytes = jpeg.len(), "snapshot encoded");
                                    publish(jpeg);
                                }
                                Err(e) => tracing::warn!(error = %e, "snapshot decode failed"),
                            }
                        });
                    }
                }
            }
        }
    }
}

async fn consume_rtp(
    mut rx: tokio::sync::broadcast::Receiver<webrtc::rtp::packet::Packet>,
    latest: Arc<Mutex<LatestParams>>,
) {
    let mut reassembler = h264::Reassembler::new();
    let mut seen_sps = false;
    let mut seen_pps = false;
    let mut seen_idr = false;
    loop {
        match rx.recv().await {
            Ok(pkt) => {
                let nals = reassembler.push(&pkt.payload, pkt.header.sequence_number);
                if nals.is_empty() {
                    continue;
                }
                let mut g = latest.lock().await;
                for nal in nals {
                    match nal.unit_type() {
                        7 => {
                            if !seen_sps {
                                tracing::debug!(bytes = nal.data.len(), "snapshot: first SPS captured");
                                seen_sps = true;
                            }
                            g.sps = Some(nal.data);
                        }
                        8 => {
                            if !seen_pps {
                                tracing::debug!(bytes = nal.data.len(), "snapshot: first PPS captured");
                                seen_pps = true;
                            }
                            g.pps = Some(nal.data);
                        }
                        5 => {
                            if !seen_idr {
                                tracing::debug!(bytes = nal.data.len(), "snapshot: first IDR captured");
                                seen_idr = true;
                            }
                            g.idr = Some(nal.data);
                        }
                        _ => {}
                    }
                }
            }
            Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                tracing::debug!(n, "snapshot consumer lagged; resetting reassembler");
                reassembler = h264::Reassembler::new();
            }
            Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
        }
    }
}
