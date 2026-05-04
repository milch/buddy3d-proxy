//! Snapshot subsystem: reassembles H.264 NAL units from the supervisor's RTP
//! broadcast channel, decodes the latest IDR via openh264, encodes JPEG, and
//! hands the bytes to the MQTT hub.

pub mod encode;
pub mod h264;

use crate::rtsp::sdp::H264Params;
use crate::supervisor::{State, Supervisor};
use bytes::Bytes;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

/// Spawn the snapshot worker. It subscribes to the supervisor's RTP
/// broadcast on every state-transition into `Streaming`, runs the
/// reassembler, and ticks every `interval` to publish the latest IDR
/// (decoded to JPEG) via `publish`.
///
/// `interval` of `Duration::ZERO` disables snapshot publishing entirely.
pub async fn run(
    supervisor: Arc<Supervisor>,
    interval: Duration,
    sps_pps: Arc<Mutex<Option<(Bytes, Bytes)>>>,
    publish: impl Fn(Bytes) + Send + Sync + 'static,
) {
    if interval.is_zero() {
        tracing::info!("snapshot disabled (interval=0)");
        return;
    }

    let publish = Arc::new(publish);
    let mut state_rx = supervisor.state_changes();
    let latest_idr: Arc<Mutex<Option<Bytes>>> = Arc::new(Mutex::new(None));
    let mut current_session: Option<tokio::task::JoinHandle<()>> = None;

    loop {
        // Re-evaluate on every state transition.
        let snap = supervisor.snapshot().await;
        if snap.state == State::Streaming {
            if current_session.as_ref().map_or(true, |h| h.is_finished()) {
                if let Some(rx) = supervisor.subscribe_rtp().await {
                    let latest_for_consumer = latest_idr.clone();
                    current_session = Some(tokio::spawn(consume_rtp(rx, latest_for_consumer)));
                }
            }
        } else if let Some(h) = current_session.take() {
            h.abort();
            *latest_idr.lock().await = None;
        }

        tokio::select! {
            res = state_rx.changed() => {
                if res.is_err() { return; }
            }
            _ = tokio::time::sleep(interval) => {
                if matches!(supervisor.snapshot().await.state, State::Streaming) {
                    let idr = latest_idr.lock().await.clone();
                    let params = sps_pps.lock().await.clone();
                    if let (Some(idr_bytes), Some((sps, pps))) = (idr, params) {
                        let publish = publish.clone();
                        tokio::task::spawn_blocking(move || {
                            let annex_b = encode::build_annex_b(&sps, &pps, &idr_bytes);
                            match encode::decode_to_jpeg(&annex_b) {
                                Ok(jpeg) => publish(jpeg),
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
    latest_idr: Arc<Mutex<Option<Bytes>>>,
) {
    let mut reassembler = h264::Reassembler::new();
    loop {
        match rx.recv().await {
            Ok(pkt) => {
                let nals = reassembler.push(&pkt.payload, pkt.header.sequence_number);
                for nal in nals {
                    if nal.is_idr() {
                        *latest_idr.lock().await = Some(nal.data);
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

/// Helper for the orchestrator caller: extract SPS + PPS bytes from a
/// negotiated `H264Params` (sprop-parameter-sets is base64-encoded
/// comma-separated parameter sets). Returns (sps, pps). Returns `None`
/// if the params don't decode to exactly two NALs.
pub fn extract_sps_pps(params: &H264Params) -> Option<(Bytes, Bytes)> {
    use base64::Engine;
    let engine = base64::engine::general_purpose::STANDARD;
    let sets: Vec<_> = params
        .sprop_parameter_sets
        .split(',')
        .filter_map(|s| engine.decode(s).ok())
        .collect();
    if sets.len() != 2 {
        return None;
    }
    Some((Bytes::from(sets[0].clone()), Bytes::from(sets[1].clone())))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rtsp::sdp::H264Params;

    #[test]
    fn extract_sps_pps_decodes_two_base64_parameter_sets() {
        let params = H264Params {
            profile_level_id: "42c01e".into(),
            // Z0LAHto= decodes to 5 bytes (SPS), aM44gA== decodes to 4 bytes (PPS)
            sprop_parameter_sets: "Z0LAHto=,aM44gA==".into(),
            packetization_mode: 1,
            payload_type: 96,
        };
        let (sps, pps) = extract_sps_pps(&params).expect("decode");
        assert_eq!(sps.len(), 5);
        assert_eq!(pps.len(), 4);
    }

    #[test]
    fn extract_sps_pps_returns_none_on_malformed() {
        let params = H264Params {
            profile_level_id: "42c01e".into(),
            sprop_parameter_sets: "not-base64".into(),
            packetization_mode: 1,
            payload_type: 96,
        };
        assert!(extract_sps_pps(&params).is_none());
    }
}
