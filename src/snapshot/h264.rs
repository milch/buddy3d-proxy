//! RTP → H.264 NAL unit reassembly per RFC 6184.
//!
//! Handles the three packet types Prusa's WebRTC stack actually uses:
//! - Single NAL unit (NAL types 1–23)
//! - STAP-A (NAL type 24): one packet aggregates multiple NAL units
//! - FU-A (NAL type 28): one NAL unit is fragmented across multiple packets
//!
//! FU-B (29) and MTAP (25/26/27) are not used by Prusa and are dropped.

use bytes::Bytes;

/// Reassembled H.264 NAL unit. Bytes do NOT include start codes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Nal {
    pub data: Bytes,
}

impl Nal {
    /// First byte: forbidden_zero_bit (1) | nal_ref_idc (2) | nal_unit_type (5).
    pub fn unit_type(&self) -> u8 {
        self.data.first().copied().unwrap_or(0) & 0x1F
    }

    /// IDR slice = NAL unit type 5.
    pub fn is_idr(&self) -> bool {
        self.unit_type() == 5
    }
}

/// Stateful reassembler. One per RTP stream. Not thread-safe — driven by a
/// single tokio task that owns the broadcast receiver.
#[derive(Default)]
pub struct Reassembler {
    /// In-progress FU-A payload (after the first fragment, before the last).
    fu_a_buf: Option<Vec<u8>>,
    /// NAL header byte to prepend when the FU-A completes.
    fu_a_nal_header: u8,
    /// Last RTP sequence number we observed; used to detect gaps that would
    /// corrupt an in-progress FU-A.
    last_seq: Option<u16>,
}

impl Reassembler {
    pub fn new() -> Self {
        Self::default()
    }

    /// Feed one RTP payload (the bytes after the RTP header). `seq` is the
    /// RTP sequence number from the packet header. Returns 0+ complete NAL
    /// units extracted from this packet.
    pub fn push(&mut self, payload: &[u8], seq: u16) -> Vec<Nal> {
        if payload.is_empty() {
            return Vec::new();
        }

        // Sequence-gap detection: if the previous packet's seq + 1 != this seq,
        // discard any in-progress FU-A.
        if let Some(prev) = self.last_seq {
            if seq != prev.wrapping_add(1) && self.fu_a_buf.is_some() {
                tracing::debug!(prev_seq = prev, this_seq = seq, "rtp gap; dropping in-progress FU-A");
                self.fu_a_buf = None;
            }
        }
        self.last_seq = Some(seq);

        let nal_header = payload[0];
        let unit_type = nal_header & 0x1F;

        match unit_type {
            1..=23 => {
                // Single NAL.
                vec![Nal { data: Bytes::copy_from_slice(payload) }]
            }
            24 => {
                // STAP-A: skip the leading STAP-A header byte; remaining payload
                // is a sequence of [u16 length][NAL bytes].
                let mut out = Vec::new();
                let mut i = 1;
                while i + 2 <= payload.len() {
                    let len = u16::from_be_bytes([payload[i], payload[i + 1]]) as usize;
                    i += 2;
                    if i + len > payload.len() {
                        break;
                    }
                    out.push(Nal { data: Bytes::copy_from_slice(&payload[i..i + len]) });
                    i += len;
                }
                out
            }
            28 => {
                // FU-A: byte 0 = FU indicator (NRI etc.), byte 1 = FU header
                // (start | end | reserved | original NAL type).
                if payload.len() < 2 {
                    return Vec::new();
                }
                let fu_header = payload[1];
                let start = (fu_header & 0x80) != 0;
                let end = (fu_header & 0x40) != 0;
                let original_type = fu_header & 0x1F;

                if start {
                    // Reconstruct the original NAL header: F+NRI from the FU
                    // indicator (high 3 bits) + Type from the FU header.
                    let nri = nal_header & 0xE0;
                    self.fu_a_nal_header = nri | original_type;
                    let mut buf = Vec::with_capacity(payload.len());
                    buf.push(self.fu_a_nal_header);
                    buf.extend_from_slice(&payload[2..]);
                    self.fu_a_buf = Some(buf);
                    return Vec::new();
                }

                if let Some(buf) = self.fu_a_buf.as_mut() {
                    buf.extend_from_slice(&payload[2..]);
                    if end {
                        let buf = self.fu_a_buf.take().expect("just checked Some");
                        return vec![Nal { data: Bytes::from(buf) }];
                    }
                }
                Vec::new()
            }
            _ => {
                // FU-B (29) and MTAP variants not used by Prusa — drop.
                Vec::new()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn idr_byte(nri: u8) -> u8 {
        // F=0, NRI=nri, Type=5 (IDR).
        (nri << 5) | 5
    }

    #[test]
    fn single_nal_emits_immediately() {
        let mut r = Reassembler::new();
        let payload = vec![idr_byte(3), 0xAA, 0xBB];
        let nals = r.push(&payload, 100);
        assert_eq!(nals.len(), 1);
        assert!(nals[0].is_idr());
        assert_eq!(nals[0].data.as_ref(), &payload[..]);
    }

    #[test]
    fn fu_a_round_trip_reconstructs_full_idr() {
        let mut r = Reassembler::new();
        // FU-A indicator: F=0, NRI=3, Type=28
        let fu_indicator = (3u8 << 5) | 28;
        // Start fragment: FU header start=1, type=5 (IDR)
        let start = vec![fu_indicator, 0x80 | 5, 0x01, 0x02];
        // Middle: start=0 end=0
        let middle = vec![fu_indicator, 0x05, 0x03, 0x04];
        // End: start=0 end=1
        let end = vec![fu_indicator, 0x40 | 5, 0x05, 0x06];
        assert!(r.push(&start, 10).is_empty());
        assert!(r.push(&middle, 11).is_empty());
        let nals = r.push(&end, 12);
        assert_eq!(nals.len(), 1);
        assert!(nals[0].is_idr());
        // Reconstructed header (NRI=3 | type=5) + payload bytes from all 3 fragments.
        assert_eq!(nals[0].data.as_ref(), &[idr_byte(3), 0x01, 0x02, 0x03, 0x04, 0x05, 0x06]);
    }

    #[test]
    fn stap_a_splits_into_multiple_nals() {
        let mut r = Reassembler::new();
        // STAP-A indicator: NRI=3, type=24
        let stap_indicator = (3u8 << 5) | 24;
        // Two NALs: SPS (type 7) of length 4 and PPS (type 8) of length 3.
        let sps = vec![(3u8 << 5) | 7, 0x10, 0x11, 0x12];
        let pps = vec![(3u8 << 5) | 8, 0x20, 0x21];
        let mut payload = vec![stap_indicator];
        payload.extend_from_slice(&(sps.len() as u16).to_be_bytes());
        payload.extend_from_slice(&sps);
        payload.extend_from_slice(&(pps.len() as u16).to_be_bytes());
        payload.extend_from_slice(&pps);
        let nals = r.push(&payload, 50);
        assert_eq!(nals.len(), 2);
        assert_eq!(nals[0].unit_type(), 7);
        assert_eq!(nals[1].unit_type(), 8);
        assert_eq!(nals[0].data.as_ref(), sps.as_slice());
        assert_eq!(nals[1].data.as_ref(), pps.as_slice());
    }

    #[test]
    fn fu_a_gap_discards_in_progress_fragment() {
        let mut r = Reassembler::new();
        let fu_indicator = (3u8 << 5) | 28;
        let start = vec![fu_indicator, 0x80 | 5, 0xAA];
        let end = vec![fu_indicator, 0x40 | 5, 0xBB];
        assert!(r.push(&start, 10).is_empty());
        // Skip seq 11 — gap. The next packet is the END of the FU-A; the
        // reassembler should have discarded the partial buffer and emit nothing.
        let nals = r.push(&end, 12);
        assert!(nals.is_empty(), "expected discard after gap, got {nals:?}");
    }
}
