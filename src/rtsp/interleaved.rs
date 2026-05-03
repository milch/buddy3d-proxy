//! RTP-over-TCP interleaved framing. RFC 2326 §10.12.
//!
//! Each frame: `$` (0x24) + channel id (u8) + length (u16 big-endian) + payload.
//! Length is the byte length of the payload, not counting the 4-byte header.

use bytes::{BufMut, BytesMut};

/// Maximum RTP payload that fits in a single interleaved frame.
pub const MAX_FRAME_PAYLOAD: usize = u16::MAX as usize;

/// Encode a single interleaved frame into the provided buffer.
/// Panics if `payload.len() > MAX_FRAME_PAYLOAD`.
pub fn encode_frame(buf: &mut BytesMut, channel: u8, payload: &[u8]) {
    assert!(
        payload.len() <= MAX_FRAME_PAYLOAD,
        "interleaved payload {} > {}",
        payload.len(),
        MAX_FRAME_PAYLOAD
    );
    buf.reserve(4 + payload.len());
    buf.put_u8(b'$');
    buf.put_u8(channel);
    buf.put_u16(payload.len() as u16);
    buf.put_slice(payload);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodes_minimal_frame() {
        let mut buf = BytesMut::new();
        encode_frame(&mut buf, 0, &[1, 2, 3]);
        // $ 0 0 3 0x01 0x02 0x03
        assert_eq!(&buf[..], &[0x24, 0x00, 0x00, 0x03, 0x01, 0x02, 0x03]);
    }

    #[test]
    fn encodes_channel_one() {
        let mut buf = BytesMut::new();
        encode_frame(&mut buf, 1, b"hello");
        assert_eq!(buf[0], 0x24);
        assert_eq!(buf[1], 1);
        assert_eq!(&buf[2..4], &[0x00, 0x05]);
        assert_eq!(&buf[4..], b"hello");
    }

    #[test]
    fn encodes_max_size_payload() {
        let payload = vec![0xAB; MAX_FRAME_PAYLOAD];
        let mut buf = BytesMut::new();
        encode_frame(&mut buf, 0, &payload);
        assert_eq!(buf.len(), 4 + MAX_FRAME_PAYLOAD);
        assert_eq!(&buf[2..4], &[0xFF, 0xFF]);
    }

    #[test]
    #[should_panic]
    fn panics_on_oversize_payload() {
        let payload = vec![0; MAX_FRAME_PAYLOAD + 1];
        let mut buf = BytesMut::new();
        encode_frame(&mut buf, 0, &payload);
    }
}
