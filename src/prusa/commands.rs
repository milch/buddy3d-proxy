//! Pre-encoded protobuf payloads for Prusa camera commands.
//!
//! These produce raw `bytes` payloads suitable for the
//! `PrusaSignaling::send_trigger` / `send_configuration` channels.
//! Field numbers were reverse-engineered from captured browser sessions
//! (see commit history for `restart-camera`, `set-mode`, `set-quality`).

/// Encode a `CameraTrigger` protobuf with `field_num: 1` and `token` at field 11.
/// Wire format: each `<tag><value>` pair, where tag = (field_num << 3) | wire_type.
/// uint32 is wire type 0 (varint), string is wire type 2 (LEN).
///
/// Known field numbers (action triggers — value=1 to fire):
/// - 9 = start_device_reboot
/// - 3 = subscribe to settings (likely "request_settings"; required before
///   the server accepts any `configuration` event)
pub fn encode_camera_trigger(field_num: u32, token: &str) -> Vec<u8> {
    let mut buf = Vec::with_capacity(token.len() + 8);
    let tag = field_num << 3;
    encode_varint(&mut buf, tag as u64);
    encode_varint(&mut buf, 1);
    encode_varint(&mut buf, ((11u64) << 3) | 2);
    encode_varint(&mut buf, token.len() as u64);
    buf.extend_from_slice(token.as_bytes());
    buf
}

/// Encode a `Configuration` protobuf for SetMode (IR / day-night):
///   field 3 (LEN, sub-message) = { field 4 (varint) = mode }
///   field 6 (LEN, string)      = camera_token
/// Mode values: 1 = Auto, 2 = Day, 3 = Night.
pub fn encode_set_mode(mode: u32, token: &str) -> Vec<u8> {
    let mut sub = Vec::with_capacity(4);
    encode_varint(&mut sub, ((4u64) << 3) | 0);
    encode_varint(&mut sub, mode as u64);

    let mut buf = Vec::with_capacity(sub.len() + token.len() + 6);
    encode_varint(&mut buf, ((3u64) << 3) | 2);
    encode_varint(&mut buf, sub.len() as u64);
    buf.extend_from_slice(&sub);
    encode_varint(&mut buf, ((6u64) << 3) | 2);
    encode_varint(&mut buf, token.len() as u64);
    buf.extend_from_slice(token.as_bytes());
    buf
}

/// Encode a `Configuration` protobuf for SetQuality (video resolution):
///   field 4 (LEN, bytes)       = empty
///   field 6 (LEN, string)      = camera_token
///   field 8 (LEN, sub-message) = { field 1 (varint) = quality }
/// Quality values: 1 = SD, 2 = HD, 3 = FHD (1080p).
pub fn encode_set_quality(quality: u32, token: &str) -> Vec<u8> {
    let mut sub = Vec::with_capacity(4);
    encode_varint(&mut sub, ((1u64) << 3) | 0);
    encode_varint(&mut sub, quality as u64);

    let mut buf = Vec::with_capacity(sub.len() + token.len() + 6);
    encode_varint(&mut buf, ((4u64) << 3) | 2);
    encode_varint(&mut buf, 0);
    encode_varint(&mut buf, ((6u64) << 3) | 2);
    encode_varint(&mut buf, token.len() as u64);
    buf.extend_from_slice(token.as_bytes());
    encode_varint(&mut buf, ((8u64) << 3) | 2);
    encode_varint(&mut buf, sub.len() as u64);
    buf.extend_from_slice(&sub);
    buf
}

pub fn encode_varint(buf: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        buf.push((value as u8) | 0x80);
        value >>= 7;
    }
    buf.push(value as u8);
}

#[cfg(test)]
mod tests {
    use super::*;

    const TOKEN: &str = "I47hvQfXx6SOPWD4bO00";

    #[test]
    fn restart_camera_matches_captured_payload() {
        // Real reboot payload from api/2025-05-03 14-05.har:
        //   `48 01 5a 14 [token]` = field 9 = 1, field 11 = token
        let bytes = encode_camera_trigger(9, TOKEN);
        let mut expected = vec![0x48, 0x01, 0x5a, 0x14];
        expected.extend_from_slice(TOKEN.as_bytes());
        assert_eq!(bytes, expected);
    }

    #[test]
    fn set_mode_matches_captured_payload() {
        // Real mode payload from api/2025-05-03 14-10.har (mode=1):
        //   `1a 02 20 01 32 14 [token]`
        let bytes = encode_set_mode(1, TOKEN);
        let mut expected = vec![0x1a, 0x02, 0x20, 0x01, 0x32, 0x14];
        expected.extend_from_slice(TOKEN.as_bytes());
        assert_eq!(bytes, expected);
    }

    #[test]
    fn set_quality_matches_captured_payload() {
        // Real quality payload from user-pasted resolution-change session:
        //   `22 00 32 14 [token] 42 02 08 03` (quality=3)
        let bytes = encode_set_quality(3, TOKEN);
        let mut expected = vec![0x22, 0x00, 0x32, 0x14];
        expected.extend_from_slice(TOKEN.as_bytes());
        expected.extend_from_slice(&[0x42, 0x02, 0x08, 0x03]);
        assert_eq!(bytes, expected);
    }
}
