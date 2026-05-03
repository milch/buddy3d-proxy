//! prost-build emits Rust code into OUT_DIR at compile time.
include!(concat!(env!("OUT_DIR"), "/buddy3d.rs"));

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message;

    #[test]
    fn client_authentication_round_trips() {
        let msg = ClientAuthentication {
            token: "test-token".to_string(),
            client_kind: "client".to_string(),
            ..Default::default()
        };
        let mut buf = Vec::new();
        msg.encode(&mut buf).unwrap();
        let decoded = ClientAuthentication::decode(buf.as_slice()).unwrap();
        assert_eq!(decoded.token, "test-token");
        assert_eq!(decoded.client_kind, "client");
    }

    #[test]
    fn trigger_round_trips() {
        let msg = Trigger {
            field1: 1,
            token: "I47hvQfXx6SOPWD4bO00".to_string(),
            ..Default::default()
        };
        let mut buf = Vec::new();
        msg.encode(&mut buf).unwrap();
        let decoded = Trigger::decode(buf.as_slice()).unwrap();
        assert_eq!(decoded.field1, 1);
        assert_eq!(decoded.token, "I47hvQfXx6SOPWD4bO00");
    }

    #[test]
    fn webrtc_signal_round_trips() {
        let msg = WebRtcSignal {
            token: "tok".to_string(),
            session_id: "sid".to_string(),
            peer_id: "pid".to_string(),
            msg_type: 4,
            direction: 2,
            ..Default::default()
        };
        let mut buf = Vec::new();
        msg.encode(&mut buf).unwrap();
        let decoded = WebRtcSignal::decode(buf.as_slice()).unwrap();
        assert_eq!(decoded.msg_type, 4);
        assert_eq!(decoded.direction, 2);
    }

    #[test]
    fn status_round_trips() {
        let msg = Status {
            token: "tok".to_string(),
            camera_id: "cam".to_string(),
            ..Default::default()
        };
        let mut buf = Vec::new();
        msg.encode(&mut buf).unwrap();
        let decoded = Status::decode(buf.as_slice()).unwrap();
        assert_eq!(decoded.token, "tok");
        assert_eq!(decoded.camera_id, "cam");
    }

    #[test]
    fn features_round_trips() {
        let msg = Features {
            token: "tok".to_string(),
            fw_version: "3.1.4".to_string(),
            features_json: r#"["WebRtc","VideoStream"]"#.to_string(),
            ..Default::default()
        };
        let mut buf = Vec::new();
        msg.encode(&mut buf).unwrap();
        let decoded = Features::decode(buf.as_slice()).unwrap();
        assert_eq!(decoded.fw_version, "3.1.4");
        assert_eq!(decoded.features_json, r#"["WebRtc","VideoStream"]"#);
    }

    // Verify that a real captured Trigger 1 wire frame decodes correctly.
    // Wire bytes: 08 01 5a 14 <20 bytes of token>
    // f1=varint(1), f11=string("I47hvQfXx6SOPWD4bO00")
    #[test]
    fn trigger_decode_real_wire() {
        let wire: &[u8] = &[
            0x08, 0x01,
            0x5a, 0x14,
            b'I', b'4', b'7', b'h', b'v', b'Q', b'f', b'X', b'x', b'6',
            b'S', b'O', b'P', b'W', b'D', b'4', b'b', b'O', b'0', b'0',
        ];
        let decoded = Trigger::decode(wire).unwrap();
        assert_eq!(decoded.field1, 1);
        assert_eq!(decoded.token, "I47hvQfXx6SOPWD4bO00");
    }

    // Verify ClientAuthentication top-level field numbers using real captured prefix.
    // Wire starts: 0a 14 [20-byte token] 12 06 "client" 1a ...
    #[test]
    fn client_auth_decode_real_prefix() {
        let token = b"I47hvQfXx6SOPWD4bO00";
        let kind = b"client";
        let mut wire = Vec::new();
        wire.push(0x0a);
        wire.push(token.len() as u8);
        wire.extend_from_slice(token);
        wire.push(0x12);
        wire.push(kind.len() as u8);
        wire.extend_from_slice(kind);
        let decoded = ClientAuthentication::decode(wire.as_slice()).unwrap();
        assert_eq!(decoded.token, "I47hvQfXx6SOPWD4bO00");
        assert_eq!(decoded.client_kind, "client");
    }
}
