//! Engine.IO v4 packet types. Engine.IO is the transport-layer protocol that
//! Socket.IO sits on. Over a WebSocket, every text frame begins with a single
//! ASCII digit indicating the packet type; binary frames are pass-through.

use serde::Deserialize;

#[derive(Debug, Clone, PartialEq)]
pub enum TextPacket {
    /// `0{"sid":"...","pingInterval":...,"pingTimeout":...,"maxPayload":...,"upgrades":[...]}`
    Open(OpenPayload),
    /// `1`
    Close,
    /// `2` (optionally `2probe`, but we ignore the probe variants on websocket-only transport).
    Ping,
    /// `3` (optionally `3probe`).
    Pong,
    /// `4<rest>` — Socket.IO payload follows.
    Message(String),
    /// `6` — server-initiated noop, used for upgrade timing. Ignore.
    Noop,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct OpenPayload {
    pub sid: String,
    #[serde(default)]
    pub upgrades: Vec<String>,
    pub ping_interval: u64,
    pub ping_timeout: u64,
    #[serde(default)]
    pub max_payload: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum ParseError {
    #[error("empty engine.io frame")]
    Empty,
    #[error("unknown engine.io packet type: {0:?}")]
    UnknownType(char),
    #[error("malformed engine.io OPEN payload: {0}")]
    BadOpen(serde_json::Error),
}

pub fn parse_text(frame: &str) -> Result<TextPacket, ParseError> {
    let mut chars = frame.chars();
    let kind = chars.next().ok_or(ParseError::Empty)?;
    let rest = chars.as_str();
    match kind {
        '0' => {
            #[derive(Deserialize)]
            struct Wire {
                sid: String,
                #[serde(default)]
                upgrades: Vec<String>,
                #[serde(rename = "pingInterval")]
                ping_interval: u64,
                #[serde(rename = "pingTimeout")]
                ping_timeout: u64,
                #[serde(rename = "maxPayload", default)]
                max_payload: u64,
            }
            let wire: Wire = serde_json::from_str(rest).map_err(ParseError::BadOpen)?;
            Ok(TextPacket::Open(OpenPayload {
                sid: wire.sid,
                upgrades: wire.upgrades,
                ping_interval: wire.ping_interval,
                ping_timeout: wire.ping_timeout,
                max_payload: wire.max_payload,
            }))
        }
        '1' => Ok(TextPacket::Close),
        '2' => Ok(TextPacket::Ping),
        '3' => Ok(TextPacket::Pong),
        '4' => Ok(TextPacket::Message(rest.to_string())),
        '6' => Ok(TextPacket::Noop),
        other => Err(ParseError::UnknownType(other)),
    }
}

pub fn encode_pong() -> String {
    "3".to_string()
}

pub fn encode_message(payload: &str) -> String {
    format!("4{payload}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_open_frame_from_har() {
        let frame = r#"0{"sid":"YmSO-Uzdh7dQZsPyP8Bo","upgrades":[],"pingInterval":25000,"pingTimeout":20000,"maxPayload":1000000}"#;
        let pkt = parse_text(frame).unwrap();
        match pkt {
            TextPacket::Open(p) => {
                assert_eq!(p.sid, "YmSO-Uzdh7dQZsPyP8Bo");
                assert_eq!(p.ping_interval, 25000);
                assert_eq!(p.ping_timeout, 20000);
                assert_eq!(p.max_payload, 1_000_000);
                assert!(p.upgrades.is_empty());
            }
            other => panic!("expected Open, got {other:?}"),
        }
    }

    #[test]
    fn parses_ping() {
        assert_eq!(parse_text("2").unwrap(), TextPacket::Ping);
    }

    #[test]
    fn parses_message() {
        let pkt = parse_text(r#"40{"sid":"abc"}"#).unwrap();
        assert_eq!(pkt, TextPacket::Message(r#"0{"sid":"abc"}"#.to_string()));
    }

    #[test]
    fn rejects_empty_frame() {
        assert!(matches!(parse_text(""), Err(ParseError::Empty)));
    }

    #[test]
    fn rejects_unknown_packet_type() {
        assert!(matches!(parse_text("9foo"), Err(ParseError::UnknownType('9'))));
    }

    #[test]
    fn encodes_pong_and_message() {
        assert_eq!(encode_pong(), "3");
        assert_eq!(encode_message("40"), "440");
    }
}
