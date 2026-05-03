//! Socket.IO v4 packet types and binary-attachment correlator.
//!
//! Socket.IO sits on top of Engine.IO MESSAGE frames (`4<rest>`). The first char
//! of `<rest>` is the Socket.IO packet type. BINARY_EVENT (`5N-...`) text frames
//! promise N subsequent binary opcode-2 frames; we accumulate them into a single
//! delivered event.

use bytes::Bytes;
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq)]
pub enum TextPacket {
    /// `0[<json>]` — CONNECT (with optional auth body).
    Connect { auth: Option<serde_json::Value> },
    /// `1` — DISCONNECT.
    Disconnect,
    /// `2[<id>]["event_name", arg1, ...]` — EVENT, no binary attachments.
    Event {
        id: Option<u64>,
        name: String,
        args: Vec<serde_json::Value>,
    },
    /// `3<id>[arg1, ...]` — ACK in response to an Event with id.
    Ack { id: u64, args: Vec<serde_json::Value> },
    /// `5N-<id?>["event_name", {placeholders}, ...]` — header for a BINARY_EVENT
    /// with N attachments still pending.
    BinaryEventHeader {
        attachment_count: u32,
        id: Option<u64>,
        name: String,
        placeholders: Vec<u32>,
    },
}

#[derive(Debug, thiserror::Error)]
pub enum ParseError {
    #[error("empty socket.io frame")]
    Empty,
    #[error("unknown socket.io packet type: {0:?}")]
    UnknownType(char),
    #[error("malformed socket.io payload: {0}")]
    BadJson(serde_json::Error),
    #[error("malformed binary-event header (expected `5N-`): {0:?}")]
    BadBinaryHeader(String),
    #[error("event payload missing event-name string")]
    MissingEventName,
}

pub fn parse_text(frame: &str) -> Result<TextPacket, ParseError> {
    let mut chars = frame.chars();
    let kind = chars.next().ok_or(ParseError::Empty)?;
    let rest = chars.as_str();
    match kind {
        '0' => {
            let auth = if rest.is_empty() {
                None
            } else {
                Some(serde_json::from_str(rest).map_err(ParseError::BadJson)?)
            };
            Ok(TextPacket::Connect { auth })
        }
        '1' => Ok(TextPacket::Disconnect),
        '2' => {
            let (id, body) = split_optional_id(rest);
            let args: Vec<serde_json::Value> =
                serde_json::from_str(body).map_err(ParseError::BadJson)?;
            let mut iter = args.into_iter();
            let name = iter
                .next()
                .and_then(|v| v.as_str().map(str::to_string))
                .ok_or(ParseError::MissingEventName)?;
            Ok(TextPacket::Event {
                id,
                name,
                args: iter.collect(),
            })
        }
        '3' => {
            let (id_opt, body) = split_optional_id(rest);
            let id = id_opt.ok_or_else(|| ParseError::BadBinaryHeader("ack without id".into()))?;
            let args: Vec<serde_json::Value> =
                serde_json::from_str(body).map_err(ParseError::BadJson)?;
            Ok(TextPacket::Ack { id, args })
        }
        '5' => {
            // `5<N>-[<id>]<json-array>`
            let dash_idx = rest
                .find('-')
                .ok_or_else(|| ParseError::BadBinaryHeader(format!("missing dash in: {rest}")))?;
            let n: u32 = rest[..dash_idx].parse().map_err(|_| {
                ParseError::BadBinaryHeader(format!(
                    "non-numeric attachment count: {}",
                    &rest[..dash_idx]
                ))
            })?;
            let after_dash = &rest[dash_idx + 1..];
            let (id, body) = split_optional_id(after_dash);
            let args: Vec<serde_json::Value> =
                serde_json::from_str(body).map_err(ParseError::BadJson)?;
            let mut iter = args.into_iter();
            let name = iter
                .next()
                .and_then(|v| v.as_str().map(str::to_string))
                .ok_or(ParseError::MissingEventName)?;
            let placeholders: Vec<u32> = iter
                .filter_map(|v| {
                    v.get("_placeholder")
                        .and_then(|p| p.as_bool())
                        .filter(|b| *b)
                        .and_then(|_| v.get("num").and_then(|n| n.as_u64().map(|x| x as u32)))
                })
                .collect();
            Ok(TextPacket::BinaryEventHeader {
                attachment_count: n,
                id,
                name,
                placeholders,
            })
        }
        other => Err(ParseError::UnknownType(other)),
    }
}

/// `<digits>[` — extract leading id if present, return (id?, remainder).
fn split_optional_id(s: &str) -> (Option<u64>, &str) {
    let split = s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len());
    if split == 0 {
        (None, s)
    } else {
        let (head, tail) = s.split_at(split);
        (head.parse().ok(), tail)
    }
}

/// Encodes a CONNECT packet with optional auth payload.
/// Returns the body to pass to `engineio::encode_message()`.
pub fn encode_connect(auth: &serde_json::Value) -> String {
    format!("0{auth}")
}

/// Encodes an EVENT (`5N-[...]`) with N binary attachments referenced as
/// `_placeholder:true, num:i`. Returns the text packet body (caller must
/// then send the binary attachments in order).
pub fn encode_binary_event(name: &str, attachment_count: u32) -> String {
    let mut placeholders: Vec<serde_json::Value> = (0..attachment_count)
        .map(|i| serde_json::json!({"_placeholder": true, "num": i}))
        .collect();
    let mut args: Vec<serde_json::Value> = vec![serde_json::Value::String(name.to_string())];
    args.append(&mut placeholders);
    let body = serde_json::to_string(&args).expect("placeholder json never fails");
    format!("5{attachment_count}-{body}")
}

/// Stateful correlator: feed it text packets and binary frames in arrival order;
/// it emits fully-assembled events as they complete.
#[derive(Default)]
pub struct Correlator {
    pending: Option<PendingBinaryEvent>,
}

struct PendingBinaryEvent {
    name: String,
    expected: u32,
    received: HashMap<u32, Bytes>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Event {
    /// A bare `2["event", ...]` event with no binary attachments.
    Text {
        name: String,
        args: Vec<serde_json::Value>,
    },
    /// A `5N-[...]` event with all N binary attachments collected, in `num` order.
    Binary {
        name: String,
        attachments: Vec<Bytes>,
    },
    /// CONNECT acknowledgement from the server.
    Connected { auth: Option<serde_json::Value> },
    /// DISCONNECT.
    Disconnected,
}

#[derive(Debug, thiserror::Error)]
pub enum CorrelatorError {
    #[error("parse error: {0}")]
    Parse(#[from] ParseError),
    #[error("received binary frame without preceding 5N- header")]
    UnexpectedBinary,
    #[error("received text frame while {received}/{expected} binary attachments still pending")]
    InterleavedText { received: u32, expected: u32 },
    #[error("received {n} binary frames but only {expected} expected")]
    TooManyBinaries { n: u32, expected: u32 },
}

impl Correlator {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn feed_text(&mut self, frame: &str) -> Result<Option<Event>, CorrelatorError> {
        if let Some(p) = &self.pending {
            return Err(CorrelatorError::InterleavedText {
                received: p.received.len() as u32,
                expected: p.expected,
            });
        }
        match parse_text(frame)? {
            TextPacket::Connect { auth } => Ok(Some(Event::Connected { auth })),
            TextPacket::Disconnect => Ok(Some(Event::Disconnected)),
            TextPacket::Event { name, args, .. } => Ok(Some(Event::Text { name, args })),
            TextPacket::Ack { .. } => Ok(None),
            TextPacket::BinaryEventHeader {
                attachment_count,
                name,
                ..
            } => {
                if attachment_count == 0 {
                    Ok(Some(Event::Binary {
                        name,
                        attachments: vec![],
                    }))
                } else {
                    self.pending = Some(PendingBinaryEvent {
                        name,
                        expected: attachment_count,
                        received: HashMap::new(),
                    });
                    Ok(None)
                }
            }
        }
    }

    pub fn feed_binary(&mut self, payload: Bytes) -> Result<Option<Event>, CorrelatorError> {
        let p = self
            .pending
            .as_mut()
            .ok_or(CorrelatorError::UnexpectedBinary)?;
        let next_num = p.received.len() as u32;
        if next_num >= p.expected {
            return Err(CorrelatorError::TooManyBinaries {
                n: next_num + 1,
                expected: p.expected,
            });
        }
        p.received.insert(next_num, payload);
        if (p.received.len() as u32) == p.expected {
            let pending = self.pending.take().unwrap();
            let attachments: Vec<Bytes> = (0..pending.expected)
                .map(|i| pending.received.get(&i).cloned().unwrap_or_default())
                .collect();
            return Ok(Some(Event::Binary {
                name: pending.name,
                attachments,
            }));
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_connect_with_auth() {
        let pkt = parse_text(r#"0{"token":"X"}"#).unwrap();
        match pkt {
            TextPacket::Connect { auth: Some(v) } => {
                assert_eq!(v["token"], "X");
            }
            other => panic!("got {other:?}"),
        }
    }

    #[test]
    fn parses_binary_event_header_from_har() {
        let pkt = parse_text(r#"51-13762549["status",{"_placeholder":true,"num":0}]"#).unwrap();
        match pkt {
            TextPacket::BinaryEventHeader {
                attachment_count,
                id,
                name,
                placeholders,
            } => {
                assert_eq!(attachment_count, 1);
                assert_eq!(id, Some(13762549));
                assert_eq!(name, "status");
                assert_eq!(placeholders, vec![0]);
            }
            other => panic!("got {other:?}"),
        }
    }

    #[test]
    fn parses_binary_event_header_no_id() {
        let pkt = parse_text(r#"51-["webrtc",{"_placeholder":true,"num":0}]"#).unwrap();
        match pkt {
            TextPacket::BinaryEventHeader {
                attachment_count,
                id,
                name,
                ..
            } => {
                assert_eq!(attachment_count, 1);
                assert!(id.is_none());
                assert_eq!(name, "webrtc");
            }
            other => panic!("got {other:?}"),
        }
    }

    #[test]
    fn encode_binary_event_uses_placeholders() {
        let body = encode_binary_event("webrtc", 1);
        assert_eq!(body, r#"51-["webrtc",{"_placeholder":true,"num":0}]"#);
    }

    #[test]
    fn correlator_assembles_binary_event() {
        let mut c = Correlator::new();
        let r = c.feed_text(r#"51-["webrtc",{"_placeholder":true,"num":0}]"#).unwrap();
        assert!(r.is_none());
        let r = c.feed_binary(Bytes::from_static(b"\x0a\x03foo")).unwrap();
        match r {
            Some(Event::Binary { name, attachments }) => {
                assert_eq!(name, "webrtc");
                assert_eq!(attachments.len(), 1);
                assert_eq!(&attachments[0][..], b"\x0a\x03foo");
            }
            other => panic!("got {other:?}"),
        }
    }

    #[test]
    fn correlator_rejects_text_during_pending_binaries() {
        let mut c = Correlator::new();
        c.feed_text(r#"52-["x",{"_placeholder":true,"num":0},{"_placeholder":true,"num":1}]"#)
            .unwrap();
        let err = c.feed_text("40").unwrap_err();
        assert!(matches!(
            err,
            CorrelatorError::InterleavedText {
                received: 0,
                expected: 2
            }
        ));
    }

    #[test]
    fn correlator_rejects_binary_without_header() {
        let mut c = Correlator::new();
        let err = c.feed_binary(Bytes::from_static(b"x")).unwrap_err();
        assert!(matches!(err, CorrelatorError::UnexpectedBinary));
    }
}
