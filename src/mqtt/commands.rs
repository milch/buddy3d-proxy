//! Inbound MQTT command parsing + dispatch.

use crate::live_outbound::LiveOutbound;
use crate::mqtt::transient::TransientSignaler;
use crate::prusa::commands::{encode_camera_trigger, encode_set_mode, encode_set_quality};
use crate::prusa::signaling::client::Outbound;
use bytes::Bytes;
use std::sync::Arc;

/// Parsed inbound command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Command {
    SetMode(u32),    // 1=Auto, 2=Day, 3=Night
    SetQuality(u32), // 1=SD, 2=HD, 3=FHD
    Reboot,
}

/// Topic suffix → command kind. Returns `None` for unrecognized payloads
/// (logged + dropped at the call site).
pub fn parse(topic_suffix: &str, payload: &[u8]) -> Option<Command> {
    let payload = std::str::from_utf8(payload).ok()?.trim();
    match topic_suffix {
        "mode/set" => match payload {
            "Auto" => Some(Command::SetMode(1)),
            "Day" => Some(Command::SetMode(2)),
            "Night" => Some(Command::SetMode(3)),
            _ => None,
        },
        "quality/set" => match payload {
            "SD" => Some(Command::SetQuality(1)),
            "HD" => Some(Command::SetQuality(2)),
            "FHD" => Some(Command::SetQuality(3)),
            _ => None,
        },
        "reboot/press" => Some(Command::Reboot),
        _ => None,
    }
}

/// Sends commands. Uses the live signaling channel when a session is up,
/// otherwise brings up a transient signaling-only connection.
pub struct Dispatcher {
    pub live_outbound: LiveOutbound,
    pub transient: Arc<TransientSignaler>,
    pub camera_token: String,
}

impl Dispatcher {
    /// Send a command. Returns Ok on successful dispatch (does NOT wait for
    /// the camera to acknowledge).
    pub async fn dispatch(&self, cmd: Command) -> Result<(), DispatchError> {
        let (kind, payload) = match cmd {
            Command::SetMode(m) => ("configuration", encode_set_mode(m, &self.camera_token)),
            Command::SetQuality(q) => ("configuration", encode_set_quality(q, &self.camera_token)),
            Command::Reboot => ("trigger", encode_camera_trigger(9, &self.camera_token)),
        };

        // Try live first.
        let live = self.live_outbound.lock().await.clone();
        if let Some(out) = live {
            let event = Outbound::BinaryEvent {
                name: kind.to_string(),
                payload: Bytes::from(payload.clone()),
                expect_ack: false,
            };
            if out.send(event).await.is_ok() {
                tracing::info!(?cmd, "sent command via live signaling");
                return Ok(());
            }
            tracing::warn!(?cmd, "live signaling send failed; falling back to transient");
        }

        self.transient
            .send(kind, payload)
            .await
            .map_err(DispatchError::Transient)?;
        tracing::info!(?cmd, "sent command via transient signaling");
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum DispatchError {
    #[error("transient signaling: {0}")]
    Transient(#[from] crate::mqtt::transient::TransientError),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mode_set_parses_three_modes() {
        assert_eq!(parse("mode/set", b"Auto"), Some(Command::SetMode(1)));
        assert_eq!(parse("mode/set", b"Day"), Some(Command::SetMode(2)));
        assert_eq!(parse("mode/set", b"Night"), Some(Command::SetMode(3)));
    }

    #[test]
    fn quality_set_parses_three_qualities() {
        assert_eq!(parse("quality/set", b"SD"), Some(Command::SetQuality(1)));
        assert_eq!(parse("quality/set", b"HD"), Some(Command::SetQuality(2)));
        assert_eq!(parse("quality/set", b"FHD"), Some(Command::SetQuality(3)));
    }

    #[test]
    fn reboot_press_accepts_any_payload() {
        assert_eq!(parse("reboot/press", b"PRESS"), Some(Command::Reboot));
        assert_eq!(parse("reboot/press", b""), Some(Command::Reboot));
        assert_eq!(parse("reboot/press", b"anything"), Some(Command::Reboot));
    }

    #[test]
    fn unrecognized_topic_returns_none() {
        assert_eq!(parse("garbage", b"PRESS"), None);
    }

    #[test]
    fn unrecognized_payload_returns_none() {
        assert_eq!(parse("mode/set", b"Bogus"), None);
        assert_eq!(parse("quality/set", b"4K"), None);
    }

    #[test]
    fn payload_whitespace_is_trimmed() {
        assert_eq!(parse("mode/set", b"  Day\n"), Some(Command::SetMode(2)));
    }
}
