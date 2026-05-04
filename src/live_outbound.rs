//! Shared registry holding the live signaling-outbound channel when a session
//! is up. The WebRtcFactory writes here on connect and clears on tear-down;
//! the MQTT command dispatcher reads to fast-path commands while a session
//! is active. When `None`, commands fall back to a transient signaling
//! connection.

use crate::prusa::signaling::client::Outbound;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};

pub type LiveOutbound = Arc<Mutex<Option<mpsc::Sender<Outbound>>>>;

pub fn empty() -> LiveOutbound {
    Arc::new(Mutex::new(None))
}
