//! Shared registries holding session-scoped state that crosses subsystem
//! boundaries.

use crate::prusa::signaling::client::Outbound;
use std::sync::Arc;
use tokio::sync::{mpsc, watch, Mutex};

/// Live signaling-outbound channel when a session is up. The WebRtcFactory
/// writes here on connect and clears on tear-down; the MQTT command
/// dispatcher reads to fast-path commands while a session is active. When
/// `None`, commands fall back to a transient signaling connection.
pub type LiveOutbound = Arc<Mutex<Option<mpsc::Sender<Outbound>>>>;

pub fn empty() -> LiveOutbound {
    Arc::new(Mutex::new(None))
}

/// Long-lived watch carrying the most recent `Status` event the camera
/// emitted on its signaling channel. Updated by the WebRtcFactory's
/// signaling-translation task on every session bring-up; consumed by the
/// MQTT subsystem to publish the camera's current mode + quality.
pub type CameraStatusWatch = watch::Sender<Option<crate::proto::Status>>;

pub fn camera_status_watch() -> Arc<CameraStatusWatch> {
    let (tx, _rx) = watch::channel(None);
    Arc::new(tx)
}
