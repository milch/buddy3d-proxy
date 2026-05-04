//! MQTT subsystem: HA auto-discovery, snapshot publishing, command routing.

pub mod commands;
pub mod discovery;
pub mod state;
pub mod transient;

mod hub;
pub use hub::{Hub, HubConfig, HubError};
