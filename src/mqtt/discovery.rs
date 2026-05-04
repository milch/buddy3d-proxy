//! Home Assistant MQTT auto-discovery payload builders. Pure functions —
//! no I/O. The `Hub` publishes these once on startup to the configured
//! discovery prefix, retained.

use serde_json::{json, Value};

/// Topic + JSON payload for an HA discovery message.
#[derive(Debug, Clone)]
pub struct DiscoveryMessage {
    pub topic: String,
    pub payload: Value,
}

/// Identity + topic configuration shared by every discovery message.
#[derive(Debug, Clone)]
pub struct DeviceIdentity {
    pub camera_id: String,
    pub camera_name: String,
    pub sw_version: String,
    pub topic_prefix: String,     // e.g. "buddy3d-proxy"
    pub discovery_prefix: String, // e.g. "homeassistant"
}

impl DeviceIdentity {
    /// HA `device` block included in every entity's config payload.
    pub fn device_block(&self) -> Value {
        json!({
            "identifiers": [format!("buddy3d-proxy-{}", self.camera_id)],
            "name": self.camera_name,
            "manufacturer": "Prusa",
            "model": "Buddy3D Camera",
            "sw_version": self.sw_version,
        })
    }

    /// Per-camera MQTT topic root (e.g. "buddy3d-proxy/cam123").
    pub fn topic_root(&self) -> String {
        format!("{}/{}", self.topic_prefix, self.camera_id)
    }

    /// HA discovery topic for one entity (component, object_id).
    pub fn discovery_topic(&self, component: &str, object_id: &str) -> String {
        format!(
            "{}/{}/buddy3d_proxy_{}/{}/config",
            self.discovery_prefix, component, self.camera_id, object_id
        )
    }
}

pub fn camera(id: &DeviceIdentity) -> DiscoveryMessage {
    DiscoveryMessage {
        topic: id.discovery_topic("camera", "snapshot"),
        payload: json!({
            "name": "Snapshot",
            "unique_id": format!("buddy3d_proxy_{}_camera", id.camera_id),
            "topic": format!("{}/snapshot", id.topic_root()),
            "availability_topic": format!("{}/availability", id.topic_root()),
            "device": id.device_block(),
        }),
    }
}

pub fn state_sensor(id: &DeviceIdentity) -> DiscoveryMessage {
    DiscoveryMessage {
        topic: id.discovery_topic("sensor", "state"),
        payload: json!({
            "name": "State",
            "unique_id": format!("buddy3d_proxy_{}_state", id.camera_id),
            "state_topic": format!("{}/state", id.topic_root()),
            "availability_topic": format!("{}/availability", id.topic_root()),
            "icon": "mdi:cctv",
            "device": id.device_block(),
        }),
    }
}

pub fn mode_select(id: &DeviceIdentity) -> DiscoveryMessage {
    DiscoveryMessage {
        topic: id.discovery_topic("select", "mode"),
        payload: json!({
            "name": "Mode",
            "unique_id": format!("buddy3d_proxy_{}_mode", id.camera_id),
            "command_topic": format!("{}/mode/set", id.topic_root()),
            "state_topic": format!("{}/mode/state", id.topic_root()),
            "options": ["Auto", "Day", "Night"],
            "availability_topic": format!("{}/availability", id.topic_root()),
            "icon": "mdi:weather-night",
            "device": id.device_block(),
        }),
    }
}

pub fn quality_select(id: &DeviceIdentity) -> DiscoveryMessage {
    DiscoveryMessage {
        topic: id.discovery_topic("select", "quality"),
        payload: json!({
            "name": "Quality",
            "unique_id": format!("buddy3d_proxy_{}_quality", id.camera_id),
            "command_topic": format!("{}/quality/set", id.topic_root()),
            "state_topic": format!("{}/quality/state", id.topic_root()),
            "options": ["SD", "HD", "FHD"],
            "availability_topic": format!("{}/availability", id.topic_root()),
            "icon": "mdi:high-definition",
            "device": id.device_block(),
        }),
    }
}

pub fn reboot_button(id: &DeviceIdentity) -> DiscoveryMessage {
    DiscoveryMessage {
        topic: id.discovery_topic("button", "reboot"),
        payload: json!({
            "name": "Reboot Camera",
            "unique_id": format!("buddy3d_proxy_{}_reboot", id.camera_id),
            "command_topic": format!("{}/reboot/press", id.topic_root()),
            "payload_press": "PRESS",
            "availability_topic": format!("{}/availability", id.topic_root()),
            "device_class": "restart",
            "device": id.device_block(),
        }),
    }
}

/// All discovery messages, in publish order.
pub fn all(id: &DeviceIdentity) -> Vec<DiscoveryMessage> {
    vec![
        camera(id),
        state_sensor(id),
        mode_select(id),
        quality_select(id),
        reboot_button(id),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture_identity() -> DeviceIdentity {
        DeviceIdentity {
            camera_id: "cam123".into(),
            camera_name: "Living Room".into(),
            sw_version: "0.2.0".into(),
            topic_prefix: "buddy3d-proxy".into(),
            discovery_prefix: "homeassistant".into(),
        }
    }

    #[test]
    fn camera_payload_uses_correct_topic_and_id() {
        let id = fixture_identity();
        let msg = camera(&id);
        assert_eq!(
            msg.topic,
            "homeassistant/camera/buddy3d_proxy_cam123/snapshot/config"
        );
        assert_eq!(msg.payload["unique_id"], "buddy3d_proxy_cam123_camera");
        assert_eq!(msg.payload["topic"], "buddy3d-proxy/cam123/snapshot");
        assert_eq!(msg.payload["availability_topic"], "buddy3d-proxy/cam123/availability");
        assert_eq!(msg.payload["device"]["identifiers"][0], "buddy3d-proxy-cam123");
    }

    #[test]
    fn mode_select_options_are_auto_day_night() {
        let id = fixture_identity();
        let msg = mode_select(&id);
        assert_eq!(msg.payload["options"], json!(["Auto", "Day", "Night"]));
        assert_eq!(msg.payload["command_topic"], "buddy3d-proxy/cam123/mode/set");
        assert_eq!(msg.payload["state_topic"], "buddy3d-proxy/cam123/mode/state");
    }

    #[test]
    fn quality_select_options_are_sd_hd_fhd() {
        let id = fixture_identity();
        let msg = quality_select(&id);
        assert_eq!(msg.payload["options"], json!(["SD", "HD", "FHD"]));
    }

    #[test]
    fn reboot_button_uses_restart_device_class() {
        let id = fixture_identity();
        let msg = reboot_button(&id);
        assert_eq!(msg.payload["device_class"], "restart");
        assert_eq!(msg.payload["payload_press"], "PRESS");
    }

    #[test]
    fn all_returns_five_messages() {
        let id = fixture_identity();
        assert_eq!(all(&id).len(), 5);
    }

    #[test]
    fn all_messages_share_same_device_identifier() {
        let id = fixture_identity();
        let identifier = json!("buddy3d-proxy-cam123");
        for msg in all(&id) {
            assert_eq!(msg.payload["device"]["identifiers"][0], identifier);
        }
    }
}
