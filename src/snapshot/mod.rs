//! Snapshot subsystem: reassembles H.264 NAL units from the supervisor's RTP
//! broadcast channel, decodes the latest IDR via openh264, encodes JPEG, and
//! hands the bytes to the MQTT hub.

pub mod encode;
pub mod h264;
