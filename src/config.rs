use std::path::PathBuf;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct Config {
    pub prusa_email: String,
    pub prusa_password: String,
    pub prusa_printer_uuid: Option<String>,
    pub prusa_camera_id: Option<String>,
    pub rtsp_port: u16,
    pub rtsp_path: Option<String>,
    pub rtsp_bind_addr: String,
    pub idle_timeout: Duration,
    pub token_store_path: PathBuf,
    pub health_port: u16,
    pub metrics_interval: Duration,
    pub mqtt_broker_url: Option<url::Url>,
    pub mqtt_username: Option<String>,
    pub mqtt_password: Option<String>,
    pub mqtt_client_id: Option<String>,
    pub mqtt_discovery_prefix: String,
    pub mqtt_topic_prefix: String,
    pub snapshot_interval: Duration,
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("required environment variable {0} is unset")]
    Missing(&'static str),
    #[error("environment variable {0} has invalid value: {1}")]
    Invalid(&'static str, String),
}

impl Config {
    pub fn from_env() -> Result<Self, ConfigError> {
        fn req(k: &'static str) -> Result<String, ConfigError> {
            std::env::var(k).map_err(|_| ConfigError::Missing(k))
        }
        fn opt(k: &str) -> Option<String> {
            std::env::var(k).ok().filter(|s| !s.is_empty())
        }
        fn parse_u16(k: &'static str, default: u16) -> Result<u16, ConfigError> {
            match std::env::var(k).ok() {
                Some(v) => v.parse().map_err(|e: std::num::ParseIntError| ConfigError::Invalid(k, e.to_string())),
                None => Ok(default),
            }
        }
        fn parse_u64(k: &'static str, default: u64) -> Result<u64, ConfigError> {
            match std::env::var(k).ok() {
                Some(v) => v.parse().map_err(|e: std::num::ParseIntError| ConfigError::Invalid(k, e.to_string())),
                None => Ok(default),
            }
        }
        fn parse_url(k: &'static str) -> Result<Option<url::Url>, ConfigError> {
            match std::env::var(k).ok().filter(|s| !s.is_empty()) {
                Some(v) => Ok(Some(v.parse().map_err(|e: url::ParseError| ConfigError::Invalid(k, e.to_string()))?)),
                None => Ok(None),
            }
        }
        Ok(Self {
            prusa_email: req("PRUSA_EMAIL")?,
            prusa_password: req("PRUSA_PASSWORD")?,
            prusa_printer_uuid: opt("PRUSA_PRINTER_UUID"),
            prusa_camera_id: opt("PRUSA_CAMERA_ID"),
            rtsp_port: parse_u16("RTSP_PORT", 8554)?,
            rtsp_path: opt("RTSP_PATH"),
            rtsp_bind_addr: opt("RTSP_BIND_ADDR").unwrap_or_else(|| "0.0.0.0".to_string()),
            idle_timeout: Duration::from_secs(parse_u64("IDLE_TIMEOUT_SECONDS", 60)?),
            token_store_path: opt("TOKEN_STORE_PATH").map(PathBuf::from).unwrap_or_else(|| PathBuf::from("/data/tokens.json")),
            health_port: parse_u16("HEALTH_PORT", 8080)?,
            metrics_interval: Duration::from_secs(parse_u64("METRICS_INTERVAL_SECONDS", 60)?),
            mqtt_broker_url: parse_url("MQTT_BROKER_URL")?,
            mqtt_username: opt("MQTT_USERNAME"),
            mqtt_password: opt("MQTT_PASSWORD"),
            mqtt_client_id: opt("MQTT_CLIENT_ID"),
            mqtt_discovery_prefix: opt("MQTT_DISCOVERY_PREFIX").unwrap_or_else(|| "homeassistant".into()),
            mqtt_topic_prefix: opt("MQTT_TOPIC_PREFIX").unwrap_or_else(|| "buddy3d-proxy".into()),
            snapshot_interval: Duration::from_secs(parse_u64("SNAPSHOT_INTERVAL_SECONDS", 10)?),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every env var Config reads. The fixture wipes all of these before each test so
    /// `.env` files (loaded by `just dotenv-load`) or stale shell exports can't leak in.
    const CONFIG_KEYS: &[&str] = &[
        "PRUSA_EMAIL",
        "PRUSA_PASSWORD",
        "PRUSA_PRINTER_UUID",
        "PRUSA_CAMERA_ID",
        "RTSP_PORT",
        "RTSP_PATH",
        "RTSP_BIND_ADDR",
        "IDLE_TIMEOUT_SECONDS",
        "TOKEN_STORE_PATH",
        "HEALTH_PORT",
        "METRICS_INTERVAL_SECONDS",
        "MQTT_BROKER_URL",
        "MQTT_USERNAME",
        "MQTT_PASSWORD",
        "MQTT_CLIENT_ID",
        "MQTT_DISCOVERY_PREFIX",
        "MQTT_TOPIC_PREFIX",
        "SNAPSHOT_INTERVAL_SECONDS",
    ];

    fn with_env<F: FnOnce()>(vars: &[(&str, &str)], f: F) {
        // Serialize via a static mutex so tests don't race on env state. Recover from
        // a poisoned lock so a panic in one test doesn't cascade-fail the others.
        use std::sync::Mutex;
        static LOCK: Mutex<()> = Mutex::new(());
        let _g = LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let saved: Vec<_> = CONFIG_KEYS
            .iter()
            .map(|k| (*k, std::env::var(k).ok()))
            .collect();
        for k in CONFIG_KEYS {
            std::env::remove_var(k);
        }
        for (k, v) in vars {
            std::env::set_var(k, v);
        }
        f();
        for (k, v) in saved {
            match v {
                Some(val) => std::env::set_var(k, val),
                None => std::env::remove_var(k),
            }
        }
    }

    #[test]
    fn loads_required_vars_with_defaults() {
        with_env(&[
            ("PRUSA_EMAIL", "user@example.com"),
            ("PRUSA_PASSWORD", "hunter2"),
        ], || {
            let cfg = Config::from_env().unwrap();
            assert_eq!(cfg.prusa_email, "user@example.com");
            assert_eq!(cfg.prusa_password, "hunter2");
            assert_eq!(cfg.rtsp_port, 8554);
            assert_eq!(cfg.idle_timeout, Duration::from_secs(60));
            assert_eq!(cfg.health_port, 8080);
            assert_eq!(cfg.metrics_interval, Duration::from_secs(60));
            assert_eq!(cfg.token_store_path, PathBuf::from("/data/tokens.json"));
            assert!(cfg.mqtt_broker_url.is_none());
            assert_eq!(cfg.mqtt_discovery_prefix, "homeassistant");
            assert_eq!(cfg.mqtt_topic_prefix, "buddy3d-proxy");
            assert_eq!(cfg.snapshot_interval, Duration::from_secs(10));
        });
    }

    #[test]
    fn errors_on_missing_email() {
        with_env(&[("PRUSA_PASSWORD", "x")], || {
            std::env::remove_var("PRUSA_EMAIL");
            assert!(matches!(Config::from_env(), Err(ConfigError::Missing("PRUSA_EMAIL"))));
        });
    }

    #[test]
    fn rejects_invalid_port() {
        with_env(&[
            ("PRUSA_EMAIL", "u@e.com"),
            ("PRUSA_PASSWORD", "p"),
            ("RTSP_PORT", "not-a-number"),
        ], || {
            assert!(matches!(Config::from_env(), Err(ConfigError::Invalid("RTSP_PORT", _))));
        });
    }

    #[test]
    fn parses_mqtt_broker_url_when_set() {
        with_env(
            &[
                ("PRUSA_EMAIL", "u@e.com"),
                ("PRUSA_PASSWORD", "p"),
                ("MQTT_BROKER_URL", "mqtts://broker.local:8883"),
            ],
            || {
                let cfg = Config::from_env().unwrap();
                let url = cfg.mqtt_broker_url.expect("url parsed");
                assert_eq!(url.scheme(), "mqtts");
                assert_eq!(url.host_str(), Some("broker.local"));
                assert_eq!(url.port(), Some(8883));
            },
        );
    }

    #[test]
    fn rejects_invalid_mqtt_broker_url() {
        with_env(
            &[
                ("PRUSA_EMAIL", "u@e.com"),
                ("PRUSA_PASSWORD", "p"),
                ("MQTT_BROKER_URL", "not a url"),
            ],
            || {
                assert!(matches!(
                    Config::from_env(),
                    Err(ConfigError::Invalid("MQTT_BROKER_URL", _))
                ));
            },
        );
    }
}
