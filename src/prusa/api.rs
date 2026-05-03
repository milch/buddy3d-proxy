//! Prusa Connect REST surface needed for camera discovery.

use crate::prusa::client::{ClientError, PrusaClient};
use reqwest::{Method, Url};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct Printer {
    pub uuid: String,
    pub name: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Camera {
    pub id: u64,
    pub name: Option<String>,
}

#[derive(Debug, Deserialize)]
struct PrintersEnvelope {
    printers: Vec<Printer>,
}

#[derive(Debug, Deserialize)]
struct CamerasEnvelope {
    cameras: Vec<Camera>,
}

pub async fn list_printers(
    client: &PrusaClient,
    connect_base: &Url,
    bearer: &str,
) -> Result<Vec<Printer>, ClientError> {
    let url = connect_base.join("/app/printers?limit=10").unwrap();
    let resp = client
        .send(client.request(Method::GET, url).bearer_auth(bearer))
        .await?;
    let env: PrintersEnvelope = resp.json().await.map_err(ClientError::Network)?;
    Ok(env.printers)
}

pub async fn list_cameras(
    client: &PrusaClient,
    connect_base: &Url,
    bearer: &str,
    printer_uuid: &str,
) -> Result<Vec<Camera>, ClientError> {
    let url = connect_base
        .join(&format!("/app/printers/{printer_uuid}/cameras"))
        .unwrap();
    let resp = client
        .send(client.request(Method::GET, url).bearer_auth(bearer))
        .await?;
    let env: CamerasEnvelope = resp.json().await.map_err(ClientError::Network)?;
    Ok(env.cameras)
}

#[derive(Debug, Clone, Deserialize)]
pub struct WebRtcConfig {
    /// Short-lived token to send as `ClientAuthentication.token` to the signaling server.
    pub token: String,
    /// ICE servers (STUN/TURN) for the PeerConnection. Field name varies — Prusa
    /// might use `ice_servers`, `iceServers`, or `webrtc_config.ice_servers`.
    #[serde(default, alias = "iceServers", alias = "ice_servers")]
    pub ice_servers: Vec<IceServerConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct IceServerConfig {
    /// Single URL or list of URLs for this server entry.
    #[serde(alias = "urls", alias = "url")]
    pub urls: serde_json::Value,
    #[serde(default)]
    pub username: Option<String>,
    #[serde(default, alias = "credential")]
    pub credential: Option<String>,
}

impl IceServerConfig {
    /// Normalize `urls` (which can be a string or an array) into a flat Vec<String>.
    pub fn url_list(&self) -> Vec<String> {
        match &self.urls {
            serde_json::Value::String(s) => vec![s.clone()],
            serde_json::Value::Array(a) => a
                .iter()
                .filter_map(|v| v.as_str().map(str::to_string))
                .collect(),
            _ => vec![],
        }
    }
}

pub async fn fetch_webrtc_config(
    client: &PrusaClient,
    bearer: &str,
) -> Result<WebRtcConfig, ClientError> {
    let url: Url = "https://camera-service-api.prusa3d.com/v1/camera-webrtc-config"
        .parse()
        .expect("hardcoded url is valid");
    let resp = client
        .send(client.request(Method::GET, url).bearer_auth(bearer))
        .await?;
    let cfg: WebRtcConfig = resp.json().await.map_err(ClientError::Network)?;
    Ok(cfg)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rate_limit::RateLimiter;
    use std::sync::Arc;
    use std::time::Duration;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[tokio::test]
    async fn list_printers_parses_envelope() {
        let server = MockServer::start().await;
        let prusa = PrusaClient::new(
            reqwest::Client::new(),
            Arc::new(RateLimiter::new(3, Duration::from_secs(60))),
        );
        Mock::given(method("GET"))
            .and(path("/app/printers"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "printers": [{"uuid": "u1", "name": "MK4"}],
            })))
            .mount(&server)
            .await;
        let printers = list_printers(&prusa, &server.uri().parse().unwrap(), "tok")
            .await
            .unwrap();
        assert_eq!(printers.len(), 1);
        assert_eq!(printers[0].uuid, "u1");
    }

    #[tokio::test]
    async fn list_cameras_parses_envelope() {
        let server = MockServer::start().await;
        let prusa = PrusaClient::new(
            reqwest::Client::new(),
            Arc::new(RateLimiter::new(3, Duration::from_secs(60))),
        );
        Mock::given(method("GET"))
            .and(path("/app/printers/u1/cameras"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "cameras": [{"id": 380125, "name": "Front"}],
            })))
            .mount(&server)
            .await;
        let cameras = list_cameras(&prusa, &server.uri().parse().unwrap(), "tok", "u1")
            .await
            .unwrap();
        assert_eq!(cameras.len(), 1);
        assert_eq!(cameras[0].id, 380125);
    }

    #[tokio::test]
    async fn webrtc_config_parses_string_and_array_urls() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/camera-webrtc-config"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "token": "WEBRTC-TOKEN",
                "ice_servers": [
                    {"urls": "stun:stun.prusa3d.com:3478"},
                    {"urls": ["turn:turn.prusa3d.com:5349"], "username": "u", "credential": "p"}
                ]
            })))
            .mount(&server)
            .await;
        // The function has a hardcoded production URL, so call the mock URL
        // directly via reqwest and exercise the WebRtcConfig deserializer.
        let resp_body: WebRtcConfig =
            reqwest::get(format!("{}/v1/camera-webrtc-config", server.uri()))
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
        assert_eq!(resp_body.token, "WEBRTC-TOKEN");
        assert_eq!(resp_body.ice_servers.len(), 2);
        assert_eq!(
            resp_body.ice_servers[0].url_list(),
            vec!["stun:stun.prusa3d.com:3478"]
        );
        assert_eq!(
            resp_body.ice_servers[1].url_list(),
            vec!["turn:turn.prusa3d.com:5349"]
        );
        assert_eq!(resp_body.ice_servers[1].username.as_deref(), Some("u"));
        assert_eq!(resp_body.ice_servers[1].credential.as_deref(), Some("p"));
    }

    #[test]
    fn ice_server_config_handles_unknown_url_shape() {
        let cfg: IceServerConfig = serde_json::from_value(serde_json::json!({
            "urls": 42
        })).unwrap();
        assert!(cfg.url_list().is_empty());
    }
}
