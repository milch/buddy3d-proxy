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
}
