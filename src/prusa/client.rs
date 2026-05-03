//! Thin wrapper around `reqwest::Client` that gates every request on the rate limiter
//! and reports the outcome (success / 4xx / 5xx-or-network) back to the limiter.

use crate::rate_limit::{Outcome, RateLimiter};
use reqwest::{Method, RequestBuilder, Response, Url};
use std::sync::Arc;

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("network error: {0}")]
    Network(#[source] reqwest::Error),
    #[error("server error: status {0}")]
    Server(reqwest::StatusCode),
    #[error("client error: status {0}")]
    Client(reqwest::StatusCode),
}

#[derive(Clone)]
pub struct PrusaClient {
    inner: reqwest::Client,
    pub(crate) limiter: Arc<RateLimiter>,
}

impl PrusaClient {
    pub fn new(inner: reqwest::Client, limiter: Arc<RateLimiter>) -> Self {
        Self { inner, limiter }
    }

    pub fn request(&self, method: Method, url: Url) -> RequestBuilder {
        self.inner.request(method, url)
    }

    /// Send a built request through the rate limiter. Reports outcome based on response
    /// status / network error.
    pub async fn send(&self, req: RequestBuilder) -> Result<Response, ClientError> {
        let permit = self.limiter.acquire().await;
        let result = req.send().await;
        let outcome = match &result {
            Ok(r) if r.status().is_server_error() => Outcome::ServerOrNetworkError,
            Ok(r) if r.status().is_client_error() => Outcome::ClientError,
            Ok(_) => Outcome::Success,
            Err(_) => Outcome::ServerOrNetworkError,
        };
        permit.complete(outcome).await;
        match result {
            Err(e) => Err(ClientError::Network(e)),
            Ok(r) if r.status().is_server_error() => Err(ClientError::Server(r.status())),
            Ok(r) if r.status().is_client_error() => Err(ClientError::Client(r.status())),
            Ok(r) => Ok(r),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use wiremock::{Mock, MockServer, ResponseTemplate, matchers::method};

    async fn fixture() -> (MockServer, PrusaClient) {
        let server = MockServer::start().await;
        let limiter = Arc::new(RateLimiter::new(3, Duration::from_secs(60)));
        let client = PrusaClient::new(reqwest::Client::new(), limiter);
        (server, client)
    }

    #[tokio::test]
    async fn success_does_not_consume_token() {
        let (server, client) = fixture().await;
        Mock::given(method("GET")).respond_with(ResponseTemplate::new(200)).mount(&server).await;
        let req = client.request(Method::GET, server.uri().parse().unwrap());
        client.send(req).await.unwrap();
        assert_eq!(client.limiter.tokens().await, 3);
    }

    #[tokio::test]
    async fn server_error_returns_typed_error_and_consumes_token() {
        let (server, client) = fixture().await;
        Mock::given(method("GET")).respond_with(ResponseTemplate::new(503)).mount(&server).await;
        let req = client.request(Method::GET, server.uri().parse().unwrap());
        let err = client.send(req).await.unwrap_err();
        assert!(matches!(err, ClientError::Server(_)));
        assert_eq!(client.limiter.tokens().await, 2);
    }

    #[tokio::test]
    async fn client_error_returns_typed_error_and_keeps_token() {
        let (server, client) = fixture().await;
        Mock::given(method("GET")).respond_with(ResponseTemplate::new(404)).mount(&server).await;
        let req = client.request(Method::GET, server.uri().parse().unwrap());
        let err = client.send(req).await.unwrap_err();
        assert!(matches!(err, ClientError::Client(_)));
        assert_eq!(client.limiter.tokens().await, 3);
    }
}
