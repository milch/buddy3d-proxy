//! OAuth2 Authorization Code with PKCE against `account.prusa3d.com`.
//!
//! Steps reproduced from the captured HAR:
//!   1. GET  /login/?next=/o/authorize/?...  → HTML page containing csrfmiddlewaretoken
//!   2. POST /login/?next=/o/authorize/?...  → 302 chain ending at connect.prusa3d.com/login/auth-callback?code=...
//!      We follow redirects manually so we can intercept the `code` query parameter.
//!   3. POST /o/token/  grant_type=authorization_code  → { access_token, refresh_token }

use crate::pkce::Pkce;
use crate::prusa::client::{ClientError, PrusaClient};
use crate::token_store::{StoredTokens, TokenStore};
use reqwest::{Method, Url};
use serde::Deserialize;
use std::sync::Arc;
use tokio::sync::Mutex;

pub const CLIENT_ID: &str = "MRHTlZhZqkNrrQ6FUPtjyusAz8nc59ErHXP8XkS4";
pub const REDIRECT_URI: &str = "https://connect.prusa3d.com/login/auth-callback";

#[derive(Debug, thiserror::Error)]
pub enum AuthError {
    #[error("http error: {0}")]
    Http(#[from] ClientError),
    #[error("could not extract csrfmiddlewaretoken from login page")]
    NoCsrf,
    #[error("login failed (likely wrong credentials, MFA enabled, or account locked)")]
    LoginRejected,
    #[error("login response did not include an auth code in the redirect chain")]
    NoAuthCode,
    #[error("token exchange returned malformed response: {0}")]
    BadTokenResponse(String),
}

#[derive(Debug, Deserialize)]
struct TokenResponse {
    access_token: String,
    refresh_token: String,
}

/// Configuration for the auth endpoints. In production these are constants;
/// tests inject `MockServer` URIs.
#[derive(Debug, Clone)]
pub struct AuthEndpoints {
    pub account_base: Url,    // e.g. "https://account.prusa3d.com"
    pub connect_base: Url,    // e.g. "https://connect.prusa3d.com"
}

impl Default for AuthEndpoints {
    fn default() -> Self {
        Self {
            account_base: "https://account.prusa3d.com".parse().unwrap(),
            connect_base: "https://connect.prusa3d.com".parse().unwrap(),
        }
    }
}

pub async fn bootstrap(
    client: &PrusaClient,
    endpoints: &AuthEndpoints,
    email: &str,
    password: &str,
) -> Result<StoredTokens, AuthError> {
    let pkce = Pkce::generate();
    let next_path = format!(
        "/o/authorize/?response_type=code&client_id={}&code_challenge_method=S256&code_challenge={}&redirect_uri={}",
        CLIENT_ID,
        pkce.challenge,
        urlencoding::encode(REDIRECT_URI),
    );
    let login_url = endpoints.account_base.join(&format!("/login/?next={}", urlencoding::encode(&next_path)))
        .map_err(|e| AuthError::BadTokenResponse(e.to_string()))?;

    // Step 1: GET the login page, extract csrfmiddlewaretoken.
    let resp = client.send(client.request(Method::GET, login_url.clone())).await?;
    let body = resp.text().await.map_err(|e| AuthError::BadTokenResponse(e.to_string()))?;
    let csrf = extract_csrf(&body).ok_or(AuthError::NoCsrf)?;

    // Step 2: POST credentials, intercept redirect chain to capture `code`.
    let form = [
        ("csrfmiddlewaretoken", csrf.as_str()),
        ("next", next_path.as_str()),
        ("email", email),
        ("password", password),
    ];
    let no_redirect_client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .cookie_store(true)
        .build()
        .map_err(|e| AuthError::BadTokenResponse(e.to_string()))?;
    let code = follow_chain_for_code(&no_redirect_client, login_url, &form).await?;

    // Step 3: exchange code for tokens.
    let token_url = endpoints.account_base.join("/o/token/")
        .map_err(|e| AuthError::BadTokenResponse(e.to_string()))?;
    let resp = client.send(
        client.request(Method::POST, token_url).form(&[
            ("grant_type", "authorization_code"),
            ("code", code.as_str()),
            ("client_id", CLIENT_ID),
            ("redirect_uri", REDIRECT_URI),
            ("code_verifier", pkce.verifier.as_str()),
        ]),
    ).await?;
    let tokens: TokenResponse = resp.json().await.map_err(|e| AuthError::BadTokenResponse(e.to_string()))?;
    let access_expires_at = crate::jwt::read_exp(&tokens.access_token)
        .map_err(|e| AuthError::BadTokenResponse(e.to_string()))?
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    Ok(StoredTokens {
        access_token: tokens.access_token,
        refresh_token: tokens.refresh_token,
        access_expires_at,
    })
}

pub async fn refresh(
    client: &PrusaClient,
    endpoints: &AuthEndpoints,
    refresh_token: &str,
) -> Result<StoredTokens, AuthError> {
    let token_url = endpoints
        .account_base
        .join("/o/token/")
        .map_err(|e| AuthError::BadTokenResponse(e.to_string()))?;
    let resp = client
        .send(client.request(Method::POST, token_url).form(&[
            ("grant_type", "refresh_token"),
            ("refresh_token", refresh_token),
            ("client_id", CLIENT_ID),
        ]))
        .await?;
    let body: TokenResponse = resp
        .json()
        .await
        .map_err(|e| AuthError::BadTokenResponse(e.to_string()))?;
    let access_expires_at = crate::jwt::read_exp(&body.access_token)
        .map_err(|e| AuthError::BadTokenResponse(e.to_string()))?
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    Ok(StoredTokens {
        access_token: body.access_token,
        refresh_token: body.refresh_token,
        access_expires_at,
    })
}

pub struct AuthOrchestrator {
    client: PrusaClient,
    endpoints: AuthEndpoints,
    store: TokenStore,
    email: String,
    password: String,
    state: Mutex<Option<StoredTokens>>,
}

impl AuthOrchestrator {
    pub fn new(
        client: PrusaClient,
        endpoints: AuthEndpoints,
        store: TokenStore,
        email: String,
        password: String,
    ) -> Self {
        Self {
            client,
            endpoints,
            store,
            email,
            password,
            state: Mutex::new(None),
        }
    }

    /// Returns a valid access token, performing refresh or full bootstrap as needed.
    pub async fn access_token(self: &Arc<Self>) -> Result<String, AuthError> {
        let mut state = self.state.lock().await;
        if state.is_none() {
            *state = self
                .store
                .load()
                .map_err(|e| AuthError::BadTokenResponse(e.to_string()))?;
        }

        let needs_refresh = match state.as_ref() {
            Some(t) => self.access_about_to_expire(t),
            None => true,
        };

        if needs_refresh {
            let new_tokens = match state.as_ref() {
                Some(t) => match refresh(&self.client, &self.endpoints, &t.refresh_token).await {
                    Ok(new) => new,
                    Err(AuthError::Http(ClientError::Client(_))) => {
                        tracing::warn!("refresh token rejected; falling back to bootstrap");
                        bootstrap(&self.client, &self.endpoints, &self.email, &self.password)
                            .await?
                    }
                    Err(e) => return Err(e),
                },
                None => {
                    bootstrap(&self.client, &self.endpoints, &self.email, &self.password).await?
                }
            };
            self.store
                .save(&new_tokens)
                .map_err(|e| AuthError::BadTokenResponse(e.to_string()))?;
            *state = Some(new_tokens);
        }

        Ok(state.as_ref().unwrap().access_token.clone())
    }

    fn access_about_to_expire(&self, tokens: &StoredTokens) -> bool {
        let now = std::time::SystemTime::now();
        let exp = tokens.access_expires();
        match exp.duration_since(now) {
            Ok(remaining) => remaining < std::time::Duration::from_secs(60),
            Err(_) => true, // already expired
        }
    }
}

fn extract_csrf(html: &str) -> Option<String> {
    // Django renders: <input type="hidden" name="csrfmiddlewaretoken" value="...">
    let needle = "name=\"csrfmiddlewaretoken\" value=\"";
    let start = html.find(needle)? + needle.len();
    let end = html[start..].find('"')? + start;
    Some(html[start..end].to_string())
}

async fn follow_chain_for_code(
    client: &reqwest::Client,
    login_url: Url,
    form: &[(&str, &str)],
) -> Result<String, AuthError> {
    let mut next_request: Option<reqwest::Request> = Some(
        client.post(login_url).form(form).build().map_err(|e| AuthError::BadTokenResponse(e.to_string()))?,
    );
    for _hop in 0..6 {
        let req = match next_request.take() { Some(r) => r, None => return Err(AuthError::NoAuthCode) };
        let resp = client.execute(req).await.map_err(|e| AuthError::BadTokenResponse(e.to_string()))?;
        if let Some(loc) = resp.headers().get(reqwest::header::LOCATION) {
            let url = resp.url().join(loc.to_str().unwrap_or_default())
                .map_err(|e| AuthError::BadTokenResponse(e.to_string()))?;
            if let Some(code) = url.query_pairs().find(|(k, _)| k == "code").map(|(_, v)| v.to_string()) {
                return Ok(code);
            }
            next_request = Some(client.get(url).build().map_err(|e| AuthError::BadTokenResponse(e.to_string()))?);
            continue;
        }
        // No Location header and no code in URL: the login form was rejected.
        return Err(AuthError::LoginRejected);
    }
    Err(AuthError::NoAuthCode)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_csrf_finds_django_token() {
        let html = r#"<form><input type="hidden" name="csrfmiddlewaretoken" value="abc123XYZ"></form>"#;
        assert_eq!(extract_csrf(html), Some("abc123XYZ".to_string()));
    }

    #[test]
    fn extract_csrf_returns_none_when_missing() {
        assert!(extract_csrf("<form></form>").is_none());
    }

    use crate::rate_limit::RateLimiter;
    use std::sync::Arc;
    use std::time::Duration;
    use wiremock::{Mock, MockServer, ResponseTemplate, matchers::{method, path}};

    fn mint_jwt(exp: u64) -> String {
        use base64::Engine;
        let header = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(b"{\"alg\":\"none\"}");
        let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(format!("{{\"sub\":\"u\",\"exp\":{}}}", exp).as_bytes());
        format!("{}.{}.sig", header, payload)
    }

    #[tokio::test]
    async fn bootstrap_flow_succeeds_with_mocked_account_server() {
        let server = MockServer::start().await;
        let limiter = Arc::new(RateLimiter::new(3, Duration::from_secs(60)));
        let prusa = PrusaClient::new(reqwest::Client::new(), limiter);
        let endpoints = AuthEndpoints {
            account_base: server.uri().parse().unwrap(),
            connect_base: server.uri().parse().unwrap(),
        };

        // Mock GET /login/ → return HTML with CSRF.
        Mock::given(method("GET")).and(path("/login/"))
            .respond_with(ResponseTemplate::new(200).set_body_string(
                r#"<input type="hidden" name="csrfmiddlewaretoken" value="csrftoken">"#,
            ))
            .mount(&server).await;

        // Mock POST /login/ → 302 to /o/authorize/
        Mock::given(method("POST")).and(path("/login/"))
            .respond_with(ResponseTemplate::new(302).insert_header("location", "/o/authorize/?response_type=code&client_id=X"))
            .mount(&server).await;

        // Mock GET /o/authorize/ → 302 to redirect_uri with code.
        let callback = format!("{}/login/auth-callback?code=AUTHCODE", server.uri());
        Mock::given(method("GET")).and(path("/o/authorize/"))
            .respond_with(ResponseTemplate::new(302).insert_header("location", callback.as_str()))
            .mount(&server).await;

        // Mock POST /o/token/ → access + refresh.
        let exp = 9_999_999_999u64;
        let access = mint_jwt(exp);
        let refresh = mint_jwt(exp);
        Mock::given(method("POST")).and(path("/o/token/"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "access_token": access, "refresh_token": refresh,
            })))
            .mount(&server).await;

        let tokens = bootstrap(&prusa, &endpoints, "u@e.com", "pw").await.unwrap();
        assert_eq!(tokens.access_expires_at, exp);
        assert!(!tokens.refresh_token.is_empty());
    }

    #[tokio::test]
    async fn bootstrap_returns_login_rejected_when_form_post_returns_200() {
        let server = MockServer::start().await;
        let limiter = Arc::new(RateLimiter::new(3, Duration::from_secs(60)));
        let prusa = PrusaClient::new(reqwest::Client::new(), limiter);
        let endpoints = AuthEndpoints {
            account_base: server.uri().parse().unwrap(),
            connect_base: server.uri().parse().unwrap(),
        };
        Mock::given(method("GET")).and(path("/login/"))
            .respond_with(ResponseTemplate::new(200).set_body_string(
                r#"<input name="csrfmiddlewaretoken" value="x">"#,
            )).mount(&server).await;
        Mock::given(method("POST")).and(path("/login/"))
            .respond_with(ResponseTemplate::new(200).set_body_string("login form re-rendered with errors"))
            .mount(&server).await;

        let err = bootstrap(&prusa, &endpoints, "u@e.com", "wrong").await.unwrap_err();
        assert!(matches!(err, AuthError::LoginRejected));
    }

    #[tokio::test]
    async fn refresh_returns_new_tokens() {
        let server = MockServer::start().await;
        let limiter = Arc::new(RateLimiter::new(3, Duration::from_secs(60)));
        let prusa = PrusaClient::new(reqwest::Client::new(), limiter);
        let endpoints = AuthEndpoints {
            account_base: server.uri().parse().unwrap(),
            connect_base: server.uri().parse().unwrap(),
        };
        let exp = 9_999_999_999u64;
        Mock::given(method("POST"))
            .and(path("/o/token/"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "access_token": mint_jwt(exp),
                "refresh_token": mint_jwt(exp),
            })))
            .mount(&server)
            .await;
        let tokens = refresh(&prusa, &endpoints, "old-refresh").await.unwrap();
        assert_eq!(tokens.access_expires_at, exp);
    }

    #[tokio::test]
    async fn refresh_returns_client_error_when_token_invalid() {
        let server = MockServer::start().await;
        let limiter = Arc::new(RateLimiter::new(3, Duration::from_secs(60)));
        let prusa = PrusaClient::new(reqwest::Client::new(), limiter);
        let endpoints = AuthEndpoints {
            account_base: server.uri().parse().unwrap(),
            connect_base: server.uri().parse().unwrap(),
        };
        Mock::given(method("POST"))
            .and(path("/o/token/"))
            .respond_with(ResponseTemplate::new(400))
            .mount(&server)
            .await;
        let err = refresh(&prusa, &endpoints, "bad").await.unwrap_err();
        assert!(matches!(err, AuthError::Http(ClientError::Client(_))));
    }

    #[tokio::test]
    async fn orchestrator_uses_persisted_token_when_fresh() {
        let server = MockServer::start().await;
        let limiter = Arc::new(RateLimiter::new(3, Duration::from_secs(60)));
        let prusa = PrusaClient::new(reqwest::Client::new(), limiter);
        let endpoints = AuthEndpoints {
            account_base: server.uri().parse().unwrap(),
            connect_base: server.uri().parse().unwrap(),
        };
        let dir = tempfile::tempdir().unwrap();
        let store = TokenStore::new(dir.path().join("tokens.json"));
        // Persist a token that doesn't expire for a long time.
        let exp = 9_999_999_999u64;
        let access = mint_jwt(exp);
        store
            .save(&StoredTokens {
                access_token: access.clone(),
                refresh_token: mint_jwt(exp),
                access_expires_at: exp,
            })
            .unwrap();

        // No mocks configured — if the orchestrator tries to refresh, the test will fail.
        let orch = Arc::new(AuthOrchestrator::new(
            prusa,
            endpoints,
            store,
            "u@e.com".into(),
            "pw".into(),
        ));
        let got = orch.access_token().await.unwrap();
        assert_eq!(got, access);
    }

    #[tokio::test]
    async fn orchestrator_refreshes_when_token_near_expiry() {
        let server = MockServer::start().await;
        let limiter = Arc::new(RateLimiter::new(3, Duration::from_secs(60)));
        let prusa = PrusaClient::new(reqwest::Client::new(), limiter);
        let endpoints = AuthEndpoints {
            account_base: server.uri().parse().unwrap(),
            connect_base: server.uri().parse().unwrap(),
        };
        let dir = tempfile::tempdir().unwrap();
        let store = TokenStore::new(dir.path().join("tokens.json"));
        // Persist an already-expired token.
        store
            .save(&StoredTokens {
                access_token: mint_jwt(1),
                refresh_token: mint_jwt(9_999_999_999),
                access_expires_at: 1,
            })
            .unwrap();

        let new_exp = 9_999_999_998u64;
        Mock::given(method("POST"))
            .and(path("/o/token/"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "access_token": mint_jwt(new_exp),
                "refresh_token": mint_jwt(new_exp),
            })))
            .mount(&server)
            .await;

        let orch = Arc::new(AuthOrchestrator::new(
            prusa,
            endpoints,
            store,
            "u@e.com".into(),
            "pw".into(),
        ));
        let _t = orch.access_token().await.unwrap();
        let cached = orch.state.lock().await.as_ref().unwrap().access_expires_at;
        assert_eq!(cached, new_exp);
    }
}
