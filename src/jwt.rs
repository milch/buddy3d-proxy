//! Read the `exp` claim from a JWT *without* verifying its signature.
//! The Prusa server will reject expired tokens regardless; we only need `exp` to drive
//! proactive refresh.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Debug, thiserror::Error)]
pub enum JwtError {
    #[error("malformed jwt: expected 3 dot-separated parts")]
    Malformed,
    #[error("jwt payload is not valid base64url")]
    Base64,
    #[error("jwt payload is not valid json")]
    Json,
    #[error("jwt payload missing `exp` claim")]
    MissingExp,
}

pub fn read_exp(jwt: &str) -> Result<SystemTime, JwtError> {
    use base64::Engine;
    let mut parts = jwt.split('.');
    let _header = parts.next().ok_or(JwtError::Malformed)?;
    let payload = parts.next().ok_or(JwtError::Malformed)?;
    let _sig = parts.next().ok_or(JwtError::Malformed)?;
    if parts.next().is_some() { return Err(JwtError::Malformed); }

    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload)
        .map_err(|_| JwtError::Base64)?;
    let json: serde_json::Value = serde_json::from_slice(&bytes).map_err(|_| JwtError::Json)?;
    let exp = json.get("exp").and_then(|v| v.as_f64()).ok_or(JwtError::MissingExp)?;
    Ok(UNIX_EPOCH + Duration::from_secs_f64(exp))
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine;
    use serde_json::json;

    fn mint(exp: f64) -> String {
        let header = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(b"{\"alg\":\"none\"}");
        let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(json!({ "sub": "u", "exp": exp }).to_string().as_bytes());
        format!("{}.{}.sig", header, payload)
    }

    #[test]
    fn reads_exp_from_well_formed_jwt() {
        let token = mint(1_777_780_278.62719);
        let exp = read_exp(&token).unwrap();
        assert_eq!(exp.duration_since(UNIX_EPOCH).unwrap().as_secs(), 1_777_780_278);
    }

    #[test]
    fn rejects_two_part_jwt() {
        assert!(matches!(read_exp("a.b"), Err(JwtError::Malformed)));
    }

    #[test]
    fn rejects_missing_exp() {
        let header = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(b"{}");
        let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(b"{\"sub\":\"u\"}");
        let token = format!("{}.{}.sig", header, payload);
        assert!(matches!(read_exp(&token), Err(JwtError::MissingExp)));
    }
}
