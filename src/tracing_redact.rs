//! A simple redaction wrapper: takes a string, replaces sensitive values with a marker.
//!
//! We don't try to redact at the tracing-subscriber level; instead, every place that
//! could log a secret runs the value through `redact()` before passing it to a
//! tracing macro. This is explicit and unmissable in code review.

const REDACTED: &str = "<redacted>";

/// Redact a string value identified as sensitive. Always returns `<redacted>`.
/// This exists so call sites read clearly: `tracing::debug!(token = %redact(t), "...")`.
pub fn redact<S: AsRef<str>>(_value: S) -> &'static str {
    REDACTED
}

/// Redact-aware Debug-like formatting for an `Option<String>`: `Some(<redacted>)` or `None`.
pub fn redact_option(value: &Option<String>) -> String {
    match value {
        Some(_) => format!("Some({REDACTED})"),
        None => "None".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redact_returns_marker() {
        assert_eq!(redact("hunter2"), "<redacted>");
        assert_eq!(redact(""), "<redacted>");
    }

    #[test]
    fn redact_option_handles_some_and_none() {
        assert_eq!(
            redact_option(&Some("secret".to_string())),
            "Some(<redacted>)"
        );
        assert_eq!(redact_option(&None), "None");
    }
}
