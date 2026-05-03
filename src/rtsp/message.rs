//! RTSP request/response wire format. Pure parsing/formatting; no IO.
//!
//! RFC 2326 §6/§7. Request line is `METHOD URI VERSION\r\n`, headers are
//! `Name: Value\r\n`, blank line ends headers, then optional body.

use std::collections::HashMap;
use std::str;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Method {
    Options,
    Describe,
    Setup,
    Play,
    Teardown,
    GetParameter,
    /// Anything we don't support gets a `501`.
    Unsupported,
}

impl Method {
    pub fn as_str(self) -> &'static str {
        match self {
            Method::Options => "OPTIONS",
            Method::Describe => "DESCRIBE",
            Method::Setup => "SETUP",
            Method::Play => "PLAY",
            Method::Teardown => "TEARDOWN",
            Method::GetParameter => "GET_PARAMETER",
            Method::Unsupported => "UNKNOWN",
        }
    }
}

impl Method {
    fn parse(s: &str) -> Self {
        match s {
            "OPTIONS" => Method::Options,
            "DESCRIBE" => Method::Describe,
            "SETUP" => Method::Setup,
            "PLAY" => Method::Play,
            "TEARDOWN" => Method::Teardown,
            "GET_PARAMETER" => Method::GetParameter,
            _ => Method::Unsupported,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Request {
    pub method: Method,
    pub uri: String,
    /// Lowercased header name -> value (last wins on dupes; fine for our needs).
    pub headers: HashMap<String, String>,
    /// `Content-Length`-bytes body, or empty.
    pub body: Vec<u8>,
}

impl Request {
    pub fn cseq(&self) -> Option<u32> {
        self.headers.get("cseq").and_then(|s| s.trim().parse().ok())
    }

    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(&name.to_ascii_lowercase()).map(String::as_str)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ParseError {
    #[error("incomplete request: need more bytes")]
    Incomplete,
    #[error("malformed request line: {0}")]
    BadRequestLine(String),
    #[error("malformed header line: {0}")]
    BadHeaderLine(String),
    #[error("invalid utf-8 in headers: {0}")]
    Utf8(#[from] str::Utf8Error),
    #[error("invalid Content-Length value: {0}")]
    BadContentLength(String),
}

/// Try to parse a single RTSP request from `buf`. Returns `(request, bytes_consumed)`
/// on success or `Incomplete` if we need more bytes. The caller is responsible for
/// removing the consumed prefix from its buffer.
pub fn parse_request(buf: &[u8]) -> Result<(Request, usize), ParseError> {
    // Find end of headers: `\r\n\r\n`.
    let header_end = match find_subslice(buf, b"\r\n\r\n") {
        Some(idx) => idx + 4,
        None => return Err(ParseError::Incomplete),
    };

    let header_text = str::from_utf8(&buf[..header_end - 4])?;
    let mut lines = header_text.split("\r\n");
    let request_line = lines
        .next()
        .ok_or_else(|| ParseError::BadRequestLine(header_text.into()))?;

    // METHOD URI VERSION
    let mut parts = request_line.splitn(3, ' ');
    let method = parts
        .next()
        .ok_or_else(|| ParseError::BadRequestLine(request_line.into()))?;
    let uri = parts
        .next()
        .ok_or_else(|| ParseError::BadRequestLine(request_line.into()))?;
    let _version = parts
        .next()
        .ok_or_else(|| ParseError::BadRequestLine(request_line.into()))?;

    let mut headers: HashMap<String, String> = HashMap::new();
    for line in lines {
        if line.is_empty() {
            continue;
        }
        let (name, value) = line
            .split_once(':')
            .ok_or_else(|| ParseError::BadHeaderLine(line.into()))?;
        headers.insert(name.trim().to_ascii_lowercase(), value.trim().to_string());
    }

    let body_len: usize = match headers.get("content-length") {
        Some(s) => s
            .parse()
            .map_err(|_| ParseError::BadContentLength(s.clone()))?,
        None => 0,
    };
    if buf.len() < header_end + body_len {
        return Err(ParseError::Incomplete);
    }
    let body = buf[header_end..header_end + body_len].to_vec();

    Ok((
        Request {
            method: Method::parse(method),
            uri: uri.to_string(),
            headers,
            body,
        },
        header_end + body_len,
    ))
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|w| w == needle)
}

/// A response to send back. The body is formatted as text (we don't send binary
/// bodies — even SDP is text/sdp).
#[derive(Debug, Clone)]
pub struct Response {
    pub status: u16,
    pub reason: &'static str,
    pub cseq: u32,
    /// Extra headers as `(Name, Value)` — name is sent as-is (CSeq, Content-Type, etc.).
    pub headers: Vec<(&'static str, String)>,
    /// Optional body. `Content-Length` header is generated from the body's byte length.
    pub body: Option<String>,
}

impl Response {
    pub fn ok(cseq: u32) -> Self {
        Self {
            status: 200,
            reason: "OK",
            cseq,
            headers: Vec::new(),
            body: None,
        }
    }

    pub fn error(status: u16, reason: &'static str, cseq: u32) -> Self {
        Self {
            status,
            reason,
            cseq,
            headers: Vec::new(),
            body: None,
        }
    }

    pub fn with_header(mut self, name: &'static str, value: impl Into<String>) -> Self {
        self.headers.push((name, value.into()));
        self
    }

    pub fn with_body(mut self, content_type: &'static str, body: String) -> Self {
        self.headers.push(("Content-Type", content_type.into()));
        self.body = Some(body);
        self
    }

    pub fn format(&self) -> String {
        let mut out = format!("RTSP/1.0 {} {}\r\nCSeq: {}\r\n", self.status, self.reason, self.cseq);
        if let Some(body) = &self.body {
            out.push_str(&format!("Content-Length: {}\r\n", body.len()));
        }
        for (name, value) in &self.headers {
            out.push_str(&format!("{name}: {value}\r\n"));
        }
        out.push_str("\r\n");
        if let Some(body) = &self.body {
            out.push_str(body);
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_options_request() {
        let raw = b"OPTIONS rtsp://localhost:8554/cam RTSP/1.0\r\nCSeq: 1\r\nUser-Agent: VLC/3.0.20\r\n\r\n";
        let (req, consumed) = parse_request(raw).unwrap();
        assert_eq!(req.method, Method::Options);
        assert_eq!(req.uri, "rtsp://localhost:8554/cam");
        assert_eq!(req.cseq(), Some(1));
        assert_eq!(req.header("user-agent"), Some("VLC/3.0.20"));
        assert_eq!(consumed, raw.len());
        assert!(req.body.is_empty());
    }

    #[test]
    fn parses_describe_request_with_accept_header() {
        let raw = b"DESCRIBE rtsp://h/x RTSP/1.0\r\nCSeq: 2\r\nAccept: application/sdp\r\n\r\n";
        let (req, _) = parse_request(raw).unwrap();
        assert_eq!(req.method, Method::Describe);
        assert_eq!(req.header("accept"), Some("application/sdp"));
    }

    #[test]
    fn parses_setup_request_with_transport() {
        let raw = b"SETUP rtsp://h/x/streamid=0 RTSP/1.0\r\nCSeq: 3\r\nTransport: RTP/AVP/TCP;unicast;interleaved=0-1\r\n\r\n";
        let (req, _) = parse_request(raw).unwrap();
        assert_eq!(req.method, Method::Setup);
        assert!(req
            .header("transport")
            .unwrap()
            .contains("interleaved=0-1"));
    }

    #[test]
    fn returns_incomplete_on_partial_input() {
        let raw = b"OPTIONS rtsp://h/x RTSP/1.0\r\nCSeq:";
        assert!(matches!(parse_request(raw), Err(ParseError::Incomplete)));
    }

    #[test]
    fn returns_incomplete_when_body_truncated() {
        // Content-Length 10 but only 3 bytes of body.
        let raw = b"PLAY r RTSP/1.0\r\nCSeq: 4\r\nContent-Length: 10\r\n\r\nabc";
        assert!(matches!(parse_request(raw), Err(ParseError::Incomplete)));
    }

    #[test]
    fn parses_two_back_to_back_requests() {
        let raw = b"OPTIONS r RTSP/1.0\r\nCSeq: 1\r\n\r\nDESCRIBE r RTSP/1.0\r\nCSeq: 2\r\n\r\n";
        let (req1, consumed) = parse_request(raw).unwrap();
        assert_eq!(req1.cseq(), Some(1));
        let (req2, _) = parse_request(&raw[consumed..]).unwrap();
        assert_eq!(req2.cseq(), Some(2));
    }

    #[test]
    fn unknown_method_parses_as_unsupported() {
        let raw = b"FROBNICATE r RTSP/1.0\r\nCSeq: 1\r\n\r\n";
        let (req, _) = parse_request(raw).unwrap();
        assert_eq!(req.method, Method::Unsupported);
    }

    #[test]
    fn formats_ok_response_with_cseq() {
        let resp = Response::ok(7).format();
        assert!(resp.starts_with("RTSP/1.0 200 OK\r\n"));
        assert!(resp.contains("CSeq: 7\r\n"));
        assert!(resp.ends_with("\r\n"));
    }

    #[test]
    fn formats_error_response_with_status_and_reason() {
        let resp = Response::error(501, "Not Implemented", 1).format();
        assert!(resp.starts_with("RTSP/1.0 501 Not Implemented\r\n"));
    }

    #[test]
    fn formats_response_with_headers_and_body() {
        let resp = Response::ok(2)
            .with_body("application/sdp", "v=0\r\n".into())
            .format();
        assert!(resp.contains("Content-Type: application/sdp\r\n"));
        assert!(resp.contains("Content-Length: 5\r\n"));
        assert!(resp.ends_with("\r\n\r\nv=0\r\n"));
    }
}
