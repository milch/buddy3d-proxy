//! Per-connection RTSP state machine. Pure logic; no IO.
//!
//! Each accepted TCP connection gets one `Session`. The IO layer feeds it
//! parsed `Request`s and reacts to the returned `SessionAction`.

use crate::rtsp::message::{Method, Request, Response};
use crate::rtsp::sdp::H264Params;

/// State of a single RTSP session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum State {
    Init,
    Described,
    SetUp,
    Playing,
    TornDown,
}

/// Side-effect for the IO layer.
#[derive(Debug, Clone)]
pub enum SessionAction {
    /// No side effect; just send the response.
    None,
    /// Start forwarding RTP packets on the configured channels.
    StartStreaming { rtp_channel: u8 },
    /// Stop streaming and close the connection.
    Stop,
}

/// Information the session needs from the supervisor to build responses.
pub struct SessionContext<'a> {
    /// Display name of the camera (used in SDP `s=`).
    pub camera_name: &'a str,
    /// Negotiated H.264 codec params, available once WebRTC has been
    /// brought up at least once.
    pub h264_params: &'a H264Params,
    /// Camera path (the part of the RTSP URI we accept). Requests for
    /// other paths get 404. Should match the configured `RTSP_PATH`.
    pub expected_path: &'a str,
    /// Newly-allocated session id to attach to the SETUP response.
    pub new_session_id: &'a str,
}

pub struct Session {
    pub state: State,
    pub session_id: Option<String>,
    /// Lower interleaved channel from the client's `Transport: interleaved=A-B`.
    pub rtp_channel: Option<u8>,
}

impl Session {
    pub fn new() -> Self {
        Self {
            state: State::Init,
            session_id: None,
            rtp_channel: None,
        }
    }

    /// Process an inbound request, mutating state and returning a response +
    /// optional side-effect for the IO layer.
    pub fn handle(&mut self, req: &Request, ctx: &SessionContext<'_>) -> (Response, SessionAction) {
        let cseq = req.cseq().unwrap_or(0);

        if !uri_matches(&req.uri, ctx.expected_path) && req.method != Method::Options {
            return (
                Response::error(404, "Not Found", cseq),
                SessionAction::None,
            );
        }

        match req.method {
            Method::Options => {
                let resp = Response::ok(cseq).with_header(
                    "Public",
                    "OPTIONS, DESCRIBE, SETUP, PLAY, TEARDOWN, GET_PARAMETER",
                );
                (resp, SessionAction::None)
            }
            Method::Describe => {
                let sdp = crate::rtsp::sdp::build_sdp(ctx.camera_name, ctx.h264_params);
                self.state = State::Described;
                let resp = Response::ok(cseq)
                    .with_body("application/sdp", sdp)
                    .with_header("Content-Base", req.uri.clone());
                (resp, SessionAction::None)
            }
            Method::Setup => {
                let transport = match req.header("transport") {
                    Some(t) => t,
                    None => {
                        return (
                            Response::error(400, "Bad Request", cseq),
                            SessionAction::None,
                        );
                    }
                };
                let interleaved = match parse_interleaved(transport) {
                    Some(pair) => pair,
                    None => {
                        return (
                            Response::error(461, "Unsupported Transport", cseq),
                            SessionAction::None,
                        );
                    }
                };
                self.session_id = Some(ctx.new_session_id.to_string());
                self.rtp_channel = Some(interleaved.0);
                self.state = State::SetUp;
                let resp = Response::ok(cseq)
                    .with_header(
                        "Transport",
                        format!(
                            "RTP/AVP/TCP;unicast;interleaved={}-{}",
                            interleaved.0, interleaved.1
                        ),
                    )
                    .with_header("Session", ctx.new_session_id);
                (resp, SessionAction::None)
            }
            Method::Play => {
                if self.state != State::SetUp {
                    return (
                        Response::error(455, "Method Not Valid In This State", cseq),
                        SessionAction::None,
                    );
                }
                let session_id = match &self.session_id {
                    Some(s) => s.clone(),
                    None => {
                        return (
                            Response::error(454, "Session Not Found", cseq),
                            SessionAction::None,
                        );
                    }
                };
                self.state = State::Playing;
                let resp = Response::ok(cseq)
                    .with_header("Session", session_id)
                    .with_header("Range", "npt=0.000-");
                let action = SessionAction::StartStreaming {
                    rtp_channel: self.rtp_channel.unwrap_or(0),
                };
                (resp, action)
            }
            Method::Teardown => {
                let session_id = self.session_id.clone().unwrap_or_default();
                self.state = State::TornDown;
                let resp = Response::ok(cseq).with_header("Session", session_id);
                (resp, SessionAction::Stop)
            }
            Method::GetParameter => {
                // Used as a keepalive by VLC. Empty 200 OK is fine.
                let session_id = self.session_id.clone().unwrap_or_default();
                let resp = if session_id.is_empty() {
                    Response::ok(cseq)
                } else {
                    Response::ok(cseq).with_header("Session", session_id)
                };
                (resp, SessionAction::None)
            }
            Method::Unsupported => (
                Response::error(501, "Not Implemented", cseq),
                SessionAction::None,
            ),
        }
    }
}

impl Default for Session {
    fn default() -> Self {
        Self::new()
    }
}

/// Match the URI's path component against `expected_path`.
/// Accepts both `rtsp://host/path` and `path` (some clients send just the path).
fn uri_matches(uri: &str, expected_path: &str) -> bool {
    let path_part = uri
        .splitn(4, '/')
        .nth(3)
        .map(|s| s.trim_start_matches('/'))
        .unwrap_or(uri.trim_start_matches('/'));
    // Allow `<path>` and `<path>/streamid=N` (SETUP appends a streamid).
    let main = path_part.split('/').next().unwrap_or("");
    main.split(';').next().unwrap_or("") == expected_path
}

/// Parse `RTP/AVP/TCP;unicast;interleaved=A-B` and return `(A, B)`.
fn parse_interleaved(transport: &str) -> Option<(u8, u8)> {
    if !transport.contains("RTP/AVP/TCP") {
        return None;
    }
    for part in transport.split(';') {
        if let Some(rest) = part.trim().strip_prefix("interleaved=") {
            let (a, b) = rest.split_once('-')?;
            return Some((a.parse().ok()?, b.parse().ok()?));
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rtsp::message::parse_request;

    fn ctx<'a>() -> SessionContext<'a> {
        SessionContext {
            camera_name: "Buddy3D Camera",
            h264_params: Box::leak(Box::new(H264Params {
                profile_level_id: "42c01e".into(),
                sprop_parameter_sets: "Z0L,aM4".into(),
                packetization_mode: 1,
            })),
            expected_path: "buddy3d-camera",
            new_session_id: "TESTSESSION",
        }
    }

    fn req(raw: &[u8]) -> Request {
        parse_request(raw).unwrap().0
    }

    #[test]
    fn options_returns_public_methods_in_any_state() {
        let mut s = Session::new();
        let ctx = ctx();
        let (resp, action) = s.handle(
            &req(b"OPTIONS rtsp://h/ RTSP/1.0\r\nCSeq: 1\r\n\r\n"),
            &ctx,
        );
        assert_eq!(resp.status, 200);
        let formatted = resp.format();
        assert!(formatted.contains("Public:"));
        assert!(formatted.contains("DESCRIBE"));
        assert!(matches!(action, SessionAction::None));
    }

    #[test]
    fn describe_returns_sdp_and_advances_state() {
        let mut s = Session::new();
        let ctx = ctx();
        let (resp, _) = s.handle(
            &req(b"DESCRIBE rtsp://h/buddy3d-camera RTSP/1.0\r\nCSeq: 2\r\n\r\n"),
            &ctx,
        );
        assert_eq!(resp.status, 200);
        let formatted = resp.format();
        assert!(formatted.contains("Content-Type: application/sdp"));
        assert!(formatted.contains("\r\n\r\nv=0\r\n"));
        assert_eq!(s.state, State::Described);
    }

    #[test]
    fn setup_with_tcp_interleaved_succeeds() {
        let mut s = Session::new();
        let ctx = ctx();
        let (resp, _) = s.handle(
            &req(b"SETUP rtsp://h/buddy3d-camera/streamid=0 RTSP/1.0\r\nCSeq: 3\r\nTransport: RTP/AVP/TCP;unicast;interleaved=0-1\r\n\r\n"),
            &ctx,
        );
        assert_eq!(resp.status, 200);
        let formatted = resp.format();
        assert!(formatted.contains("Session: TESTSESSION"));
        assert!(formatted.contains("interleaved=0-1"));
        assert_eq!(s.state, State::SetUp);
        assert_eq!(s.rtp_channel, Some(0));
    }

    #[test]
    fn setup_rejects_udp_transport() {
        let mut s = Session::new();
        let ctx = ctx();
        let (resp, _) = s.handle(
            &req(b"SETUP rtsp://h/buddy3d-camera/streamid=0 RTSP/1.0\r\nCSeq: 3\r\nTransport: RTP/AVP;unicast;client_port=8000-8001\r\n\r\n"),
            &ctx,
        );
        assert_eq!(resp.status, 461);
    }

    #[test]
    fn play_before_setup_returns_455() {
        let mut s = Session::new();
        let ctx = ctx();
        let (resp, _) = s.handle(
            &req(b"PLAY rtsp://h/buddy3d-camera RTSP/1.0\r\nCSeq: 4\r\n\r\n"),
            &ctx,
        );
        assert_eq!(resp.status, 455);
    }

    #[test]
    fn play_after_setup_starts_streaming() {
        let mut s = Session::new();
        let ctx = ctx();
        s.handle(
            &req(b"SETUP rtsp://h/buddy3d-camera/streamid=0 RTSP/1.0\r\nCSeq: 3\r\nTransport: RTP/AVP/TCP;unicast;interleaved=0-1\r\n\r\n"),
            &ctx,
        );
        let (resp, action) = s.handle(
            &req(b"PLAY rtsp://h/buddy3d-camera RTSP/1.0\r\nCSeq: 4\r\nSession: TESTSESSION\r\n\r\n"),
            &ctx,
        );
        assert_eq!(resp.status, 200);
        match action {
            SessionAction::StartStreaming { rtp_channel } => assert_eq!(rtp_channel, 0),
            other => panic!("expected StartStreaming, got {other:?}"),
        }
        assert_eq!(s.state, State::Playing);
    }

    #[test]
    fn teardown_returns_stop_action() {
        let mut s = Session::new();
        let ctx = ctx();
        s.handle(
            &req(b"SETUP rtsp://h/buddy3d-camera/streamid=0 RTSP/1.0\r\nCSeq: 3\r\nTransport: RTP/AVP/TCP;unicast;interleaved=0-1\r\n\r\n"),
            &ctx,
        );
        let (resp, action) = s.handle(
            &req(b"TEARDOWN rtsp://h/buddy3d-camera RTSP/1.0\r\nCSeq: 5\r\nSession: TESTSESSION\r\n\r\n"),
            &ctx,
        );
        assert_eq!(resp.status, 200);
        assert!(matches!(action, SessionAction::Stop));
    }

    #[test]
    fn unknown_method_returns_501() {
        let mut s = Session::new();
        let ctx = ctx();
        let (resp, _) = s.handle(
            &req(b"PAUSE rtsp://h/buddy3d-camera RTSP/1.0\r\nCSeq: 9\r\n\r\n"),
            &ctx,
        );
        assert_eq!(resp.status, 501);
    }

    #[test]
    fn wrong_path_returns_404() {
        let mut s = Session::new();
        let ctx = ctx();
        let (resp, _) = s.handle(
            &req(b"DESCRIBE rtsp://h/wrong-camera RTSP/1.0\r\nCSeq: 2\r\n\r\n"),
            &ctx,
        );
        assert_eq!(resp.status, 404);
    }

    #[test]
    fn parse_interleaved_handles_typical_input() {
        assert_eq!(
            parse_interleaved("RTP/AVP/TCP;unicast;interleaved=0-1"),
            Some((0, 1))
        );
        assert_eq!(
            parse_interleaved("RTP/AVP/TCP;unicast;interleaved=2-3;mode=play"),
            Some((2, 3))
        );
        assert_eq!(parse_interleaved("RTP/AVP;unicast"), None);
    }
}
