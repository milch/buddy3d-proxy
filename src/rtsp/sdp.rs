//! SDP generator for the RTSP DESCRIBE response. Pure formatting/parsing.
//!
//! The shape we emit is the minimum that VLC, ffmpeg, Frigate, and go2rtc
//! all accept for an H.264 video-only stream over interleaved RTP/AVP/TCP.

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct H264Params {
    /// Hex-encoded profile-level-id (e.g. "42c01e" for constrained baseline 3.0).
    pub profile_level_id: String,
    /// Comma-separated base64 SPS,PPS,... NAL units.
    pub sprop_parameter_sets: String,
    /// Packetization mode (1 = non-interleaved single-NAL/STAP-A/FU-A).
    pub packetization_mode: u8,
}

/// Build an SDP for a video-only H.264 stream.
///
/// `session_name` is shown in some clients (e.g. VLC's title bar). Pass
/// the camera display name (or "Buddy3D Proxy" if unknown).
pub fn build_sdp(session_name: &str, params: &H264Params) -> String {
    format!(
        "v=0\r\n\
         o=- 0 0 IN IP4 0.0.0.0\r\n\
         s={name}\r\n\
         c=IN IP4 0.0.0.0\r\n\
         t=0 0\r\n\
         m=video 0 RTP/AVP 96\r\n\
         a=rtpmap:96 H264/90000\r\n\
         a=fmtp:96 packetization-mode={mode};profile-level-id={pli};sprop-parameter-sets={pps}\r\n\
         a=control:streamid=0\r\n",
        name = session_name,
        mode = params.packetization_mode,
        pli = params.profile_level_id,
        pps = params.sprop_parameter_sets,
    )
}

/// Parse a webrtc-rs negotiated SDP (the offer from the camera, available via
/// `pc.remote_description()` after `set_remote_description`) and extract the
/// H.264 codec params. Returns `None` if the SDP doesn't include H.264 with the
/// expected `fmtp` line.
pub fn extract_h264_params(remote_sdp: &str) -> Option<H264Params> {
    // Find every `a=fmtp:N <kvs>` line; one of them should mention sprop-parameter-sets.
    for line in remote_sdp.lines() {
        let line = line.trim_end_matches('\r');
        let kv_part = match line.strip_prefix("a=fmtp:") {
            Some(s) => s,
            None => continue,
        };
        // `<pt> <kvs>` — skip the payload type number.
        let kv_part = match kv_part.split_once(' ') {
            Some((_pt, rest)) => rest,
            None => continue,
        };
        let mut profile_level_id = None;
        let mut sprop = None;
        let mut packetization_mode = 1u8;
        for kv in kv_part.split(';') {
            let (k, v) = match kv.split_once('=') {
                Some(pair) => pair,
                None => continue,
            };
            match k.trim() {
                "profile-level-id" => profile_level_id = Some(v.trim().to_string()),
                "sprop-parameter-sets" => sprop = Some(v.trim().to_string()),
                "packetization-mode" => {
                    if let Ok(n) = v.trim().parse() {
                        packetization_mode = n;
                    }
                }
                _ => {}
            }
        }
        if let (Some(pli), Some(p)) = (profile_level_id, sprop) {
            return Some(H264Params {
                profile_level_id: pli,
                sprop_parameter_sets: p,
                packetization_mode,
            });
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_sdp_emits_required_lines() {
        let params = H264Params {
            profile_level_id: "42c01e".into(),
            sprop_parameter_sets: "Z0LAHtoHgUUg,aM48gA==".into(),
            packetization_mode: 1,
        };
        let sdp = build_sdp("Buddy3D Camera", &params);
        assert!(sdp.starts_with("v=0\r\n"));
        assert!(sdp.contains("\r\ns=Buddy3D Camera\r\n"));
        assert!(sdp.contains("\r\nm=video 0 RTP/AVP 96\r\n"));
        assert!(sdp.contains("a=rtpmap:96 H264/90000\r\n"));
        assert!(sdp.contains("profile-level-id=42c01e"));
        assert!(sdp.contains("sprop-parameter-sets=Z0LAHtoHgUUg,aM48gA=="));
        assert!(sdp.contains("packetization-mode=1"));
        assert!(sdp.ends_with("a=control:streamid=0\r\n"));
    }

    #[test]
    fn extract_h264_params_picks_up_offer_fmtp() {
        let offer = "v=0\r\n\
                     o=rtc 643167161 0 IN IP4 127.0.0.1\r\n\
                     m=video 9 UDP/TLS/RTP/SAVPF 96\r\n\
                     a=rtpmap:96 H264/90000\r\n\
                     a=fmtp:96 level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=42c01e;sprop-parameter-sets=Z0LAHtoHgUUg,aM48gA==\r\n";
        let params = extract_h264_params(offer).unwrap();
        assert_eq!(params.profile_level_id, "42c01e");
        assert_eq!(params.sprop_parameter_sets, "Z0LAHtoHgUUg,aM48gA==");
        assert_eq!(params.packetization_mode, 1);
    }

    #[test]
    fn extract_h264_params_returns_none_for_no_h264() {
        let offer = "v=0\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111\r\na=rtpmap:111 opus/48000/2\r\n";
        assert!(extract_h264_params(offer).is_none());
    }

    #[test]
    fn extract_h264_params_skips_fmtp_without_sprop() {
        // First fmtp line has no sprop; second one has both.
        let offer = "m=video 9 UDP/TLS/RTP/SAVPF 100 96\r\n\
                     a=rtpmap:100 VP8/90000\r\n\
                     a=fmtp:100 max-fr=30\r\n\
                     a=rtpmap:96 H264/90000\r\n\
                     a=fmtp:96 packetization-mode=1;profile-level-id=42e01f;sprop-parameter-sets=ABCD,EFGH\r\n";
        let params = extract_h264_params(offer).unwrap();
        assert_eq!(params.profile_level_id, "42e01f");
    }
}
