//! H.264 IDR → JPEG via openh264 + image. Synchronous; intended to be called
//! from `tokio::task::spawn_blocking`.

use bytes::Bytes;
use image::codecs::jpeg::JpegEncoder;
use openh264::decoder::Decoder;
use openh264::formats::YUVSource;

const JPEG_QUALITY: u8 = 85;

/// Annex-B start code prepended before each NAL unit.
const START_CODE: &[u8] = &[0x00, 0x00, 0x00, 0x01];

#[derive(Debug, thiserror::Error)]
pub enum EncodeError {
    #[error("openh264 init failed: {0}")]
    Init(String),
    #[error("openh264 decode failed: {0}")]
    Decode(String),
    #[error("openh264 produced no frame from this IDR")]
    NoFrame,
    #[error("jpeg encode failed: {0}")]
    Jpeg(#[from] image::ImageError),
}

/// Build an Annex-B byte stream from raw NAL units (no start codes).
/// Order matters: SPS first, then PPS, then IDR.
pub fn build_annex_b(sps: &[u8], pps: &[u8], idr: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(START_CODE.len() * 3 + sps.len() + pps.len() + idr.len());
    out.extend_from_slice(START_CODE);
    out.extend_from_slice(sps);
    out.extend_from_slice(START_CODE);
    out.extend_from_slice(pps);
    out.extend_from_slice(START_CODE);
    out.extend_from_slice(idr);
    out
}

/// Decode `annex_b` (which must contain SPS + PPS + an IDR) into a single
/// JPEG-encoded frame. Synchronous and CPU-bound.
pub fn decode_to_jpeg(annex_b: &[u8]) -> Result<Bytes, EncodeError> {
    let mut decoder = Decoder::new().map_err(|e| EncodeError::Init(e.to_string()))?;
    let frame = decoder
        .decode(annex_b)
        .map_err(|e| EncodeError::Decode(e.to_string()))?
        .ok_or(EncodeError::NoFrame)?;

    let (width, height) = frame.dimensions();
    let mut rgb = vec![0u8; frame.estimate_rgb_u8_size()];
    frame.write_rgb8(&mut rgb);

    let mut jpeg = Vec::with_capacity(rgb.len() / 4);
    JpegEncoder::new_with_quality(&mut jpeg, JPEG_QUALITY).encode(
        &rgb,
        width as u32,
        height as u32,
        image::ExtendedColorType::Rgb8,
    )?;
    Ok(Bytes::from(jpeg))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_annex_b_orders_sps_pps_idr_with_start_codes() {
        let sps = b"SPS";
        let pps = b"PPS";
        let idr = b"IDR";
        let out = build_annex_b(sps, pps, idr);
        assert_eq!(
            out.as_slice(),
            &[
                0, 0, 0, 1, b'S', b'P', b'S',
                0, 0, 0, 1, b'P', b'P', b'S',
                0, 0, 0, 1, b'I', b'D', b'R',
            ]
        );
    }

    /// Sanity test: feed openh264 a minimal byte stream and assert the
    /// wrapper plumbs the API without panicking. invalid IDR → either
    /// NoFrame or Decode error.
    #[test]
    fn decode_to_jpeg_handles_invalid_input_cleanly() {
        let bogus = vec![0, 0, 0, 1, 0x67, 0x00];
        let res = decode_to_jpeg(&bogus);
        assert!(res.is_err(), "expected error from invalid IDR, got Ok");
    }
}
