//! Binary frame header and payload codec (WIRE.md §2.2-§2.4).
//!
//! One WebSocket binary message carries exactly one frame: an 8-byte
//! big-endian header (`type`, `flags`, two reserved bytes, `stream_id`)
//! followed by the payload. [`decode`] enforces every rule that needs no
//! connection state; rules that do (`highest_opened`, credit, the drain
//! boundary, stream states) belong to the connection reader.

use bytes::{BufMut, Bytes, BytesMut};
use serde::Serialize;

use crate::error::{ErrorCode, truncate_utf8};

/// Size of the fixed frame header.
pub(crate) const HEADER_LEN: usize = 8;
/// Largest frame payload.
pub(crate) const MAX_PAYLOAD: usize = 65_536;
/// Largest WebSocket message: header plus [`MAX_PAYLOAD`].
pub(crate) const MAX_MESSAGE: usize = HEADER_LEN + MAX_PAYLOAD;
/// Largest stream-0 (control channel) payload.
pub(crate) const MAX_CONTROL_PAYLOAD: usize = 16_384;
/// DATA chunk size the writer uses (the spec's recommended chunk).
pub(crate) const DATA_CHUNK: usize = 16_384;
/// Longest RESET message we send (WIRE.md §2.3: SHOULD be at most 256 bytes).
pub(crate) const MAX_RESET_MESSAGE: usize = 256;

const STREAM_ID_HIGH_BIT: u32 = 0x8000_0000;

const TYPE_OPEN: u8 = 0x00;
const TYPE_DATA: u8 = 0x01;
const TYPE_WINDOW: u8 = 0x02;
const TYPE_CLOSE: u8 = 0x03;
const TYPE_RESET: u8 = 0x04;

/// A decoded frame of one of the five v1 types.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Frame {
    /// The server opened `stream_id` (odd, non-zero).
    Open { stream_id: u32 },
    /// Stream bytes. On stream 0, one control message of at most
    /// [`MAX_CONTROL_PAYLOAD`] bytes.
    Data { stream_id: u32, payload: Bytes },
    /// Flow-control credit, `1..=2^31-1`.
    Window { stream_id: u32, increment: u32 },
    /// Half-close. `trailing` is the number of payload bytes that were
    /// ignored; the reader counts frames where it is non-zero. Encoding
    /// always writes an empty payload.
    Close { stream_id: u32, trailing: usize },
    /// Abort the stream. `message` is decoded lossily.
    Reset {
        stream_id: u32,
        code: ErrorCode,
        message: String,
    },
}

impl Frame {
    pub(crate) fn stream_id(&self) -> u32 {
        match *self {
            Self::Open { stream_id }
            | Self::Data { stream_id, .. }
            | Self::Window { stream_id, .. }
            | Self::Close { stream_id, .. }
            | Self::Reset { stream_id, .. } => stream_id,
        }
    }

    /// Wire form of this frame. Flags and reserved bytes are written as 0.
    #[cfg(test)]
    pub(crate) fn encode(&self) -> Bytes {
        match self {
            Self::Open { stream_id } => header_only(TYPE_OPEN, *stream_id),
            Self::Data { stream_id, payload } => encode_data(*stream_id, payload),
            Self::Window {
                stream_id,
                increment,
            } => encode_window(*stream_id, *increment),
            Self::Close { stream_id, .. } => encode_close(*stream_id),
            Self::Reset {
                stream_id,
                code,
                message,
            } => encode_reset(*stream_id, *code, message),
        }
    }
}

/// Result of [`decode`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Decoded {
    Frame(Frame),
    /// A frame type outside the v1 set: ignore and count (WIRE.md §2.2).
    Unknown {
        frame_type: u8,
        stream_id: u32,
        payload_len: usize,
    },
}

/// A frame the peer must not have sent. Every message names a number taken
/// from the offending frame.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub(crate) enum FrameError {
    /// Connection-fatal: send `error{code}` and close with the code's close code.
    #[error("{code}: {message}")]
    Connection { code: ErrorCode, message: String },
    /// Scoped to one stream: answer with `RESET(code)` on `stream_id`.
    ///
    /// The decoder cannot know stream state, so the reader must first apply
    /// the stream-id checks (WIRE.md §2.5): an id above `highest_opened` is a
    /// connection `PROTOCOL_ERROR`, and a frame for a dead stream is
    /// discarded and counted. Only a live stream gets the RESET.
    #[error("stream {stream_id} {code}: {message}")]
    Stream {
        stream_id: u32,
        code: ErrorCode,
        message: String,
    },
}

#[cfg(test)]
impl FrameError {
    pub(crate) fn code(&self) -> ErrorCode {
        match *self {
            Self::Connection { code, .. } | Self::Stream { code, .. } => code,
        }
    }

    pub(crate) fn message(&self) -> &str {
        match self {
            Self::Connection { message, .. } | Self::Stream { message, .. } => message,
        }
    }
}

fn conn_err(code: ErrorCode, message: String) -> FrameError {
    FrameError::Connection { code, message }
}

/// Decodes one WebSocket binary message (WIRE.md §2.2-§2.4).
///
/// Checks run in this order: message length, the stream id's reserved high
/// bit, unknown type, non-DATA on stream 0, then type-specific payload rules.
/// The high-bit check deliberately precedes the type check, so an unknown
/// type with a bad stream id is still fatal. DATA payloads are slices of
/// `msg`, not copies.
pub(crate) fn decode(msg: Bytes) -> Result<Decoded, FrameError> {
    let len = msg.len();
    if len < HEADER_LEN {
        return Err(conn_err(
            ErrorCode::PROTOCOL_ERROR,
            format!("message of {len} bytes is shorter than the {HEADER_LEN}-byte frame header"),
        ));
    }
    if len > MAX_MESSAGE {
        return Err(conn_err(
            ErrorCode::FRAME_SIZE_ERROR,
            format!("message of {len} bytes exceeds the {MAX_MESSAGE}-byte limit"),
        ));
    }
    let frame_type = msg[0];
    // msg[1] (flags) and msg[2..4] (reserved) are ignored on receive.
    let stream_id = u32::from_be_bytes([msg[4], msg[5], msg[6], msg[7]]);
    let payload_len = len - HEADER_LEN;

    if stream_id & STREAM_ID_HIGH_BIT != 0 {
        return Err(conn_err(
            ErrorCode::PROTOCOL_ERROR,
            format!("stream id {stream_id:#010x} has the reserved high bit set"),
        ));
    }

    let name = match frame_type {
        TYPE_OPEN => "OPEN",
        TYPE_DATA => "DATA",
        TYPE_WINDOW => "WINDOW",
        TYPE_CLOSE => "CLOSE",
        TYPE_RESET => "RESET",
        _ => {
            return Ok(Decoded::Unknown {
                frame_type,
                stream_id,
                payload_len,
            });
        }
    };

    if stream_id == 0 && frame_type != TYPE_DATA {
        return Err(conn_err(
            ErrorCode::PROTOCOL_ERROR,
            format!(
                "{name} (type {frame_type:#04x}) on stream 0: only DATA is legal on the control channel"
            ),
        ));
    }

    let frame = match frame_type {
        TYPE_OPEN => {
            if stream_id % 2 == 0 {
                return Err(conn_err(
                    ErrorCode::PROTOCOL_ERROR,
                    format!("OPEN for even stream id {stream_id}: server-opened ids are odd"),
                ));
            }
            Frame::Open { stream_id }
        }
        TYPE_DATA => {
            if stream_id == 0 && payload_len > MAX_CONTROL_PAYLOAD {
                return Err(conn_err(
                    ErrorCode::ENHANCE_YOUR_CALM,
                    format!(
                        "stream 0 payload of {payload_len} bytes exceeds the {MAX_CONTROL_PAYLOAD}-byte control channel limit"
                    ),
                ));
            }
            Frame::Data {
                stream_id,
                payload: msg.slice(HEADER_LEN..),
            }
        }
        TYPE_WINDOW => {
            if payload_len != 4 {
                return Err(conn_err(
                    ErrorCode::FRAME_SIZE_ERROR,
                    format!(
                        "WINDOW on stream {stream_id} has a {payload_len}-byte payload, must be exactly 4"
                    ),
                ));
            }
            let increment = u32::from_be_bytes([msg[8], msg[9], msg[10], msg[11]]);
            if increment == 0 {
                return Err(FrameError::Stream {
                    stream_id,
                    code: ErrorCode::PROTOCOL_ERROR,
                    message: format!(
                        "WINDOW increment 0 on stream {stream_id}: legal range is 1..=2^31-1"
                    ),
                });
            }
            if increment & STREAM_ID_HIGH_BIT != 0 {
                return Err(conn_err(
                    ErrorCode::FLOW_CONTROL_ERROR,
                    format!("WINDOW increment {increment} on stream {stream_id} exceeds 2^31-1"),
                ));
            }
            Frame::Window {
                stream_id,
                increment,
            }
        }
        TYPE_CLOSE => Frame::Close {
            stream_id,
            trailing: payload_len,
        },
        TYPE_RESET => {
            if payload_len < 4 {
                return Err(conn_err(
                    ErrorCode::FRAME_SIZE_ERROR,
                    format!(
                        "RESET on stream {stream_id} has a {payload_len}-byte payload, must be at least 4"
                    ),
                ));
            }
            let code = u32::from_be_bytes([msg[8], msg[9], msg[10], msg[11]]);
            Frame::Reset {
                stream_id,
                code: ErrorCode(code),
                message: String::from_utf8_lossy(&msg[12..]).into_owned(),
            }
        }
        _ => unreachable!("frame type {frame_type:#04x} was matched above"),
    };
    Ok(Decoded::Frame(frame))
}

fn with_header(frame_type: u8, stream_id: u32, payload_len: usize) -> BytesMut {
    debug_assert_eq!(
        stream_id & STREAM_ID_HIGH_BIT,
        0,
        "stream id {stream_id:#x}"
    );
    let mut buf = BytesMut::with_capacity(HEADER_LEN + payload_len);
    buf.put_u8(frame_type);
    buf.put_u8(0);
    buf.put_u16(0);
    buf.put_u32(stream_id);
    buf
}

fn header_only(frame_type: u8, stream_id: u32) -> Bytes {
    with_header(frame_type, stream_id, 0).freeze()
}

/// A DATA frame carrying `payload` (at most [`MAX_PAYLOAD`] bytes).
pub(crate) fn encode_data(stream_id: u32, payload: &[u8]) -> Bytes {
    debug_assert!(payload.len() <= MAX_PAYLOAD, "{} bytes", payload.len());
    let mut buf = with_header(TYPE_DATA, stream_id, payload.len());
    buf.put_slice(payload);
    buf.freeze()
}

/// A WINDOW frame granting `increment` (`1..=2^31-1`) bytes of credit.
pub(crate) fn encode_window(stream_id: u32, increment: u32) -> Bytes {
    debug_assert!(
        increment != 0 && increment & STREAM_ID_HIGH_BIT == 0,
        "increment {increment}"
    );
    let mut buf = with_header(TYPE_WINDOW, stream_id, 4);
    buf.put_u32(increment);
    buf.freeze()
}

/// A CLOSE (half-close) frame.
pub(crate) fn encode_close(stream_id: u32) -> Bytes {
    header_only(TYPE_CLOSE, stream_id)
}

/// A RESET frame. `message` is truncated to [`MAX_RESET_MESSAGE`] bytes on a
/// character boundary.
pub(crate) fn encode_reset(stream_id: u32, code: ErrorCode, message: &str) -> Bytes {
    let message = truncate_utf8(message, MAX_RESET_MESSAGE);
    let mut buf = with_header(TYPE_RESET, stream_id, 4 + message.len());
    buf.put_u32(code.0);
    buf.put_slice(message.as_bytes());
    buf.freeze()
}

/// The encoded control message is larger than [`MAX_CONTROL_PAYLOAD`].
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(crate) struct ControlTooLarge {
    /// Size of the JSON payload.
    pub bytes: usize,
}

/// A stream-0 DATA frame carrying `msg` as JSON.
pub(crate) fn encode_control<T: Serialize>(msg: &T) -> Result<Bytes, ControlTooLarge> {
    let mut writer = with_header(TYPE_DATA, 0, 256).writer();
    serde_json::to_writer(&mut writer, msg)
        .expect("control messages serialize infallibly: plain structs and string-keyed maps");
    let buf = writer.into_inner();
    let bytes = buf.len() - HEADER_LEN;
    if bytes > MAX_CONTROL_PAYLOAD {
        return Err(ControlTooLarge { bytes });
    }
    Ok(buf.freeze())
}

#[cfg(test)]
pub(crate) mod spec_fixtures {
    //! Locates a ws-mixer-spec checkout for the fixture-driven tests.

    use std::path::{Path, PathBuf};

    /// The spec checkout's `spec/` directory, or `None` (after explaining on
    /// stderr) when there is none, so the fixture tests skip instead of fail.
    ///
    /// Resolution order: `$WSMIXER_SPEC_DIR` (either the repo root or its
    /// `spec/` directory; set but missing is a hard failure), then `.spec`
    /// (the `make fetch-spec` checkout), then `../ws-mixer-spec`.
    pub(crate) fn spec_dir() -> Option<PathBuf> {
        if let Some(dir) = std::env::var_os("WSMIXER_SPEC_DIR") {
            let dir = PathBuf::from(dir);
            return Some(resolve(&dir).unwrap_or_else(|| {
                panic!(
                    "WSMIXER_SPEC_DIR={}: found neither spec/fixtures nor fixtures under it",
                    dir.display()
                )
            }));
        }
        let root = Path::new(env!("CARGO_MANIFEST_DIR"));
        for candidate in [root.join(".spec"), root.join("../ws-mixer-spec")] {
            if let Some(dir) = resolve(&candidate) {
                return Some(dir);
            }
        }
        eprintln!(
            "skipping spec fixture test: no spec checkout (set WSMIXER_SPEC_DIR or run `make fetch-spec`)"
        );
        None
    }

    fn resolve(dir: &Path) -> Option<PathBuf> {
        if dir.join("spec/fixtures").is_dir() {
            Some(dir.join("spec"))
        } else if dir.join("fixtures").is_dir() {
            Some(dir.to_path_buf())
        } else {
            None
        }
    }

    /// Every `*.json` file under `dir`, recursively, sorted.
    pub(crate) fn json_files(dir: &Path) -> Vec<PathBuf> {
        let mut out = Vec::new();
        let mut pending = vec![dir.to_path_buf()];
        while let Some(d) = pending.pop() {
            for entry in std::fs::read_dir(&d).unwrap_or_else(|e| panic!("{}: {e}", d.display())) {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    pending.push(path);
                } else if path.extension().is_some_and(|e| e == "json") {
                    out.push(path);
                }
            }
        }
        out.sort();
        out
    }

    pub(crate) fn from_hex(s: &str) -> Vec<u8> {
        assert!(s.len() % 2 == 0, "odd-length hex");
        (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).expect("hex digit"))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::spec_fixtures::{from_hex, json_files, spec_dir};
    use super::*;
    use serde_json::Value;

    fn dec(raw: &[u8]) -> Result<Decoded, FrameError> {
        decode(Bytes::copy_from_slice(raw))
    }

    fn frame(raw: &[u8]) -> Frame {
        match dec(raw) {
            Ok(Decoded::Frame(f)) => f,
            other => panic!("expected a frame, got {other:?}"),
        }
    }

    fn assert_has_digit(e: &FrameError) {
        assert!(
            e.message().bytes().any(|b| b.is_ascii_digit()),
            "error message carries no number: {e}"
        );
    }

    #[test]
    fn spec_frame_fixtures() {
        let Some(spec) = spec_dir() else { return };
        let root = spec.join("fixtures/frames");
        let files = json_files(&root);
        assert!(
            files.len() >= 29,
            "only {} frame fixtures under {}, expected at least 29",
            files.len(),
            root.display()
        );
        for path in &files {
            let name = path.file_name().unwrap().to_string_lossy();
            let fx: Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
            let raw = from_hex(fx["hex"].as_str().unwrap());
            let got = dec(&raw);
            if fx["valid"].as_bool().unwrap() {
                let got = got.unwrap_or_else(|e| panic!("{name}: expected valid, got {e}"));
                check_decoded(&name, &got, &fx["decoded"]);
                continue;
            }
            let err = match got {
                Err(e) => e,
                Ok(d) => panic!("{name}: expected an error, decoded {d:?}"),
            };
            let expect = &fx["expect"];
            assert_eq!(
                err.code().name(),
                expect["error_code"].as_str().unwrap(),
                "{name}: {err}"
            );
            let fatal = expect["connection_fatal"].as_bool().unwrap();
            assert_eq!(
                matches!(err, FrameError::Connection { .. }),
                fatal,
                "{name}: {err:?}"
            );
            if let Some(needle) = expect["message_contains"].as_str() {
                assert!(
                    err.message().contains(needle),
                    "{name}: {needle:?} not in {err}"
                );
            }
            assert_has_digit(&err);
        }
        eprintln!("spec frame fixtures exercised: {}", files.len());
    }

    fn check_decoded(name: &str, got: &Decoded, want: &Value) {
        let want_u64 = |k: &str| want.get(k).and_then(Value::as_u64);
        let (ty, stream_id) = match got {
            Decoded::Frame(f) => (
                match f {
                    Frame::Open { .. } => "OPEN".to_owned(),
                    Frame::Data { .. } => "DATA".to_owned(),
                    Frame::Window { .. } => "WINDOW".to_owned(),
                    Frame::Close { .. } => "CLOSE".to_owned(),
                    Frame::Reset { .. } => "RESET".to_owned(),
                },
                f.stream_id(),
            ),
            Decoded::Unknown {
                frame_type,
                stream_id,
                ..
            } => (format!("UNKNOWN({frame_type:#04x})"), *stream_id),
        };
        assert_eq!(ty, want["type"].as_str().unwrap(), "{name}: type");
        assert_eq!(
            Some(u64::from(stream_id)),
            want_u64("stream_id"),
            "{name}: stream_id"
        );

        let payload_hex = want.get("payload_hex").and_then(Value::as_str);
        let payload_len = payload_hex
            .map(|h| h.len() / 2)
            .or(want_u64("payload_length").map(|n| n as usize));
        match got {
            Decoded::Frame(Frame::Data { payload, .. }) => {
                if let Some(h) = payload_hex {
                    assert_eq!(payload.as_ref(), from_hex(h).as_slice(), "{name}: payload");
                }
                assert_eq!(Some(payload.len()), payload_len, "{name}: payload length");
            }
            Decoded::Frame(Frame::Close { trailing, .. }) => {
                assert_eq!(Some(*trailing), payload_len, "{name}: trailing bytes");
            }
            Decoded::Frame(Frame::Open { .. }) => {
                assert_eq!(payload_len, Some(0), "{name}: OPEN payload");
            }
            Decoded::Frame(Frame::Window { increment, .. }) => {
                assert_eq!(
                    Some(u64::from(*increment)),
                    want_u64("increment"),
                    "{name}: increment"
                );
            }
            Decoded::Frame(Frame::Reset { code, message, .. }) => {
                assert_eq!(Some(u64::from(code.0)), want_u64("code"), "{name}: code");
                assert_eq!(
                    Some(message.as_str()),
                    want["message"].as_str(),
                    "{name}: message"
                );
            }
            Decoded::Unknown { payload_len: n, .. } => {
                assert_eq!(Some(*n), payload_len, "{name}: payload length");
            }
        }
    }

    #[test]
    fn undersize_messages_are_protocol_errors() {
        for n in 0..HEADER_LEN {
            let err = dec(&vec![1u8; n]).unwrap_err();
            assert_eq!(err.code(), ErrorCode::PROTOCOL_ERROR, "{n} bytes");
            assert!(matches!(err, FrameError::Connection { .. }));
            assert!(err.message().contains(&format!("{n} bytes")), "{err}");
        }
    }

    #[test]
    fn oversize_messages_are_frame_size_errors() {
        let ok = encode_data(1, &vec![0xab; MAX_PAYLOAD]);
        assert_eq!(ok.len(), MAX_MESSAGE);
        assert!(matches!(
            decode(ok.clone()),
            Ok(Decoded::Frame(Frame::Data { ref payload, .. })) if payload.len() == MAX_PAYLOAD
        ));
        let mut big = BytesMut::from(&ok[..]);
        big.put_u8(0);
        let err = decode(big.freeze()).unwrap_err();
        assert_eq!(err.code(), ErrorCode::FRAME_SIZE_ERROR);
        assert!(err.message().contains("65545"), "{err}");
        // Size is checked before anything else, even for unknown types.
        let mut unknown = vec![0x7f, 0, 0, 0, 0, 0, 0, 1];
        unknown.resize(MAX_MESSAGE + 1, 0);
        assert_eq!(
            dec(&unknown).unwrap_err().code(),
            ErrorCode::FRAME_SIZE_ERROR
        );
    }

    #[test]
    fn data_payload_is_zero_copy() {
        let msg = encode_data(3, b"hello");
        let base = msg.as_ptr();
        match decode(msg) {
            Ok(Decoded::Frame(Frame::Data { payload, .. })) => {
                assert_eq!(payload.as_ptr(), base.wrapping_add(HEADER_LEN));
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn stream_zero_payload_limit() {
        let at = vec![b' '; MAX_CONTROL_PAYLOAD];
        assert!(dec(&encode_data(0, &at)).is_ok());
        let over = vec![b' '; MAX_CONTROL_PAYLOAD + 1];
        let err = dec(&encode_data(0, &over)).unwrap_err();
        assert_eq!(err.code(), ErrorCode::ENHANCE_YOUR_CALM);
        assert!(err.message().contains("16385"), "{err}");
        // Non-zero streams may carry up to 64 KiB.
        assert!(dec(&encode_data(1, &over)).is_ok());
    }

    #[test]
    fn window_payload_length_and_zero() {
        for len in [0usize, 1, 3, 5, 8] {
            let mut raw = vec![TYPE_WINDOW, 0, 0, 0, 0, 0, 0, 5];
            raw.resize(HEADER_LEN + len, 0x01);
            let err = dec(&raw).unwrap_err();
            assert_eq!(err.code(), ErrorCode::FRAME_SIZE_ERROR, "len {len}");
            assert!(matches!(err, FrameError::Connection { .. }), "len {len}");
            assert!(err.message().contains(&format!("{len}-byte")), "{err}");
        }
        let err = dec(&[TYPE_WINDOW, 0, 0, 0, 0, 0, 0, 5, 0, 0, 0, 0]).unwrap_err();
        assert_eq!(
            err,
            FrameError::Stream {
                stream_id: 5,
                code: ErrorCode::PROTOCOL_ERROR,
                message: "WINDOW increment 0 on stream 5: legal range is 1..=2^31-1".into(),
            }
        );
        assert_eq!(
            frame(&encode_window(5, 0x7fff_ffff)),
            Frame::Window {
                stream_id: 5,
                increment: 0x7fff_ffff
            }
        );
        let err = dec(&[TYPE_WINDOW, 0, 0, 0, 0, 0, 0, 5, 0x80, 0, 0, 0]).unwrap_err();
        assert_eq!(err.code(), ErrorCode::FLOW_CONTROL_ERROR);
        assert!(err.message().contains("2147483648"), "{err}");
    }

    #[test]
    fn reset_short_payload_and_lossy_message() {
        for len in 0..4 {
            let mut raw = vec![TYPE_RESET, 0, 0, 0, 0, 0, 0, 1];
            raw.resize(HEADER_LEN + len, 0);
            let err = dec(&raw).unwrap_err();
            assert_eq!(err.code(), ErrorCode::FRAME_SIZE_ERROR, "len {len}");
            assert!(matches!(err, FrameError::Connection { .. }));
        }
        let mut raw = vec![TYPE_RESET, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 7];
        raw.extend_from_slice(b"bad \xff\xfe utf8 \xe2\x82");
        assert_eq!(
            frame(&raw),
            Frame::Reset {
                stream_id: 1,
                code: ErrorCode::CANCEL,
                message: "bad \u{fffd}\u{fffd} utf8 \u{fffd}".into(),
            }
        );
    }

    #[test]
    fn reset_message_truncated_on_send() {
        let long = "é".repeat(200);
        match frame(&encode_reset(1, ErrorCode::CANCEL, &long)) {
            Frame::Reset { message, .. } => {
                assert_eq!(message.len(), MAX_RESET_MESSAGE);
                assert_eq!(message, "é".repeat(128));
            }
            other => panic!("{other:?}"),
        }
        let odd = format!("a{}", "é".repeat(200));
        match frame(&encode_reset(1, ErrorCode::CANCEL, &odd)) {
            Frame::Reset { message, .. } => assert_eq!(message.len(), MAX_RESET_MESSAGE - 1),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn close_trailing_bytes_ignored_and_reported() {
        assert_eq!(
            frame(&[TYPE_CLOSE, 0, 0, 0, 0, 0, 0, 9, 1, 2, 3]),
            Frame::Close {
                stream_id: 9,
                trailing: 3
            }
        );
        assert_eq!(encode_close(9).as_ref(), &[TYPE_CLOSE, 0, 0, 0, 0, 0, 0, 9]);
    }

    #[test]
    fn unknown_type_is_reported_not_rejected() {
        for ty in [0x05u8, 0x10, 0x7f, 0xff] {
            assert_eq!(
                dec(&[ty, 0xff, 0xff, 0xff, 0, 0, 0, 0, 1, 2]).unwrap(),
                Decoded::Unknown {
                    frame_type: ty,
                    stream_id: 0,
                    payload_len: 2
                }
            );
        }
        // The stream id's high bit is checked before the type.
        let err = dec(&[0x05, 0, 0, 0, 0x80, 0, 0, 1]).unwrap_err();
        assert_eq!(err.code(), ErrorCode::PROTOCOL_ERROR);
        assert!(err.message().contains("0x80000001"), "{err}");
    }

    #[test]
    fn flags_and_reserved_ignored_on_receive_zero_on_send() {
        let raw = [TYPE_DATA, 0xff, 0x12, 0x34, 0, 0, 0, 1, b'a'];
        let f = frame(&raw);
        assert_eq!(
            f,
            Frame::Data {
                stream_id: 1,
                payload: Bytes::from_static(b"a")
            }
        );
        assert_eq!(f.encode().as_ref(), &[TYPE_DATA, 0, 0, 0, 0, 0, 0, 1, b'a']);
    }

    #[test]
    fn stream_zero_and_even_open() {
        for ty in [TYPE_OPEN, TYPE_WINDOW, TYPE_CLOSE, TYPE_RESET] {
            let err = dec(&[ty, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]).unwrap_err();
            assert_eq!(err.code(), ErrorCode::PROTOCOL_ERROR, "type {ty}");
            assert!(matches!(err, FrameError::Connection { .. }));
        }
        let err = dec(&[TYPE_OPEN, 0, 0, 0, 0, 0, 0, 4]).unwrap_err();
        assert_eq!(err.code(), ErrorCode::PROTOCOL_ERROR);
        assert!(err.message().contains('4'), "{err}");
    }

    #[test]
    fn round_trip_every_type() {
        fn random_bytes(rng: &mut fastrand::Rng, len: usize) -> Vec<u8> {
            std::iter::repeat_with(|| rng.u8(..)).take(len).collect()
        }
        let mut rng = fastrand::Rng::with_seed(0x5eed_f00d);
        for _ in 0..2000 {
            let stream_id = rng.u32(0..0x4000_0000) * 2 + 1;
            let len = match rng.u8(0..4) {
                0 => 0,
                1 => rng.usize(0..64),
                2 => rng.usize(0..=DATA_CHUNK),
                _ => rng.usize(0..=MAX_PAYLOAD),
            };
            let payload = random_bytes(&mut rng, len);
            let control_len = rng.usize(0..=MAX_CONTROL_PAYLOAD);
            let control = random_bytes(&mut rng, control_len);
            let msg_len = rng.usize(0..100);
            let message: String = std::iter::repeat_with(|| rng.char(..))
                .take(msg_len)
                .collect();
            let message = truncate_utf8(&message, MAX_RESET_MESSAGE).to_owned();
            let frames = [
                Frame::Open { stream_id },
                Frame::Data {
                    stream_id,
                    payload: Bytes::from(payload),
                },
                Frame::Data {
                    stream_id: 0,
                    payload: Bytes::from(control),
                },
                Frame::Window {
                    stream_id,
                    increment: rng.u32(1..=0x7fff_ffff),
                },
                Frame::Close {
                    stream_id,
                    trailing: 0,
                },
                Frame::Reset {
                    stream_id,
                    code: ErrorCode(rng.u32(..)),
                    message,
                },
            ];
            for f in frames {
                let wire = f.encode();
                assert_eq!(wire[1..4], [0, 0, 0]);
                assert_eq!(decode(wire.clone()), Ok(Decoded::Frame(f.clone())));
                assert_eq!(frame(&wire).encode(), wire);
            }
        }
    }

    #[test]
    fn mutated_input_never_panics() {
        let seeds = [
            encode_data(0, br#"{"t":"ping","id":1}"#),
            encode_data(3, b"hello"),
            encode_window(5, 1024),
            encode_close(7),
            encode_reset(9, ErrorCode::CANCEL, "bye"),
            header_only(TYPE_OPEN, 11),
        ];
        let mut rng = fastrand::Rng::with_seed(0xdec0_de01);
        for i in 0..20_000 {
            let mut b = if rng.bool() {
                seeds[rng.usize(..seeds.len())].to_vec()
            } else {
                let len = rng.usize(0..32);
                std::iter::repeat_with(|| rng.u8(..)).take(len).collect()
            };
            for _ in 0..rng.usize(0..4) {
                match rng.u8(0..3) {
                    0 if !b.is_empty() => {
                        let j = rng.usize(..b.len());
                        b[j] = rng.u8(..);
                    }
                    1 if !b.is_empty() => {
                        b.truncate(rng.usize(..b.len()));
                    }
                    _ => b.push(rng.u8(..)),
                }
            }
            if i % 1000 == 0 {
                b.resize(MAX_MESSAGE + rng.usize(0..2), rng.u8(..));
            }
            let _ = decode(Bytes::from(b));
        }
    }

    #[test]
    fn control_encoding() {
        #[derive(Serialize)]
        struct Msg<'a> {
            t: &'a str,
            id: u64,
        }
        let wire = encode_control(&Msg { t: "ping", id: 7 }).unwrap();
        assert_eq!(&wire[..HEADER_LEN], &[TYPE_DATA, 0, 0, 0, 0, 0, 0, 0]);
        assert_eq!(&wire[HEADER_LEN..], br#"{"t":"ping","id":7}"#);

        let big = "x".repeat(MAX_CONTROL_PAYLOAD);
        let err = encode_control(&Msg { t: &big, id: 1 }).unwrap_err();
        assert_eq!(err.bytes, MAX_CONTROL_PAYLOAD + r#"{"t":"","id":1}"#.len());
    }
}
