//! Test support for code that consumes this crate (`test-util` feature):
//! constructors for every event payload, and a [`Stream`] backed by memory
//! instead of a connection ([`Stream::new_in_memory`]), whose far end is the
//! [`PeerHandle`] in this module.
//!
//! The payload types are `#[non_exhaustive]`, so they cannot be built with a
//! struct literal outside this crate. `new_for_test()` returns a value with
//! sensible defaults, and a `with_<field>` setter exists for every field:
//!
//! ```
//! use ws_mixer::{DisconnectReason, Event, Phase};
//!
//! let event = Event::Disconnected(
//!     DisconnectReason::new_for_test()
//!         .with_phase(Phase::Handshake)
//!         .with_ws_code(Some(4011))
//!         .with_fatal(true),
//! );
//! # let _ = event;
//! ```

use std::io;
use std::sync::Arc;
use std::time::Duration;

use serde_json::{Map, Value};
use tokio::sync::mpsc;

use crate::conn::shared::{ConnShared, WriteJob, lock};
use crate::control::{ServerInfo, Welcome};
use crate::error::{ErrorCode, StreamError};
use crate::event::{
    AppMessage, ConnId, Connected, DisconnectReason, DrainNotice, DrainReason, Phase,
};
use crate::frame::{self, Decoded, Frame};
use crate::stats::Counters;
use crate::stream::{Stream, StreamId, StreamPhase, StreamShared};

macro_rules! setters {
    ($ty:ident { $($setter:ident => $field:ident: $t:ty),* $(,)? }) => {
        #[cfg_attr(docsrs, doc(cfg(feature = "test-util")))]
        impl $ty {
            $(
                #[doc = concat!("Sets `", stringify!($field), "`.")]
                #[must_use]
                pub fn $setter(mut self, value: $t) -> Self {
                    self.$field = value;
                    self
                }
            )*
        }
    };
}

#[cfg_attr(docsrs, doc(cfg(feature = "test-util")))]
impl ConnId {
    /// A connection id with the given value.
    pub fn new_for_test(id: u64) -> Self {
        Self(id)
    }
}

#[cfg_attr(docsrs, doc(cfg(feature = "test-util")))]
impl Welcome {
    /// `v` 1, session `"test-session"`, window 262144, max_streams 64,
    /// ping_interval 30000, ping_timeout 90000, nothing else.
    pub fn new_for_test() -> Self {
        Self {
            v: 1,
            session: "test-session".into(),
            window: 262_144,
            max_streams: 64,
            ping_interval: 30_000,
            ping_timeout: 90_000,
            server: None,
            capabilities: Vec::new(),
            meta: None,
        }
    }
}

setters!(Welcome {
    with_v => v: u32,
    with_session => session: String,
    with_window => window: u32,
    with_max_streams => max_streams: u32,
    with_ping_interval => ping_interval: u64,
    with_ping_timeout => ping_timeout: u64,
    with_server => server: Option<ServerInfo>,
    with_capabilities => capabilities: Vec<String>,
    with_meta => meta: Option<Map<String, Value>>,
});

#[cfg_attr(docsrs, doc(cfg(feature = "test-util")))]
impl ServerInfo {
    /// A server identity.
    pub fn new_for_test(name: &str, version: &str, node: &str) -> Self {
        Self {
            name: name.into(),
            version: version.into(),
            node: node.into(),
        }
    }
}

#[cfg_attr(docsrs, doc(cfg(feature = "test-util")))]
impl Connected {
    /// Connection 1, [`Welcome::new_for_test`], `reconnect` false.
    pub fn new_for_test() -> Self {
        let welcome = Welcome::new_for_test();
        Self {
            conn_id: ConnId(1),
            session: welcome.session.clone(),
            welcome,
            reconnect: false,
        }
    }
}

setters!(Connected {
    with_conn_id => conn_id: ConnId,
    with_session => session: String,
    with_welcome => welcome: Welcome,
    with_reconnect => reconnect: bool,
});

#[cfg_attr(docsrs, doc(cfg(feature = "test-util")))]
impl AppMessage {
    /// Connection 1, an empty body.
    pub fn new_for_test() -> Self {
        Self {
            conn_id: ConnId(1),
            body: Map::new(),
        }
    }
}

setters!(AppMessage {
    with_conn_id => conn_id: ConnId,
    with_body => body: Map<String, Value>,
});

#[cfg_attr(docsrs, doc(cfg(feature = "test-util")))]
impl DrainNotice {
    /// Connection 1, `rollout`, `last_stream_id` 0, no deadline, no
    /// `retry_after`, no message.
    pub fn new_for_test() -> Self {
        Self {
            conn_id: ConnId(1),
            reason: DrainReason::Rollout,
            raw_reason: DrainReason::Rollout.as_str().into(),
            last_stream_id: 0,
            deadline: None,
            retry_after: None,
            message: None,
        }
    }
}

setters!(DrainNotice {
    with_conn_id => conn_id: ConnId,
    with_reason => reason: DrainReason,
    with_raw_reason => raw_reason: String,
    with_last_stream_id => last_stream_id: u32,
    with_deadline => deadline: Option<Duration>,
    with_retry_after => retry_after: Option<Duration>,
    with_message => message: Option<String>,
});

#[cfg_attr(docsrs, doc(cfg(feature = "test-util")))]
impl DisconnectReason {
    /// Connection 1, phase `Connected`, non-fatal, message
    /// `"test disconnect"`, every optional field empty.
    pub fn new_for_test() -> Self {
        crate::conn::base_reason(Some(ConnId(1)), Phase::Connected, "test disconnect".into())
    }
}

setters!(DisconnectReason {
    with_conn_id => conn_id: Option<ConnId>,
    with_phase => phase: Phase,
    with_ws_code => ws_code: Option<u32>,
    with_error_code => error_code: Option<ErrorCode>,
    with_error_name => error_name: Option<&'static str>,
    with_http_status => http_status: Option<u16>,
    with_fatal => fatal: bool,
    with_message => message: String,
    with_close_reason => close_reason: Option<String>,
    with_cause => cause: Option<Arc<dyn std::error::Error + Send + Sync>>,
    with_retry_in => retry_in: Option<Duration>,
});

/// What a [`Stream`] made with [`Stream::new_in_memory`] sent to its peer.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
#[cfg_attr(docsrs, doc(cfg(feature = "test-util")))]
pub enum PeerEvent {
    /// Bytes the stream wrote, in order. Chunk boundaries are not meaningful.
    Data(Vec<u8>),
    /// The stream half-closed its write side (`CLOSE`).
    Close,
    /// The stream was reset (`RESET`), e.g. by [`Stream::reset`] or by being
    /// dropped before it was fully closed (`CANCEL`).
    Reset {
        /// The `RESET` code.
        code: ErrorCode,
        /// The `RESET` message.
        message: String,
    },
}

/// The far end of a [`Stream`] made with [`Stream::new_in_memory`]: plays the
/// server side of one stream.
///
/// Dropping it ends the stream's "connection": pending and later reads and
/// writes on the stream fail with
/// [`StreamError::ConnectionLost`].
#[cfg_attr(docsrs, doc(cfg(feature = "test-util")))]
pub struct PeerHandle {
    conn: Arc<ConnShared>,
    stream: Arc<StreamShared>,
    from_stream: mpsc::UnboundedReceiver<PeerEvent>,
}

impl std::fmt::Debug for PeerHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PeerHandle")
            .field("id", &self.stream.id)
            .finish_non_exhaustive()
    }
}

#[cfg_attr(docsrs, doc(cfg(feature = "test-util")))]
impl Stream {
    /// A stream with id `id` backed by memory, and the [`PeerHandle`] that
    /// plays the server side of it. No socket and no connection are
    /// involved; flow control is unlimited in both directions.
    ///
    /// [`conn_id`](Stream::conn_id) is `0` and [`session`](Stream::session)
    /// is `"in-memory"`. Must be called within a tokio runtime: a task
    /// forwards what the stream writes to the handle.
    pub fn new_in_memory(id: StreamId) -> (Stream, PeerHandle) {
        let conn = Arc::new(ConnShared::new(
            ConnId(0),
            "in-memory".into(),
            u32::MAX,
            u32::MAX,
            u32::MAX,
            Arc::new(Counters::default()),
        ));
        let stream = Arc::new(StreamShared::new(id, u32::MAX, u32::MAX));
        {
            let mut s = lock(&stream.state);
            s.send_window = i64::MAX / 2;
            s.recv_window = i64::MAX / 2;
        }
        {
            let mut cs = conn.lock();
            cs.highest_opened = id;
            cs.streams.insert(id, stream.clone());
        }
        let (tx, from_stream) = mpsc::unbounded_channel();
        tokio::spawn(pump(conn.clone(), id, tx));
        let handle = PeerHandle {
            conn: conn.clone(),
            stream: stream.clone(),
            from_stream,
        };
        (Stream::new(stream, conn), handle)
    }
}

/// Stands in for the connection's writer: hands every frame the stream
/// queues to the peer handle, without any back-pressure.
async fn pump(conn: Arc<ConnShared>, id: StreamId, tx: mpsc::UnboundedSender<PeerEvent>) {
    loop {
        match conn.next_write() {
            WriteJob::Frame(b) => {
                let event = match frame::decode(b) {
                    Ok(Decoded::Frame(Frame::Data { stream_id, payload })) if stream_id == id => {
                        PeerEvent::Data(payload.to_vec())
                    }
                    Ok(Decoded::Frame(Frame::Close { stream_id, .. })) if stream_id == id => {
                        PeerEvent::Close
                    }
                    Ok(Decoded::Frame(Frame::Reset {
                        stream_id,
                        code,
                        message,
                    })) if stream_id == id => PeerEvent::Reset { code, message },
                    // WINDOW: credit is unlimited here.
                    _ => continue,
                };
                if tx.send(event).is_err() {
                    return;
                }
            }
            WriteJob::Close(_) | WriteJob::Exit => return,
            WriteJob::Idle => {
                {
                    let cs = conn.lock();
                    if cs.streams.is_empty() && cs.control.is_empty() && cs.ready.is_empty() {
                        // Fully closed or reset, and everything delivered.
                        return;
                    }
                }
                tokio::select! {
                    _ = conn.writer_wake.notified() => {}
                    _ = conn.cancel.cancelled() => return,
                }
            }
        }
    }
}

#[cfg_attr(docsrs, doc(cfg(feature = "test-util")))]
impl PeerHandle {
    /// Delivers `data` to the stream's reader, like a `DATA` frame. Fails
    /// with `BrokenPipe` after [`close`](Self::close), and with the stream's
    /// error after a reset.
    pub fn write(&self, data: &[u8]) -> io::Result<()> {
        let _cs = self.conn.lock();
        let mut s = lock(&self.stream.state);
        if let Some((code, message)) = &s.reset {
            return Err(StreamError::Reset {
                code: *code,
                message: message.clone(),
            }
            .into());
        }
        if s.remote_closed || s.phase == StreamPhase::Closed {
            return Err(StreamError::WriteClosed.into());
        }
        if !data.is_empty() {
            s.inbuf.push_back(bytes::Bytes::copy_from_slice(data));
            s.wake_read();
        }
        Ok(())
    }

    /// Half-closes the peer's side, like a `CLOSE` frame: the stream reads
    /// the remaining buffered data, then end of stream. A no-op if already
    /// closed or reset.
    pub fn close(&self) {
        let mut cs = self.conn.lock();
        let mut s = lock(&self.stream.state);
        match s.phase {
            StreamPhase::Open => {
                s.phase = StreamPhase::HalfClosedRemote;
                s.remote_closed = true;
                s.wake_read();
            }
            StreamPhase::HalfClosedLocal => {
                s.phase = StreamPhase::Closed;
                s.remote_closed = true;
                s.wake_all();
                drop(s);
                self.retire(&mut cs);
            }
            StreamPhase::HalfClosedRemote | StreamPhase::Closed => {}
        }
    }

    /// Removes the stream from the table and lets the pump notice that
    /// nothing more can come.
    fn retire(&self, cs: &mut crate::conn::shared::ConnState) {
        self.conn.retire_locked(cs, self.stream.id);
        self.conn.writer_wake.notify_one();
    }

    /// Resets the stream from the peer's side, like a `RESET` frame:
    /// buffered data is discarded and pending reads and writes fail with
    /// [`StreamError::Reset`]. A no-op if the
    /// stream is already closed or reset.
    pub fn reset(&self, code: ErrorCode, message: &str) {
        let mut cs = self.conn.lock();
        let mut s = lock(&self.stream.state);
        if s.phase == StreamPhase::Closed {
            return;
        }
        s.phase = StreamPhase::Closed;
        s.reset = Some((code, message.to_owned()));
        s.inbuf.clear();
        s.out.clear();
        s.wake_all();
        drop(s);
        self.retire(&mut cs);
    }

    /// The next thing the stream sent: data, its `CLOSE`, or its `RESET`.
    /// `None` once the stream can send nothing more (it was fully closed or
    /// reset, and everything it sent has been returned).
    pub async fn recv(&mut self) -> Option<PeerEvent> {
        self.from_stream.recv().await
    }

    /// Reads everything the stream writes until its `CLOSE`. Fails with
    /// [`StreamError::Reset`] if the stream sends
    /// `RESET` instead, or `UnexpectedEof` if it can send nothing more.
    pub async fn read_to_end(&mut self) -> io::Result<Vec<u8>> {
        let mut out = Vec::new();
        loop {
            match self.recv().await {
                Some(PeerEvent::Data(d)) => out.extend_from_slice(&d),
                Some(PeerEvent::Close) => return Ok(out),
                Some(PeerEvent::Reset { code, message }) => {
                    return Err(StreamError::Reset { code, message }.into());
                }
                None => {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "the stream ended without CLOSE",
                    ));
                }
            }
        }
    }
}

impl Drop for PeerHandle {
    fn drop(&mut self) {
        self.conn
            .abnormal("the in-memory peer was dropped".into(), None);
        self.conn.cancel.cancel();
    }
}

#[cfg(test)]
mod tests {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::*;

    fn kind(e: &io::Error) -> &StreamError {
        e.get_ref().unwrap().downcast_ref().unwrap()
    }

    #[tokio::test]
    async fn read_and_write_both_ways_with_half_close() {
        let (mut s, mut peer) = Stream::new_in_memory(1);
        assert_eq!(
            (s.id(), s.conn_id(), s.session()),
            (1, ConnId(0), "in-memory")
        );
        peer.write(b"GET / HTTP/1.1\r\n\r\n").unwrap();
        peer.close();
        assert!(matches!(
            kind(&peer.write(b"late").unwrap_err()),
            StreamError::WriteClosed
        ));
        let mut req = Vec::new();
        s.read_to_end(&mut req).await.unwrap();
        assert_eq!(req, b"GET / HTTP/1.1\r\n\r\n");
        // Larger than any window or chunk, written before the peer reads.
        let body = vec![7u8; 1 << 20];
        s.write_all(&body).await.unwrap();
        s.close_write().await.unwrap();
        assert_eq!(peer.read_to_end().await.unwrap(), body);
        assert_eq!(peer.recv().await, None, "fully closed");
    }

    #[tokio::test]
    async fn stream_closes_first() {
        let (mut s, mut peer) = Stream::new_in_memory(3);
        s.write_all(b"early").await.unwrap();
        s.close_write().await.unwrap();
        assert_eq!(peer.recv().await, Some(PeerEvent::Data(b"early".to_vec())));
        assert_eq!(peer.recv().await, Some(PeerEvent::Close));
        peer.write(b"still readable").unwrap();
        peer.close();
        let mut got = Vec::new();
        s.read_to_end(&mut got).await.unwrap();
        assert_eq!(got, b"still readable");
        assert_eq!(peer.recv().await, None);
    }

    #[tokio::test]
    async fn peer_reset_fails_reads_and_writes() {
        let (mut s, mut peer) = Stream::new_in_memory(5);
        peer.write(b"dropped by the reset").unwrap();
        peer.reset(ErrorCode::CANCEL, "client went away");
        let e = s.read_u8().await.unwrap_err();
        assert_eq!(e.kind(), io::ErrorKind::ConnectionReset);
        assert!(
            matches!(kind(&e), StreamError::Reset { code, message } if *code == ErrorCode::CANCEL && message == "client went away")
        );
        assert!(s.write_all(b"x").await.is_err());
        assert!(peer.write(b"x").is_err());
        assert_eq!(peer.recv().await, None, "never RESET in answer to RESET");
    }

    #[tokio::test]
    async fn stream_reset_and_drop_are_observed() {
        let (s, mut peer) = Stream::new_in_memory(7);
        s.reset(ErrorCode(0x1000_0001), "handler failed");
        assert_eq!(
            peer.recv().await,
            Some(PeerEvent::Reset {
                code: ErrorCode(0x1000_0001),
                message: "handler failed".into()
            })
        );
        assert_eq!(peer.recv().await, None);

        let (s, mut peer) = Stream::new_in_memory(9);
        drop(s);
        assert!(matches!(
            peer.read_to_end().await.unwrap_err().get_ref().unwrap().downcast_ref(),
            Some(StreamError::Reset { code, .. }) if *code == ErrorCode::CANCEL
        ));

        let (s, mut peer) = Stream::new_in_memory(11);
        s.refuse();
        assert!(matches!(
            peer.recv().await,
            Some(PeerEvent::Reset { code, .. }) if code == ErrorCode::REFUSED_STREAM
        ));
    }

    #[tokio::test]
    async fn dropping_the_peer_loses_the_connection() {
        let (mut s, peer) = Stream::new_in_memory(1);
        let reader = tokio::spawn(async move {
            let mut buf = [0u8; 8];
            s.read(&mut buf).await
        });
        tokio::task::yield_now().await;
        drop(peer);
        let e = reader.await.unwrap().unwrap_err();
        assert_eq!(e.kind(), io::ErrorKind::ConnectionAborted);
    }

    #[tokio::test]
    async fn split_halves() {
        let (s, mut peer) = Stream::new_in_memory(1);
        let (mut r, mut w) = s.into_split();
        peer.write(b"ping").unwrap();
        let mut buf = [0u8; 4];
        r.read_exact(&mut buf).await.unwrap();
        w.write_all(b"pong").await.unwrap();
        assert_eq!(peer.recv().await, Some(PeerEvent::Data(b"pong".to_vec())));
    }

    #[test]
    fn constructors_and_setters() {
        let w = Welcome::new_for_test()
            .with_session("s-9".into())
            .with_server(Some(ServerInfo::new_for_test("edge", "1.0", "n1")));
        assert_eq!((w.v, w.window, w.max_streams), (1, 262_144, 64));
        let c = Connected::new_for_test()
            .with_conn_id(ConnId::new_for_test(9))
            .with_welcome(w.clone())
            .with_reconnect(true);
        assert_eq!((c.conn_id.get(), c.reconnect, &c.welcome), (9, true, &w));
        let mut body = Map::new();
        body.insert("k".into(), Value::from(1));
        let a = AppMessage::new_for_test().with_body(body.clone());
        assert_eq!(a.body, body);
        let d = DrainNotice::new_for_test()
            .with_reason(DrainReason::Replaced)
            .with_deadline(Some(Duration::from_secs(5)));
        assert_eq!(
            (d.reason, d.deadline),
            (DrainReason::Replaced, Some(Duration::from_secs(5)))
        );
        let r = DisconnectReason::new_for_test()
            .with_ws_code(Some(4001))
            .with_fatal(true);
        assert!(r.is_sdk_bug() && r.fatal);
        assert_eq!(r.phase, Phase::Connected);
    }
}
