//! Server-opened byte streams.
//!
//! Each [`Stream`] is flow controlled per WIRE.md §2.6: writes wait for
//! credit, and credit is returned to the peer as the application reads.

use std::collections::VecDeque;
use std::fmt;
use std::io;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use bytes::{Buf, Bytes};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use crate::conn::shared::{ConnShared, lock};
use crate::error::{ErrorCode, StreamError};
use crate::event::{ConnId, DisconnectReason};
use crate::frame::{self, DATA_CHUNK};

/// A stream id. Server-opened streams have odd ids; `0` is the control channel.
pub type StreamId = u32;

/// DATA chunks a stream may queue ahead of the writer (Go streamOutQueueCap):
/// small, so writes block and round-robin rotates per 16 KiB chunk.
const OUT_QUEUE_CAP: usize = 2;

/// A node of the WIRE.md §2.5 state machine. Streams start `Open`; `idle`
/// never exists on the client because only the server opens.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(crate) enum StreamPhase {
    Open,
    HalfClosedLocal,
    HalfClosedRemote,
    Closed,
}

pub(crate) struct StreamState {
    pub(crate) phase: StreamPhase,
    /// Credit we may still spend sending DATA.
    pub(crate) send_window: i64,
    /// Credit the peer may still spend sending DATA to us.
    pub(crate) recv_window: i64,
    /// Bytes delivered to the application, not yet credited back.
    pub(crate) unacked: i64,
    /// Received DATA, kept across CLOSE, dropped on RESET.
    pub(crate) inbuf: VecDeque<Bytes>,
    /// Encoded DATA frames waiting for the writer, at most [`OUT_QUEUE_CAP`].
    pub(crate) out: VecDeque<Bytes>,
    pub(crate) remote_closed: bool,
    pub(crate) local_closed: bool,
    /// Set by a RESET in either direction.
    pub(crate) reset: Option<(ErrorCode, String)>,
    /// Set when the connection ended while the stream was live.
    pub(crate) conn_lost: Option<Arc<DisconnectReason>>,
    read_waker: Option<Waker>,
    write_waker: Option<Waker>,
    flush_waker: Option<Waker>,
}

impl StreamState {
    pub(crate) fn wake_read(&mut self) {
        if let Some(w) = self.read_waker.take() {
            w.wake();
        }
    }

    /// Wakes writers after credit arrived or the out queue shrank.
    pub(crate) fn wake_write(&mut self) {
        if let Some(w) = self.write_waker.take() {
            w.wake();
        }
        if self.out.is_empty() {
            if let Some(w) = self.flush_waker.take() {
                w.wake();
            }
        }
    }

    pub(crate) fn wake_all(&mut self) {
        self.wake_read();
        if let Some(w) = self.write_waker.take() {
            w.wake();
        }
        if let Some(w) = self.flush_waker.take() {
            w.wake();
        }
    }

    fn error(&self) -> Option<io::Error> {
        if let Some((code, message)) = &self.reset {
            return Some(
                StreamError::Reset {
                    code: *code,
                    message: message.clone(),
                }
                .into(),
            );
        }
        self.conn_lost
            .as_ref()
            .map(|r| StreamError::ConnectionLost(r.clone()).into())
    }
}

/// Per-stream state shared between the application handle and the connection
/// tasks: flow-control windows, buffers and wakers.
pub(crate) struct StreamShared {
    pub(crate) id: StreamId,
    pub(crate) state: Mutex<StreamState>,
}

impl StreamShared {
    pub(crate) fn new(id: StreamId, recv_window: u32, send_window: u32) -> Self {
        Self {
            id,
            state: Mutex::new(StreamState {
                phase: StreamPhase::Open,
                send_window: i64::from(send_window),
                recv_window: i64::from(recv_window),
                unacked: 0,
                inbuf: VecDeque::new(),
                out: VecDeque::new(),
                remote_closed: false,
                local_closed: false,
                reset: None,
                conn_lost: None,
                read_waker: None,
                write_waker: None,
                flush_waker: None,
            }),
        }
    }
}

/// The application's reference to a stream, shared by [`Stream`] and both
/// halves of [`Stream::into_split`]. Dropping the last one resets a stream
/// that is not fully closed with `CANCEL`.
struct Handle {
    stream: Arc<StreamShared>,
    conn: Arc<ConnShared>,
}

impl Handle {
    fn reset(&self, code: ErrorCode, message: &str) {
        let mut cs = self.conn.lock();
        self.conn
            .reset_stream_locked(&mut cs, &self.stream, code, message);
    }

    /// Whether consumed data has reached the WINDOW threshold: half our
    /// window, never after the peer's CLOSE (WIRE.md §2.5 sending table:
    /// WINDOW in half-closed(remote)).
    fn credit_due(&self, s: &StreamState) -> bool {
        !s.remote_closed
            && s.phase != StreamPhase::Closed
            && s.unacked >= i64::from(self.conn.our_window / 2)
    }

    fn poll_read(&self, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        // The copy runs under the stream lock only; the connection lock is
        // taken afterwards, and only to queue a WINDOW.
        let mut s = lock(&self.stream.state);
        if !s.inbuf.is_empty() {
            let mut n = 0;
            while buf.remaining() > 0 {
                let Some(front) = s.inbuf.front_mut() else {
                    break;
                };
                let take = front.len().min(buf.remaining());
                buf.put_slice(&front[..take]);
                front.advance(take);
                if front.is_empty() {
                    s.inbuf.pop_front();
                }
                n += take;
            }
            s.unacked += n as i64;
            if self.credit_due(&s) {
                drop(s);
                let mut cs = self.conn.lock();
                let mut s = lock(&self.stream.state);
                if cs.terminal.is_none() && self.credit_due(&s) {
                    let inc = s.unacked;
                    s.recv_window += inc;
                    s.unacked = 0;
                    cs.control
                        .push_back(frame::encode_window(self.stream.id, inc as u32));
                    drop(s);
                    drop(cs);
                    self.conn.writer_wake.notify_one();
                }
            }
            return Poll::Ready(Ok(()));
        }
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        if let Some((code, message)) = &s.reset {
            return Poll::Ready(Err(StreamError::Reset {
                code: *code,
                message: message.clone(),
            }
            .into()));
        }
        if s.remote_closed {
            return Poll::Ready(Ok(()));
        }
        if let Some(r) = &s.conn_lost {
            return Poll::Ready(Err(StreamError::ConnectionLost(r.clone()).into()));
        }
        s.read_waker = Some(cx.waker().clone());
        Poll::Pending
    }

    /// How many bytes of a `len`-byte write fit now: errors, `0` for an
    /// empty write, or `Pending` with the waker stored when there is no
    /// credit or the out queue is full.
    fn writable(s: &mut StreamState, cx: &mut Context<'_>, len: usize) -> Poll<io::Result<usize>> {
        if let Some(e) = s.error() {
            return Poll::Ready(Err(e));
        }
        if !matches!(s.phase, StreamPhase::Open | StreamPhase::HalfClosedRemote) {
            return Poll::Ready(Err(StreamError::WriteClosed.into()));
        }
        if len == 0 {
            return Poll::Ready(Ok(0));
        }
        if s.send_window <= 0 || s.out.len() >= OUT_QUEUE_CAP {
            s.write_waker = Some(cx.waker().clone());
            return Poll::Pending;
        }
        Poll::Ready(Ok(len.min(s.send_window as usize).min(DATA_CHUNK)))
    }

    fn poll_write(&self, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        // The chunk is sized under the stream lock and copied with no lock
        // held; both locks are taken only to re-check and enqueue.
        let mut n =
            std::task::ready!(Self::writable(&mut lock(&self.stream.state), cx, buf.len()))?;
        loop {
            if n == 0 {
                return Poll::Ready(Ok(0));
            }
            let chunk = frame::encode_data(self.stream.id, &buf[..n]);
            let mut cs = self.conn.lock();
            let mut s = lock(&self.stream.state);
            let fits = std::task::ready!(Self::writable(&mut s, cx, buf.len()))?;
            if fits < n {
                // Credit shrank meanwhile: rebuild a smaller chunk.
                n = fits;
                continue;
            }
            s.out.push_back(chunk);
            s.send_window -= n as i64;
            drop(s);
            if cs.in_ready.insert(self.stream.id) {
                cs.ready.push_back(self.stream.id);
            }
            drop(cs);
            self.conn.writer_wake.notify_one();
            return Poll::Ready(Ok(n));
        }
    }

    fn poll_flush(&self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let mut s = lock(&self.stream.state);
        if let Some((code, message)) = &s.reset {
            return Poll::Ready(Err(StreamError::Reset {
                code: *code,
                message: message.clone(),
            }
            .into()));
        }
        if s.out.is_empty() && (s.conn_lost.is_none() || s.phase == StreamPhase::Closed) {
            return Poll::Ready(Ok(()));
        }
        if let Some(r) = &s.conn_lost {
            return Poll::Ready(Err(StreamError::ConnectionLost(r.clone()).into()));
        }
        s.flush_waker = Some(cx.waker().clone());
        Poll::Pending
    }

    /// Flushes, then queues CLOSE. CLOSE travels on the control queue, which
    /// jumps ahead of DATA, so it must only be queued once this stream's own
    /// DATA has been handed to the writer (WIRE.md §2.6 rule 3).
    fn poll_shutdown(&self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if lock(&self.stream.state).local_closed {
            return Poll::Ready(Ok(()));
        }
        std::task::ready!(self.poll_flush(cx))?;
        let mut cs = self.conn.lock();
        let mut s = lock(&self.stream.state);
        if s.local_closed {
            return Poll::Ready(Ok(()));
        }
        if let Some(e) = s.error() {
            return Poll::Ready(Err(e));
        }
        let closed = match s.phase {
            StreamPhase::Open => {
                s.phase = StreamPhase::HalfClosedLocal;
                false
            }
            StreamPhase::HalfClosedRemote => {
                s.phase = StreamPhase::Closed;
                true
            }
            StreamPhase::HalfClosedLocal | StreamPhase::Closed => {
                return Poll::Ready(Ok(()));
            }
        };
        s.local_closed = true;
        s.wake_all();
        drop(s);
        cs.control.push_back(frame::encode_close(self.stream.id));
        if closed {
            self.conn.retire_locked(&mut cs, self.stream.id);
        }
        drop(cs);
        self.conn.writer_wake.notify_one();
        Poll::Ready(Ok(()))
    }
}

impl Drop for Handle {
    fn drop(&mut self) {
        let open = {
            let s = lock(&self.stream.state);
            s.phase != StreamPhase::Closed && s.conn_lost.is_none()
        };
        if open {
            self.reset(ErrorCode::CANCEL, "stream dropped");
        }
    }
}

/// A bidirectional byte stream opened by the server.
///
/// Reads return buffered data first, then `Ok(0)` after the peer's `CLOSE`,
/// or an error after a `RESET` or when the connection dies. Errors wrap a
/// [`StreamError`](crate::StreamError) that can be recovered with
/// [`io::Error::get_ref`] and `downcast_ref`: a dead connection is never
/// reported as a clean EOF.
///
/// Dropping a stream that is not fully closed resets it with `CANCEL`.
pub struct Stream {
    h: Arc<Handle>,
}

impl Stream {
    pub(crate) fn new(stream: Arc<StreamShared>, conn: Arc<ConnShared>) -> Self {
        Self {
            h: Arc::new(Handle { stream, conn }),
        }
    }

    /// The stream id.
    pub fn id(&self) -> StreamId {
        self.h.stream.id
    }

    /// The connection carrying this stream.
    pub fn conn_id(&self) -> ConnId {
        self.h.conn.conn_id
    }

    /// The server-assigned session id of the connection carrying this stream.
    pub fn session(&self) -> &str {
        &self.h.conn.session
    }

    /// Half-closes the write side: flushes queued data, then sends `CLOSE`.
    /// Reading continues to work. Equivalent to
    /// [`AsyncWriteExt::shutdown`](tokio::io::AsyncWriteExt::shutdown).
    pub async fn close_write(&mut self) -> io::Result<()> {
        std::future::poll_fn(|cx| self.h.poll_shutdown(cx)).await
    }

    /// Resets the stream with `code`, discarding buffered data in both
    /// directions. Never blocks; wakes pending readers and writers. A no-op if
    /// the stream is already closed or reset.
    pub fn reset(&self, code: ErrorCode, message: &str) {
        self.h.reset(code, message);
    }

    /// Declines the stream with `REFUSED_STREAM`, which tells the server that
    /// nothing was processed and the request may be retried elsewhere.
    pub fn refuse(self) {
        self.h.reset(
            ErrorCode::REFUSED_STREAM,
            "stream refused by the application",
        );
    }

    /// Splits the stream into independently owned read and write halves.
    pub fn into_split(self) -> (StreamReadHalf, StreamWriteHalf) {
        (
            StreamReadHalf { h: self.h.clone() },
            StreamWriteHalf { h: self.h },
        )
    }
}

impl fmt::Debug for Stream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Stream")
            .field("id", &self.id())
            .field("conn_id", &self.conn_id())
            .finish_non_exhaustive()
    }
}

impl AsyncRead for Stream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        self.h.poll_read(cx, buf)
    }
}

impl AsyncWrite for Stream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.h.poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.h.poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.h.poll_shutdown(cx)
    }
}

/// The read half of a [`Stream`], from [`Stream::into_split`].
pub struct StreamReadHalf {
    h: Arc<Handle>,
}

impl StreamReadHalf {
    /// The stream id.
    pub fn id(&self) -> StreamId {
        self.h.stream.id
    }
}

impl fmt::Debug for StreamReadHalf {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StreamReadHalf")
            .field("id", &self.id())
            .finish_non_exhaustive()
    }
}

impl AsyncRead for StreamReadHalf {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        self.h.poll_read(cx, buf)
    }
}

/// The write half of a [`Stream`], from [`Stream::into_split`].
pub struct StreamWriteHalf {
    h: Arc<Handle>,
}

impl StreamWriteHalf {
    /// The stream id.
    pub fn id(&self) -> StreamId {
        self.h.stream.id
    }
}

impl fmt::Debug for StreamWriteHalf {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StreamWriteHalf")
            .field("id", &self.id())
            .finish_non_exhaustive()
    }
}

impl AsyncWrite for StreamWriteHalf {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.h.poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.h.poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.h.poll_shutdown(cx)
    }
}
