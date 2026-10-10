//! Cumulative client counters.

use std::sync::atomic::{AtomicU64, Ordering};

/// A snapshot of a [`Client`](crate::Client)'s counters, cumulative across
/// all of its connections.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct Stats {
    /// Dials started.
    pub dials: u64,
    /// `welcome` messages received.
    pub connects: u64,
    /// Streams opened by the server and accepted.
    pub streams_accepted: u64,
    /// `OPEN` frames refused (`STREAM_LIMIT`, `REFUSED_STREAM`).
    pub refused_opens: u64,
    /// Frames with an unknown type, ignored per spec.
    pub unknown_frames: u64,
    /// `CLOSE` frames that carried a payload, which was ignored per spec.
    pub close_frames_with_payload: u64,
    /// Frames for a previously open, now dead stream, discarded per spec.
    pub stale_frames: u64,
    /// Duplicate `pong` messages, ignored per spec.
    pub duplicate_pongs: u64,
    /// `drain` messages with an unknown reason.
    pub unknown_drain_reasons: u64,
    /// Connections failed because the peer violated the protocol.
    pub protocol_violations: u64,
    /// Bytes received in WebSocket binary messages.
    pub bytes_in: u64,
    /// Bytes sent in WebSocket binary messages.
    pub bytes_out: u64,
}

/// The live counters behind [`Stats`], shared by every connection of one
/// client so the snapshot is cumulative across reconnects.
#[derive(Debug, Default)]
pub(crate) struct Counters {
    pub(crate) dials: AtomicU64,
    pub(crate) connects: AtomicU64,
    pub(crate) streams_accepted: AtomicU64,
    pub(crate) refused_opens: AtomicU64,
    pub(crate) unknown_frames: AtomicU64,
    pub(crate) close_frames_with_payload: AtomicU64,
    pub(crate) stale_frames: AtomicU64,
    pub(crate) duplicate_pongs: AtomicU64,
    pub(crate) unknown_drain_reasons: AtomicU64,
    pub(crate) protocol_violations: AtomicU64,
    pub(crate) bytes_in: AtomicU64,
    pub(crate) bytes_out: AtomicU64,
}

impl Counters {
    pub(crate) fn inc(counter: &AtomicU64) {
        counter.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn add(counter: &AtomicU64, n: usize) {
        counter.fetch_add(n as u64, Ordering::Relaxed);
    }

    pub(crate) fn snapshot(&self) -> Stats {
        let get = |c: &AtomicU64| c.load(Ordering::Relaxed);
        Stats {
            dials: get(&self.dials),
            connects: get(&self.connects),
            streams_accepted: get(&self.streams_accepted),
            refused_opens: get(&self.refused_opens),
            unknown_frames: get(&self.unknown_frames),
            close_frames_with_payload: get(&self.close_frames_with_payload),
            stale_frames: get(&self.stale_frames),
            duplicate_pongs: get(&self.duplicate_pongs),
            unknown_drain_reasons: get(&self.unknown_drain_reasons),
            protocol_violations: get(&self.protocol_violations),
            bytes_in: get(&self.bytes_in),
            bytes_out: get(&self.bytes_out),
        }
    }
}
