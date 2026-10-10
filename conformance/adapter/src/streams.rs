//! Per-stream bookkeeping: the continuous reader, the write/close_write FIFO
//! and the out-of-band reset (conformance/README.md, "write/close_write are
//! serialized per stream id; reset runs out-of-band").

use std::collections::{HashMap, HashSet};
use std::future::poll_fn;
use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use serde_json::{Value, json};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use ws_mixer::{ConnId, ErrorCode, Stream, StreamError, StreamId};

use crate::emit::{ack, cmd_error, emit, t_ms};
use crate::lock;

/// Bound of each stream's write/close_write queue.
const QUEUE_CAP: usize = 64;
/// How long a command naming a stream the adapter has not seen yet waits for
/// its `Event::Stream`: the runner paces on the wire, so a `write` can
/// arrive before the event loop has registered the stream.
const STREAM_WAIT: Duration = Duration::from_secs(5);
/// Poll period for a RESET that arrives after read EOF (see `watch_late_reset`).
const LATE_RESET_POLL: Duration = Duration::from_millis(10);
/// How long `reset` waits for the aborted queue's worker before acking, so
/// the write the RESET unblocked reports its error first (as Go does).
const RESET_DRAIN: Duration = Duration::from_secs(1);

/// One app-visible stream. The `Stream` sits behind a std mutex so the
/// reader, the FIFO worker and `reset` can share it: every access is a
/// single non-blocking poll, and reads and writes keep separate wakers
/// inside the SDK.
pub struct Cell {
    id: StreamId,
    conn: ConnId,
    stream: Mutex<Stream>,
    /// (read closed, write closed)
    halves: Mutex<(bool, bool)>,
}

impl Cell {
    fn poll_read(&self, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<io::Result<usize>> {
        let mut rb = ReadBuf::new(buf);
        let mut s = lock(&self.stream);
        Pin::new(&mut *s)
            .poll_read(cx, &mut rb)
            .map_ok(|()| rb.filled().len())
    }

    async fn write_all(&self, mut data: &[u8]) -> io::Result<()> {
        while !data.is_empty() {
            let n = poll_fn(|cx| Pin::new(&mut *lock(&self.stream)).poll_write(cx, data)).await?;
            if n == 0 {
                return Err(io::ErrorKind::WriteZero.into());
            }
            data = &data[n..];
        }
        poll_fn(|cx| Pin::new(&mut *lock(&self.stream)).poll_flush(cx)).await
    }

    async fn close_write(&self) -> io::Result<()> {
        poll_fn(|cx| Pin::new(&mut *lock(&self.stream)).poll_shutdown(cx)).await
    }

    /// Records that one half closed; returns whether both are now closed.
    fn note_half(&self, read: bool) -> bool {
        let mut h = lock(&self.halves);
        if read {
            h.0 = true;
        } else {
            h.1 = true;
        }
        h.0 && h.1
    }
}

/// Dropping it ends the worker once the jobs already queued are done.
struct Queue {
    tx: mpsc::Sender<Job>,
    aborted: Arc<AtomicBool>,
    worker: JoinHandle<()>,
}

struct Job {
    seq: Value,
    op: Op,
}

pub enum Op {
    Write(Vec<u8>),
    CloseWrite,
}

pub struct Streams {
    live: Mutex<HashMap<StreamId, Arc<Cell>>>,
    /// Ids retired since the last connection ended: a command naming one
    /// fails at once instead of waiting `STREAM_WAIT`. Cleared when a
    /// connection ends, since the next one may reuse ids.
    retired: Mutex<HashSet<StreamId>>,
    /// Bumped on every registration, to wake `wait`.
    registered: watch::Sender<u64>,
    queues: Mutex<HashMap<StreamId, Queue>>,
}

impl Streams {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            live: Mutex::new(HashMap::new()),
            retired: Mutex::new(HashSet::new()),
            registered: watch::Sender::new(0),
            queues: Mutex::new(HashMap::new()),
        })
    }

    /// Takes ownership of a server-opened stream: registers it, emits
    /// `stream_opened` and starts reading it continuously so credit flows
    /// back to the server.
    pub fn open(self: &Arc<Self>, stream: Stream) {
        let cell = Arc::new(Cell {
            id: stream.id(),
            conn: stream.conn_id(),
            stream: Mutex::new(stream),
            halves: Mutex::new((false, false)),
        });
        // A reconnect may reuse an id; the newest stream wins.
        {
            let mut live = lock(&self.live);
            live.insert(cell.id, cell.clone());
            lock(&self.retired).remove(&cell.id);
        }
        self.registered.send_modify(|g| *g += 1);
        emit(json!({"event": "stream_opened", "id": cell.id}));
        tokio::spawn(self.clone().read_loop(cell));
    }

    /// Forgets `cell` and its queue if it is still the registered stream for
    /// its id.
    fn retire(&self, cell: &Arc<Cell>) {
        let mut live = lock(&self.live);
        if live.get(&cell.id).is_some_and(|c| Arc::ptr_eq(c, cell)) {
            live.remove(&cell.id);
            lock(&self.retired).insert(cell.id);
            lock(&self.queues).remove(&cell.id);
        }
    }

    fn is_live(&self, cell: &Arc<Cell>) -> bool {
        lock(&self.live)
            .get(&cell.id)
            .is_some_and(|c| Arc::ptr_eq(c, cell))
    }

    /// Forgets every stream of a connection that ended. Their readers see
    /// `ConnectionLost` (or are already parked on EOF) and stop on their own.
    /// `None` (a dial that never got a connection) has no streams.
    pub fn conn_ended(&self, conn: Option<ConnId>) {
        let Some(conn) = conn else { return };
        let mut live = lock(&self.live);
        let mut queues = lock(&self.queues);
        live.retain(|id, c| {
            let keep = c.conn != conn;
            if !keep {
                queues.remove(id);
            }
            keep
        });
        lock(&self.retired).clear();
    }

    /// The stream registered as `id`, waiting up to `STREAM_WAIT` for one
    /// not seen yet. `None` at once for an id already retired.
    async fn wait(&self, id: StreamId) -> Option<Arc<Cell>> {
        let mut rx = self.registered.subscribe();
        let deadline = tokio::time::Instant::now() + STREAM_WAIT;
        loop {
            {
                let live = lock(&self.live);
                if let Some(c) = live.get(&id) {
                    return Some(c.clone());
                }
                if lock(&self.retired).contains(&id) {
                    return None;
                }
            }
            match tokio::time::timeout_at(deadline, rx.changed()).await {
                Ok(Ok(())) => {}
                _ => return None,
            }
        }
    }

    async fn read_loop(self: Arc<Self>, cell: Arc<Cell>) {
        let mut buf = vec![0u8; 64 * 1024];
        loop {
            match poll_fn(|cx| cell.poll_read(cx, &mut buf)).await {
                Ok(0) => {
                    emit(
                        json!({"event": "stream_closed", "id": cell.id, "direction": "read", "t_ms": t_ms()}),
                    );
                    if cell.note_half(true) {
                        emit(
                            json!({"event": "stream_closed", "id": cell.id, "direction": "both", "t_ms": t_ms()}),
                        );
                        self.retire(&cell);
                    } else {
                        self.watch_late_reset(&cell).await;
                    }
                    return;
                }
                Ok(n) => emit(json!({
                    "event": "data",
                    "id": cell.id,
                    "data_b64": data_encoding::BASE64.encode(&buf[..n]),
                    "t_ms": t_ms(),
                })),
                Err(e) => {
                    self.report_reset(&cell, &e);
                    return;
                }
            }
        }
    }

    /// After read EOF the SDK can still reset the stream: the peer's RESET,
    /// or one the SDK sends itself (DATA after CLOSE -> STREAM_CLOSED). A
    /// read past EOF returns `Ok(0)` immediately rather than waiting for
    /// that, so poll with a no-op waker until the reset shows up or the
    /// stream is retired (both halves closed, or its connection ended).
    async fn watch_late_reset(&self, cell: &Arc<Cell>) {
        let mut byte = [0u8; 1];
        while self.is_live(cell) {
            match cell.poll_read(&mut Context::from_waker(Waker::noop()), &mut byte) {
                Poll::Ready(Err(e)) => {
                    self.report_reset(cell, &e);
                    return;
                }
                Poll::Ready(Ok(_)) | Poll::Pending => {}
            }
            tokio::time::sleep(LATE_RESET_POLL).await;
        }
    }

    /// Emits `stream_reset` for a reset in either direction. A lost
    /// connection is reported once, by `disconnected`.
    fn report_reset(&self, cell: &Arc<Cell>, e: &io::Error) {
        let inner = e.get_ref().and_then(|i| i.downcast_ref::<StreamError>());
        if let Some(StreamError::Reset { code, message }) = inner {
            emit(json!({
                "event": "stream_reset",
                "id": cell.id,
                "code": code.0,
                "name": code.name(),
                "message": message,
                "t_ms": t_ms(),
            }));
        } else if !matches!(inner, Some(StreamError::ConnectionLost(_))) {
            eprintln!("adapter: stream {} read failed: {e}", cell.id);
        }
        self.retire(cell);
    }

    /// Queues a write or close_write behind earlier ones for the same id.
    /// Never blocks: a full queue is a `queue_full` error ack.
    pub fn enqueue(self: &Arc<Self>, id: StreamId, seq: Value, op: Op) {
        let mut queues = lock(&self.queues);
        let q = queues.entry(id).or_insert_with(|| {
            let (tx, rx) = mpsc::channel(QUEUE_CAP);
            let aborted = Arc::new(AtomicBool::new(false));
            let worker = tokio::spawn(self.clone().worker(id, rx, aborted.clone()));
            Queue {
                tx,
                aborted,
                worker,
            }
        });
        match q.tx.try_send(Job { seq, op }) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(job)) => cmd_error(
                &job.seq,
                format!(
                    "queue_full: stream {id}'s write/close_write queue (cap {QUEUE_CAP}) is full"
                ),
            ),
            Err(mpsc::error::TrySendError::Closed(job)) => {
                cmd_error(&job.seq, format!("stream {id}'s queue is gone"));
            }
        }
    }

    async fn worker(
        self: Arc<Self>,
        id: StreamId,
        mut rx: mpsc::Receiver<Job>,
        aborted: Arc<AtomicBool>,
    ) {
        while let Some(Job { seq, op }) = rx.recv().await {
            if aborted.load(Ordering::Acquire) {
                cmd_error(&seq, "reset: queued command aborted by RESET");
                continue;
            }
            let name = match op {
                Op::Write(_) => "write",
                Op::CloseWrite => "close_write",
            };
            let Some(cell) = self.wait(id).await else {
                cmd_error(&seq, format!("{name}: no such stream {id}"));
                continue;
            };
            match op {
                Op::Write(data) => match cell.write_all(&data).await {
                    Ok(()) => ack(&seq),
                    Err(e) => cmd_error(&seq, format!("write: {e}")),
                },
                Op::CloseWrite => match cell.close_write().await {
                    Ok(()) => {
                        ack(&seq);
                        emit(
                            json!({"event": "stream_closed", "id": id, "direction": "write", "t_ms": t_ms()}),
                        );
                        if cell.note_half(false) {
                            emit(
                                json!({"event": "stream_closed", "id": id, "direction": "both", "t_ms": t_ms()}),
                            );
                            self.retire(&cell);
                        }
                    }
                    Err(e) => cmd_error(&seq, format!("close_write: {e}")),
                },
            }
        }
    }

    /// First half of RESET, run synchronously on the stdin loop: aborts
    /// whatever is still queued for `id` and returns that queue's worker.
    pub fn abort_queue(&self, id: StreamId) -> Option<JoinHandle<()>> {
        let q = lock(&self.queues).remove(&id)?;
        q.aborted.store(true, Ordering::Release);
        Some(q.worker)
    }

    /// RESET, out of band: resets the stream, which also fails a write
    /// blocked on credit, and lets `worker` report that before the ack.
    pub async fn reset(
        &self,
        seq: Value,
        id: StreamId,
        code: u32,
        message: String,
        worker: Option<JoinHandle<()>>,
    ) {
        let Some(cell) = self.wait(id).await else {
            cmd_error(&seq, format!("reset: no such stream {id}"));
            return;
        };
        lock(&cell.stream).reset(ErrorCode(code), &message);
        if let Some(w) = worker {
            let _ = tokio::time::timeout(RESET_DRAIN, w).await;
        }
        ack(&seq);
    }
}
