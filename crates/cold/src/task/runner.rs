//! Cold storage task runner.
//!
//! The [`ColdStorageTask`] processes requests from two channels and
//! dispatches them to the storage backend. Internally it runs two
//! concurrent subtasks:
//!
//! - **Dispatcher** — consumes [`PermittedReadRequest`]s and spawns a
//!   handler for each. The permit attached to the request bounds
//!   concurrency and travels with the message for the lifetime of the
//!   handler.
//! - **Writer** — consumes [`ColdWriteRequest`]s sequentially. Before
//!   each write, it drains all in-flight readers by acquiring every
//!   permit on the shared semaphore, giving the write exclusive backend
//!   access. The drain is wrapped in a cancel-select so shutdown can
//!   preempt a stuck reader.
//!
//! The shared [`Semaphore`] is the single backpressure mechanism. Its
//! permits are acquired on the handle side (see
//! [`ColdStorageReadHandle`](crate::ColdStorageReadHandle)), travel into
//! the channel in [`PermittedReadRequest`], and are released when the
//! spawned handler's future is dropped (on completion, panic, or
//! deadline expiry).
//!
//! Transaction, receipt, and header lookups are served from an LRU cache,
//! avoiding repeated backend reads for frequently queried items.
//!
//! # Log Streaming
//!
//! Log-streaming producers (`StreamLogs`) run independently and are
//! tracked separately for graceful shutdown. They are NOT drained
//! before writes. Backends must provide their own read isolation
//! (snapshot semantics) for streaming reads.

use super::cache::ColdCache;
use crate::{
    ColdReadRequest, ColdReceipt, ColdResult, ColdStorage, ColdStorageError, ColdStorageHandle,
    ColdStorageRead, ColdWriteRequest, Confirmed, HeaderSpecifier, LogStream, PermittedReadRequest,
    ReceiptSpecifier, Responder, TransactionSpecifier,
};
use signet_storage_types::{RecoveredTx, SealedHeader};
use std::{future::Future, sync::Arc, time::Duration};
use tokio::sync::{Mutex, Semaphore, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tracing::{debug, instrument, warn};

/// Default maximum deadline for streaming operations.
const DEFAULT_MAX_STREAM_DEADLINE: Duration = Duration::from_secs(60);

/// Default per-request deadline for non-streaming reads.
///
/// Long enough to absorb normal backend jitter (pool contention,
/// brief network hiccups) and well below the 12s write cadence at
/// which a stuck reader would otherwise repressurize the write mpsc
/// and regenerate the original backpressure-induced crash.
const DEFAULT_READ_DEADLINE: Duration = Duration::from_secs(5);

/// Channel size for cold storage read requests.
///
/// Sized to match [`MAX_CONCURRENT_READERS`]: because callers acquire
/// a semaphore permit before sending, at most that many items can be
/// in the channel simultaneously. `try_send` from a caller holding a
/// permit is therefore guaranteed to have capacity.
const READ_CHANNEL_SIZE: usize = MAX_CONCURRENT_READERS;

/// Channel size for cold storage write requests.
const WRITE_CHANNEL_SIZE: usize = 256;

/// Maximum concurrent read request handlers.
const MAX_CONCURRENT_READERS: usize = 64;

/// Maximum concurrent streaming operations.
const MAX_CONCURRENT_STREAMS: usize = 8;

/// Channel buffer size for streaming operations.
const STREAM_CHANNEL_BUFFER: usize = 256;

/// Shared state for the cold storage task, holding the read backend and cache.
///
/// This is wrapped in an `Arc` so that spawned read handlers can access
/// the backend and cache without moving ownership.
struct ColdStorageTaskInner<B> {
    read_backend: B,
    cache: Mutex<ColdCache>,
    max_stream_deadline: Duration,
    stream_semaphore: Arc<Semaphore>,
    /// Tracks long-lived stream producer tasks separately from short reads.
    /// Not drained before writes — backends provide their own read isolation.
    /// Drained on graceful shutdown.
    stream_tracker: TaskTracker,
}

impl<B: ColdStorageRead> ColdStorageTaskInner<B> {
    /// Fetch a header from the backend and cache the result.
    async fn fetch_and_cache_header(
        &self,
        spec: HeaderSpecifier,
    ) -> ColdResult<Option<SealedHeader>> {
        let r = self.read_backend.get_header(spec).await;
        if let Ok(Some(ref h)) = r {
            self.cache.lock().await.put_header(h.number, h.clone());
        }
        r
    }

    /// Fetch a transaction from the backend and cache the result.
    async fn fetch_and_cache_tx(
        &self,
        spec: TransactionSpecifier,
    ) -> ColdResult<Option<Confirmed<RecoveredTx>>> {
        let r = self.read_backend.get_transaction(spec).await;
        if let Ok(Some(ref c)) = r {
            let meta = c.meta();
            self.cache
                .lock()
                .await
                .put_tx((meta.block_number(), meta.transaction_index()), c.clone());
        }
        r
    }

    /// Fetch a receipt from the backend and cache the result.
    async fn fetch_and_cache_receipt(
        &self,
        spec: ReceiptSpecifier,
    ) -> ColdResult<Option<ColdReceipt>> {
        let r = self.read_backend.get_receipt(spec).await;
        if let Ok(Some(ref c)) = r {
            self.cache.lock().await.put_receipt((c.block_number, c.transaction_index), c.clone());
        }
        r
    }

    /// Handle a read request with a wall-clock deadline.
    ///
    /// On normal completion, the result is sent via the variant's
    /// oneshot sender. On deadline expiry, the working future is
    /// dropped and the caller receives [`ColdStorageError::Timeout`].
    /// [`ColdReadRequest::StreamLogs`] bypasses the deadline — it has
    /// its own stream deadline.
    async fn handle_read_req(self: Arc<Self>, req: ColdReadRequest, deadline: Duration) {
        let op = req.variant_name();
        match req {
            ColdReadRequest::GetHeader { spec, resp } => {
                let this = Arc::clone(&self);
                run_with_deadline(deadline, op, resp, async move {
                    if let HeaderSpecifier::Number(n) = &spec
                        && let Some(hit) = this.cache.lock().await.get_header(n)
                    {
                        return Ok(Some(hit));
                    }
                    this.fetch_and_cache_header(spec).await
                })
                .await;
            }
            ColdReadRequest::GetHeaders { specs, resp } => {
                let this = Arc::clone(&self);
                run_with_deadline(deadline, op, resp, async move {
                    this.read_backend.get_headers(specs).await
                })
                .await;
            }
            ColdReadRequest::GetTransaction { spec, resp } => {
                let this = Arc::clone(&self);
                run_with_deadline(deadline, op, resp, async move {
                    if let TransactionSpecifier::BlockAndIndex { block, index } = &spec
                        && let Some(hit) = this.cache.lock().await.get_tx(&(*block, *index))
                    {
                        return Ok(Some(hit));
                    }
                    this.fetch_and_cache_tx(spec).await
                })
                .await;
            }
            ColdReadRequest::GetTransactionsInBlock { block, resp } => {
                let this = Arc::clone(&self);
                run_with_deadline(deadline, op, resp, async move {
                    this.read_backend.get_transactions_in_block(block).await
                })
                .await;
            }
            ColdReadRequest::GetTransactionCount { block, resp } => {
                let this = Arc::clone(&self);
                run_with_deadline(deadline, op, resp, async move {
                    this.read_backend.get_transaction_count(block).await
                })
                .await;
            }
            ColdReadRequest::GetReceipt { spec, resp } => {
                let this = Arc::clone(&self);
                run_with_deadline(deadline, op, resp, async move {
                    if let ReceiptSpecifier::BlockAndIndex { block, index } = &spec
                        && let Some(hit) = this.cache.lock().await.get_receipt(&(*block, *index))
                    {
                        return Ok(Some(hit));
                    }
                    this.fetch_and_cache_receipt(spec).await
                })
                .await;
            }
            ColdReadRequest::GetReceiptsInBlock { block, resp } => {
                let this = Arc::clone(&self);
                run_with_deadline(deadline, op, resp, async move {
                    this.read_backend.get_receipts_in_block(block).await
                })
                .await;
            }
            ColdReadRequest::GetSignetEvents { spec, resp } => {
                let this = Arc::clone(&self);
                run_with_deadline(deadline, op, resp, async move {
                    this.read_backend.get_signet_events(spec).await
                })
                .await;
            }
            ColdReadRequest::GetZenithHeader { spec, resp } => {
                let this = Arc::clone(&self);
                run_with_deadline(deadline, op, resp, async move {
                    this.read_backend.get_zenith_header(spec).await
                })
                .await;
            }
            ColdReadRequest::GetZenithHeaders { spec, resp } => {
                let this = Arc::clone(&self);
                run_with_deadline(deadline, op, resp, async move {
                    this.read_backend.get_zenith_headers(spec).await
                })
                .await;
            }
            ColdReadRequest::GetLogs { filter, max_logs, resp } => {
                let this = Arc::clone(&self);
                run_with_deadline(deadline, op, resp, async move {
                    this.read_backend.get_logs(&filter, max_logs).await
                })
                .await;
            }
            ColdReadRequest::StreamLogs { filter, max_logs, deadline: stream_deadline, resp } => {
                // Streams use their own deadline and produce a stream
                // handle almost immediately. Not wrapped in the read
                // deadline.
                let _ =
                    resp.send(self.handle_stream_logs(*filter, max_logs, stream_deadline).await);
            }
            ColdReadRequest::GetLatestBlock { resp } => {
                let this = Arc::clone(&self);
                run_with_deadline(deadline, op, resp, async move {
                    this.read_backend.get_latest_block().await
                })
                .await;
            }
        }
    }

    /// Stream logs matching a filter.
    ///
    /// Acquires a concurrency permit, resolves the block range, then
    /// spawns a producer task that delegates to
    /// [`ColdStorageRead::produce_log_stream`].
    async fn handle_stream_logs(
        self: &Arc<Self>,
        filter: crate::Filter,
        max_logs: usize,
        deadline: Duration,
    ) -> ColdResult<LogStream> {
        let permit = self
            .stream_semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| ColdStorageError::Cancelled)?;

        let from = filter.get_from_block().unwrap_or(0);
        let to = match filter.get_to_block() {
            Some(to) => to,
            None => {
                let Some(latest) = self.read_backend.get_latest_block().await? else {
                    let (_tx, rx) = mpsc::channel(1);
                    return Ok(ReceiverStream::new(rx));
                };
                latest
            }
        };

        let effective = deadline.min(self.max_stream_deadline);
        let deadline_instant = tokio::time::Instant::now() + effective;
        let (sender, rx) = mpsc::channel(STREAM_CHANNEL_BUFFER);
        let stream_backend = self.read_backend.clone();

        self.stream_tracker.spawn(async move {
            let _permit = permit;
            let params =
                crate::StreamParams { from, to, max_logs, sender, deadline: deadline_instant };
            stream_backend.produce_log_stream(&filter, params).await;
        });

        Ok(ReceiverStream::new(rx))
    }
}

/// Await `fut` with a wall-clock deadline and send the result via
/// `resp`.
///
/// On deadline expiry, emits a WARN trace tagged with the operation
/// name and sends [`ColdStorageError::Timeout`] to the caller. The
/// working future is dropped, which also drops anything it holds —
/// including the caller's concurrency permit.
async fn run_with_deadline<T, F>(deadline: Duration, op: &'static str, resp: Responder<T>, fut: F)
where
    F: Future<Output = ColdResult<T>>,
{
    let result = match tokio::time::timeout(deadline, fut).await {
        Ok(r) => r,
        Err(_) => {
            warn!(operation = op, ?deadline, "cold read deadline exceeded");
            Err(ColdStorageError::Timeout)
        }
    };
    let _ = resp.send(result);
}

/// The cold storage task that processes requests.
///
/// This task receives requests over separate read and write channels
/// and dispatches them to the storage backend. Internally it runs a
/// read dispatcher and a writer as concurrent subtasks, joined for
/// graceful shutdown. See the module-level documentation for details.
///
/// # Caching
///
/// Transaction, receipt, and header lookups are served from an LRU cache
/// when possible. Cache entries are invalidated on
/// [`truncate_above`](crate::ColdStorageWrite::truncate_above) to handle reorgs.
pub struct ColdStorageTask<B: ColdStorage> {
    inner: Arc<ColdStorageTaskInner<B>>,
    write_backend: B,
    read_receiver: mpsc::Receiver<PermittedReadRequest>,
    write_receiver: mpsc::Receiver<ColdWriteRequest>,
    cancel_token: CancellationToken,
    /// Tracks spawned read handlers so the graceful-shutdown path can
    /// wait for them to finish. Not used for backpressure.
    task_tracker: TaskTracker,
    /// The shared backpressure pool.
    ///
    /// Callers acquire a permit on the handle side before sending.
    /// Writers acquire every permit at once as the drain-before-write
    /// barrier.
    read_semaphore: Arc<Semaphore>,
    /// Per-request deadline applied to non-streaming reads.
    read_deadline: Duration,
}

impl<B: ColdStorage> std::fmt::Debug for ColdStorageTask<B> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ColdStorageTask")
            .field("read_deadline", &self.read_deadline)
            .finish_non_exhaustive()
    }
}

impl<B: ColdStorage> ColdStorageTask<B> {
    /// Create a new cold storage task and return its handle.
    pub fn new(backend: B, cancel_token: CancellationToken) -> (Self, ColdStorageHandle) {
        let (read_sender, read_receiver) = mpsc::channel(READ_CHANNEL_SIZE);
        let (write_sender, write_receiver) = mpsc::channel(WRITE_CHANNEL_SIZE);
        let read_semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_READERS));
        let read_backend = backend.clone();
        let task = Self {
            inner: Arc::new(ColdStorageTaskInner {
                read_backend,
                cache: Mutex::new(ColdCache::new()),
                max_stream_deadline: DEFAULT_MAX_STREAM_DEADLINE,
                stream_semaphore: Arc::new(Semaphore::new(MAX_CONCURRENT_STREAMS)),
                stream_tracker: TaskTracker::new(),
            }),
            write_backend: backend,
            read_receiver,
            write_receiver,
            cancel_token,
            task_tracker: TaskTracker::new(),
            read_semaphore: Arc::clone(&read_semaphore),
            read_deadline: DEFAULT_READ_DEADLINE,
        };
        let handle = ColdStorageHandle::new(read_sender, read_semaphore, write_sender);
        (task, handle)
    }

    /// Override the per-request deadline for non-streaming reads.
    ///
    /// Defaults to 5 seconds. The deadline is a guardrail against
    /// backend-layer pathology (stuck connection, runaway query);
    /// legitimate work should complete well within it.
    pub const fn with_read_deadline(mut self, deadline: Duration) -> Self {
        self.read_deadline = deadline;
        self
    }

    /// Spawn the task and return the handle.
    ///
    /// The task will run until the cancellation token is triggered or
    /// the channels are closed.
    pub fn spawn(backend: B, cancel_token: CancellationToken) -> ColdStorageHandle {
        let (task, handle) = Self::new(backend, cancel_token);
        tokio::spawn(task.run());
        handle
    }

    /// Run the task, processing requests until shutdown.
    #[instrument(skip(self), name = "cold_storage_task")]
    pub async fn run(self) {
        debug!("Cold storage task started");

        let Self {
            inner,
            write_backend,
            read_receiver,
            write_receiver,
            cancel_token,
            task_tracker,
            read_semaphore,
            read_deadline,
        } = self;

        let dispatcher = run_dispatcher(
            read_receiver,
            Arc::clone(&inner),
            task_tracker.clone(),
            cancel_token.clone(),
            read_deadline,
        );
        let writer = run_writer(
            write_receiver,
            write_backend,
            Arc::clone(&inner),
            read_semaphore,
            cancel_token,
        );

        tokio::join!(dispatcher, writer);

        // Graceful shutdown: drain reads first (short-lived, bounded
        // by `read_deadline`), then streams (bounded by per-stream
        // deadline). Reads must drain first because StreamLogs
        // handlers can spawn stream producers — draining streams
        // before reads could miss a newly-spawned producer.
        debug!("Waiting for in-progress read handlers to complete");
        task_tracker.close();
        task_tracker.wait().await;
        debug!("Waiting for in-progress stream producers to complete");
        inner.stream_tracker.close();
        inner.stream_tracker.wait().await;
        debug!("Cold storage task shut down gracefully");
    }
}

/// Dispatcher subtask: pulls [`PermittedReadRequest`]s and spawns a
/// handler for each, wrapping the work in the read deadline.
///
/// Runs concurrently with the writer so that permits attached to
/// queued messages always have a consumer, preventing the drain from
/// stranding permits in the channel.
async fn run_dispatcher<B: ColdStorage>(
    mut read_receiver: mpsc::Receiver<PermittedReadRequest>,
    inner: Arc<ColdStorageTaskInner<B>>,
    task_tracker: TaskTracker,
    cancel_token: CancellationToken,
    deadline: Duration,
) {
    loop {
        tokio::select! {
            biased;
            _ = cancel_token.cancelled() => {
                debug!("Read dispatcher received cancellation signal");
                break;
            }
            maybe = read_receiver.recv() => {
                let Some(PermittedReadRequest { permit, req }) = maybe else {
                    debug!("Cold storage read channel closed");
                    break;
                };
                let inner = Arc::clone(&inner);
                task_tracker.spawn(async move {
                    // The permit is released when this future is dropped,
                    // on normal completion, panic, or deadline expiry.
                    let _permit = permit;
                    inner.handle_read_req(req, deadline).await;
                });
            }
        }
    }
}

/// Writer subtask: consumes writes sequentially, draining all
/// in-flight readers before each write via
/// [`Semaphore::acquire_many_owned`].
///
/// The drain is wrapped in a cancel-select so that a stuck reader
/// holding its permit cannot wedge shutdown. Pulling a write from the
/// channel and then exiting on cancel drops the write's oneshot
/// sender; the caller sees `RecvError` mapped to `Cancelled`.
async fn run_writer<B: ColdStorage>(
    mut write_receiver: mpsc::Receiver<ColdWriteRequest>,
    mut write_backend: B,
    inner: Arc<ColdStorageTaskInner<B>>,
    read_semaphore: Arc<Semaphore>,
    cancel_token: CancellationToken,
) {
    loop {
        tokio::select! {
            biased;
            _ = cancel_token.cancelled() => {
                debug!("Writer received cancellation signal");
                break;
            }
            maybe = write_receiver.recv() => {
                let Some(req) = maybe else {
                    debug!("Cold storage write channel closed");
                    break;
                };

                // Drain in-flight readers by acquiring every permit. Only
                // completes once no reader holds a permit, which means no
                // reader is touching the backend.
                //
                // `acquire_many_owned` only errors if the semaphore is
                // closed; the semaphore lives for the lifetime of this
                // task, so the error is unreachable.
                let _drain = tokio::select! {
                    _ = cancel_token.cancelled() => {
                        debug!("Cancellation while waiting for read drain");
                        break;
                    }
                    d = read_semaphore
                        .clone()
                        .acquire_many_owned(MAX_CONCURRENT_READERS as u32) =>
                    {
                        d.expect("read semaphore outlives the writer task")
                    }
                };

                handle_write(&mut write_backend, &inner, req).await;
                // `_drain` drops here, restoring all permits.
            }
        }
    }
}

/// Execute a single write request against the exclusively owned
/// write backend. Invalidates cache entries after destructive
/// operations.
///
/// The caller must have drained all in-flight readers before calling
/// this. No synchronization is performed here.
async fn handle_write<B: ColdStorage>(
    backend: &mut B,
    inner: &Arc<ColdStorageTaskInner<B>>,
    req: ColdWriteRequest,
) {
    match req {
        ColdWriteRequest::AppendBlock(boxed) => {
            let result = backend.append_block(boxed.data).await;
            let _ = boxed.resp.send(result);
        }
        ColdWriteRequest::AppendBlocks { data, resp } => {
            let result = backend.append_blocks(data).await;
            let _ = resp.send(result);
        }
        ColdWriteRequest::TruncateAbove { block, resp } => {
            let result = backend.truncate_above(block).await;
            if result.is_ok() {
                inner.cache.lock().await.invalidate_above(block);
            }
            let _ = resp.send(result);
        }
        ColdWriteRequest::DrainAbove { block, resp } => {
            let result = backend.drain_above(block).await;
            if result.is_ok() {
                inner.cache.lock().await.invalidate_above(block);
            }
            let _ = resp.send(result);
        }
    }
}
