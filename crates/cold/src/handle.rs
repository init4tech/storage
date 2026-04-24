//! Unified cold-storage handle.
//!
//! A single [`ColdStorage<B>`] type wraps an `Arc<Inner<B>>` and provides all
//! read, write, and streaming operations. Concurrency is enforced via
//! semaphores rather than dedicated reader/writer tasks.
//!
//! # Concurrency
//!
//! - Reads are gated by a `read_sem` (up to 64 in flight).
//! - Writes are gated by a `write_sem` (serialized: 1 in flight) and
//!   additionally acquire all `read_sem` permits as a drain barrier, so no
//!   read is in flight while a write commits.
//! - Log streams are gated by a `stream_sem` (up to 8 in flight) and do not
//!   participate in the drain barrier.

use crate::{
    BlockData, ColdReceipt, ColdResult, ColdStorageBackend, ColdStorageError, Confirmed, Filter,
    HeaderSpecifier, LogStream, ReceiptSpecifier, RpcLog, SignetEventsSpecifier, StreamParams,
    TransactionSpecifier, ZenithHeaderSpecifier, cache::ColdCache, metrics,
};
use alloy::primitives::{B256, BlockNumber};
use parking_lot::Mutex;
use signet_storage_types::{DbSignetEvent, DbZenithHeader, RecoveredTx, SealedHeader};
use std::{
    sync::{Arc, Weak},
    time::Duration,
};
use tokio::{
    sync::{Semaphore, mpsc},
    time::Instant,
};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::{
    sync::{CancellationToken, DropGuard},
    task::TaskTracker,
};
use tracing::Instrument;

/// Default maximum deadline for streaming operations.
const DEFAULT_MAX_STREAM_DEADLINE: Duration = Duration::from_secs(60);

/// Maximum concurrent read operations.
const MAX_CONCURRENT_READERS: usize = 64;

/// Maximum concurrent write operations.
const MAX_CONCURRENT_WRITES: usize = 1;

/// Maximum concurrent streaming operations.
const MAX_CONCURRENT_STREAMS: usize = 8;

/// Channel buffer size for streaming operations.
const STREAM_CHANNEL_BUFFER: usize = 256;

/// Shared inner state for [`ColdStorage`].
pub(crate) struct Inner<B> {
    pub(crate) backend: B,
    pub(crate) cache: Mutex<ColdCache>,
    pub(crate) max_stream_deadline: Duration,
    pub(crate) read_sem: Arc<Semaphore>,
    pub(crate) write_sem: Arc<Semaphore>,
    pub(crate) stream_sem: Arc<Semaphore>,
    pub(crate) tracker: TaskTracker,
    /// Fires `shutdown` (a child of the user's cancel token) when `Inner`
    /// drops, waking the coordinator task so it exits without waiting on
    /// user-side cancel.
    _shutdown_guard: DropGuard,
}

/// Unified handle for interacting with a cold storage backend.
///
/// `ColdStorage<B>` is cheap to [`Clone`] — it is just an `Arc` around the
/// shared inner state. All operations dispatch through semaphore-gated
/// [`TaskTracker`]-spawned tasks.
pub struct ColdStorage<B: ColdStorageBackend> {
    inner: Arc<Inner<B>>,
}

impl<B: ColdStorageBackend> Clone for ColdStorage<B> {
    fn clone(&self) -> Self {
        Self { inner: Arc::clone(&self.inner) }
    }
}

impl<B: ColdStorageBackend> std::fmt::Debug for ColdStorage<B> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ColdStorage").finish_non_exhaustive()
    }
}

impl<B: ColdStorageBackend> ColdStorage<B> {
    /// Create a new cold storage handle wrapping `backend`.
    ///
    /// A hidden coordinator task watches `cancel` and, on fire, closes the
    /// read/write/stream semaphores and the task tracker. After cancel,
    /// all handle methods fail fast with [`ColdStorageError::TaskTerminated`]
    /// on permit acquisition; in-flight spawned tasks drain to completion
    /// bounded by backend timeouts.
    pub fn new(backend: B, cancel: CancellationToken) -> Self {
        // `shutdown` fires on user-side cancel (via the parent) OR when
        // `Inner` drops (via the DropGuard). The coordinator holds only a
        // `Weak<Inner>` so it never pins the backend.
        let shutdown = cancel.child_token();
        let shutdown_guard = shutdown.clone().drop_guard();
        let inner = Arc::new(Inner {
            backend,
            cache: Mutex::new(ColdCache::new()),
            max_stream_deadline: DEFAULT_MAX_STREAM_DEADLINE,
            read_sem: Arc::new(Semaphore::new(MAX_CONCURRENT_READERS)),
            write_sem: Arc::new(Semaphore::new(MAX_CONCURRENT_WRITES)),
            stream_sem: Arc::new(Semaphore::new(MAX_CONCURRENT_STREAMS)),
            tracker: TaskTracker::new(),
            _shutdown_guard: shutdown_guard,
        });
        let weak: Weak<Inner<B>> = Arc::downgrade(&inner);
        // Shutdown coordinator: must NOT be tracked by `tracker`, otherwise
        // `tracker.wait()` would deadlock waiting on this task. Holds only a
        // `Weak` ref so `Inner` (and the backend) can drop when all handles
        // and in-flight tasks are gone.
        tokio::spawn(async move {
            shutdown.cancelled().await;
            let Some(inner) = weak.upgrade() else { return };
            inner.read_sem.close();
            inner.write_sem.close();
            inner.stream_sem.close();
            inner.tracker.close();
        });
        Self { inner }
    }

    /// Close the task tracker and wait for all in-flight tasks to finish.
    ///
    /// Idempotent with the shutdown coordinator: safe to call whether or not
    /// the cancel token has fired.
    pub async fn wait_shutdown(&self) {
        self.inner.tracker.close();
        self.inner.tracker.wait().await;
    }

    /// Spawn a read task under the `read_sem` permit.
    async fn spawn_read<T, F, Fut>(&self, op: &'static str, f: F) -> ColdResult<T>
    where
        T: Send + 'static,
        F: FnOnce(Arc<Inner<B>>) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = ColdResult<T>> + Send,
    {
        let wait = Instant::now();
        let permit = self
            .inner
            .read_sem
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| ColdStorageError::TaskTerminated)?;
        metrics::record_permit_wait("read", wait.elapsed());
        let inner = Arc::clone(&self.inner);
        self.inner
            .tracker
            .spawn(
                async move {
                    let _p = permit;
                    let _guard = metrics::InFlightGuard::new("read");
                    let start = Instant::now();
                    let result = f(inner).await;
                    metrics::record_op_duration(op, start.elapsed());
                    if let Err(ref e) = result {
                        metrics::record_op_error(op, e.kind());
                    }
                    result
                }
                .in_current_span(),
            )
            .await
            .map_err(|_| ColdStorageError::TaskTerminated)?
    }

    /// Spawn a write task under the `write_sem` permit, holding a full drain
    /// on `read_sem` for the duration of the write.
    ///
    /// Acquisition order is: `write_sem` first, then all `MAX_CONCURRENT_READERS`
    /// read permits. This ensures concurrent writers queue on `write_sem` so
    /// only one writer at a time waits on the drain, preventing starvation of
    /// the read pool.
    ///
    /// Drop order inside the spawned task releases the drain before the write
    /// permit, so readers regain access immediately once the write completes.
    async fn spawn_write<T, F, Fut>(&self, op: &'static str, f: F) -> ColdResult<T>
    where
        T: Send + 'static,
        F: FnOnce(Arc<Inner<B>>) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = ColdResult<T>> + Send,
    {
        let wait = Instant::now();
        let write_permit = self
            .inner
            .write_sem
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| ColdStorageError::TaskTerminated)?;
        metrics::record_permit_wait("write", wait.elapsed());

        let drain_wait = Instant::now();
        let drain = self
            .inner
            .read_sem
            .clone()
            .acquire_many_owned(MAX_CONCURRENT_READERS as u32)
            .await
            .map_err(|_| ColdStorageError::TaskTerminated)?;
        metrics::record_permit_wait("drain", drain_wait.elapsed());

        let inner = Arc::clone(&self.inner);
        self.inner
            .tracker
            .spawn(
                async move {
                    let _w = write_permit;
                    let _d = drain;
                    let _guard = metrics::InFlightGuard::new("write");
                    let start = Instant::now();
                    let result = f(inner).await;
                    metrics::record_op_duration(op, start.elapsed());
                    if let Err(ref e) = result {
                        metrics::record_op_error(op, e.kind());
                    }
                    result
                }
                .in_current_span(),
            )
            .await
            .map_err(|_| ColdStorageError::TaskTerminated)?
    }

    // ==========================================================================
    // Headers
    // ==========================================================================

    /// Get a header by specifier.
    #[tracing::instrument(skip(self, spec), fields(op = "get_header"))]
    pub async fn get_header(&self, spec: HeaderSpecifier) -> ColdResult<Option<SealedHeader>> {
        let op_start = Instant::now();
        if let HeaderSpecifier::Number(n) = &spec
            && let Some(hit) = self.inner.cache.lock().get_header(n)
        {
            metrics::record_op_duration("get_header", op_start.elapsed());
            return Ok(Some(hit));
        }
        self.spawn_read("get_header", move |inner| async move {
            let result = inner.backend.get_header(spec).await;
            if let Ok(Some(ref h)) = result {
                inner.cache.lock().put_header(h.number, h.clone());
            }
            result
        })
        .await
    }

    /// Get a header by block number.
    pub async fn get_header_by_number(
        &self,
        block: BlockNumber,
    ) -> ColdResult<Option<SealedHeader>> {
        self.get_header(HeaderSpecifier::Number(block)).await
    }

    /// Get a header by block hash.
    pub async fn get_header_by_hash(&self, hash: B256) -> ColdResult<Option<SealedHeader>> {
        self.get_header(HeaderSpecifier::Hash(hash)).await
    }

    /// Get multiple headers by specifiers.
    #[tracing::instrument(skip(self, specs), fields(op = "get_headers"))]
    pub async fn get_headers(
        &self,
        specs: Vec<HeaderSpecifier>,
    ) -> ColdResult<Vec<Option<SealedHeader>>> {
        self.spawn_read("get_headers", move |inner| async move {
            inner.backend.get_headers(specs).await
        })
        .await
    }

    // ==========================================================================
    // Transactions
    // ==========================================================================

    /// Get a transaction by specifier, with block confirmation metadata.
    #[tracing::instrument(skip(self, spec), fields(op = "get_transaction"))]
    pub async fn get_transaction(
        &self,
        spec: TransactionSpecifier,
    ) -> ColdResult<Option<Confirmed<RecoveredTx>>> {
        let op_start = Instant::now();
        if let TransactionSpecifier::BlockAndIndex { block, index } = &spec
            && let Some(hit) = self.inner.cache.lock().get_tx(&(*block, *index))
        {
            metrics::record_op_duration("get_transaction", op_start.elapsed());
            return Ok(Some(hit));
        }
        self.spawn_read("get_transaction", move |inner| async move {
            let result = inner.backend.get_transaction(spec).await;
            if let Ok(Some(ref c)) = result {
                let meta = c.meta();
                inner
                    .cache
                    .lock()
                    .put_tx((meta.block_number(), meta.transaction_index()), c.clone());
            }
            result
        })
        .await
    }

    /// Get a transaction by hash.
    pub async fn get_tx_by_hash(&self, hash: B256) -> ColdResult<Option<Confirmed<RecoveredTx>>> {
        self.get_transaction(TransactionSpecifier::Hash(hash)).await
    }

    /// Get a transaction by block number and index.
    pub async fn get_tx_by_block_and_index(
        &self,
        block: BlockNumber,
        index: u64,
    ) -> ColdResult<Option<Confirmed<RecoveredTx>>> {
        self.get_transaction(TransactionSpecifier::BlockAndIndex { block, index }).await
    }

    /// Get a transaction by block hash and index.
    pub async fn get_tx_by_block_hash_and_index(
        &self,
        block_hash: B256,
        index: u64,
    ) -> ColdResult<Option<Confirmed<RecoveredTx>>> {
        self.get_transaction(TransactionSpecifier::BlockHashAndIndex { block_hash, index }).await
    }

    /// Get all transactions in a block.
    #[tracing::instrument(skip(self), fields(op = "get_transactions_in_block"))]
    pub async fn get_transactions_in_block(
        &self,
        block: BlockNumber,
    ) -> ColdResult<Vec<RecoveredTx>> {
        self.spawn_read("get_transactions_in_block", move |inner| async move {
            inner.backend.get_transactions_in_block(block).await
        })
        .await
    }

    /// Get the transaction count for a block.
    #[tracing::instrument(skip(self), fields(op = "get_transaction_count"))]
    pub async fn get_transaction_count(&self, block: BlockNumber) -> ColdResult<u64> {
        self.spawn_read("get_transaction_count", move |inner| async move {
            inner.backend.get_transaction_count(block).await
        })
        .await
    }

    // ==========================================================================
    // Receipts
    // ==========================================================================

    /// Get a receipt by specifier.
    #[tracing::instrument(skip(self, spec), fields(op = "get_receipt"))]
    pub async fn get_receipt(&self, spec: ReceiptSpecifier) -> ColdResult<Option<ColdReceipt>> {
        let op_start = Instant::now();
        if let ReceiptSpecifier::BlockAndIndex { block, index } = &spec
            && let Some(hit) = self.inner.cache.lock().get_receipt(&(*block, *index))
        {
            metrics::record_op_duration("get_receipt", op_start.elapsed());
            return Ok(Some(hit));
        }
        self.spawn_read("get_receipt", move |inner| async move {
            let result = inner.backend.get_receipt(spec).await;
            if let Ok(Some(ref c)) = result {
                inner.cache.lock().put_receipt((c.block_number, c.transaction_index), c.clone());
            }
            result
        })
        .await
    }

    /// Get a receipt by transaction hash.
    pub async fn get_receipt_by_tx_hash(&self, hash: B256) -> ColdResult<Option<ColdReceipt>> {
        self.get_receipt(ReceiptSpecifier::TxHash(hash)).await
    }

    /// Get a receipt by block number and index.
    pub async fn get_receipt_by_block_and_index(
        &self,
        block: BlockNumber,
        index: u64,
    ) -> ColdResult<Option<ColdReceipt>> {
        self.get_receipt(ReceiptSpecifier::BlockAndIndex { block, index }).await
    }

    /// Get all receipts in a block.
    #[tracing::instrument(skip(self), fields(op = "get_receipts_in_block"))]
    pub async fn get_receipts_in_block(&self, block: BlockNumber) -> ColdResult<Vec<ColdReceipt>> {
        self.spawn_read("get_receipts_in_block", move |inner| async move {
            inner.backend.get_receipts_in_block(block).await
        })
        .await
    }

    // ==========================================================================
    // SignetEvents
    // ==========================================================================

    /// Get signet events by specifier.
    #[tracing::instrument(skip(self, spec), fields(op = "get_signet_events"))]
    pub async fn get_signet_events(
        &self,
        spec: SignetEventsSpecifier,
    ) -> ColdResult<Vec<DbSignetEvent>> {
        self.spawn_read("get_signet_events", move |inner| async move {
            inner.backend.get_signet_events(spec).await
        })
        .await
    }

    /// Get signet events in a block.
    pub async fn get_signet_events_in_block(
        &self,
        block: BlockNumber,
    ) -> ColdResult<Vec<DbSignetEvent>> {
        self.get_signet_events(SignetEventsSpecifier::Block(block)).await
    }

    /// Get signet events in a range of blocks.
    pub async fn get_signet_events_in_range(
        &self,
        start: BlockNumber,
        end: BlockNumber,
    ) -> ColdResult<Vec<DbSignetEvent>> {
        self.get_signet_events(SignetEventsSpecifier::BlockRange { start, end }).await
    }

    // ==========================================================================
    // ZenithHeaders
    // ==========================================================================

    /// Get a zenith header by block number.
    pub async fn get_zenith_header(
        &self,
        block: BlockNumber,
    ) -> ColdResult<Option<DbZenithHeader>> {
        self.get_zenith_header_by_spec(ZenithHeaderSpecifier::Number(block)).await
    }

    /// Get a zenith header by specifier.
    #[tracing::instrument(skip(self, spec), fields(op = "get_zenith_header_by_spec"))]
    async fn get_zenith_header_by_spec(
        &self,
        spec: ZenithHeaderSpecifier,
    ) -> ColdResult<Option<DbZenithHeader>> {
        self.spawn_read("get_zenith_header_by_spec", move |inner| async move {
            inner.backend.get_zenith_header(spec).await
        })
        .await
    }

    /// Get zenith headers by specifier.
    #[tracing::instrument(skip(self, spec), fields(op = "get_zenith_headers"))]
    pub async fn get_zenith_headers(
        &self,
        spec: ZenithHeaderSpecifier,
    ) -> ColdResult<Vec<DbZenithHeader>> {
        self.spawn_read("get_zenith_headers", move |inner| async move {
            inner.backend.get_zenith_headers(spec).await
        })
        .await
    }

    /// Get zenith headers in a range of blocks.
    pub async fn get_zenith_headers_in_range(
        &self,
        start: BlockNumber,
        end: BlockNumber,
    ) -> ColdResult<Vec<DbZenithHeader>> {
        self.get_zenith_headers(ZenithHeaderSpecifier::Range { start, end }).await
    }

    // ==========================================================================
    // Logs
    // ==========================================================================

    /// Filter logs by block range, address, and topics.
    ///
    /// Follows `eth_getLogs` semantics. Returns matching logs ordered by
    /// `(block_number, tx_index, log_index)`.
    #[tracing::instrument(skip(self, filter), fields(op = "get_logs"))]
    pub async fn get_logs(&self, filter: Filter, max_logs: usize) -> ColdResult<Vec<RpcLog>> {
        self.spawn_read("get_logs", move |inner| async move {
            inner.backend.get_logs(&filter, max_logs).await
        })
        .await
    }

    /// Stream logs matching a filter.
    ///
    /// Returns a [`LogStream`] that yields matching logs in order.
    /// Consume with `StreamExt::next()` until `None`. If the last item is
    /// `Err(...)`, an error occurred (deadline, too many logs, reorg).
    ///
    /// The `deadline` is clamped to the handle's configured maximum.
    #[tracing::instrument(skip(self, filter), fields(op = "stream_logs"))]
    pub async fn stream_logs(
        &self,
        filter: Filter,
        max_logs: usize,
        deadline: Duration,
    ) -> ColdResult<LogStream> {
        let from = filter.get_from_block().unwrap_or(0);
        // Resolve `to` BEFORE acquiring `stream_sem`. Holding the permit
        // across setup I/O (especially when falling back to
        // `get_latest_block`) lets a stuck backend pin all 8 permits and
        // prevent any new stream from starting. Setup reads intentionally
        // bypass `read_sem` and the drain barrier: a stream asking for
        // "latest" should observe latest at setup time even alongside an
        // in-flight write.
        let to = match filter.get_to_block() {
            Some(to) => to,
            None => match self.inner.backend.get_latest_block().await? {
                Some(latest) => latest,
                None => {
                    let (_tx, rx) = mpsc::channel(1);
                    return Ok(ReceiverStream::new(rx));
                }
            },
        };

        let wait = Instant::now();
        let permit = self
            .inner
            .stream_sem
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| ColdStorageError::TaskTerminated)?;
        metrics::record_permit_wait("stream", wait.elapsed());

        let effective = deadline.min(self.inner.max_stream_deadline);
        let deadline_instant = Instant::now() + effective;
        let (sender, rx) = mpsc::channel(STREAM_CHANNEL_BUFFER);
        let inner = Arc::clone(&self.inner);
        let started = Instant::now();
        self.inner.tracker.spawn(
            async move {
                let _p = permit;
                let _guard = metrics::InFlightGuard::new("stream");
                let params =
                    StreamParams { from, to, max_logs, sender, deadline: deadline_instant };
                inner.backend.produce_log_stream(&filter, params).await;
                metrics::record_stream_lifetime(started.elapsed());
            }
            .in_current_span(),
        );
        Ok(ReceiverStream::new(rx))
    }

    // ==========================================================================
    // Metadata
    // ==========================================================================

    /// Get the latest block number in storage.
    #[tracing::instrument(skip(self), fields(op = "get_latest_block"))]
    pub async fn get_latest_block(&self) -> ColdResult<Option<BlockNumber>> {
        self.spawn_read("get_latest_block", move |inner| async move {
            inner.backend.get_latest_block().await
        })
        .await
    }

    // ==========================================================================
    // Writes
    // ==========================================================================

    /// Append a single block to cold storage.
    #[tracing::instrument(skip(self, data), fields(op = "append_block"))]
    pub async fn append_block(&self, data: BlockData) -> ColdResult<()> {
        self.spawn_write("append_block", move |inner| async move {
            inner.backend.append_block(data).await
        })
        .await
    }

    /// Append multiple blocks to cold storage.
    #[tracing::instrument(skip(self, data), fields(op = "append_blocks"))]
    pub async fn append_blocks(&self, data: Vec<BlockData>) -> ColdResult<()> {
        self.spawn_write("append_blocks", move |inner| async move {
            inner.backend.append_blocks(data).await
        })
        .await
    }

    /// Truncate all data above the given block number.
    ///
    /// This removes block N+1 and higher from all tables and invalidates any
    /// cached lookups above `block`.
    #[tracing::instrument(skip(self), fields(op = "truncate_above"))]
    pub async fn truncate_above(&self, block: BlockNumber) -> ColdResult<()> {
        self.spawn_write("truncate_above", move |inner| async move {
            let result = inner.backend.truncate_above(block).await;
            if result.is_ok() {
                inner.cache.lock().invalidate_above(block);
            }
            result
        })
        .await
    }

    /// Read and remove all blocks above the given block number.
    ///
    /// Returns receipts for each block above `block` in ascending order,
    /// then truncates. Index 0 = block+1, index 1 = block+2, etc.
    #[tracing::instrument(skip(self), fields(op = "drain_above"))]
    pub async fn drain_above(&self, block: BlockNumber) -> ColdResult<Vec<Vec<ColdReceipt>>> {
        self.spawn_write("drain_above", move |inner| async move {
            let result = inner.backend.drain_above(block).await;
            if result.is_ok() {
                inner.cache.lock().invalidate_above(block);
            }
            result
        })
        .await
    }
}
