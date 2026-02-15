//! Cold storage task runner.
//!
//! The [`ColdStorageTask`] processes requests from channels and dispatches
//! them to the storage backend. Reads and writes use separate channels:
//!
//! - **Reads**: Processed concurrently (up to 64 in flight) via spawned tasks
//! - **Writes**: Processed sequentially (inline await) to maintain ordering
//!
//! Transaction, receipt, and header lookups are served from an LRU cache,
//! avoiding repeated backend reads for frequently queried items.
//!
//! # Log Streaming
//!
//! The task owns the streaming configuration (max deadline, concurrency
//! limit) and delegates the streaming loop to the backend via
//! [`ColdStorage::produce_log_stream`]. Callers supply a per-request
//! deadline that is clamped to the task's configured maximum.

use super::cache::ColdCache;
use crate::{
    ColdReadRequest, ColdReceipt, ColdResult, ColdStorage, ColdStorageError, ColdStorageHandle,
    ColdWriteRequest, Confirmed, HeaderSpecifier, LogStream, ReceiptSpecifier,
    TransactionSpecifier,
};
use signet_storage_types::{RecoveredTx, SealedHeader};
use std::{sync::Arc, time::Duration};
use tokio::sync::{Mutex, Semaphore, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tracing::{debug, instrument};

/// Default maximum deadline for streaming operations.
const DEFAULT_MAX_STREAM_DEADLINE: Duration = Duration::from_secs(60);

/// Channel size for cold storage read requests.
const READ_CHANNEL_SIZE: usize = 256;

/// Channel size for cold storage write requests.
const WRITE_CHANNEL_SIZE: usize = 256;

/// Maximum concurrent read request handlers.
const MAX_CONCURRENT_READERS: usize = 64;

/// Maximum concurrent streaming operations.
const MAX_CONCURRENT_STREAMS: usize = 8;

/// Channel buffer size for streaming operations.
const STREAM_CHANNEL_BUFFER: usize = 256;

/// Shared state for the cold storage task, holding the backend and cache.
///
/// This is wrapped in an `Arc` so that spawned read handlers can access
/// the backend and cache without moving ownership.
struct ColdStorageTaskInner<B> {
    backend: B,
    cache: Mutex<ColdCache>,
    max_stream_deadline: Duration,
    stream_semaphore: Arc<Semaphore>,
}

impl<B: ColdStorage> ColdStorageTaskInner<B> {
    /// Fetch a header from the backend and cache the result.
    async fn fetch_and_cache_header(
        &self,
        spec: HeaderSpecifier,
    ) -> ColdResult<Option<SealedHeader>> {
        let r = self.backend.get_header(spec).await;
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
        let r = self.backend.get_transaction(spec).await;
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
        let r = self.backend.get_receipt(spec).await;
        if let Ok(Some(ref c)) = r {
            self.cache.lock().await.put_receipt((c.block_number, c.transaction_index), c.clone());
        }
        r
    }

    /// Handle a read request, checking the cache first where applicable.
    async fn handle_read(self: &Arc<Self>, req: ColdReadRequest) {
        match req {
            ColdReadRequest::GetHeader { spec, resp } => {
                let result = if let HeaderSpecifier::Number(n) = &spec {
                    if let Some(hit) = self.cache.lock().await.get_header(n) {
                        Ok(Some(hit))
                    } else {
                        self.fetch_and_cache_header(spec).await
                    }
                } else {
                    self.fetch_and_cache_header(spec).await
                };
                let _ = resp.send(result);
            }
            ColdReadRequest::GetHeaders { specs, resp } => {
                let _ = resp.send(self.backend.get_headers(specs).await);
            }
            ColdReadRequest::GetTransaction { spec, resp } => {
                let result = if let TransactionSpecifier::BlockAndIndex { block, index } = &spec {
                    if let Some(hit) = self.cache.lock().await.get_tx(&(*block, *index)) {
                        Ok(Some(hit))
                    } else {
                        self.fetch_and_cache_tx(spec).await
                    }
                } else {
                    self.fetch_and_cache_tx(spec).await
                };
                let _ = resp.send(result);
            }
            ColdReadRequest::GetTransactionsInBlock { block, resp } => {
                let _ = resp.send(self.backend.get_transactions_in_block(block).await);
            }
            ColdReadRequest::GetTransactionCount { block, resp } => {
                let _ = resp.send(self.backend.get_transaction_count(block).await);
            }
            ColdReadRequest::GetReceipt { spec, resp } => {
                let result = if let ReceiptSpecifier::BlockAndIndex { block, index } = &spec {
                    if let Some(hit) = self.cache.lock().await.get_receipt(&(*block, *index)) {
                        Ok(Some(hit))
                    } else {
                        self.fetch_and_cache_receipt(spec).await
                    }
                } else {
                    self.fetch_and_cache_receipt(spec).await
                };
                let _ = resp.send(result);
            }
            ColdReadRequest::GetReceiptsInBlock { block, resp } => {
                let _ = resp.send(self.backend.get_receipts_in_block(block).await);
            }
            ColdReadRequest::GetSignetEvents { spec, resp } => {
                let _ = resp.send(self.backend.get_signet_events(spec).await);
            }
            ColdReadRequest::GetZenithHeader { spec, resp } => {
                let _ = resp.send(self.backend.get_zenith_header(spec).await);
            }
            ColdReadRequest::GetZenithHeaders { spec, resp } => {
                let _ = resp.send(self.backend.get_zenith_headers(spec).await);
            }
            ColdReadRequest::GetLogs { filter, max_logs, resp } => {
                let _ = resp.send(self.backend.get_logs(*filter, max_logs).await);
            }
            ColdReadRequest::StreamLogs { filter, max_logs, deadline, resp } => {
                let _ = resp.send(self.handle_stream_logs(*filter, max_logs, deadline).await);
            }
            ColdReadRequest::GetLatestBlock { resp } => {
                let _ = resp.send(self.backend.get_latest_block().await);
            }
        }
    }

    /// Stream logs matching a filter.
    ///
    /// Acquires a concurrency permit, resolves the block range, anchors
    /// to the `to` block hash, then spawns a producer task that delegates
    /// to [`ColdStorage::produce_log_stream`].
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
                let Some(latest) = self.backend.get_latest_block().await? else {
                    let (_tx, rx) = mpsc::channel(1);
                    return Ok(ReceiverStream::new(rx));
                };
                latest
            }
        };

        // Anchor to the `to` block hash for reorg detection.
        let Some(anchor_header) = self.backend.get_header(HeaderSpecifier::Number(to)).await?
        else {
            // Block disappeared between range resolution and anchor fetch.
            let (_tx, rx) = mpsc::channel(1);
            return Ok(ReceiverStream::new(rx));
        };
        let anchor_hash = anchor_header.hash();

        let effective = deadline.min(self.max_stream_deadline);
        let deadline_instant = tokio::time::Instant::now() + effective;
        let (sender, rx) = mpsc::channel(STREAM_CHANNEL_BUFFER);
        let inner = Arc::clone(self);

        tokio::spawn(async move {
            let _permit = permit;
            inner
                .backend
                .produce_log_stream(
                    &filter,
                    from,
                    to,
                    anchor_hash,
                    max_logs,
                    sender,
                    deadline_instant,
                )
                .await;
        });

        Ok(ReceiverStream::new(rx))
    }

    /// Handle a write request, invalidating the cache on truncation.
    async fn handle_write(&self, req: ColdWriteRequest) {
        match req {
            ColdWriteRequest::AppendBlock(boxed) => {
                let result = self.backend.append_block(boxed.data).await;
                let _ = boxed.resp.send(result);
            }
            ColdWriteRequest::AppendBlocks { data, resp } => {
                let result = self.backend.append_blocks(data).await;
                let _ = resp.send(result);
            }
            ColdWriteRequest::TruncateAbove { block, resp } => {
                let result = self.backend.truncate_above(block).await;
                if result.is_ok() {
                    self.cache.lock().await.invalidate_above(block);
                }
                let _ = resp.send(result);
            }
        }
    }
}

/// The cold storage task that processes requests.
///
/// This task receives requests over separate read and write channels and
/// dispatches them to the storage backend. It supports graceful shutdown
/// via a cancellation token.
///
/// # Processing Model
///
/// - **Reads**: Spawned as concurrent tasks (up to 64 in flight).
///   Multiple reads can execute in parallel.
/// - **Writes**: Processed inline (sequential). Each write completes before
///   the next is started, ensuring ordering.
///
/// This design prioritizes write ordering for correctness while allowing
/// read throughput to scale with concurrency.
///
/// # Log Streaming
///
/// The task owns the streaming configuration (max deadline, concurrency
/// limit) and delegates the streaming loop to the backend via
/// [`ColdStorage::produce_log_stream`]. Callers supply a per-request
/// deadline that is clamped to the task's configured maximum.
///
/// # Caching
///
/// Transaction, receipt, and header lookups are served from an LRU cache
/// when possible. Cache entries are invalidated on
/// [`truncate_above`](crate::ColdStorage::truncate_above) to handle reorgs.
pub struct ColdStorageTask<B: ColdStorage> {
    inner: Arc<ColdStorageTaskInner<B>>,
    read_receiver: mpsc::Receiver<ColdReadRequest>,
    write_receiver: mpsc::Receiver<ColdWriteRequest>,
    cancel_token: CancellationToken,
    /// Task tracker for concurrent read handlers only.
    task_tracker: TaskTracker,
}

impl<B: ColdStorage> std::fmt::Debug for ColdStorageTask<B> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ColdStorageTask").finish_non_exhaustive()
    }
}

impl<B: ColdStorage> ColdStorageTask<B> {
    /// Create a new cold storage task and return its handle.
    pub fn new(backend: B, cancel_token: CancellationToken) -> (Self, ColdStorageHandle) {
        let (read_sender, read_receiver) = mpsc::channel(READ_CHANNEL_SIZE);
        let (write_sender, write_receiver) = mpsc::channel(WRITE_CHANNEL_SIZE);
        let task = Self {
            inner: Arc::new(ColdStorageTaskInner {
                backend,
                cache: Mutex::new(ColdCache::new()),
                max_stream_deadline: DEFAULT_MAX_STREAM_DEADLINE,
                stream_semaphore: Arc::new(Semaphore::new(MAX_CONCURRENT_STREAMS)),
            }),
            read_receiver,
            write_receiver,
            cancel_token,
            task_tracker: TaskTracker::new(),
        };
        let handle = ColdStorageHandle::new(read_sender, write_sender);
        (task, handle)
    }

    /// Spawn the task and return the handle.
    ///
    /// The task will run until the cancellation token is triggered or the
    /// channels are closed.
    pub fn spawn(backend: B, cancel_token: CancellationToken) -> ColdStorageHandle {
        let (task, handle) = Self::new(backend, cancel_token);
        tokio::spawn(task.run());
        handle
    }

    /// Run the task, processing requests until shutdown.
    #[instrument(skip(self), name = "cold_storage_task")]
    pub async fn run(mut self) {
        debug!("Cold storage task started");

        loop {
            tokio::select! {
                biased;

                _ = self.cancel_token.cancelled() => {
                    debug!("Cold storage task received cancellation signal");
                    break;
                }

                maybe_write = self.write_receiver.recv() => {
                    let Some(req) = maybe_write else {
                        debug!("Cold storage write channel closed");
                        break;
                    };
                    self.inner.handle_write(req).await;
                }

                maybe_read = self.read_receiver.recv() => {
                    let Some(req) = maybe_read else {
                        debug!("Cold storage read channel closed");
                        break;
                    };

                    // Apply backpressure: wait if we've hit the concurrent reader limit
                    while self.task_tracker.len() >= MAX_CONCURRENT_READERS {
                        tokio::select! {
                            _ = self.cancel_token.cancelled() => {
                                debug!("Cancellation while waiting for read task slot");
                                break;
                            }
                            _ = self.task_tracker.wait() => {}
                        }
                    }

                    let inner = Arc::clone(&self.inner);
                    self.task_tracker.spawn(async move {
                        inner.handle_read(req).await;
                    });
                }
            }
        }

        // Graceful shutdown: wait for in-progress read tasks to complete
        debug!("Waiting for in-progress read handlers to complete");
        self.task_tracker.close();
        self.task_tracker.wait().await;
        debug!("Cold storage task shut down gracefully");
    }
}
