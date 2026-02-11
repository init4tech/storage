//! Cold storage task runner.
//!
//! The [`ColdStorageTask`] processes requests from channels and dispatches
//! them to the storage backend. Reads and writes use separate channels:
//!
//! - **Reads**: Processed concurrently (up to 64 in flight) via spawned tasks
//! - **Writes**: Processed sequentially (inline await) to maintain ordering
//!
//! Hash-based transaction and receipt lookups are served from an LRU cache,
//! avoiding repeated backend and `hash_slow()` calls for frequently queried
//! items.

use super::cache::ColdCache;
use crate::{ColdReadRequest, ColdStorage, ColdStorageHandle, ColdWriteRequest};
use std::sync::Arc;
use tokio::sync::{Mutex, mpsc};
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tracing::{debug, instrument};

/// Channel size for cold storage read requests.
const READ_CHANNEL_SIZE: usize = 256;

/// Channel size for cold storage write requests.
const WRITE_CHANNEL_SIZE: usize = 256;

/// Maximum concurrent read request handlers.
const MAX_CONCURRENT_READERS: usize = 64;

/// Shared state for the cold storage task, holding the backend and cache.
///
/// This is wrapped in an `Arc` so that spawned read handlers can access
/// the backend and cache without moving ownership.
struct ColdStorageTaskInner<B> {
    backend: B,
    cache: Mutex<ColdCache>,
}

impl<B: ColdStorage> ColdStorageTaskInner<B> {
    /// Handle a read request, checking the cache first where applicable.
    async fn handle_read(&self, req: ColdReadRequest) {
        match req {
            ColdReadRequest::GetHeader { spec, resp } => {
                let result = match spec {
                    crate::HeaderSpecifier::Number(n) => {
                        let cached = self.cache.lock().await.get_header(&n);
                        match cached {
                            Some(header) => Ok(Some(header)),
                            None => {
                                let r = self.backend.get_header(spec).await;
                                if let Ok(Some(ref h)) = r {
                                    self.cache.lock().await.put_header(n, h.clone());
                                }
                                r
                            }
                        }
                    }
                    _ => {
                        let r = self.backend.get_header(spec).await;
                        if let Ok(Some(ref h)) = r {
                            self.cache.lock().await.put_header(h.number, h.clone());
                        }
                        r
                    }
                };
                let _ = resp.send(result);
            }
            ColdReadRequest::GetHeaders { specs, resp } => {
                let result = self.backend.get_headers(specs).await;
                let _ = resp.send(result);
            }
            ColdReadRequest::GetTransaction { spec, resp } => {
                let result = match spec {
                    crate::TransactionSpecifier::BlockAndIndex { block, index } => {
                        let cached = self.cache.lock().await.get_tx(&(block, index));
                        match cached {
                            Some(confirmed) => Ok(Some(confirmed)),
                            None => {
                                let r = self.backend.get_transaction(spec).await;
                                if let Ok(Some(ref c)) = r {
                                    self.cache.lock().await.put_tx((block, index), c.clone());
                                }
                                r
                            }
                        }
                    }
                    _ => {
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
                };
                let _ = resp.send(result);
            }
            ColdReadRequest::GetTransactionsInBlock { block, resp } => {
                let result = self.backend.get_transactions_in_block(block).await;
                let _ = resp.send(result);
            }
            ColdReadRequest::GetTransactionCount { block, resp } => {
                let result = self.backend.get_transaction_count(block).await;
                let _ = resp.send(result);
            }
            ColdReadRequest::GetReceipt { spec, resp } => {
                let result = match spec {
                    crate::ReceiptSpecifier::BlockAndIndex { block, index } => {
                        let cached = self.cache.lock().await.get_receipt(&(block, index));
                        match cached {
                            Some(confirmed) => Ok(Some(confirmed)),
                            None => {
                                let r = self.backend.get_receipt(spec).await;
                                if let Ok(Some(ref c)) = r {
                                    self.cache.lock().await.put_receipt((block, index), c.clone());
                                }
                                r
                            }
                        }
                    }
                    _ => {
                        let r = self.backend.get_receipt(spec).await;
                        if let Ok(Some(ref c)) = r {
                            let meta = c.meta();
                            self.cache.lock().await.put_receipt(
                                (meta.block_number(), meta.transaction_index()),
                                c.clone(),
                            );
                        }
                        r
                    }
                };
                let _ = resp.send(result);
            }
            ColdReadRequest::GetReceiptsInBlock { block, resp } => {
                let result = self.backend.get_receipts_in_block(block).await;
                let _ = resp.send(result);
            }
            ColdReadRequest::GetSignetEvents { spec, resp } => {
                let result = self.backend.get_signet_events(spec).await;
                let _ = resp.send(result);
            }
            ColdReadRequest::GetZenithHeader { spec, resp } => {
                let result = self.backend.get_zenith_header(spec).await;
                let _ = resp.send(result);
            }
            ColdReadRequest::GetZenithHeaders { spec, resp } => {
                let result = self.backend.get_zenith_headers(spec).await;
                let _ = resp.send(result);
            }
            ColdReadRequest::GetLatestBlock { resp } => {
                let result = self.backend.get_latest_block().await;
                let _ = resp.send(result);
            }
        }
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
            inner: Arc::new(ColdStorageTaskInner { backend, cache: Mutex::new(ColdCache::new()) }),
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

                // Check for cancellation first
                _ = self.cancel_token.cancelled() => {
                    debug!("Cold storage task received cancellation signal");
                    break;
                }

                // Process writes sequentially (inline await, not spawned)
                maybe_write = self.write_receiver.recv() => {
                    match maybe_write {
                        Some(write_req) => {
                            self.inner.handle_write(write_req).await;
                        }
                        None => {
                            debug!("Cold storage write channel closed");
                            break;
                        }
                    }
                }

                // Process reads concurrently (spawned)
                maybe_read = self.read_receiver.recv() => {
                    match maybe_read {
                        Some(read_req) => {
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
                                inner.handle_read(read_req).await;
                            });
                        }
                        None => {
                            debug!("Cold storage read channel closed");
                            break;
                        }
                    }
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
