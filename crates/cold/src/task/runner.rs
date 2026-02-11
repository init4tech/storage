//! Cold storage task runner.
//!
//! The [`ColdStorageTask`] processes requests from channels and dispatches
//! them to the storage backend. Reads and writes use separate channels:
//!
//! - **Reads**: Processed concurrently (up to 64 in flight) via spawned tasks
//! - **Writes**: Processed sequentially (inline await) to maintain ordering
//!
//! When the `cache` feature is enabled, hash-based transaction and receipt
//! lookups are served from an LRU cache, avoiding repeated backend and
//! `hash_slow()` calls for frequently queried items.

use crate::{ColdReadRequest, ColdStorage, ColdStorageHandle, ColdWriteRequest};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tracing::{debug, instrument};

#[cfg(feature = "cache")]
mod cold_cache {
    use crate::Confirmed;
    use alloy::primitives::B256;
    use lru::LruCache;
    use signet_storage_types::{Receipt, TransactionSigned};
    use std::num::NonZeroUsize;

    /// Default capacity for each LRU cache map.
    const DEFAULT_CACHE_CAPACITY: usize = 1024;

    /// LRU caches for hash-based transaction and receipt lookups.
    pub(super) struct ColdCache {
        transactions: LruCache<B256, Confirmed<TransactionSigned>>,
        receipts: LruCache<B256, Confirmed<Receipt>>,
    }

    impl ColdCache {
        /// Create a new cache with the default capacity.
        pub(super) fn new() -> Self {
            let cap = NonZeroUsize::new(DEFAULT_CACHE_CAPACITY).expect("non-zero");
            Self { transactions: LruCache::new(cap), receipts: LruCache::new(cap) }
        }

        /// Look up a cached transaction by hash.
        pub(super) fn get_tx(&mut self, hash: &B256) -> Option<Confirmed<TransactionSigned>> {
            self.transactions.get(hash).cloned()
        }

        /// Insert a transaction into the cache.
        pub(super) fn put_tx(&mut self, hash: B256, confirmed: Confirmed<TransactionSigned>) {
            self.transactions.put(hash, confirmed);
        }

        /// Look up a cached receipt by transaction hash.
        pub(super) fn get_receipt(&mut self, hash: &B256) -> Option<Confirmed<Receipt>> {
            self.receipts.get(hash).cloned()
        }

        /// Insert a receipt into the cache.
        pub(super) fn put_receipt(&mut self, hash: B256, confirmed: Confirmed<Receipt>) {
            self.receipts.put(hash, confirmed);
        }

        /// Invalidate all entries with a block number above the given cutoff.
        pub(super) fn invalidate_above(&mut self, block: u64) {
            let tx_keys: Vec<_> = self
                .transactions
                .iter()
                .filter(|(_, v)| v.meta().block_number() > block)
                .map(|(k, _)| *k)
                .collect();
            for key in tx_keys {
                self.transactions.pop(&key);
            }
            let rx_keys: Vec<_> = self
                .receipts
                .iter()
                .filter(|(_, v)| v.meta().block_number() > block)
                .map(|(k, _)| *k)
                .collect();
            for key in rx_keys {
                self.receipts.pop(&key);
            }
        }
    }
}

/// Channel size for cold storage read requests.
const READ_CHANNEL_SIZE: usize = 256;

/// Channel size for cold storage write requests.
const WRITE_CHANNEL_SIZE: usize = 256;

/// Maximum concurrent read request handlers.
const MAX_CONCURRENT_READERS: usize = 64;

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
/// When the `cache` feature is enabled, hash-based transaction and receipt
/// lookups are served from an LRU cache. Cache entries are invalidated
/// on [`truncate_above`](crate::ColdStorage::truncate_above) to handle
/// reorgs.
pub struct ColdStorageTask<B: ColdStorage> {
    backend: Arc<B>,
    read_receiver: mpsc::Receiver<ColdReadRequest>,
    write_receiver: mpsc::Receiver<ColdWriteRequest>,
    cancel_token: CancellationToken,
    /// Task tracker for concurrent read handlers only.
    task_tracker: TaskTracker,
    #[cfg(feature = "cache")]
    cache: Arc<tokio::sync::Mutex<cold_cache::ColdCache>>,
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
            backend: Arc::new(backend),
            read_receiver,
            write_receiver,
            cancel_token,
            task_tracker: TaskTracker::new(),
            #[cfg(feature = "cache")]
            cache: Arc::new(tokio::sync::Mutex::new(cold_cache::ColdCache::new())),
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

    /// Try to serve a read request from the LRU cache without spawning a
    /// task. Returns `Some(req)` if the request was not served (cache miss
    /// or non-cacheable), `None` if the response was sent from cache.
    #[cfg(feature = "cache")]
    async fn try_serve_cached(
        cache: &tokio::sync::Mutex<cold_cache::ColdCache>,
        req: ColdReadRequest,
    ) -> Option<ColdReadRequest> {
        match req {
            ColdReadRequest::GetTransaction {
                spec: crate::TransactionSpecifier::Hash(hash),
                resp,
            } => {
                if let Some(confirmed) = cache.lock().await.get_tx(&hash) {
                    let _ = resp.send(Ok(Some(confirmed)));
                    return None;
                }
                // Reconstruct the request on cache miss.
                Some(ColdReadRequest::GetTransaction {
                    spec: crate::TransactionSpecifier::Hash(hash),
                    resp,
                })
            }
            ColdReadRequest::GetReceipt { spec: crate::ReceiptSpecifier::TxHash(hash), resp } => {
                if let Some(confirmed) = cache.lock().await.get_receipt(&hash) {
                    let _ = resp.send(Ok(Some(confirmed)));
                    return None;
                }
                Some(ColdReadRequest::GetReceipt {
                    spec: crate::ReceiptSpecifier::TxHash(hash),
                    resp,
                })
            }
            other => Some(other),
        }
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
                            Self::handle_write(
                                Arc::clone(&self.backend),
                                write_req,
                                #[cfg(feature = "cache")]
                                Arc::clone(&self.cache),
                            ).await;
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
                            // Try to serve from cache before spawning a task.
                            #[cfg(feature = "cache")]
                            let read_req = match Self::try_serve_cached(&self.cache, read_req).await {
                                Some(req) => req,
                                None => continue,
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

                            let backend = Arc::clone(&self.backend);
                            #[cfg(feature = "cache")]
                            let cache = Arc::clone(&self.cache);
                            self.task_tracker.spawn(async move {
                                Self::handle_read(
                                    backend,
                                    read_req,
                                    #[cfg(feature = "cache")]
                                    cache,
                                ).await;
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

    async fn handle_read(
        backend: Arc<B>,
        req: ColdReadRequest,
        #[cfg(feature = "cache")] cache: Arc<tokio::sync::Mutex<cold_cache::ColdCache>>,
    ) {
        match req {
            ColdReadRequest::GetHeader { spec, resp } => {
                let result = backend.get_header(spec).await;
                let _ = resp.send(result);
            }
            ColdReadRequest::GetHeaders { specs, resp } => {
                let result = backend.get_headers(specs).await;
                let _ = resp.send(result);
            }
            ColdReadRequest::GetTransaction { spec, resp } => {
                let result = backend.get_transaction(spec).await;
                #[cfg(feature = "cache")]
                if let Ok(Some(ref confirmed)) = result {
                    let hash = *confirmed.inner().tx_hash();
                    cache.lock().await.put_tx(hash, confirmed.clone());
                }
                let _ = resp.send(result);
            }
            ColdReadRequest::GetTransactionsInBlock { block, resp } => {
                let result = backend.get_transactions_in_block(block).await;
                let _ = resp.send(result);
            }
            ColdReadRequest::GetTransactionCount { block, resp } => {
                let result = backend.get_transaction_count(block).await;
                let _ = resp.send(result);
            }
            ColdReadRequest::GetReceipt { spec, resp } => {
                let result = backend.get_receipt(spec).await;
                #[cfg(feature = "cache")]
                if let Ok(Some(ref confirmed)) = result
                    && let crate::ReceiptSpecifier::TxHash(hash) = &spec
                {
                    cache.lock().await.put_receipt(*hash, confirmed.clone());
                }
                let _ = resp.send(result);
            }
            ColdReadRequest::GetReceiptsInBlock { block, resp } => {
                let result = backend.get_receipts_in_block(block).await;
                let _ = resp.send(result);
            }
            ColdReadRequest::GetSignetEvents { spec, resp } => {
                let result = backend.get_signet_events(spec).await;
                let _ = resp.send(result);
            }
            ColdReadRequest::GetZenithHeader { spec, resp } => {
                let result = backend.get_zenith_header(spec).await;
                let _ = resp.send(result);
            }
            ColdReadRequest::GetZenithHeaders { spec, resp } => {
                let result = backend.get_zenith_headers(spec).await;
                let _ = resp.send(result);
            }
            ColdReadRequest::GetLatestBlock { resp } => {
                let result = backend.get_latest_block().await;
                let _ = resp.send(result);
            }
        }
    }

    async fn handle_write(
        backend: Arc<B>,
        req: ColdWriteRequest,
        #[cfg(feature = "cache")] cache: Arc<tokio::sync::Mutex<cold_cache::ColdCache>>,
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
                #[cfg(feature = "cache")]
                if result.is_ok() {
                    cache.lock().await.invalidate_above(block);
                }
                let _ = resp.send(result);
            }
        }
    }
}
