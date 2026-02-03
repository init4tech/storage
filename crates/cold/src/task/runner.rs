//! Cold storage task runner.
//!
//! The [`ColdStorageTask`] processes requests from channels and dispatches
//! them to the storage backend. Reads and writes use separate channels:
//!
//! - **Reads**: Processed concurrently (up to 64 in flight) via spawned tasks
//! - **Writes**: Processed sequentially (inline await) to maintain ordering

use crate::{ColdReadRequest, ColdStorage, ColdStorageHandle, ColdWriteRequest};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tracing::{debug, instrument};

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
/// - **Reads**: Spawned as concurrent tasks (up to [`MAX_CONCURRENT_READERS`]).
///   Multiple reads can execute in parallel.
/// - **Writes**: Processed inline (sequential). Each write completes before
///   the next is started, ensuring ordering.
///
/// This design prioritizes write ordering for correctness while allowing
/// read throughput to scale with concurrency.
pub struct ColdStorageTask<B: ColdStorage> {
    backend: Arc<B>,
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
            backend: Arc::new(backend),
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
                            Self::handle_write(Arc::clone(&self.backend), write_req).await;
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

                            let backend = Arc::clone(&self.backend);
                            self.task_tracker.spawn(async move {
                                Self::handle_read(backend, read_req).await;
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

    async fn handle_read(backend: Arc<B>, req: ColdReadRequest) {
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

    async fn handle_write(backend: Arc<B>, req: ColdWriteRequest) {
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
                let _ = resp.send(result);
            }
        }
    }
}
