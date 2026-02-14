//! Ergonomic handles for interacting with cold storage.
//!
//! This module provides two handle types:
//!
//! - [`ColdStorageHandle`]: Full access (reads and writes)
//! - [`ColdStorageReadHandle`]: Read-only access
//!
//! Both handles can be cloned and shared across tasks. They use separate
//! channels for reads and writes, allowing concurrent read processing while
//! maintaining sequential write ordering.

use crate::{
    AppendBlockRequest, BlockData, ColdReadRequest, ColdReceipt, ColdResult, ColdStorageError,
    ColdWriteRequest, Confirmed, Filter, HeaderSpecifier, ReceiptSpecifier, RpcLog,
    SignetEventsSpecifier, TransactionSpecifier, ZenithHeaderSpecifier,
};
use alloy::primitives::{B256, BlockNumber};
use signet_storage_types::{DbSignetEvent, DbZenithHeader, RecoveredTx, SealedHeader};
use tokio::sync::{mpsc, oneshot};

/// Map a [`mpsc::error::TrySendError`] to the appropriate
/// [`ColdStorageError`] variant.
fn map_dispatch_error<T>(e: mpsc::error::TrySendError<T>) -> ColdStorageError {
    match e {
        mpsc::error::TrySendError::Full(_) => ColdStorageError::Backpressure,
        mpsc::error::TrySendError::Closed(_) => ColdStorageError::TaskTerminated,
    }
}

/// Read-only handle for interacting with the cold storage task.
///
/// This handle provides read access only and cannot perform write operations.
/// It shares the read channel with [`ColdStorageHandle`], allowing multiple
/// readers to coexist without affecting write throughput.
///
/// # Usage
///
/// Obtain a read handle via [`ColdStorageHandle::reader`]:
///
/// ```ignore
/// let handle = ColdStorageTask::spawn(backend, cancel);
/// let reader = handle.reader();
///
/// // Use reader for queries
/// let header = reader.get_header_by_number(100).await?;
/// ```
///
/// # Thread Safety
///
/// This handle is `Clone + Send + Sync` and can be shared across tasks.
/// Multiple readers can query concurrently without blocking writes.
#[derive(Clone, Debug)]
pub struct ColdStorageReadHandle {
    sender: mpsc::Sender<ColdReadRequest>,
}

impl ColdStorageReadHandle {
    /// Create a new read-only handle with the given sender.
    pub(crate) const fn new(sender: mpsc::Sender<ColdReadRequest>) -> Self {
        Self { sender }
    }

    /// Send a read request and wait for the response.
    async fn send<T>(
        &self,
        req: ColdReadRequest,
        rx: oneshot::Receiver<ColdResult<T>>,
    ) -> ColdResult<T> {
        self.sender.send(req).await.map_err(|_| ColdStorageError::Cancelled)?;
        rx.await.map_err(|_| ColdStorageError::Cancelled)?
    }

    // ==========================================================================
    // Headers
    // ==========================================================================

    /// Get a header by specifier.
    pub async fn get_header(&self, spec: HeaderSpecifier) -> ColdResult<Option<SealedHeader>> {
        let (resp, rx) = oneshot::channel();
        self.send(ColdReadRequest::GetHeader { spec, resp }, rx).await
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
    pub async fn get_headers(
        &self,
        specs: Vec<HeaderSpecifier>,
    ) -> ColdResult<Vec<Option<SealedHeader>>> {
        let (resp, rx) = oneshot::channel();
        self.send(ColdReadRequest::GetHeaders { specs, resp }, rx).await
    }

    // ==========================================================================
    // Transactions
    // ==========================================================================

    /// Get a transaction by specifier, with block confirmation metadata.
    pub async fn get_transaction(
        &self,
        spec: TransactionSpecifier,
    ) -> ColdResult<Option<Confirmed<RecoveredTx>>> {
        let (resp, rx) = oneshot::channel();
        self.send(ColdReadRequest::GetTransaction { spec, resp }, rx).await
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
    pub async fn get_transactions_in_block(
        &self,
        block: BlockNumber,
    ) -> ColdResult<Vec<RecoveredTx>> {
        let (resp, rx) = oneshot::channel();
        self.send(ColdReadRequest::GetTransactionsInBlock { block, resp }, rx).await
    }

    /// Get the transaction count for a block.
    pub async fn get_transaction_count(&self, block: BlockNumber) -> ColdResult<u64> {
        let (resp, rx) = oneshot::channel();
        self.send(ColdReadRequest::GetTransactionCount { block, resp }, rx).await
    }

    // ==========================================================================
    // Receipts
    // ==========================================================================

    /// Get a receipt by specifier.
    pub async fn get_receipt(&self, spec: ReceiptSpecifier) -> ColdResult<Option<ColdReceipt>> {
        let (resp, rx) = oneshot::channel();
        self.send(ColdReadRequest::GetReceipt { spec, resp }, rx).await
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
    pub async fn get_receipts_in_block(&self, block: BlockNumber) -> ColdResult<Vec<ColdReceipt>> {
        let (resp, rx) = oneshot::channel();
        self.send(ColdReadRequest::GetReceiptsInBlock { block, resp }, rx).await
    }

    // ==========================================================================
    // SignetEvents
    // ==========================================================================

    /// Get signet events by specifier.
    pub async fn get_signet_events(
        &self,
        spec: SignetEventsSpecifier,
    ) -> ColdResult<Vec<DbSignetEvent>> {
        let (resp, rx) = oneshot::channel();
        self.send(ColdReadRequest::GetSignetEvents { spec, resp }, rx).await
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
        let (resp, rx) = oneshot::channel();
        self.send(
            ColdReadRequest::GetZenithHeader { spec: ZenithHeaderSpecifier::Number(block), resp },
            rx,
        )
        .await
    }

    /// Get zenith headers by specifier.
    pub async fn get_zenith_headers(
        &self,
        spec: ZenithHeaderSpecifier,
    ) -> ColdResult<Vec<DbZenithHeader>> {
        let (resp, rx) = oneshot::channel();
        self.send(ColdReadRequest::GetZenithHeaders { spec, resp }, rx).await
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
    /// Follows `eth_getLogs` semantics. Returns at most `max_logs` matching
    /// logs ordered by `(block_number, tx_index, log_index)`.
    pub async fn get_logs(&self, filter: Filter, max_logs: usize) -> ColdResult<Vec<RpcLog>> {
        let (resp, rx) = oneshot::channel();
        self.send(ColdReadRequest::GetLogs { filter: Box::new(filter), max_logs, resp }, rx).await
    }

    // ==========================================================================
    // Metadata
    // ==========================================================================

    /// Get the latest block number in storage.
    pub async fn get_latest_block(&self) -> ColdResult<Option<BlockNumber>> {
        let (resp, rx) = oneshot::channel();
        self.send(ColdReadRequest::GetLatestBlock { resp }, rx).await
    }
}

/// Handle for interacting with the cold storage task.
///
/// This handle provides full access to both read and write operations.
/// It can be cloned and shared across tasks.
///
/// # Channel Separation
///
/// Internally, this handle uses separate channels for reads and writes:
///
/// - **Read channel**: Shared with [`ColdStorageReadHandle`]. Reads are
///   processed concurrently (up to 64 in flight).
/// - **Write channel**: Exclusive to this handle. Writes are processed
///   sequentially to maintain ordering.
///
/// This design allows read-heavy workloads to proceed without being blocked
/// by write operations, while ensuring write ordering is preserved.
///
/// # Read Access
///
/// All read methods from [`ColdStorageReadHandle`] are available on this
/// handle via [`Deref`](std::ops::Deref).
///
/// # Usage
///
/// ```ignore
/// let handle = ColdStorageTask::spawn(backend, cancel);
///
/// // Full access: reads and writes
/// handle.append_block(data).await?;
/// let header = handle.get_header_by_number(100).await?;
///
/// // Get a read-only handle for query-only use cases
/// let reader = handle.reader();
/// ```
///
/// # Thread Safety
///
/// This handle is `Clone + Send + Sync` and can be shared across tasks.
#[derive(Clone, Debug)]
pub struct ColdStorageHandle {
    reader: ColdStorageReadHandle,
    write_sender: mpsc::Sender<ColdWriteRequest>,
}

impl std::ops::Deref for ColdStorageHandle {
    type Target = ColdStorageReadHandle;

    fn deref(&self) -> &Self::Target {
        &self.reader
    }
}

impl ColdStorageHandle {
    /// Create a new handle with the given senders.
    pub(crate) const fn new(
        read_sender: mpsc::Sender<ColdReadRequest>,
        write_sender: mpsc::Sender<ColdWriteRequest>,
    ) -> Self {
        Self { reader: ColdStorageReadHandle::new(read_sender), write_sender }
    }

    /// Get a read-only handle that shares the read channel.
    ///
    /// The returned handle can only perform read operations and cannot
    /// modify storage. Multiple read handles can coexist and query
    /// concurrently without affecting write throughput.
    pub fn reader(&self) -> ColdStorageReadHandle {
        self.reader.clone()
    }

    /// Send a write request and wait for the response.
    async fn send_write<T>(
        &self,
        req: ColdWriteRequest,
        rx: oneshot::Receiver<ColdResult<T>>,
    ) -> ColdResult<T> {
        self.write_sender.send(req).await.map_err(|_| ColdStorageError::Cancelled)?;
        rx.await.map_err(|_| ColdStorageError::Cancelled)?
    }

    // ==========================================================================
    // Write Operations
    // ==========================================================================

    /// Append a single block to cold storage.
    pub async fn append_block(&self, data: BlockData) -> ColdResult<()> {
        let (resp, rx) = oneshot::channel();
        self.send_write(
            ColdWriteRequest::AppendBlock(Box::new(AppendBlockRequest { data, resp })),
            rx,
        )
        .await
    }

    /// Append multiple blocks to cold storage.
    pub async fn append_blocks(&self, data: Vec<BlockData>) -> ColdResult<()> {
        let (resp, rx) = oneshot::channel();
        self.send_write(ColdWriteRequest::AppendBlocks { data, resp }, rx).await
    }

    /// Truncate all data above the given block number.
    ///
    /// This removes block N+1 and higher from all tables.
    pub async fn truncate_above(&self, block: BlockNumber) -> ColdResult<()> {
        let (resp, rx) = oneshot::channel();
        self.send_write(ColdWriteRequest::TruncateAbove { block, resp }, rx).await
    }

    // ==========================================================================
    // Synchronous Fire-and-Forget Dispatch
    // ==========================================================================

    /// Dispatch append blocks without waiting for response (non-blocking).
    ///
    /// Unlike [`append_blocks`](Self::append_blocks), this method returns
    /// immediately without waiting for the write to complete. The write
    /// result is discarded.
    ///
    /// # Errors
    ///
    /// - [`ColdStorageError::Backpressure`]: Channel is full. The task is alive
    ///   but cannot keep up. Transient; may retry or accept the gap.
    /// - [`ColdStorageError::TaskTerminated`]: Channel is closed. The task has
    ///   stopped and must be restarted.
    ///
    /// In both cases, hot storage already contains the data and remains
    /// authoritative.
    pub fn dispatch_append_blocks(&self, data: Vec<BlockData>) -> ColdResult<()> {
        let (resp, _rx) = oneshot::channel();
        self.write_sender
            .try_send(ColdWriteRequest::AppendBlocks { data, resp })
            .map_err(map_dispatch_error)
    }

    /// Dispatch truncate without waiting for response (non-blocking).
    ///
    /// Unlike [`truncate_above`](Self::truncate_above), this method returns
    /// immediately without waiting for the truncate to complete. The result
    /// is discarded.
    ///
    /// # Errors
    ///
    /// Same as [`dispatch_append_blocks`](Self::dispatch_append_blocks). If
    /// cold storage falls behind during a reorg, it may temporarily contain
    /// stale data until the truncate is processed or replayed.
    pub fn dispatch_truncate_above(&self, block: BlockNumber) -> ColdResult<()> {
        let (resp, _rx) = oneshot::channel();
        self.write_sender
            .try_send(ColdWriteRequest::TruncateAbove { block, resp })
            .map_err(map_dispatch_error)
    }
}
