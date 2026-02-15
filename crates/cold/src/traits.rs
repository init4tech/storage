//! Core trait definition for cold storage backends.
//!
//! The [`ColdStorage`] trait defines the interface that all cold storage
//! backends must implement. Backends are responsible for data organization,
//! indexing, and keying - the trait is agnostic to these implementation
//! details.

use crate::{
    ColdReceipt, ColdResult, ColdStorageError, Confirmed, Filter, HeaderSpecifier,
    ReceiptSpecifier, RpcLog, SignetEventsSpecifier, TransactionSpecifier, ZenithHeaderSpecifier,
};
use alloy::primitives::{B256, BlockNumber};
use signet_storage_types::{
    DbSignetEvent, DbZenithHeader, ExecutedBlock, Receipt, RecoveredTx, SealedHeader,
};
use std::future::Future;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

/// A stream of log results backed by a bounded channel.
///
/// Each item is a `ColdResult<RpcLog>`. The stream produces `Ok(log)` items
/// until complete, or yields a final `Err(e)` on failure. The stream ends
/// (`None`) when all matching logs have been delivered or after an error.
///
/// # Partial Delivery
///
/// One or more `Ok(log)` items may be delivered before a terminal
/// `Err(...)`. Consumers must be prepared for partial results — for
/// example, a reorg or deadline expiry can interrupt a stream that has
/// already yielded some logs.
///
/// # Resource Management
///
/// The stream holds a backend concurrency permit. Dropping the stream
/// releases the permit. Drop early if results are no longer needed.
pub type LogStream = ReceiverStream<ColdResult<RpcLog>>;

/// Data for appending a complete block to cold storage.
#[derive(Debug, Clone)]
pub struct BlockData {
    /// The sealed block header (contains cached hash).
    pub header: SealedHeader,
    /// The transactions in the block, with recovered senders.
    pub transactions: Vec<RecoveredTx>,
    /// The receipts for the transactions.
    pub receipts: Vec<Receipt>,
    /// The signet events in the block.
    pub signet_events: Vec<DbSignetEvent>,
    /// The zenith header for the block, if present.
    pub zenith_header: Option<DbZenithHeader>,
}

impl BlockData {
    /// Create new block data.
    pub const fn new(
        header: SealedHeader,
        transactions: Vec<RecoveredTx>,
        receipts: Vec<Receipt>,
        signet_events: Vec<DbSignetEvent>,
        zenith_header: Option<DbZenithHeader>,
    ) -> Self {
        Self { header, transactions, receipts, signet_events, zenith_header }
    }

    /// Get the block number of the block.
    pub fn block_number(&self) -> BlockNumber {
        self.header.number
    }
}

impl From<ExecutedBlock> for BlockData {
    fn from(block: ExecutedBlock) -> Self {
        Self::new(
            block.header,
            block.transactions,
            block.receipts,
            block.signet_events,
            block.zenith_header,
        )
    }
}

/// Unified cold storage backend trait.
///
/// Backend is responsible for all data organization, indexing, and keying.
/// The trait is agnostic to how the backend stores or indexes data.
///
/// All methods are async and return futures that are `Send`.
///
/// # Implementation Guide
///
/// Implementers must ensure:
///
/// - **Append-only ordering**: `append_block` must enforce monotonically
///   increasing block numbers. Attempting to append a block with a number <=
///   the current latest should return an error.
///
/// - **Atomic truncation**: `truncate_above` must remove all data for blocks
///   N+1 and higher atomically. Partial truncation is not acceptable.
///
/// - **Index maintenance**: Hash-based lookups (e.g., header by hash,
///   transaction by hash) require the implementation to maintain appropriate
///   indexes. These indexes must be updated during `append_block` and cleaned
///   during `truncate_above`.
///
/// - **Consistent reads**: Read operations should return consistent snapshots.
///   A read started before a write completes should not see partial data from
///   that write.
///
pub trait ColdStorage: Send + Sync + 'static {
    // --- Headers ---

    /// Get a header by specifier.
    fn get_header(
        &self,
        spec: HeaderSpecifier,
    ) -> impl Future<Output = ColdResult<Option<SealedHeader>>> + Send;

    /// Get multiple headers by specifiers.
    fn get_headers(
        &self,
        specs: Vec<HeaderSpecifier>,
    ) -> impl Future<Output = ColdResult<Vec<Option<SealedHeader>>>> + Send;

    // --- Transactions ---

    /// Get a transaction by specifier, with block confirmation metadata.
    fn get_transaction(
        &self,
        spec: TransactionSpecifier,
    ) -> impl Future<Output = ColdResult<Option<Confirmed<RecoveredTx>>>> + Send;

    /// Get all transactions in a block.
    fn get_transactions_in_block(
        &self,
        block: BlockNumber,
    ) -> impl Future<Output = ColdResult<Vec<RecoveredTx>>> + Send;

    /// Get the number of transactions in a block.
    fn get_transaction_count(
        &self,
        block: BlockNumber,
    ) -> impl Future<Output = ColdResult<u64>> + Send;

    // --- Receipts ---

    /// Get a receipt by specifier.
    fn get_receipt(
        &self,
        spec: ReceiptSpecifier,
    ) -> impl Future<Output = ColdResult<Option<ColdReceipt>>> + Send;

    /// Get all receipts in a block.
    fn get_receipts_in_block(
        &self,
        block: BlockNumber,
    ) -> impl Future<Output = ColdResult<Vec<ColdReceipt>>> + Send;

    // --- SignetEvents ---

    /// Get signet events by specifier.
    fn get_signet_events(
        &self,
        spec: SignetEventsSpecifier,
    ) -> impl Future<Output = ColdResult<Vec<DbSignetEvent>>> + Send;

    // --- ZenithHeaders ---

    /// Get a zenith header by specifier.
    fn get_zenith_header(
        &self,
        spec: ZenithHeaderSpecifier,
    ) -> impl Future<Output = ColdResult<Option<DbZenithHeader>>> + Send;

    /// Get multiple zenith headers by specifier.
    fn get_zenith_headers(
        &self,
        spec: ZenithHeaderSpecifier,
    ) -> impl Future<Output = ColdResult<Vec<DbZenithHeader>>> + Send;

    // --- Metadata ---

    /// Get the latest block number in storage.
    fn get_latest_block(&self) -> impl Future<Output = ColdResult<Option<BlockNumber>>> + Send;

    // --- Logs ---

    /// Filter logs by block range, address, and topics.
    ///
    /// Follows `eth_getLogs` semantics: returns all logs matching the
    /// filter criteria, ordered by `(block_number, tx_index, log_index)`.
    ///
    /// # Errors
    ///
    /// Returns [`ColdStorageError::TooManyLogs`] if the query would produce
    /// more than `max_logs` results. No partial results are returned — the
    /// caller must narrow the filter or increase the limit.
    ///
    /// [`ColdStorageError::TooManyLogs`]: crate::ColdStorageError::TooManyLogs
    fn get_logs(
        &self,
        filter: Filter,
        max_logs: usize,
    ) -> impl Future<Output = ColdResult<Vec<RpcLog>>> + Send;

    /// Get the hash of a block header by block number.
    ///
    /// Returns `Ok(Some(hash))` if the block exists, `Ok(None)` if not.
    /// Used by the streaming task for reorg detection.
    fn get_block_hash(
        &self,
        block: BlockNumber,
    ) -> impl Future<Output = ColdResult<Option<B256>>> + Send;

    /// Fetch matching logs from a single block.
    ///
    /// Returns logs ordered by `(tx_index, log_index)` within the block.
    /// `remaining` is the maximum number of logs to return.
    ///
    /// # Errors
    ///
    /// Returns [`ColdStorageError::TooManyLogs`] if the block contains
    /// more matching logs than `remaining`.
    ///
    /// [`ColdStorageError::TooManyLogs`]: crate::ColdStorageError::TooManyLogs
    fn get_logs_block(
        &self,
        filter: &Filter,
        block_num: BlockNumber,
        remaining: usize,
    ) -> impl Future<Output = ColdResult<Vec<RpcLog>>> + Send;

    // --- Streaming ---

    /// Produce a log stream by iterating blocks and sending matching logs.
    ///
    /// Implementations should hold a consistent read snapshot for the
    /// duration when possible — backends with snapshot semantics (MDBX,
    /// PostgreSQL with REPEATABLE READ) need no additional reorg detection.
    ///
    /// The default implementation calls [`produce_log_stream_default`],
    /// which uses per-block [`get_block_hash`] checks for reorg detection.
    /// Backends that hold a single transaction should override for
    /// efficiency and consistency.
    ///
    /// All errors are sent through `sender`. When this method returns,
    /// the sender is dropped, closing the stream.
    ///
    /// [`get_block_hash`]: ColdStorage::get_block_hash
    /// [`produce_log_stream_default`]: crate::produce_log_stream_default
    #[allow(clippy::too_many_arguments)]
    fn produce_log_stream(
        &self,
        filter: &Filter,
        from: BlockNumber,
        to: BlockNumber,
        max_logs: usize,
        sender: mpsc::Sender<ColdResult<RpcLog>>,
        deadline: tokio::time::Instant,
    ) -> impl Future<Output = ()> + Send {
        produce_log_stream_default(self, filter, from, to, max_logs, sender, deadline)
    }

    // --- Write operations ---

    /// Append a single block to cold storage.
    fn append_block(&self, data: BlockData) -> impl Future<Output = ColdResult<()>> + Send;

    /// Append multiple blocks to cold storage.
    fn append_blocks(&self, data: Vec<BlockData>) -> impl Future<Output = ColdResult<()>> + Send;

    /// Truncate all data above the given block number (exclusive).
    ///
    /// This removes block N+1 and higher from all tables. Used for reorg handling.
    fn truncate_above(&self, block: BlockNumber) -> impl Future<Output = ColdResult<()>> + Send;
}

/// Default log-streaming implementation for backends without snapshot
/// semantics.
///
/// Captures an anchor hash from the `to` block at the start and
/// re-checks it before each block to detect reorgs. Calls
/// [`ColdStorage::get_logs_block`] per block.
///
/// Backends that hold a consistent read snapshot (MDBX, PostgreSQL
/// with REPEATABLE READ) should override
/// [`ColdStorage::produce_log_stream`] instead of using this function.
#[allow(clippy::too_many_arguments)]
pub async fn produce_log_stream_default<B: ColdStorage + ?Sized>(
    backend: &B,
    filter: &Filter,
    from: BlockNumber,
    to: BlockNumber,
    max_logs: usize,
    sender: mpsc::Sender<ColdResult<RpcLog>>,
    deadline: tokio::time::Instant,
) {
    // Capture anchor hash for reorg detection.
    let anchor_hash = match backend.get_block_hash(to).await {
        Ok(Some(h)) => h,
        Ok(None) => return, // block doesn't exist; empty stream
        Err(e) => {
            let _ = sender.send(Err(e)).await;
            return;
        }
    };

    let mut total = 0usize;

    for block_num in from..=to {
        if tokio::time::Instant::now() > deadline {
            let _ = sender.send(Err(ColdStorageError::StreamDeadlineExceeded)).await;
            return;
        }

        // Reorg detection: verify anchor block hash unchanged.
        match backend.get_block_hash(to).await {
            Ok(Some(h)) if h == anchor_hash => {}
            Ok(_) => {
                let _ = sender.send(Err(ColdStorageError::ReorgDetected)).await;
                return;
            }
            Err(e) => {
                let _ = sender.send(Err(e)).await;
                return;
            }
        }

        let remaining = max_logs.saturating_sub(total);
        let block_logs = match backend.get_logs_block(filter, block_num, remaining).await {
            Ok(logs) => logs,
            Err(e) => {
                let _ = sender.send(Err(e)).await;
                return;
            }
        };

        total += block_logs.len();

        for log in block_logs {
            match tokio::time::timeout_at(deadline, sender.send(Ok(log))).await {
                Ok(Ok(())) => {}
                Ok(Err(_)) => return, // receiver dropped
                Err(_) => {
                    let _ = sender.send(Err(ColdStorageError::StreamDeadlineExceeded)).await;
                    return;
                }
            }
        }

        if total >= max_logs {
            return;
        }
    }
}
