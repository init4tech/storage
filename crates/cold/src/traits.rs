//! Core trait definitions for cold storage backends.
//!
//! The cold storage interface is split into three traits:
//!
//! - [`ColdStorageRead`] — read-only access (`&self`, `Clone`)
//! - [`ColdStorageWrite`] — write access (`&mut self`)
//! - [`ColdStorage`] — supertrait combining both, with `drain_above`

use crate::{
    ColdReceipt, ColdResult, Confirmed, Filter, HeaderSpecifier, ReceiptSpecifier, RpcLog,
    SignetEventsSpecifier, StreamParams, TransactionSpecifier, ZenithHeaderSpecifier,
};
use alloy::primitives::BlockNumber;
use signet_storage_types::{
    DbSignetEvent, DbZenithHeader, ExecutedBlock, Receipt, RecoveredTx, SealedHeader,
};
use std::future::Future;
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

/// Read-only cold storage backend trait.
///
/// All methods take `&self` and return `Send` futures. Implementations
/// must be `Clone + Send + Sync + 'static` so that read backends can be
/// shared across tasks (e.g. via `Arc` or cheap cloning).
///
/// # Implementation Guide
///
/// Implementers must ensure:
///
/// - **Consistent reads**: Read operations should return consistent
///   snapshots. A read started before a write completes should not see
///   partial data from that write.
pub trait ColdStorageRead: Clone + Send + Sync + 'static {
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
        filter: &Filter,
        max_logs: usize,
    ) -> impl Future<Output = ColdResult<Vec<RpcLog>>> + Send;

    // --- Streaming ---

    /// Produce a log stream by iterating blocks and sending matching logs.
    ///
    /// # Concurrency
    ///
    /// Stream producers run concurrently with writes (`append_block`,
    /// `truncate_above`, `drain_above`). They are NOT serialized by the
    /// task runner's read/write barrier. Implementations MUST hold a
    /// consistent read snapshot for the duration of the stream.
    ///
    /// Backends with snapshot semantics (MDBX read transactions,
    /// PostgreSQL `REPEATABLE READ`) naturally satisfy this requirement.
    ///
    /// Backends without snapshot semantics can delegate to
    /// [`produce_log_stream_default`], which uses per-block
    /// [`get_header`] / [`get_logs`] calls with anchor-hash reorg
    /// detection. This provides best-effort consistency but is not
    /// immune to partial reads during concurrent writes.
    ///
    /// All errors are sent through `sender`. When this method returns,
    /// the sender is dropped, closing the stream.
    ///
    /// [`get_header`]: ColdStorageRead::get_header
    /// [`get_logs`]: ColdStorageRead::get_logs
    /// [`produce_log_stream_default`]: crate::produce_log_stream_default
    fn produce_log_stream(
        &self,
        filter: &Filter,
        params: StreamParams,
    ) -> impl Future<Output = ()> + Send;
}

/// Write-only cold storage backend trait.
///
/// All methods take `&mut self` and return `Send` futures. The write
/// backend is exclusively owned by the task runner — no synchronization
/// is needed.
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
pub trait ColdStorageWrite: Send + 'static {
    /// Append a single block to cold storage.
    fn append_block(&mut self, data: BlockData) -> impl Future<Output = ColdResult<()>> + Send;

    /// Append multiple blocks to cold storage.
    fn append_blocks(
        &mut self,
        data: Vec<BlockData>,
    ) -> impl Future<Output = ColdResult<()>> + Send;

    /// Truncate all data above the given block number (exclusive).
    ///
    /// This removes block N+1 and higher from all tables. Used for reorg handling.
    fn truncate_above(&mut self, block: BlockNumber)
    -> impl Future<Output = ColdResult<()>> + Send;
}

/// Combined read and write cold storage backend trait.
///
/// Combines [`ColdStorageRead`] and [`ColdStorageWrite`] and provides
/// [`drain_above`](ColdStorage::drain_above), which reads receipts then
/// truncates. The default implementation is correct but not atomic;
/// backends should override with an atomic version when possible.
pub trait ColdStorage: ColdStorageRead + ColdStorageWrite {
    /// Read and remove all blocks above the given block number.
    ///
    /// Returns receipts for each block above `block` in ascending order,
    /// then truncates. Index 0 = block+1, index 1 = block+2, etc.
    /// Blocks with no receipts have empty vecs.
    ///
    /// The default implementation composes `get_latest_block` +
    /// `get_receipts_in_block` + `truncate_above`. It is correct but
    /// not atomic. Backends should override with an atomic version
    /// when possible.
    fn drain_above(
        &mut self,
        block: BlockNumber,
    ) -> impl Future<Output = ColdResult<Vec<Vec<ColdReceipt>>>> + Send {
        async move {
            let mut all_receipts = Vec::new();
            if let Some(latest) = self.get_latest_block().await? {
                for n in (block + 1)..=latest {
                    all_receipts.push(self.get_receipts_in_block(n).await?);
                }
            }
            self.truncate_above(block).await?;
            Ok(all_receipts)
        }
    }
}
