//! Core trait definition for cold storage backends.
//!
//! The [`ColdStorage`] trait defines the interface that all cold storage
//! backends must implement. Backends are responsible for data organization,
//! indexing, and keying - the trait is agnostic to these implementation details.

use alloy::{consensus::Header, primitives::BlockNumber};
use signet_storage_types::{
    DbSignetEvent, DbZenithHeader, ExecutedBlock, IndexedReceipt, Receipt, TransactionSigned,
};
use std::future::Future;

use super::{
    ColdResult, Confirmed, Filter, HeaderSpecifier, ReceiptSpecifier, RpcLog,
    SignetEventsSpecifier, TransactionSpecifier, ZenithHeaderSpecifier,
};

/// Data for appending a complete block to cold storage.
#[derive(Debug, Clone)]
pub struct BlockData {
    /// The block header.
    pub header: Header,
    /// The transactions in the block.
    pub transactions: Vec<TransactionSigned>,
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
        header: Header,
        transactions: Vec<TransactionSigned>,
        receipts: Vec<Receipt>,
        signet_events: Vec<DbSignetEvent>,
        zenith_header: Option<DbZenithHeader>,
    ) -> Self {
        Self { header, transactions, receipts, signet_events, zenith_header }
    }

    /// Get the block number of the block.
    pub const fn block_number(&self) -> BlockNumber {
        self.header.number
    }
}

/// All data needed to build a complete RPC receipt response.
///
/// Bundles a [`Confirmed`] receipt with its transaction, block header,
/// the prior cumulative gas (needed to compute per-tx `gas_used`),
/// and the index of this receipt's first log among all logs in the block
/// (needed for `logIndex` in RPC responses).
#[derive(Debug, Clone)]
pub struct ReceiptContext {
    /// The block header.
    pub header: Header,
    /// The transaction that produced this receipt.
    pub transaction: TransactionSigned,
    /// The receipt with block confirmation metadata.
    pub receipt: Confirmed<Receipt>,
    /// Cumulative gas used by all preceding transactions in the block.
    /// Zero for the first transaction.
    pub prior_cumulative_gas: u64,
    /// Index of this receipt's first log among all logs in the block.
    /// Equal to the sum of log counts from all preceding receipts.
    pub first_log_index: u64,
}

impl ReceiptContext {
    /// Create a new receipt context.
    pub const fn new(
        header: Header,
        transaction: TransactionSigned,
        receipt: Confirmed<Receipt>,
        prior_cumulative_gas: u64,
        first_log_index: u64,
    ) -> Self {
        Self { header, transaction, receipt, prior_cumulative_gas, first_log_index }
    }
}

impl From<ExecutedBlock> for BlockData {
    fn from(block: ExecutedBlock) -> Self {
        Self::new(
            block.header.into_inner(),
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
    ) -> impl Future<Output = ColdResult<Option<Header>>> + Send;

    /// Get multiple headers by specifiers.
    fn get_headers(
        &self,
        specs: Vec<HeaderSpecifier>,
    ) -> impl Future<Output = ColdResult<Vec<Option<Header>>>> + Send;

    // --- Transactions ---

    /// Get a transaction by specifier, with block confirmation metadata.
    fn get_transaction(
        &self,
        spec: TransactionSpecifier,
    ) -> impl Future<Output = ColdResult<Option<Confirmed<TransactionSigned>>>> + Send;

    /// Get all transactions in a block.
    fn get_transactions_in_block(
        &self,
        block: BlockNumber,
    ) -> impl Future<Output = ColdResult<Vec<TransactionSigned>>> + Send;

    /// Get the number of transactions in a block.
    fn get_transaction_count(
        &self,
        block: BlockNumber,
    ) -> impl Future<Output = ColdResult<u64>> + Send;

    // --- Receipts ---

    /// Get a receipt by specifier, with block confirmation metadata.
    fn get_receipt(
        &self,
        spec: ReceiptSpecifier,
    ) -> impl Future<Output = ColdResult<Option<Confirmed<Receipt>>>> + Send;

    /// Get all receipts in a block, with precomputed metadata.
    fn get_receipts_in_block(
        &self,
        block: BlockNumber,
    ) -> impl Future<Output = ColdResult<Vec<IndexedReceipt>>> + Send;

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
    /// filter criteria, ordered by (block_number, tx_index, log_index).
    fn get_logs(&self, filter: Filter) -> impl Future<Output = ColdResult<Vec<RpcLog>>> + Send;

    // --- Composite queries ---

    /// Get a receipt with all context needed for RPC responses.
    ///
    /// Returns the receipt, its transaction, the block header, confirmation
    /// metadata, the cumulative gas used by preceding transactions, and the
    /// index of this receipt's first log among all logs in the block.
    /// Returns `None` if the receipt does not exist.
    ///
    /// The default implementation composes existing trait methods. Backends
    /// that can serve this more efficiently (e.g., in a single transaction)
    /// should override.
    fn get_receipt_with_context(
        &self,
        spec: ReceiptSpecifier,
    ) -> impl Future<Output = ColdResult<Option<ReceiptContext>>> + Send {
        async move {
            let Some(receipt) = self.get_receipt(spec).await? else {
                return Ok(None);
            };
            let block = receipt.meta().block_number();
            let index = receipt.meta().transaction_index();

            let Some(header) = self.get_header(HeaderSpecifier::Number(block)).await? else {
                return Ok(None);
            };
            let Some(tx) =
                self.get_transaction(TransactionSpecifier::BlockAndIndex { block, index }).await?
            else {
                return Ok(None);
            };

            let receipts = self.get_receipts_in_block(block).await?;
            let idx = index as usize;
            let first_log_index = receipts.get(idx).map_or(0, |r| r.first_log_index);
            let prior_cumulative_gas = idx
                .checked_sub(1)
                .and_then(|i| receipts.get(i))
                .map_or(0, |r| r.receipt.inner.cumulative_gas_used);

            Ok(Some(ReceiptContext::new(
                header,
                tx.into_inner(),
                receipt,
                prior_cumulative_gas,
                first_log_index,
            )))
        }
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
