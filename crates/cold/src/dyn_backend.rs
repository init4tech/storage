//! Object-safe mirror of [`ColdStorageBackend`].
//!
//! [`DynColdStorageBackend`] re-declares every method on
//! [`ColdStorageRead`], [`ColdStorageWrite`], and [`ColdStorageBackend`]
//! with an explicit `Pin<Box<dyn Future + Send + 'a>>` return type so
//! the trait is object-safe. A blanket impl auto-implements it for
//! every `B: ColdStorageBackend`, and `Arc<dyn DynColdStorageBackend>`
//! re-implements the strong traits by delegating to the boxed methods.
//!
//! # Plumbing, Not API
//!
//! This trait exists so [`ColdStorage`]'s default type parameter
//! (`Arc<dyn DynColdStorageBackend>`) is nameable in error messages
//! and downstream signatures. Backends should implement
//! [`ColdStorageBackend`] — the blanket impl handles this trait.
//!
//! [`ColdStorage`]: crate::ColdStorage
//! [`ColdStorageBackend`]: crate::ColdStorageBackend
//! [`ColdStorageRead`]: crate::ColdStorageRead
//! [`ColdStorageWrite`]: crate::ColdStorageWrite

use crate::{
    BlockData, ColdReceipt, ColdResult, Confirmed, Filter, HeaderSpecifier, ReceiptSpecifier,
    RpcLog, SignetEventsSpecifier, StreamParams, TransactionSpecifier, ZenithHeaderSpecifier,
};
use alloy::primitives::BlockNumber;
use signet_storage_types::{DbSignetEvent, DbZenithHeader, RecoveredTx, SealedHeader};
use std::{future::Future, pin::Pin, sync::Arc, time::Duration};

/// Object-safe mirror of [`ColdStorageBackend`]. Auto-implemented by a
/// blanket impl over every `B: ColdStorageBackend`; do not implement
/// directly.
///
/// [`ColdStorageBackend`]: crate::ColdStorageBackend
#[allow(clippy::type_complexity)]
pub trait DynColdStorageBackend: Send + Sync + 'static {
    /// Get a header by specifier.
    fn dyn_get_header<'a>(
        &'a self,
        spec: HeaderSpecifier,
    ) -> Pin<Box<dyn Future<Output = ColdResult<Option<SealedHeader>>> + Send + 'a>>;

    /// Get multiple headers by specifiers.
    fn dyn_get_headers<'a>(
        &'a self,
        specs: Vec<HeaderSpecifier>,
    ) -> Pin<Box<dyn Future<Output = ColdResult<Vec<Option<SealedHeader>>>> + Send + 'a>>;

    /// Get a transaction by specifier, with block confirmation metadata.
    fn dyn_get_transaction<'a>(
        &'a self,
        spec: TransactionSpecifier,
    ) -> Pin<Box<dyn Future<Output = ColdResult<Option<Confirmed<RecoveredTx>>>> + Send + 'a>>;

    /// Get all transactions in a block.
    fn dyn_get_transactions_in_block<'a>(
        &'a self,
        block: BlockNumber,
    ) -> Pin<Box<dyn Future<Output = ColdResult<Vec<RecoveredTx>>> + Send + 'a>>;

    /// Get the number of transactions in a block.
    fn dyn_get_transaction_count<'a>(
        &'a self,
        block: BlockNumber,
    ) -> Pin<Box<dyn Future<Output = ColdResult<u64>> + Send + 'a>>;

    /// Get a receipt by specifier.
    fn dyn_get_receipt<'a>(
        &'a self,
        spec: ReceiptSpecifier,
    ) -> Pin<Box<dyn Future<Output = ColdResult<Option<ColdReceipt>>> + Send + 'a>>;

    /// Get all receipts in a block.
    fn dyn_get_receipts_in_block<'a>(
        &'a self,
        block: BlockNumber,
    ) -> Pin<Box<dyn Future<Output = ColdResult<Vec<ColdReceipt>>> + Send + 'a>>;

    /// Get signet events by specifier.
    fn dyn_get_signet_events<'a>(
        &'a self,
        spec: SignetEventsSpecifier,
    ) -> Pin<Box<dyn Future<Output = ColdResult<Vec<DbSignetEvent>>> + Send + 'a>>;

    /// Get a zenith header by specifier.
    fn dyn_get_zenith_header<'a>(
        &'a self,
        spec: ZenithHeaderSpecifier,
    ) -> Pin<Box<dyn Future<Output = ColdResult<Option<DbZenithHeader>>> + Send + 'a>>;

    /// Get multiple zenith headers by specifier.
    fn dyn_get_zenith_headers<'a>(
        &'a self,
        spec: ZenithHeaderSpecifier,
    ) -> Pin<Box<dyn Future<Output = ColdResult<Vec<DbZenithHeader>>> + Send + 'a>>;

    /// Get the latest block number in storage.
    fn dyn_get_latest_block<'a>(
        &'a self,
    ) -> Pin<Box<dyn Future<Output = ColdResult<Option<BlockNumber>>> + Send + 'a>>;

    /// Filter logs by block range, address, and topics.
    fn dyn_get_logs<'a>(
        &'a self,
        filter: &'a Filter,
        max_logs: usize,
    ) -> Pin<Box<dyn Future<Output = ColdResult<Vec<RpcLog>>> + Send + 'a>>;

    /// Produce a log stream by iterating blocks and sending matching logs.
    fn dyn_produce_log_stream<'a>(
        &'a self,
        filter: &'a Filter,
        params: StreamParams,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>>;

    /// Append a single block to cold storage.
    fn dyn_append_block<'a>(
        &'a self,
        data: BlockData,
    ) -> Pin<Box<dyn Future<Output = ColdResult<()>> + Send + 'a>>;

    /// Append multiple blocks to cold storage.
    fn dyn_append_blocks<'a>(
        &'a self,
        data: Vec<BlockData>,
    ) -> Pin<Box<dyn Future<Output = ColdResult<()>> + Send + 'a>>;

    /// Truncate all data above the given block number (exclusive).
    fn dyn_truncate_above<'a>(
        &'a self,
        block: BlockNumber,
    ) -> Pin<Box<dyn Future<Output = ColdResult<()>> + Send + 'a>>;

    /// Read and remove all blocks above the given block number.
    fn dyn_drain_above<'a>(
        &'a self,
        block: BlockNumber,
    ) -> Pin<Box<dyn Future<Output = ColdResult<Vec<Vec<ColdReceipt>>>> + Send + 'a>>;

    /// Configured read deadline, if any.
    fn dyn_read_timeout(&self) -> Option<Duration>;

    /// Configured write deadline, if any.
    fn dyn_write_timeout(&self) -> Option<Duration>;
}

// Sanity check: ensure the trait is object-safe. The line below fails
// to compile if any method violates object-safety.
const _: fn() = || {
    fn _assert_object_safe(_: &dyn DynColdStorageBackend) {}
};

// Suppress unused-import warnings until later tasks consume Arc.
const _: fn() = || {
    let _: Option<Arc<dyn DynColdStorageBackend>> = None;
};
