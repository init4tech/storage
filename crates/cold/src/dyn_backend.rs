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
//! # Filter Cloning on the Erased Path
//!
//! The [`ColdStorageRead`] impl for `Arc<dyn DynColdStorageBackend>`
//! clones the [`Filter`](crate::Filter) inside `get_logs` and
//! `produce_log_stream`. The dyn methods unify `&self` and `&Filter`
//! into a single lifetime, which cannot be expressed by the
//! independent-lifetime trait signatures without an owned bridge. The
//! concrete `ColdStorage<B>` path is unaffected.
//!
//! # Maintainer Note: Recursion Hazard for Borrowed Arguments
//!
//! Any method on the `Arc<dyn DynColdStorageBackend>` impls that
//! cannot use the direct `(**self).dyn_<name>(...)` form (because a
//! borrowed argument forces it through a `self.clone()` + `async move`
//! bridge) MUST dispatch via qualified path on the inner trait object,
//! e.g. `DynColdStorageBackend::dyn_<name>(this.as_ref(), ...)`.
//!
//! Writing `this.dyn_<name>(...)` on a cloned `Arc<dyn ...>` resolves
//! to the blanket impl (`Arc<dyn ...>: ColdStorageBackend` ⇒
//! `Arc<dyn ...>: DynColdStorageBackend`), which calls back into the
//! strong-trait impl and recurses infinitely. See `get_logs` and
//! `produce_log_stream` for the canonical pattern.
//!
//! [`ColdStorage`]: crate::ColdStorage
//! [`ColdStorageBackend`]: crate::ColdStorageBackend
//! [`ColdStorageRead`]: crate::ColdStorageRead
//! [`ColdStorageWrite`]: crate::ColdStorageWrite

use crate::{
    BlockData, ColdReceipt, ColdResult, ColdStorageBackend, ColdStorageRead, ColdStorageWrite,
    Confirmed, Filter, HeaderSpecifier, ReceiptSpecifier, RpcLog, SignetEventsSpecifier,
    StreamParams, TransactionSpecifier, ZenithHeaderSpecifier,
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

impl<B: ColdStorageBackend> DynColdStorageBackend for B {
    fn dyn_get_header<'a>(
        &'a self,
        spec: HeaderSpecifier,
    ) -> Pin<Box<dyn Future<Output = ColdResult<Option<SealedHeader>>> + Send + 'a>> {
        Box::pin(<B as ColdStorageRead>::get_header(self, spec))
    }

    fn dyn_get_headers<'a>(
        &'a self,
        specs: Vec<HeaderSpecifier>,
    ) -> Pin<Box<dyn Future<Output = ColdResult<Vec<Option<SealedHeader>>>> + Send + 'a>> {
        Box::pin(<B as ColdStorageRead>::get_headers(self, specs))
    }

    fn dyn_get_transaction<'a>(
        &'a self,
        spec: TransactionSpecifier,
    ) -> Pin<Box<dyn Future<Output = ColdResult<Option<Confirmed<RecoveredTx>>>> + Send + 'a>> {
        Box::pin(<B as ColdStorageRead>::get_transaction(self, spec))
    }

    fn dyn_get_transactions_in_block<'a>(
        &'a self,
        block: BlockNumber,
    ) -> Pin<Box<dyn Future<Output = ColdResult<Vec<RecoveredTx>>> + Send + 'a>> {
        Box::pin(<B as ColdStorageRead>::get_transactions_in_block(self, block))
    }

    fn dyn_get_transaction_count<'a>(
        &'a self,
        block: BlockNumber,
    ) -> Pin<Box<dyn Future<Output = ColdResult<u64>> + Send + 'a>> {
        Box::pin(<B as ColdStorageRead>::get_transaction_count(self, block))
    }

    fn dyn_get_receipt<'a>(
        &'a self,
        spec: ReceiptSpecifier,
    ) -> Pin<Box<dyn Future<Output = ColdResult<Option<ColdReceipt>>> + Send + 'a>> {
        Box::pin(<B as ColdStorageRead>::get_receipt(self, spec))
    }

    fn dyn_get_receipts_in_block<'a>(
        &'a self,
        block: BlockNumber,
    ) -> Pin<Box<dyn Future<Output = ColdResult<Vec<ColdReceipt>>> + Send + 'a>> {
        Box::pin(<B as ColdStorageRead>::get_receipts_in_block(self, block))
    }

    fn dyn_get_signet_events<'a>(
        &'a self,
        spec: SignetEventsSpecifier,
    ) -> Pin<Box<dyn Future<Output = ColdResult<Vec<DbSignetEvent>>> + Send + 'a>> {
        Box::pin(<B as ColdStorageRead>::get_signet_events(self, spec))
    }

    fn dyn_get_zenith_header<'a>(
        &'a self,
        spec: ZenithHeaderSpecifier,
    ) -> Pin<Box<dyn Future<Output = ColdResult<Option<DbZenithHeader>>> + Send + 'a>> {
        Box::pin(<B as ColdStorageRead>::get_zenith_header(self, spec))
    }

    fn dyn_get_zenith_headers<'a>(
        &'a self,
        spec: ZenithHeaderSpecifier,
    ) -> Pin<Box<dyn Future<Output = ColdResult<Vec<DbZenithHeader>>> + Send + 'a>> {
        Box::pin(<B as ColdStorageRead>::get_zenith_headers(self, spec))
    }

    fn dyn_get_latest_block<'a>(
        &'a self,
    ) -> Pin<Box<dyn Future<Output = ColdResult<Option<BlockNumber>>> + Send + 'a>> {
        Box::pin(<B as ColdStorageRead>::get_latest_block(self))
    }

    fn dyn_get_logs<'a>(
        &'a self,
        filter: &'a Filter,
        max_logs: usize,
    ) -> Pin<Box<dyn Future<Output = ColdResult<Vec<RpcLog>>> + Send + 'a>> {
        Box::pin(<B as ColdStorageRead>::get_logs(self, filter, max_logs))
    }

    fn dyn_produce_log_stream<'a>(
        &'a self,
        filter: &'a Filter,
        params: StreamParams,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(<B as ColdStorageRead>::produce_log_stream(self, filter, params))
    }

    fn dyn_append_block<'a>(
        &'a self,
        data: BlockData,
    ) -> Pin<Box<dyn Future<Output = ColdResult<()>> + Send + 'a>> {
        Box::pin(<B as ColdStorageWrite>::append_block(self, data))
    }

    fn dyn_append_blocks<'a>(
        &'a self,
        data: Vec<BlockData>,
    ) -> Pin<Box<dyn Future<Output = ColdResult<()>> + Send + 'a>> {
        Box::pin(<B as ColdStorageWrite>::append_blocks(self, data))
    }

    fn dyn_truncate_above<'a>(
        &'a self,
        block: BlockNumber,
    ) -> Pin<Box<dyn Future<Output = ColdResult<()>> + Send + 'a>> {
        Box::pin(<B as ColdStorageWrite>::truncate_above(self, block))
    }

    fn dyn_drain_above<'a>(
        &'a self,
        block: BlockNumber,
    ) -> Pin<Box<dyn Future<Output = ColdResult<Vec<Vec<ColdReceipt>>>> + Send + 'a>> {
        Box::pin(<B as ColdStorageBackend>::drain_above(self, block))
    }

    fn dyn_read_timeout(&self) -> Option<Duration> {
        <B as ColdStorageBackend>::read_timeout(self)
    }

    fn dyn_write_timeout(&self) -> Option<Duration> {
        <B as ColdStorageBackend>::write_timeout(self)
    }
}

// Compile-time check that the trait is object-safe.
const _: fn() = || {
    fn _assert_object_safe(_: &dyn DynColdStorageBackend) {}
};

impl ColdStorageRead for Arc<dyn DynColdStorageBackend> {
    fn get_header(
        &self,
        spec: HeaderSpecifier,
    ) -> impl Future<Output = ColdResult<Option<SealedHeader>>> + Send {
        (**self).dyn_get_header(spec)
    }

    fn get_headers(
        &self,
        specs: Vec<HeaderSpecifier>,
    ) -> impl Future<Output = ColdResult<Vec<Option<SealedHeader>>>> + Send {
        (**self).dyn_get_headers(specs)
    }

    fn get_transaction(
        &self,
        spec: TransactionSpecifier,
    ) -> impl Future<Output = ColdResult<Option<Confirmed<RecoveredTx>>>> + Send {
        (**self).dyn_get_transaction(spec)
    }

    fn get_transactions_in_block(
        &self,
        block: BlockNumber,
    ) -> impl Future<Output = ColdResult<Vec<RecoveredTx>>> + Send {
        (**self).dyn_get_transactions_in_block(block)
    }

    fn get_transaction_count(
        &self,
        block: BlockNumber,
    ) -> impl Future<Output = ColdResult<u64>> + Send {
        (**self).dyn_get_transaction_count(block)
    }

    fn get_receipt(
        &self,
        spec: ReceiptSpecifier,
    ) -> impl Future<Output = ColdResult<Option<ColdReceipt>>> + Send {
        (**self).dyn_get_receipt(spec)
    }

    fn get_receipts_in_block(
        &self,
        block: BlockNumber,
    ) -> impl Future<Output = ColdResult<Vec<ColdReceipt>>> + Send {
        (**self).dyn_get_receipts_in_block(block)
    }

    fn get_signet_events(
        &self,
        spec: SignetEventsSpecifier,
    ) -> impl Future<Output = ColdResult<Vec<DbSignetEvent>>> + Send {
        (**self).dyn_get_signet_events(spec)
    }

    fn get_zenith_header(
        &self,
        spec: ZenithHeaderSpecifier,
    ) -> impl Future<Output = ColdResult<Option<DbZenithHeader>>> + Send {
        (**self).dyn_get_zenith_header(spec)
    }

    fn get_zenith_headers(
        &self,
        spec: ZenithHeaderSpecifier,
    ) -> impl Future<Output = ColdResult<Vec<DbZenithHeader>>> + Send {
        (**self).dyn_get_zenith_headers(spec)
    }

    fn get_latest_block(&self) -> impl Future<Output = ColdResult<Option<BlockNumber>>> + Send {
        (**self).dyn_get_latest_block()
    }

    fn get_logs(
        &self,
        filter: &Filter,
        max_logs: usize,
    ) -> impl Future<Output = ColdResult<Vec<RpcLog>>> + Send {
        let this = self.clone();
        let filter = filter.clone();
        // Call dyn_get_logs via the inner trait object directly (not through
        // the Arc's blanket DynColdStorageBackend impl), which would re-enter
        // ColdStorageRead::get_logs on Arc<dyn ...> and recurse infinitely.
        async move { DynColdStorageBackend::dyn_get_logs(this.as_ref(), &filter, max_logs).await }
    }

    fn produce_log_stream(
        &self,
        filter: &Filter,
        params: StreamParams,
    ) -> impl Future<Output = ()> + Send {
        let this = self.clone();
        let filter = filter.clone();
        // Same recursion hazard as `get_logs` above — call through
        // `as_ref()` + qualified path so dispatch lands on the inner
        // trait object's vtable, not the Arc's blanket impl.
        async move {
            DynColdStorageBackend::dyn_produce_log_stream(this.as_ref(), &filter, params).await
        }
    }
}

impl ColdStorageWrite for Arc<dyn DynColdStorageBackend> {
    fn append_block(&self, data: BlockData) -> impl Future<Output = ColdResult<()>> + Send {
        (**self).dyn_append_block(data)
    }

    fn append_blocks(&self, data: Vec<BlockData>) -> impl Future<Output = ColdResult<()>> + Send {
        (**self).dyn_append_blocks(data)
    }

    fn truncate_above(&self, block: BlockNumber) -> impl Future<Output = ColdResult<()>> + Send {
        (**self).dyn_truncate_above(block)
    }
}

impl ColdStorageBackend for Arc<dyn DynColdStorageBackend> {
    fn read_timeout(&self) -> Option<Duration> {
        (**self).dyn_read_timeout()
    }

    fn write_timeout(&self) -> Option<Duration> {
        (**self).dyn_write_timeout()
    }

    fn drain_above(
        &self,
        block: BlockNumber,
    ) -> impl Future<Output = ColdResult<Vec<Vec<ColdReceipt>>>> + Send {
        (**self).dyn_drain_above(block)
    }
}

// Compile-time check that `Arc<dyn DynColdStorageBackend>` satisfies the
// bound `ColdStorage` will require.
const _: fn() = || {
    const fn _assert_bound<B: ColdStorageBackend>() {}
    _assert_bound::<Arc<dyn DynColdStorageBackend>>();
};
