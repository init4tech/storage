//! Object-safe mirror of [`ColdStorageBackend`].
//!
//! [`DynColdStorageBackend`] re-declares every method on
//! [`ColdStorageRead`], [`ColdStorageWrite`], and [`ColdStorageBackend`]
//! with an explicit [`StorageFuture`] return type so the trait is
//! object-safe. A blanket impl auto-implements it for every
//! `B: ColdStorageBackend`, and [`ErasedBackend`] re-implements the
//! strong traits by delegating to the boxed methods.
//!
//! # Plumbing, Not API
//!
//! This trait exists so [`ColdStorage`]'s default type parameter
//! ([`ErasedBackend`]) is nameable in error messages and downstream
//! signatures. Backends should implement [`ColdStorageBackend`] — the
//! blanket impl handles this trait.
//!
//! # Why a Newtype, Not a Type Alias
//!
//! [`ErasedBackend`] is a newtype wrapping `Arc<dyn DynColdStorageBackend>`
//! rather than a plain alias. A type alias exposes the dyn trait-object
//! lifetime to trait resolution; when an `ErasedBackend` is captured
//! into a spawned future, rustc invents a fresh `'0` lifetime for the
//! dyn object and asks `for<'0> Arc<dyn DynColdStorageBackend + '0>:
//! ColdStorageRead`. The `'static`-bounded impl does not satisfy this
//! HRTB and downstream `Send` checks fail. A concrete newtype has no
//! dyn lifetime in its surface type, so resolution is trivial.
//!
//! # Filter Cloning on the Erased Path
//!
//! The [`ColdStorageRead`] impl for [`ErasedBackend`] clones the
//! [`Filter`] inside `get_logs` and `produce_log_stream`. The dyn
//! methods unify `&self` and `&Filter` into a single lifetime, which
//! cannot be expressed by the independent-lifetime trait signatures
//! without an owned bridge. The concrete `ColdStorage<B>` path is
//! unaffected.
//!
//! # Maintainer Note: Recursion Hazard for Borrowed Arguments
//!
//! Any method on the [`ErasedBackend`] impls that cannot use the
//! direct `self.0.dyn_<name>(...)` form (because a borrowed argument
//! forces it through a `self.clone()` + `async move` bridge) MUST
//! dispatch via qualified path on the inner trait object, e.g.
//! `DynColdStorageBackend::dyn_<name>(this.0.as_ref(), ...)`.
//!
//! Writing `this.dyn_<name>(...)` on a cloned [`ErasedBackend`]
//! resolves to the blanket impl (`ErasedBackend: ColdStorageBackend`
//! ⇒ `ErasedBackend: DynColdStorageBackend`), which calls back into
//! the strong-trait impl and recurses infinitely. See `get_logs` and
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

/// Boxed, pinned, `Send`-able future returned from object-safe
/// [`DynColdStorageBackend`] methods.
pub type StorageFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Type-erased cold storage backend, shareable across tasks.
///
/// This is the default `B` for [`ColdStorage`](crate::ColdStorage): a
/// handle written as plain `ColdStorage` uses this backend. Construct
/// one with [`ErasedBackend::new`] or
/// [`ColdStorage::new_erased`](crate::ColdStorage::new_erased).
///
/// # Why a Newtype
///
/// Wrapping the `Arc<dyn ...>` in a struct keeps the trait-object
/// lifetime out of the public type signature. See the module-level
/// docs for the HRTB resolution problem this avoids.
pub struct ErasedBackend(Arc<dyn DynColdStorageBackend>);

impl ErasedBackend {
    /// Erase a concrete backend behind `Arc<dyn DynColdStorageBackend>`.
    pub fn new<B: ColdStorageBackend>(backend: B) -> Self {
        Self(Arc::new(backend))
    }

    /// Wrap an existing trait object.
    ///
    /// Prefer [`ErasedBackend::new`] for concrete backends. Use this
    /// only when you already hold an `Arc<dyn DynColdStorageBackend>`,
    /// e.g. when bridging from another type-erased channel.
    pub const fn from_arc(arc: Arc<dyn DynColdStorageBackend>) -> Self {
        Self(arc)
    }

    /// Borrow the inner trait object.
    pub fn as_dyn(&self) -> &(dyn DynColdStorageBackend + 'static) {
        &*self.0
    }

    /// Consume the newtype and return the inner `Arc<dyn ...>`.
    pub fn into_arc(self) -> Arc<dyn DynColdStorageBackend> {
        self.0
    }
}

impl Clone for ErasedBackend {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl std::fmt::Debug for ErasedBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("ErasedBackend").finish()
    }
}

/// Object-safe mirror of [`ColdStorageBackend`]. Auto-implemented by a
/// blanket impl over every `B: ColdStorageBackend`; do not implement
/// directly.
///
/// [`ColdStorageBackend`]: crate::ColdStorageBackend
pub trait DynColdStorageBackend: Send + Sync + 'static {
    /// Get a header by specifier.
    fn dyn_get_header<'a>(
        &'a self,
        spec: HeaderSpecifier,
    ) -> StorageFuture<'a, ColdResult<Option<SealedHeader>>>;

    /// Get multiple headers by specifiers.
    fn dyn_get_headers<'a>(
        &'a self,
        specs: Vec<HeaderSpecifier>,
    ) -> StorageFuture<'a, ColdResult<Vec<Option<SealedHeader>>>>;

    /// Get a transaction by specifier, with block confirmation metadata.
    fn dyn_get_transaction<'a>(
        &'a self,
        spec: TransactionSpecifier,
    ) -> StorageFuture<'a, ColdResult<Option<Confirmed<RecoveredTx>>>>;

    /// Get all transactions in a block.
    fn dyn_get_transactions_in_block<'a>(
        &'a self,
        block: BlockNumber,
    ) -> StorageFuture<'a, ColdResult<Vec<RecoveredTx>>>;

    /// Get the number of transactions in a block.
    fn dyn_get_transaction_count<'a>(
        &'a self,
        block: BlockNumber,
    ) -> StorageFuture<'a, ColdResult<u64>>;

    /// Get a receipt by specifier.
    fn dyn_get_receipt<'a>(
        &'a self,
        spec: ReceiptSpecifier,
    ) -> StorageFuture<'a, ColdResult<Option<ColdReceipt>>>;

    /// Get all receipts in a block.
    fn dyn_get_receipts_in_block<'a>(
        &'a self,
        block: BlockNumber,
    ) -> StorageFuture<'a, ColdResult<Vec<ColdReceipt>>>;

    /// Get signet events by specifier.
    fn dyn_get_signet_events<'a>(
        &'a self,
        spec: SignetEventsSpecifier,
    ) -> StorageFuture<'a, ColdResult<Vec<DbSignetEvent>>>;

    /// Get a zenith header by specifier.
    fn dyn_get_zenith_header<'a>(
        &'a self,
        spec: ZenithHeaderSpecifier,
    ) -> StorageFuture<'a, ColdResult<Option<DbZenithHeader>>>;

    /// Get multiple zenith headers by specifier.
    fn dyn_get_zenith_headers<'a>(
        &'a self,
        spec: ZenithHeaderSpecifier,
    ) -> StorageFuture<'a, ColdResult<Vec<DbZenithHeader>>>;

    /// Get the latest block number in storage.
    fn dyn_get_latest_block<'a>(&'a self) -> StorageFuture<'a, ColdResult<Option<BlockNumber>>>;

    /// Filter logs by block range, address, and topics.
    fn dyn_get_logs<'a>(
        &'a self,
        filter: &'a Filter,
        max_logs: usize,
    ) -> StorageFuture<'a, ColdResult<Vec<RpcLog>>>;

    /// Produce a log stream by iterating blocks and sending matching logs.
    fn dyn_produce_log_stream<'a>(
        &'a self,
        filter: &'a Filter,
        params: StreamParams,
    ) -> StorageFuture<'a, ()>;

    /// Append a single block to cold storage.
    fn dyn_append_block<'a>(&'a self, data: BlockData) -> StorageFuture<'a, ColdResult<()>>;

    /// Append multiple blocks to cold storage.
    fn dyn_append_blocks<'a>(&'a self, data: Vec<BlockData>) -> StorageFuture<'a, ColdResult<()>>;

    /// Truncate all data above the given block number (exclusive).
    fn dyn_truncate_above<'a>(&'a self, block: BlockNumber) -> StorageFuture<'a, ColdResult<()>>;

    /// Read and remove all blocks above the given block number.
    fn dyn_drain_above<'a>(
        &'a self,
        block: BlockNumber,
    ) -> StorageFuture<'a, ColdResult<Vec<Vec<ColdReceipt>>>>;

    /// Configured read deadline, if any.
    fn dyn_read_timeout(&self) -> Option<Duration>;

    /// Configured write deadline, if any.
    fn dyn_write_timeout(&self) -> Option<Duration>;
}

impl<B: ColdStorageBackend> DynColdStorageBackend for B {
    fn dyn_get_header<'a>(
        &'a self,
        spec: HeaderSpecifier,
    ) -> StorageFuture<'a, ColdResult<Option<SealedHeader>>> {
        Box::pin(<B as ColdStorageRead>::get_header(self, spec))
    }

    fn dyn_get_headers<'a>(
        &'a self,
        specs: Vec<HeaderSpecifier>,
    ) -> StorageFuture<'a, ColdResult<Vec<Option<SealedHeader>>>> {
        Box::pin(<B as ColdStorageRead>::get_headers(self, specs))
    }

    fn dyn_get_transaction<'a>(
        &'a self,
        spec: TransactionSpecifier,
    ) -> StorageFuture<'a, ColdResult<Option<Confirmed<RecoveredTx>>>> {
        Box::pin(<B as ColdStorageRead>::get_transaction(self, spec))
    }

    fn dyn_get_transactions_in_block<'a>(
        &'a self,
        block: BlockNumber,
    ) -> StorageFuture<'a, ColdResult<Vec<RecoveredTx>>> {
        Box::pin(<B as ColdStorageRead>::get_transactions_in_block(self, block))
    }

    fn dyn_get_transaction_count<'a>(
        &'a self,
        block: BlockNumber,
    ) -> StorageFuture<'a, ColdResult<u64>> {
        Box::pin(<B as ColdStorageRead>::get_transaction_count(self, block))
    }

    fn dyn_get_receipt<'a>(
        &'a self,
        spec: ReceiptSpecifier,
    ) -> StorageFuture<'a, ColdResult<Option<ColdReceipt>>> {
        Box::pin(<B as ColdStorageRead>::get_receipt(self, spec))
    }

    fn dyn_get_receipts_in_block<'a>(
        &'a self,
        block: BlockNumber,
    ) -> StorageFuture<'a, ColdResult<Vec<ColdReceipt>>> {
        Box::pin(<B as ColdStorageRead>::get_receipts_in_block(self, block))
    }

    fn dyn_get_signet_events<'a>(
        &'a self,
        spec: SignetEventsSpecifier,
    ) -> StorageFuture<'a, ColdResult<Vec<DbSignetEvent>>> {
        Box::pin(<B as ColdStorageRead>::get_signet_events(self, spec))
    }

    fn dyn_get_zenith_header<'a>(
        &'a self,
        spec: ZenithHeaderSpecifier,
    ) -> StorageFuture<'a, ColdResult<Option<DbZenithHeader>>> {
        Box::pin(<B as ColdStorageRead>::get_zenith_header(self, spec))
    }

    fn dyn_get_zenith_headers<'a>(
        &'a self,
        spec: ZenithHeaderSpecifier,
    ) -> StorageFuture<'a, ColdResult<Vec<DbZenithHeader>>> {
        Box::pin(<B as ColdStorageRead>::get_zenith_headers(self, spec))
    }

    fn dyn_get_latest_block<'a>(&'a self) -> StorageFuture<'a, ColdResult<Option<BlockNumber>>> {
        Box::pin(<B as ColdStorageRead>::get_latest_block(self))
    }

    fn dyn_get_logs<'a>(
        &'a self,
        filter: &'a Filter,
        max_logs: usize,
    ) -> StorageFuture<'a, ColdResult<Vec<RpcLog>>> {
        Box::pin(<B as ColdStorageRead>::get_logs(self, filter, max_logs))
    }

    fn dyn_produce_log_stream<'a>(
        &'a self,
        filter: &'a Filter,
        params: StreamParams,
    ) -> StorageFuture<'a, ()> {
        Box::pin(<B as ColdStorageRead>::produce_log_stream(self, filter, params))
    }

    fn dyn_append_block<'a>(&'a self, data: BlockData) -> StorageFuture<'a, ColdResult<()>> {
        Box::pin(<B as ColdStorageWrite>::append_block(self, data))
    }

    fn dyn_append_blocks<'a>(&'a self, data: Vec<BlockData>) -> StorageFuture<'a, ColdResult<()>> {
        Box::pin(<B as ColdStorageWrite>::append_blocks(self, data))
    }

    fn dyn_truncate_above<'a>(&'a self, block: BlockNumber) -> StorageFuture<'a, ColdResult<()>> {
        Box::pin(<B as ColdStorageWrite>::truncate_above(self, block))
    }

    fn dyn_drain_above<'a>(
        &'a self,
        block: BlockNumber,
    ) -> StorageFuture<'a, ColdResult<Vec<Vec<ColdReceipt>>>> {
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

impl ColdStorageRead for ErasedBackend {
    fn get_header(
        &self,
        spec: HeaderSpecifier,
    ) -> impl Future<Output = ColdResult<Option<SealedHeader>>> + Send {
        self.0.dyn_get_header(spec)
    }

    fn get_headers(
        &self,
        specs: Vec<HeaderSpecifier>,
    ) -> impl Future<Output = ColdResult<Vec<Option<SealedHeader>>>> + Send {
        self.0.dyn_get_headers(specs)
    }

    fn get_transaction(
        &self,
        spec: TransactionSpecifier,
    ) -> impl Future<Output = ColdResult<Option<Confirmed<RecoveredTx>>>> + Send {
        self.0.dyn_get_transaction(spec)
    }

    fn get_transactions_in_block(
        &self,
        block: BlockNumber,
    ) -> impl Future<Output = ColdResult<Vec<RecoveredTx>>> + Send {
        self.0.dyn_get_transactions_in_block(block)
    }

    fn get_transaction_count(
        &self,
        block: BlockNumber,
    ) -> impl Future<Output = ColdResult<u64>> + Send {
        self.0.dyn_get_transaction_count(block)
    }

    fn get_receipt(
        &self,
        spec: ReceiptSpecifier,
    ) -> impl Future<Output = ColdResult<Option<ColdReceipt>>> + Send {
        self.0.dyn_get_receipt(spec)
    }

    fn get_receipts_in_block(
        &self,
        block: BlockNumber,
    ) -> impl Future<Output = ColdResult<Vec<ColdReceipt>>> + Send {
        self.0.dyn_get_receipts_in_block(block)
    }

    fn get_signet_events(
        &self,
        spec: SignetEventsSpecifier,
    ) -> impl Future<Output = ColdResult<Vec<DbSignetEvent>>> + Send {
        self.0.dyn_get_signet_events(spec)
    }

    fn get_zenith_header(
        &self,
        spec: ZenithHeaderSpecifier,
    ) -> impl Future<Output = ColdResult<Option<DbZenithHeader>>> + Send {
        self.0.dyn_get_zenith_header(spec)
    }

    fn get_zenith_headers(
        &self,
        spec: ZenithHeaderSpecifier,
    ) -> impl Future<Output = ColdResult<Vec<DbZenithHeader>>> + Send {
        self.0.dyn_get_zenith_headers(spec)
    }

    fn get_latest_block(&self) -> impl Future<Output = ColdResult<Option<BlockNumber>>> + Send {
        self.0.dyn_get_latest_block()
    }

    fn get_logs(
        &self,
        filter: &Filter,
        max_logs: usize,
    ) -> impl Future<Output = ColdResult<Vec<RpcLog>>> + Send {
        let this = self.clone();
        let filter = filter.clone();
        // Call dyn_get_logs via the inner trait object directly (not through
        // the newtype's blanket DynColdStorageBackend impl), which would
        // re-enter ColdStorageRead::get_logs on ErasedBackend and recurse
        // infinitely.
        async move { DynColdStorageBackend::dyn_get_logs(this.0.as_ref(), &filter, max_logs).await }
    }

    fn produce_log_stream(
        &self,
        filter: &Filter,
        params: StreamParams,
    ) -> impl Future<Output = ()> + Send {
        let this = self.clone();
        let filter = filter.clone();
        // Same recursion hazard as `get_logs` above — call through the
        // inner Arc + qualified path so dispatch lands on the trait
        // object's vtable, not the newtype's blanket impl.
        async move {
            DynColdStorageBackend::dyn_produce_log_stream(this.0.as_ref(), &filter, params).await
        }
    }
}

impl ColdStorageWrite for ErasedBackend {
    fn append_block(&self, data: BlockData) -> impl Future<Output = ColdResult<()>> + Send {
        self.0.dyn_append_block(data)
    }

    fn append_blocks(&self, data: Vec<BlockData>) -> impl Future<Output = ColdResult<()>> + Send {
        self.0.dyn_append_blocks(data)
    }

    fn truncate_above(&self, block: BlockNumber) -> impl Future<Output = ColdResult<()>> + Send {
        self.0.dyn_truncate_above(block)
    }
}

impl ColdStorageBackend for ErasedBackend {
    fn read_timeout(&self) -> Option<Duration> {
        self.0.dyn_read_timeout()
    }

    fn write_timeout(&self) -> Option<Duration> {
        self.0.dyn_write_timeout()
    }

    fn drain_above(
        &self,
        block: BlockNumber,
    ) -> impl Future<Output = ColdResult<Vec<Vec<ColdReceipt>>>> + Send {
        self.0.dyn_drain_above(block)
    }
}

// Compile-time check that `ErasedBackend` satisfies the bound
// `ColdStorage` will require.
const _: fn() = || {
    const fn _assert_bound<B: ColdStorageBackend>() {}
    _assert_bound::<ErasedBackend>();
};
