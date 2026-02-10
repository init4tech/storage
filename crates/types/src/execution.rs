//! Executed block types for unified storage.
//!
//! This module provides the [`ExecutedBlock`] type which contains all data
//! needed by both hot and cold storage systems for a single executed block.

use crate::{DbSignetEvent, DbZenithHeader, Receipt, SealedHeader, TransactionSigned};
use alloy::primitives::BlockNumber;
use trevm::revm::database::BundleState;

/// Complete execution output for a block.
///
/// This type unifies the data requirements of both hot and cold storage:
/// - Hot storage uses `header` and `bundle` for state/history tracking
/// - Cold storage uses all fields for archival storage
///
/// # Example
///
/// ```
/// # use signet_storage_types::{ExecutedBlock, ExecutedBlockBuilder, SealedHeader};
/// # use trevm::revm::database::BundleState;
/// # use alloy::consensus::Header;
/// # fn example(header: SealedHeader, bundle: BundleState) {
/// let block = ExecutedBlockBuilder::new()
///     .header(header)
///     .bundle(bundle)
///     .build();
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct ExecutedBlock {
    /// The sealed block header (contains cached hash).
    pub header: SealedHeader,
    /// The state changes from execution (accounts, storage, bytecode).
    pub bundle: BundleState,
    /// The signed transactions in the block.
    pub transactions: Vec<TransactionSigned>,
    /// The receipts from execution.
    pub receipts: Vec<Receipt>,
    /// Extracted signet events from the block.
    pub signet_events: Vec<DbSignetEvent>,
    /// The zenith header, if present.
    pub zenith_header: Option<DbZenithHeader>,
}

impl ExecutedBlock {
    /// Create a new executed block.
    pub const fn new(
        header: SealedHeader,
        bundle: BundleState,
        transactions: Vec<TransactionSigned>,
        receipts: Vec<Receipt>,
        signet_events: Vec<DbSignetEvent>,
        zenith_header: Option<DbZenithHeader>,
    ) -> Self {
        Self { header, bundle, transactions, receipts, signet_events, zenith_header }
    }

    /// Get the block number.
    pub fn block_number(&self) -> BlockNumber {
        self.header.number
    }

    /// Get a reference to the header.
    pub const fn header(&self) -> &SealedHeader {
        &self.header
    }

    /// Get a reference to the bundle state.
    pub const fn bundle(&self) -> &BundleState {
        &self.bundle
    }
}

/// Builder for [`ExecutedBlock`].
///
/// Use this builder to construct an [`ExecutedBlock`] incrementally.
/// The `header` and `bundle` fields are required; all others default to empty.
#[derive(Debug, Default)]
pub struct ExecutedBlockBuilder {
    header: Option<SealedHeader>,
    bundle: Option<BundleState>,
    transactions: Vec<TransactionSigned>,
    receipts: Vec<Receipt>,
    signet_events: Vec<DbSignetEvent>,
    zenith_header: Option<DbZenithHeader>,
}

impl ExecutedBlockBuilder {
    /// Create a new builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the sealed header (required).
    pub fn header(mut self, header: SealedHeader) -> Self {
        self.header = Some(header);
        self
    }

    /// Set the bundle state (required).
    pub fn bundle(mut self, bundle: BundleState) -> Self {
        self.bundle = Some(bundle);
        self
    }

    /// Set the transactions.
    pub fn transactions(mut self, transactions: Vec<TransactionSigned>) -> Self {
        self.transactions = transactions;
        self
    }

    /// Set the receipts.
    pub fn receipts(mut self, receipts: Vec<Receipt>) -> Self {
        self.receipts = receipts;
        self
    }

    /// Set the signet events.
    pub fn signet_events(mut self, events: Vec<DbSignetEvent>) -> Self {
        self.signet_events = events;
        self
    }

    /// Set the zenith header.
    pub const fn zenith_header(mut self, header: Option<DbZenithHeader>) -> Self {
        self.zenith_header = header;
        self
    }

    /// Build the [`ExecutedBlock`].
    ///
    /// # Panics
    ///
    /// Panics if `header` or `bundle` have not been set.
    pub fn build(self) -> ExecutedBlock {
        ExecutedBlock {
            header: self.header.expect("header is required"),
            bundle: self.bundle.expect("bundle is required"),
            transactions: self.transactions,
            receipts: self.receipts,
            signet_events: self.signet_events,
            zenith_header: self.zenith_header,
        }
    }
}
