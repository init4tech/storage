//! Executed block types for unified storage.
//!
//! This module provides the [`ExecutedBlock`] type which contains all data
//! needed by both hot and cold storage systems for a single executed block.

use crate::{DbSignetEvent, DbZenithHeader, Receipt, RecoveredTx, SealedHeader};
use alloy::primitives::{B256, BlockNumber};
use core::fmt;
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
///     .build()
///     .unwrap();
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct ExecutedBlock {
    /// The sealed block header (contains cached hash).
    pub header: SealedHeader,
    /// The state changes from execution (accounts, storage, bytecode).
    pub bundle: BundleState,
    /// The signed transactions in the block, with recovered senders.
    pub transactions: Vec<RecoveredTx>,
    /// The receipts from execution.
    pub receipts: Vec<Receipt>,
    /// Extracted signet events from the block.
    pub signet_events: Vec<DbSignetEvent>,
    /// The zenith header, if present.
    pub zenith_header: Option<DbZenithHeader>,
    /// keccak256 of the wire-encoded `Journal::V1` bytes emitted for this
    /// block, when produced. Persisted into the `JournalHashes` hot table by
    /// `append_blocks` so producing and syncing nodes can re-seed the rolling
    /// previous-journal hash across restarts and reverts. `None` for callers
    /// that do not produce a journal (e.g. block-only nodes); no entry is
    /// written in that case.
    pub journal_hash: Option<B256>,
}

impl ExecutedBlock {
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
    transactions: Vec<RecoveredTx>,
    receipts: Vec<Receipt>,
    signet_events: Vec<DbSignetEvent>,
    zenith_header: Option<DbZenithHeader>,
    journal_hash: Option<B256>,
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
    pub fn transactions(mut self, transactions: Vec<RecoveredTx>) -> Self {
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

    /// Set the journal hash (keccak256 of the wire-encoded `Journal::V1`).
    /// Leave the method uncalled for block-only nodes that do not produce
    /// a journal - the field defaults to `None`.
    pub const fn journal_hash(mut self, hash: B256) -> Self {
        self.journal_hash = Some(hash);
        self
    }

    /// Build the [`ExecutedBlock`].
    ///
    /// # Errors
    ///
    /// Returns [`MissingFieldError`] if `header` or `bundle` have not been set.
    pub fn build(self) -> Result<ExecutedBlock, MissingFieldError> {
        Ok(ExecutedBlock {
            header: self.header.ok_or(MissingFieldError("header"))?,
            bundle: self.bundle.ok_or(MissingFieldError("bundle"))?,
            transactions: self.transactions,
            receipts: self.receipts,
            signet_events: self.signet_events,
            zenith_header: self.zenith_header,
            journal_hash: self.journal_hash,
        })
    }
}

/// Error returned when building an [`ExecutedBlock`] with missing required
/// fields.
#[derive(Debug, Clone, Copy)]
pub struct MissingFieldError(&'static str);

impl fmt::Display for MissingFieldError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "missing required field: {}", self.0)
    }
}

impl std::error::Error for MissingFieldError {}
