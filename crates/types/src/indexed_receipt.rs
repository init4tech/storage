//! Receipt type with precomputed block-level metadata.

use crate::Receipt;
use alloy::primitives::B256;

/// A receipt with precomputed block-level metadata.
///
/// Cold storage backends store this type instead of raw [`Receipt`] to
/// avoid recomputing per-receipt metadata at query time.
///
/// # Fields
///
/// - `tx_hash`: avoids joining with the transactions table during log
///   queries.
/// - `first_log_index`: the absolute index of this receipt's first log
///   within the block (sum of log counts from all preceding receipts).
///   Avoids O(N) iteration over prior receipts when building
///   [`ReceiptContext`](crate::ReceiptContext) or [`RichLog`](crate::RichLog).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexedReceipt {
    /// The receipt.
    pub receipt: Receipt,
    /// Hash of the transaction that produced this receipt.
    pub tx_hash: B256,
    /// Index of this receipt's first log among all logs in the block.
    ///
    /// Equal to the sum of log counts from all preceding receipts.
    /// Zero for the first receipt in a block.
    pub first_log_index: u64,
}
