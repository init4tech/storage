//! Transaction location within a block.

use alloy::primitives::BlockNumber;

/// Location of a transaction within a block.
///
/// This is a 16-byte fixed-size type that stores the block number and
/// transaction index. It is used as the value type in the `ColdTxHashIndex`
/// table to map transaction hashes to their location.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TxLocation {
    /// The block number containing the transaction.
    pub block: BlockNumber,
    /// The index of the transaction within the block.
    pub index: u64,
}

impl TxLocation {
    /// Create a new transaction location.
    pub const fn new(block: BlockNumber, index: u64) -> Self {
        Self { block, index }
    }
}
