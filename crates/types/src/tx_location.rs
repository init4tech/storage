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

    /// Returns the block number.
    pub const fn block(&self) -> BlockNumber {
        self.block
    }

    /// Returns the transaction index within the block.
    pub const fn index(&self) -> u64 {
        self.index
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tx_location_new() {
        let loc = TxLocation::new(100, 5);
        assert_eq!(loc.block(), 100);
        assert_eq!(loc.index(), 5);
    }

    #[test]
    fn test_tx_location_equality() {
        let loc1 = TxLocation::new(100, 5);
        let loc2 = TxLocation::new(100, 5);
        let loc3 = TxLocation::new(100, 6);

        assert_eq!(loc1, loc2);
        assert_ne!(loc1, loc3);
    }
}
