//! MDBX table definitions for cold storage.
//!
//! This module defines all tables used by cold storage by manually implementing
//! the [`Table`], [`SingleKey`], and [`DualKey`] traits.

use alloy::{consensus::Header, primitives::B256, primitives::BlockNumber};
use signet_hot::ser::KeySer;
use signet_hot::tables::{DualKey, SingleKey, Table};
use signet_storage_types::{DbSignetEvent, DbZenithHeader, Receipt, TransactionSigned, TxLocation};

// ============================================================================
// Primary Data Tables
// ============================================================================

/// Headers indexed by block number.
///
/// Supports: `HeaderSpecifier::Number`
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ColdHeaders;

impl Table for ColdHeaders {
    const NAME: &'static str = "ColdHeaders";
    const INT_KEY: bool = true;
    type Key = BlockNumber;
    type Value = Header;
}

impl SingleKey for ColdHeaders {}

/// Transactions indexed by (block number, tx index).
///
/// Uses DUPSORT to allow efficient per-block queries.
///
/// Supports:
/// - `TransactionSpecifier::BlockAndIndex`
/// - `GetTransactionsInBlock`
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ColdTransactions;

impl Table for ColdTransactions {
    const NAME: &'static str = "ColdTransactions";
    const INT_KEY: bool = true;
    const DUAL_KEY_SIZE: Option<usize> = Some(<u64 as KeySer>::SIZE);
    type Key = BlockNumber;
    type Value = TransactionSigned;
}

impl DualKey for ColdTransactions {
    type Key2 = u64;
}

/// Receipts indexed by (block number, tx index).
///
/// Uses DUPSORT to allow efficient per-block queries.
///
/// Supports:
/// - `ReceiptSpecifier::BlockAndIndex`
/// - `GetReceiptsInBlock`
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ColdReceipts;

impl Table for ColdReceipts {
    const NAME: &'static str = "ColdReceipts";
    const INT_KEY: bool = true;
    const DUAL_KEY_SIZE: Option<usize> = Some(<u64 as KeySer>::SIZE);
    type Key = BlockNumber;
    type Value = Receipt;
}

impl DualKey for ColdReceipts {
    type Key2 = u64;
}

/// Signet events indexed by (block number, event index).
///
/// Uses DUPSORT to allow efficient per-block and range queries.
///
/// Supports:
/// - `SignetEventsSpecifier::Block`
/// - `SignetEventsSpecifier::BlockRange`
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ColdSignetEvents;

impl Table for ColdSignetEvents {
    const NAME: &'static str = "ColdSignetEvents";
    const INT_KEY: bool = true;
    const DUAL_KEY_SIZE: Option<usize> = Some(<u64 as KeySer>::SIZE);
    type Key = BlockNumber;
    type Value = DbSignetEvent;
}

impl DualKey for ColdSignetEvents {
    type Key2 = u64;
}

/// Zenith headers indexed by block number.
///
/// Supports:
/// - `ZenithHeaderSpecifier::Number`
/// - `ZenithHeaderSpecifier::Range`
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ColdZenithHeaders;

impl Table for ColdZenithHeaders {
    const NAME: &'static str = "ColdZenithHeaders";
    const INT_KEY: bool = true;
    type Key = BlockNumber;
    type Value = DbZenithHeader;
}

impl SingleKey for ColdZenithHeaders {}

// ============================================================================
// Index Tables
// ============================================================================

/// Block hash to block number index.
///
/// Supports:
/// - `HeaderSpecifier::Hash`
/// - `TransactionSpecifier::BlockHashAndIndex`
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ColdBlockHashIndex;

impl Table for ColdBlockHashIndex {
    const NAME: &'static str = "ColdBlockHashIndex";
    type Key = B256;
    type Value = BlockNumber;
}

impl SingleKey for ColdBlockHashIndex {}

/// Transaction hash to (block number, tx index) index.
///
/// Supports:
/// - `TransactionSpecifier::Hash`
/// - `ReceiptSpecifier::TxHash`
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ColdTxHashIndex;

impl Table for ColdTxHashIndex {
    const NAME: &'static str = "ColdTxHashIndex";
    const FIXED_VAL_SIZE: Option<usize> = Some(16);
    type Key = B256;
    type Value = TxLocation;
}

impl SingleKey for ColdTxHashIndex {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_names() {
        assert_eq!(ColdHeaders::NAME, "ColdHeaders");
        assert_eq!(ColdTransactions::NAME, "ColdTransactions");
        assert_eq!(ColdReceipts::NAME, "ColdReceipts");
        assert_eq!(ColdSignetEvents::NAME, "ColdSignetEvents");
        assert_eq!(ColdZenithHeaders::NAME, "ColdZenithHeaders");
        assert_eq!(ColdBlockHashIndex::NAME, "ColdBlockHashIndex");
        assert_eq!(ColdTxHashIndex::NAME, "ColdTxHashIndex");
    }

    #[test]
    fn test_single_key_tables() {
        // Single key tables should implement SingleKey
        fn assert_single_key<T: SingleKey>() {}

        assert_single_key::<ColdHeaders>();
        assert_single_key::<ColdZenithHeaders>();
        assert_single_key::<ColdBlockHashIndex>();
        assert_single_key::<ColdTxHashIndex>();
    }

    #[test]
    fn test_dual_key_tables() {
        // Dual key tables should implement DualKey
        fn assert_dual_key<T: DualKey>() {}

        assert_dual_key::<ColdTransactions>();
        assert_dual_key::<ColdReceipts>();
        assert_dual_key::<ColdSignetEvents>();
    }

    #[test]
    fn test_int_key_tables() {
        // Tables with int_key should have INT_KEY = true
        const { assert!(ColdHeaders::INT_KEY) };
        const { assert!(ColdTransactions::INT_KEY) };
        const { assert!(ColdReceipts::INT_KEY) };
        const { assert!(ColdSignetEvents::INT_KEY) };
        const { assert!(ColdZenithHeaders::INT_KEY) };

        // Non-int_key tables should have INT_KEY = false
        const { assert!(!ColdBlockHashIndex::INT_KEY) };
        const { assert!(!ColdTxHashIndex::INT_KEY) };
    }

    #[test]
    fn test_fixed_val_size() {
        // ColdTxHashIndex should have fixed value size of 16
        assert_eq!(ColdTxHashIndex::FIXED_VAL_SIZE, Some(16));

        // ColdBlockHashIndex has BlockNumber (u64) = 8 bytes
        assert_eq!(ColdBlockHashIndex::FIXED_VAL_SIZE, Some(8));

        // Variable-size value tables should not have fixed value size
        assert_eq!(ColdHeaders::FIXED_VAL_SIZE, None);
        assert_eq!(ColdZenithHeaders::FIXED_VAL_SIZE, None);
    }
}
