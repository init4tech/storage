//! Specifier enums for cold storage lookups.
//!
//! These types define how to locate data in cold storage, supporting
//! the standard Ethereum JSON-RPC lookup patterns.

use alloy::primitives::{Address, B256, BlockNumber};

/// Specifier for header lookups.
#[derive(Debug, Clone, Copy)]
pub enum HeaderSpecifier {
    /// Lookup by block number.
    Number(BlockNumber),
    /// Lookup by block hash.
    Hash(B256),
}

impl From<BlockNumber> for HeaderSpecifier {
    fn from(number: BlockNumber) -> Self {
        Self::Number(number)
    }
}

impl From<B256> for HeaderSpecifier {
    fn from(hash: B256) -> Self {
        Self::Hash(hash)
    }
}

/// Specifier for transaction lookups.
#[derive(Debug, Clone, Copy)]
pub enum TransactionSpecifier {
    /// Lookup by transaction hash.
    Hash(B256),
    /// Lookup by block number and transaction index within the block.
    BlockAndIndex {
        /// The block number.
        block: BlockNumber,
        /// The transaction index within the block.
        index: u64,
    },
    /// Lookup by block hash and transaction index within the block.
    BlockHashAndIndex {
        /// The block hash.
        block_hash: B256,
        /// The transaction index within the block.
        index: u64,
    },
}

impl From<B256> for TransactionSpecifier {
    fn from(hash: B256) -> Self {
        Self::Hash(hash)
    }
}

/// Specifier for receipt lookups.
#[derive(Debug, Clone, Copy)]
pub enum ReceiptSpecifier {
    /// Lookup by transaction hash.
    TxHash(B256),
    /// Lookup by block number and transaction index within the block.
    BlockAndIndex {
        /// The block number.
        block: BlockNumber,
        /// The transaction index within the block.
        index: u64,
    },
}

impl From<B256> for ReceiptSpecifier {
    fn from(tx_hash: B256) -> Self {
        Self::TxHash(tx_hash)
    }
}

/// Specifier for SignetEvents lookups.
#[derive(Debug, Clone, Copy)]
pub enum SignetEventsSpecifier {
    /// Lookup all events in a single block.
    Block(BlockNumber),
    /// Lookup all events in a range of blocks (inclusive).
    BlockRange {
        /// The start block number (inclusive).
        start: BlockNumber,
        /// The end block number (inclusive).
        end: BlockNumber,
    },
}

impl From<BlockNumber> for SignetEventsSpecifier {
    fn from(block: BlockNumber) -> Self {
        Self::Block(block)
    }
}

/// Specifier for ZenithHeader lookups.
#[derive(Debug, Clone, Copy)]
pub enum ZenithHeaderSpecifier {
    /// Lookup by block number.
    Number(BlockNumber),
    /// Lookup a range of blocks (inclusive).
    Range {
        /// The start block number (inclusive).
        start: BlockNumber,
        /// The end block number (inclusive).
        end: BlockNumber,
    },
}

impl From<BlockNumber> for ZenithHeaderSpecifier {
    fn from(number: BlockNumber) -> Self {
        Self::Number(number)
    }
}

/// Filter for log queries, following `eth_getLogs` semantics.
///
/// All filter fields are optional except the block range, which is
/// required to prevent unbounded scans.
///
/// # Topic Matching
///
/// Each topic position is independently filtered:
/// - `None` matches any value at that position.
/// - `Some(vec![a, b])` matches if the topic at that position equals
///   `a` **or** `b` (OR within a position).
///
/// Positions are combined with AND: a log must satisfy *all* non-`None`
/// topic filters simultaneously.
#[derive(Debug, Clone, Default)]
pub struct LogFilter {
    /// Start block number (inclusive).
    pub from_block: BlockNumber,
    /// End block number (inclusive).
    pub to_block: BlockNumber,
    /// Filter by emitting contract address. `None` matches any address.
    pub address: Option<Vec<Address>>,
    /// Topic filters for positions 0–3.
    pub topics: [Option<Vec<B256>>; 4],
}
