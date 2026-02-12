//! Rich log type with full block and transaction context.

use alloy::primitives::{B256, BlockNumber, Log};

/// A log entry with full block and transaction context.
///
/// Contains all metadata needed for `eth_getLogs` RPC responses:
/// block number, block hash, transaction hash, transaction index,
/// log index within the block, and log index within the transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RichLog {
    /// The log entry (address, topics, and data).
    pub log: Log,
    /// The block number containing this log.
    pub block_number: BlockNumber,
    /// The block hash containing this log.
    pub block_hash: B256,
    /// The hash of the transaction that emitted this log.
    pub tx_hash: B256,
    /// The index of the transaction within the block.
    pub tx_index: u64,
    /// The index of the log within the block (across all transactions).
    ///
    /// This is the value returned as `logIndex` in `eth_getLogs` RPC
    /// responses. It equals the sum of log counts from all preceding
    /// transactions plus this log's position within its own transaction.
    pub block_log_index: u64,
    /// The index of the log within its transaction.
    pub tx_log_index: u64,
}
