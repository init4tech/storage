//! Unified receipt type for cold storage queries.

use alloy::{
    consensus::{Receipt as ConsensusReceipt, TxType},
    primitives::{B256, BlockNumber},
};
use signet_storage_types::{IndexedReceipt, SealedHeader};

use crate::RpcLog;

/// A receipt with enriched RPC log metadata and block context.
///
/// This is the unified return type for all cold storage receipt queries.
/// It contains the consensus receipt with RPC-enriched logs (each log
/// carries block and transaction metadata), plus per-transaction fields
/// needed for RPC response assembly.
#[derive(Debug, Clone)]
pub struct ColdReceipt {
    /// The consensus receipt with RPC-enriched logs.
    pub receipt: ConsensusReceipt<RpcLog>,
    /// Transaction type (legacy, EIP-2930, EIP-1559, EIP-4844).
    pub tx_type: TxType,
    /// Hash of the transaction that produced this receipt.
    pub tx_hash: B256,
    /// Gas used by this transaction alone.
    pub gas_used: u64,
    /// Block number containing this receipt.
    pub block_number: BlockNumber,
    /// Hash of the block containing this receipt.
    pub block_hash: B256,
    /// Block timestamp.
    pub block_timestamp: u64,
    /// Index of this transaction within the block.
    pub transaction_index: u64,
}

impl ColdReceipt {
    /// Build a [`ColdReceipt`] from an [`IndexedReceipt`], its containing
    /// [`SealedHeader`], and the transaction index within the block.
    ///
    /// Converts each consensus log into an [`RpcLog`] with full block and
    /// transaction metadata attached.
    pub fn new(ir: IndexedReceipt, header: &SealedHeader, transaction_index: u64) -> Self {
        let block_number = header.number;
        let block_hash = header.hash();
        let block_timestamp = header.timestamp;

        let rpc_logs: Vec<RpcLog> = ir
            .receipt
            .inner
            .logs
            .iter()
            .enumerate()
            .map(|(log_idx, log)| RpcLog {
                inner: log.clone(),
                block_hash: Some(block_hash),
                block_number: Some(block_number),
                block_timestamp: Some(block_timestamp),
                transaction_hash: Some(ir.tx_hash),
                transaction_index: Some(transaction_index),
                log_index: Some(ir.first_log_index + log_idx as u64),
                removed: false,
            })
            .collect();

        let receipt = ConsensusReceipt {
            status: ir.receipt.inner.status,
            cumulative_gas_used: ir.receipt.inner.cumulative_gas_used,
            logs: rpc_logs,
        };

        Self {
            receipt,
            tx_type: ir.receipt.tx_type,
            tx_hash: ir.tx_hash,
            gas_used: ir.gas_used,
            block_number,
            block_hash,
            block_timestamp,
            transaction_index,
        }
    }
}
