//! Concrete response types for RPC endpoints.
//!
//! These types replace `serde_json::Value` and `json!` macro usage with
//! properly typed structs that implement Serialize.

use alloy::primitives::{Address, B256, Bytes, U64, U256};
use serde::Serialize;

/// RPC representation of an Ethereum log.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RpcLog {
    /// Log index within the block.
    pub log_index: U64,
    /// Transaction index within the block.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transaction_index: Option<U64>,
    /// Transaction hash.
    pub transaction_hash: B256,
    /// Block hash.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub block_hash: Option<B256>,
    /// Block number.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub block_number: Option<U64>,
    /// Address that emitted the log.
    pub address: Address,
    /// Log data.
    pub data: Bytes,
    /// Log topics.
    pub topics: Vec<B256>,
}

/// RPC representation of a transaction receipt.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RpcReceipt {
    /// Transaction hash.
    pub transaction_hash: B256,
    /// Transaction index within the block.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transaction_index: Option<U64>,
    /// Block hash.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub block_hash: Option<B256>,
    /// Block number.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub block_number: Option<U64>,
    /// Cumulative gas used in the block up to this transaction.
    pub cumulative_gas_used: U64,
    /// Gas used by this transaction.
    pub gas_used: U64,
    /// Transaction status (0x1 for success, 0x0 for failure).
    pub status: U64,
    /// Recipient address (None for contract creation).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub to: Option<Address>,
    /// Logs emitted by this transaction.
    pub logs: Vec<RpcLog>,
}

/// RPC representation of an Ethereum transaction.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RpcTransaction {
    /// Transaction hash.
    pub hash: B256,
    /// Nonce.
    pub nonce: U64,
    /// Recipient address (None for contract creation).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub to: Option<Address>,
    /// Value transferred.
    pub value: U256,
    /// Gas limit.
    pub gas: U64,
    /// Input data.
    pub input: Bytes,
}

/// RPC representation of an Ethereum block.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RpcBlock {
    /// Block hash.
    pub hash: B256,
    /// Block number.
    pub number: U64,
    /// Parent block hash.
    pub parent_hash: B256,
    /// Block timestamp.
    pub timestamp: U64,
    /// Gas limit.
    pub gas_limit: U64,
    /// Gas used.
    pub gas_used: U64,
    /// Base fee per gas (EIP-1559).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base_fee_per_gas: Option<U64>,
    /// Transactions (hashes or full objects depending on request).
    pub transactions: BlockTransactions,
    /// Uncle hashes (always empty for Signet).
    pub uncles: Vec<B256>,
}

/// Transactions in a block - either hashes only or full transaction objects.
#[derive(Debug, Clone, Serialize)]
#[serde(untagged)]
pub enum BlockTransactions {
    /// Just transaction hashes.
    Hashes(Vec<B256>),
    /// Full transaction objects.
    Full(Vec<RpcTransaction>),
}
