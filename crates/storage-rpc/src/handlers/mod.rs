//! RPC method handlers.
//!
//! This module contains the handler implementations for Ethereum JSON-RPC methods.
//! Handlers are async functions that receive storage context via ajj's state
//! mechanism and return [`RpcResult`]s.
//!
//! # Module Organization
//!
//! - **Stub endpoints**: Static responses (chain ID, protocol version, syncing)
//! - **Block queries** (`block`): Block data by number/hash (cold path)
//! - **Transaction queries** (`transaction`): Transaction data (cold path)
//! - **Receipt queries** (`receipt`): Transaction receipt data (cold path)
//! - **State queries** (`state`): Account balance, nonce, code, storage (hot path)
//! - **Gas estimation** (`gas`): Gas price, priority fee (hot path)
//! - **EVM execution** (`evm`): eth_call, estimateGas, sendRawTransaction (hot path)

// Cold path handlers
pub mod block;
pub mod receipt;
pub mod transaction;
// Hot path handlers
pub mod evm;
pub mod gas;
pub mod state;

use crate::error::{METHOD_NOT_SUPPORTED, RpcResult};
use crate::router::RpcContext;
use ajj::{ErrorPayload, ResponsePayload};
use alloy::primitives::U64;
use serde::{Deserialize, Serialize};
use signet_hot::HotKv;
use std::borrow::Cow;

/// Default chain ID for local development (31337 = 0x7A69).
pub const DEFAULT_CHAIN_ID: u64 = 31337;

/// Sync status response.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(untagged)]
pub enum SyncStatus {
    /// Not syncing (false).
    NotSyncing(bool),
    /// Syncing with progress info.
    Syncing {
        /// Starting block.
        #[serde(rename = "startingBlock")]
        starting_block: U64,
        /// Current block.
        #[serde(rename = "currentBlock")]
        current_block: U64,
        /// Highest block.
        #[serde(rename = "highestBlock")]
        highest_block: U64,
    },
}

// ============================================================================
// Stub Endpoints (return static values)
// ============================================================================

/// Handler for `eth_chainId`.
///
/// Returns the chain ID used for signing replay-protected transactions.
pub(crate) async fn eth_chain_id<H: HotKv>(state: RpcContext<H>) -> RpcResult<U64> {
    ResponsePayload(Ok(U64::from(state.chain_id)))
}

/// Handler for `eth_protocolVersion`.
///
/// Returns the current Ethereum protocol version.
pub async fn eth_protocol_version() -> RpcResult<String> {
    ResponsePayload(Ok("1.0".to_string()))
}

/// Handler for `eth_syncing`.
///
/// Returns false if not syncing, or sync status object if syncing.
pub async fn eth_syncing() -> RpcResult<SyncStatus> {
    // Signet doesn't sync in the traditional sense
    ResponsePayload(Ok(SyncStatus::NotSyncing(false)))
}

// ============================================================================
// Unsupported Endpoints (return errors)
// ============================================================================

/// Create a "method not supported" response.
fn unsupported(method: &str) -> RpcResult<()> {
    ResponsePayload(Err(ErrorPayload {
        code: METHOD_NOT_SUPPORTED,
        message: Cow::Owned(format!("Method '{method}' is not supported")),
        data: None,
    }))
}

/// Handler for `eth_coinbase`.
///
/// Not supported: Signet doesn't have a coinbase address.
pub async fn eth_coinbase() -> RpcResult<()> {
    unsupported("eth_coinbase")
}

/// Handler for `eth_accounts`.
///
/// Not supported: This is a read-only node without wallet functionality.
pub async fn eth_accounts() -> RpcResult<()> {
    unsupported("eth_accounts")
}

/// Handler for `eth_blobBaseFee`.
///
/// Not supported: Signet doesn't use blob transactions.
pub async fn eth_blob_base_fee() -> RpcResult<()> {
    unsupported("eth_blobBaseFee")
}

/// Handler for `eth_getUncleCountByBlockHash`.
///
/// Not supported: Signet doesn't have uncles.
pub async fn eth_get_uncle_count_by_block_hash() -> RpcResult<()> {
    unsupported("eth_getUncleCountByBlockHash")
}

/// Handler for `eth_getUncleCountByBlockNumber`.
///
/// Not supported: Signet doesn't have uncles.
pub async fn eth_get_uncle_count_by_block_number() -> RpcResult<()> {
    unsupported("eth_getUncleCountByBlockNumber")
}

/// Handler for `eth_getUncleByBlockHashAndIndex`.
///
/// Not supported: Signet doesn't have uncles.
pub async fn eth_get_uncle_by_block_hash_and_index() -> RpcResult<()> {
    unsupported("eth_getUncleByBlockHashAndIndex")
}

/// Handler for `eth_getUncleByBlockNumberAndIndex`.
///
/// Not supported: Signet doesn't have uncles.
pub async fn eth_get_uncle_by_block_number_and_index() -> RpcResult<()> {
    unsupported("eth_getUncleByBlockNumberAndIndex")
}

/// Handler for `eth_mining`.
///
/// Not supported: Signet doesn't use proof-of-work mining.
pub async fn eth_mining() -> RpcResult<()> {
    unsupported("eth_mining")
}

/// Handler for `eth_hashrate`.
///
/// Not supported: Signet doesn't use proof-of-work mining.
pub async fn eth_hashrate() -> RpcResult<()> {
    unsupported("eth_hashrate")
}

/// Handler for `eth_getWork`.
///
/// Not supported: Signet doesn't use proof-of-work mining.
pub async fn eth_get_work() -> RpcResult<()> {
    unsupported("eth_getWork")
}

/// Handler for `eth_submitWork`.
///
/// Not supported: Signet doesn't use proof-of-work mining.
pub async fn eth_submit_work() -> RpcResult<()> {
    unsupported("eth_submitWork")
}

/// Handler for `eth_submitHashrate`.
///
/// Not supported: Signet doesn't use proof-of-work mining.
pub async fn eth_submit_hashrate() -> RpcResult<()> {
    unsupported("eth_submitHashrate")
}

/// Handler for `eth_sendTransaction`.
///
/// Not supported: This is a read-only node without wallet functionality.
pub async fn eth_send_transaction() -> RpcResult<()> {
    unsupported("eth_sendTransaction")
}

/// Handler for `eth_sign`.
///
/// Not supported: This is a read-only node without wallet functionality.
pub async fn eth_sign() -> RpcResult<()> {
    unsupported("eth_sign")
}

/// Handler for `eth_signTransaction`.
///
/// Not supported: This is a read-only node without wallet functionality.
pub async fn eth_sign_transaction() -> RpcResult<()> {
    unsupported("eth_signTransaction")
}

/// Handler for `eth_getProof`.
///
/// Not supported: Merkle proofs not implemented yet.
pub async fn eth_get_proof() -> RpcResult<()> {
    unsupported("eth_getProof")
}

/// Handler for `eth_createAccessList`.
///
/// Not supported: Access list creation not implemented yet.
pub async fn eth_create_access_list() -> RpcResult<()> {
    unsupported("eth_createAccessList")
}

/// Handler for `eth_newPendingTransactionFilter`.
///
/// Not supported: Signet doesn't have a pending transaction pool.
pub async fn eth_new_pending_transaction_filter() -> RpcResult<()> {
    unsupported("eth_newPendingTransactionFilter")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_eth_chain_id() {
        let storage = crate::handlers::state::tests::create_test_storage();
        let ctx = RpcContext { storage, chain_id: DEFAULT_CHAIN_ID };
        let result = eth_chain_id(ctx).await;
        assert_eq!(result.0.unwrap(), U64::from(31337));
    }

    #[tokio::test]
    async fn test_eth_protocol_version() {
        let result = eth_protocol_version().await;
        assert_eq!(result.0.unwrap(), "1.0");
    }

    #[tokio::test]
    async fn test_eth_syncing() {
        let result = eth_syncing().await;
        match result.0.unwrap() {
            SyncStatus::NotSyncing(syncing) => assert!(!syncing),
            _ => panic!("Expected NotSyncing"),
        }
    }

    #[tokio::test]
    async fn test_unsupported_methods() {
        let result = eth_coinbase().await;
        let err = result.0.unwrap_err();
        assert_eq!(err.code, METHOD_NOT_SUPPORTED);

        let result = eth_mining().await;
        assert!(result.0.is_err());

        let result = eth_send_transaction().await;
        assert!(result.0.is_err());
    }
}
