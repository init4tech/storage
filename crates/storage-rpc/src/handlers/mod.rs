//! RPC method handlers.
//!
//! This module contains the handler implementations for Ethereum JSON-RPC methods.
//! Handlers are async functions that take storage context and return RPC results.

use crate::error::{RpcError, RpcResult};
use alloy::primitives::U64;
use serde::{Deserialize, Serialize};

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
pub async fn eth_chain_id(chain_id: u64) -> RpcResult<U64> {
    Ok(U64::from(chain_id))
}

/// Handler for `eth_protocolVersion`.
///
/// Returns the current Ethereum protocol version.
pub async fn eth_protocol_version() -> RpcResult<String> {
    Ok("1.0".to_string())
}

/// Handler for `eth_syncing`.
///
/// Returns false if not syncing, or sync status object if syncing.
pub async fn eth_syncing() -> RpcResult<SyncStatus> {
    // Signet doesn't sync in the traditional sense
    Ok(SyncStatus::NotSyncing(false))
}

// ============================================================================
// Unsupported Endpoints (return errors)
// ============================================================================

/// Handler for unsupported methods.
///
/// Returns a "method not supported" error for methods that are not applicable
/// to Signet's architecture.
pub async fn unsupported(method: &'static str) -> RpcResult<()> {
    Err(RpcError::method_not_supported(method))
}

/// Handler for `eth_coinbase`.
///
/// Not supported: Signet doesn't have a coinbase address.
pub async fn eth_coinbase() -> RpcResult<()> {
    unsupported("eth_coinbase").await
}

/// Handler for `eth_accounts`.
///
/// Not supported: This is a read-only node without wallet functionality.
pub async fn eth_accounts() -> RpcResult<()> {
    unsupported("eth_accounts").await
}

/// Handler for `eth_blobBaseFee`.
///
/// Not supported: Signet doesn't use blob transactions.
pub async fn eth_blob_base_fee() -> RpcResult<()> {
    unsupported("eth_blobBaseFee").await
}

/// Handler for `eth_getUncleCountByBlockHash`.
///
/// Not supported: Signet doesn't have uncles.
pub async fn eth_get_uncle_count_by_block_hash() -> RpcResult<()> {
    unsupported("eth_getUncleCountByBlockHash").await
}

/// Handler for `eth_getUncleCountByBlockNumber`.
///
/// Not supported: Signet doesn't have uncles.
pub async fn eth_get_uncle_count_by_block_number() -> RpcResult<()> {
    unsupported("eth_getUncleCountByBlockNumber").await
}

/// Handler for `eth_getUncleByBlockHashAndIndex`.
///
/// Not supported: Signet doesn't have uncles.
pub async fn eth_get_uncle_by_block_hash_and_index() -> RpcResult<()> {
    unsupported("eth_getUncleByBlockHashAndIndex").await
}

/// Handler for `eth_getUncleByBlockNumberAndIndex`.
///
/// Not supported: Signet doesn't have uncles.
pub async fn eth_get_uncle_by_block_number_and_index() -> RpcResult<()> {
    unsupported("eth_getUncleByBlockNumberAndIndex").await
}

/// Handler for `eth_mining`.
///
/// Not supported: Signet doesn't use proof-of-work mining.
pub async fn eth_mining() -> RpcResult<()> {
    unsupported("eth_mining").await
}

/// Handler for `eth_hashrate`.
///
/// Not supported: Signet doesn't use proof-of-work mining.
pub async fn eth_hashrate() -> RpcResult<()> {
    unsupported("eth_hashrate").await
}

/// Handler for `eth_getWork`.
///
/// Not supported: Signet doesn't use proof-of-work mining.
pub async fn eth_get_work() -> RpcResult<()> {
    unsupported("eth_getWork").await
}

/// Handler for `eth_submitWork`.
///
/// Not supported: Signet doesn't use proof-of-work mining.
pub async fn eth_submit_work() -> RpcResult<()> {
    unsupported("eth_submitWork").await
}

/// Handler for `eth_submitHashrate`.
///
/// Not supported: Signet doesn't use proof-of-work mining.
pub async fn eth_submit_hashrate() -> RpcResult<()> {
    unsupported("eth_submitHashrate").await
}

/// Handler for `eth_sendTransaction`.
///
/// Not supported: This is a read-only node without wallet functionality.
pub async fn eth_send_transaction() -> RpcResult<()> {
    unsupported("eth_sendTransaction").await
}

/// Handler for `eth_sign`.
///
/// Not supported: This is a read-only node without wallet functionality.
pub async fn eth_sign() -> RpcResult<()> {
    unsupported("eth_sign").await
}

/// Handler for `eth_signTransaction`.
///
/// Not supported: This is a read-only node without wallet functionality.
pub async fn eth_sign_transaction() -> RpcResult<()> {
    unsupported("eth_signTransaction").await
}

/// Handler for `eth_getProof`.
///
/// Not supported: Merkle proofs not implemented yet.
pub async fn eth_get_proof() -> RpcResult<()> {
    unsupported("eth_getProof").await
}

/// Handler for `eth_createAccessList`.
///
/// Not supported: Access list creation not implemented yet.
pub async fn eth_create_access_list() -> RpcResult<()> {
    unsupported("eth_createAccessList").await
}

/// Handler for `eth_newPendingTransactionFilter`.
///
/// Not supported: Signet doesn't have a pending transaction pool.
pub async fn eth_new_pending_transaction_filter() -> RpcResult<()> {
    unsupported("eth_newPendingTransactionFilter").await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_eth_chain_id() {
        let result = eth_chain_id(DEFAULT_CHAIN_ID).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), U64::from(31337));
    }

    #[tokio::test]
    async fn test_eth_protocol_version() {
        let result = eth_protocol_version().await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "1.0");
    }

    #[tokio::test]
    async fn test_eth_syncing() {
        let result = eth_syncing().await;
        assert!(result.is_ok());
        match result.unwrap() {
            SyncStatus::NotSyncing(syncing) => assert!(!syncing),
            _ => panic!("Expected NotSyncing"),
        }
    }

    #[tokio::test]
    async fn test_unsupported_methods() {
        // Test a few unsupported methods
        let result = eth_coinbase().await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.code, -32004);

        let result = eth_mining().await;
        assert!(result.is_err());

        let result = eth_send_transaction().await;
        assert!(result.is_err());
    }
}
