//! JSON-RPC router configuration.
//!
//! This module provides the [`RpcContext`] type for creating an ajj router
//! with all Ethereum JSON-RPC methods registered.
//!
//! # Example
//!
//! ```ignore
//! use signet_storage_rpc::RpcContext;
//! use signet_storage::UnifiedStorage;
//! use std::sync::Arc;
//!
//! // Create storage instance
//! let storage = Arc::new(UnifiedStorage::new(hot, cold));
//!
//! // Build the router
//! let ctx = RpcContext { storage, chain_id: 31337 };
//! let router = ctx.build();
//!
//! // Serve over HTTP
//! let listener = tokio::net::TcpListener::bind("0.0.0.0:8545").await?;
//! axum::serve(listener, router.into_axum("/")).await?;
//! ```

use crate::handlers::{self, block, evm, gas, receipt, state, transaction};
use ajj::Router;
use signet_hot::{HotKv, model::HotKvRead};
use signet_storage::UnifiedStorage;
use std::sync::Arc;
use trevm::revm::database::DBErrorMarker;

/// Router state providing storage context to all handlers.
#[derive(Debug)]
pub struct RpcContext<H: HotKv> {
    /// Unified storage backend.
    pub storage: Arc<UnifiedStorage<H>>,
    /// Chain ID for `eth_chainId`.
    pub chain_id: u64,
}

impl<H: HotKv> Clone for RpcContext<H> {
    fn clone(&self) -> Self {
        Self { storage: self.storage.clone(), chain_id: self.chain_id }
    }
}

impl<H: HotKv + Send + Sync + 'static> RpcContext<H>
where
    <<H as HotKv>::RoTx as HotKvRead>::Error: DBErrorMarker,
{
    /// Build the ajj router with all Ethereum JSON-RPC methods registered.
    ///
    /// Storage is passed to handlers via ajj's state mechanism.
    pub fn build(self) -> Router<()> {
        Router::<RpcContext<H>>::new()
            // ============================================================
            // Stub endpoints (state-dependent)
            // ============================================================
            .route("eth_chainId", handlers::eth_chain_id::<H>)
            // ============================================================
            // Block endpoints (cold path)
            // ============================================================
            .route("eth_blockNumber", block::eth_block_number::<H>)
            .route("eth_getBlockByHash", block::eth_get_block_by_hash::<H>)
            .route("eth_getBlockByNumber", block::eth_get_block_by_number::<H>)
            .route(
                "eth_getBlockTransactionCountByHash",
                block::eth_get_block_transaction_count_by_hash::<H>,
            )
            .route(
                "eth_getBlockTransactionCountByNumber",
                block::eth_get_block_transaction_count_by_number::<H>,
            )
            .route("eth_getBlockReceipts", block::eth_get_block_receipts::<H>)
            // ============================================================
            // Transaction endpoints (cold path)
            // ============================================================
            .route("eth_getTransactionByHash", transaction::eth_get_transaction_by_hash::<H>)
            .route("eth_getRawTransactionByHash", transaction::eth_get_raw_transaction_by_hash::<H>)
            // ============================================================
            // Receipt endpoints (cold path)
            // ============================================================
            .route("eth_getTransactionReceipt", receipt::eth_get_transaction_receipt::<H>)
            // ============================================================
            // State endpoints (hot path)
            // ============================================================
            .route("eth_getBalance", state::eth_get_balance::<H>)
            .route("eth_getTransactionCount", state::eth_get_transaction_count::<H>)
            .route("eth_getCode", state::eth_get_code::<H>)
            .route("eth_getStorageAt", state::eth_get_storage_at::<H>)
            // ============================================================
            // Gas endpoints (hot path)
            // ============================================================
            .route("eth_gasPrice", gas::eth_gas_price::<H>)
            .route("eth_maxPriorityFeePerGas", gas::eth_max_priority_fee_per_gas::<H>)
            // ============================================================
            // EVM execution endpoints (hot path)
            // ============================================================
            .route("eth_call", evm::eth_call::<H>)
            .route("eth_estimateGas", evm::eth_estimate_gas::<H>)
            .route("eth_sendRawTransaction", evm::eth_send_raw_transaction::<H>)
            // ============================================================
            // Provide state, switching to Router<()>
            // ============================================================
            .with_state::<()>(self)
            // ============================================================
            // Stub endpoints (stateless)
            // ============================================================
            .route("eth_protocolVersion", handlers::eth_protocol_version)
            .route("eth_syncing", handlers::eth_syncing)
            // ============================================================
            // Unsupported endpoints (stateless, return errors)
            // ============================================================
            .route("eth_coinbase", handlers::eth_coinbase)
            .route("eth_accounts", handlers::eth_accounts)
            .route("eth_blobBaseFee", handlers::eth_blob_base_fee)
            .route("eth_getUncleCountByBlockHash", handlers::eth_get_uncle_count_by_block_hash)
            .route("eth_getUncleCountByBlockNumber", handlers::eth_get_uncle_count_by_block_number)
            .route(
                "eth_getUncleByBlockHashAndIndex",
                handlers::eth_get_uncle_by_block_hash_and_index,
            )
            .route(
                "eth_getUncleByBlockNumberAndIndex",
                handlers::eth_get_uncle_by_block_number_and_index,
            )
            .route("eth_mining", handlers::eth_mining)
            .route("eth_hashrate", handlers::eth_hashrate)
            .route("eth_getWork", handlers::eth_get_work)
            .route("eth_submitWork", handlers::eth_submit_work)
            .route("eth_submitHashrate", handlers::eth_submit_hashrate)
            .route("eth_sendTransaction", handlers::eth_send_transaction)
            .route("eth_sign", handlers::eth_sign)
            .route("eth_signTransaction", handlers::eth_sign_transaction)
            .route("eth_getProof", handlers::eth_get_proof)
            .route("eth_createAccessList", handlers::eth_create_access_list)
            .route("eth_newPendingTransactionFilter", handlers::eth_new_pending_transaction_filter)
    }

    /// Build the ajj router and convert to an axum router for HTTP serving.
    ///
    /// The RPC endpoint will be available at the specified path.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let axum_router = RpcContext { storage, chain_id: 31337 }
    ///     .build_axum("/rpc");
    ///
    /// let listener = tokio::net::TcpListener::bind("0.0.0.0:8545").await?;
    /// axum::serve(listener, axum_router).await?;
    /// ```
    pub fn build_axum(self, path: &str) -> axum::Router {
        self.build().into_axum(path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::handlers::DEFAULT_CHAIN_ID;

    #[tokio::test]
    async fn test_router_builder_defaults() {
        let ctx: RpcContext<signet_hot::mem::MemKv> = RpcContext {
            storage: crate::handlers::state::tests::create_test_storage(),
            chain_id: DEFAULT_CHAIN_ID,
        };
        assert_eq!(ctx.chain_id, DEFAULT_CHAIN_ID);
    }

    #[tokio::test]
    async fn test_router_builder_chain_id() {
        let ctx: RpcContext<signet_hot::mem::MemKv> = RpcContext {
            storage: crate::handlers::state::tests::create_test_storage(),
            chain_id: 1,
        };
        assert_eq!(ctx.chain_id, 1);
    }
}
