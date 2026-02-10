//! JSON-RPC router configuration.
//!
//! This module provides the [`RpcRouter`] builder for creating an ajj router
//! with all Ethereum JSON-RPC methods registered.
//!
//! # Example
//!
//! ```ignore
//! use signet_storage_rpc::RpcRouter;
//! use signet_storage::UnifiedStorage;
//!
//! // Create storage instance
//! let storage = UnifiedStorage::new(hot, cold);
//!
//! // Build the router
//! let router = RpcRouter::new()
//!     .with_chain_id(31337)
//!     .build(storage);
//!
//! // Serve over HTTP
//! let listener = tokio::net::TcpListener::bind("0.0.0.0:8545").await?;
//! axum::serve(listener, router.into_axum("/")).await?;
//! ```

use crate::handlers::{self, DEFAULT_CHAIN_ID, block, evm, gas, receipt, state, transaction};
use ajj::Router;
use signet_hot::{HotKv, model::HotKvRead};
use signet_storage::UnifiedStorage;
use std::sync::Arc;
use trevm::revm::database::DBErrorMarker;

/// Router state providing storage context to all handlers.
pub(crate) struct RpcContext<H: HotKv> {
    /// Unified storage backend.
    pub(crate) storage: Arc<UnifiedStorage<H>>,
    /// Chain ID for `eth_chainId`.
    pub(crate) chain_id: u64,
}

impl<H: HotKv> Clone for RpcContext<H> {
    fn clone(&self) -> Self {
        Self { storage: self.storage.clone(), chain_id: self.chain_id }
    }
}

/// Builder for creating an RPC router.
#[derive(Debug, Clone, Copy)]
pub struct RpcRouter {
    chain_id: u64,
}

impl Default for RpcRouter {
    fn default() -> Self {
        Self::new()
    }
}

impl RpcRouter {
    /// Create a new router builder with default settings.
    pub const fn new() -> Self {
        Self { chain_id: DEFAULT_CHAIN_ID }
    }

    /// Set the chain ID to return from `eth_chainId`.
    pub const fn with_chain_id(mut self, chain_id: u64) -> Self {
        self.chain_id = chain_id;
        self
    }

    /// Build the ajj router with the given storage backend.
    ///
    /// This registers all Ethereum JSON-RPC methods with the router.
    /// Storage is passed to handlers via ajj's state mechanism.
    pub fn build<H: HotKv + Send + Sync + 'static>(
        self,
        storage: Arc<UnifiedStorage<H>>,
    ) -> Router<()>
    where
        <<H as HotKv>::RoTx as HotKvRead>::Error: DBErrorMarker,
    {
        let ctx = RpcContext { storage, chain_id: self.chain_id };

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
            .with_state::<()>(ctx)
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
    /// let axum_router = RpcRouter::new()
    ///     .build_axum(storage, "/rpc");
    ///
    /// let listener = tokio::net::TcpListener::bind("0.0.0.0:8545").await?;
    /// axum::serve(listener, axum_router).await?;
    /// ```
    pub fn build_axum<H: HotKv + Send + Sync + 'static>(
        self,
        storage: Arc<UnifiedStorage<H>>,
        path: &str,
    ) -> axum::Router
    where
        <<H as HotKv>::RoTx as HotKvRead>::Error: DBErrorMarker,
    {
        self.build(storage).into_axum(path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_router_builder_defaults() {
        let builder = RpcRouter::new();
        assert_eq!(builder.chain_id, DEFAULT_CHAIN_ID);
    }

    #[test]
    fn test_router_builder_chain_id() {
        let builder = RpcRouter::new().with_chain_id(1);
        assert_eq!(builder.chain_id, 1);
    }
}
