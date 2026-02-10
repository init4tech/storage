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

use crate::handlers::{self, evm, gas, state, DEFAULT_CHAIN_ID};
use ajj::Router;
use alloy::primitives::{Address, Bytes, B256};
use alloy::rpc::types::TransactionRequest;
use signet_hot::{HotKv, model::HotKvRead};
use signet_storage::UnifiedStorage;
use std::sync::Arc;
use trevm::revm::database::DBErrorMarker;

/// RPC context containing storage and configuration.
///
/// This context is cloneable (via Arc) to satisfy ajj's state requirements.
#[derive(Debug)]
pub struct RpcContext<H: HotKv> {
    /// Unified storage backend.
    pub storage: Arc<UnifiedStorage<H>>,
    /// Chain ID for this network.
    pub chain_id: u64,
}

impl<H: HotKv> Clone for RpcContext<H> {
    fn clone(&self) -> Self {
        Self {
            storage: Arc::clone(&self.storage),
            chain_id: self.chain_id,
        }
    }
}

impl<H: HotKv> RpcContext<H> {
    /// Create a new RPC context.
    pub fn new(storage: Arc<UnifiedStorage<H>>, chain_id: u64) -> Self {
        Self { storage, chain_id }
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
        Self {
            chain_id: DEFAULT_CHAIN_ID,
        }
    }

    /// Set the chain ID to return from `eth_chainId`.
    pub const fn with_chain_id(mut self, chain_id: u64) -> Self {
        self.chain_id = chain_id;
        self
    }

    /// Build the ajj router with the given storage backend.
    ///
    /// This registers all Ethereum JSON-RPC methods with the router.
    pub fn build<H: HotKv + Send + Sync + 'static>(
        self,
        storage: Arc<UnifiedStorage<H>>,
    ) -> Router<()>
    where
        <<H as HotKv>::RoTx as HotKvRead>::Error: DBErrorMarker,
    {
        let ctx = RpcContext::new(storage, self.chain_id);

        Router::new()
            // Stub endpoints (return static values)
            .route("eth_chainId", {
                let chain_id = ctx.chain_id;
                move || {
                    let chain_id = chain_id;
                    async move { handlers::eth_chain_id(chain_id).await }
                }
            })
            .route("eth_protocolVersion", || async {
                handlers::eth_protocol_version().await
            })
            .route("eth_syncing", || async { handlers::eth_syncing().await })
            // ================================================================
            // State query endpoints (hot path)
            // ================================================================
            .route("eth_getBalance", {
                let storage = Arc::clone(&ctx.storage);
                move |address: Address| {
                    let storage = Arc::clone(&storage);
                    async move { state::eth_get_balance(&storage, address, None).await }
                }
            })
            .route("eth_getTransactionCount", {
                let storage = Arc::clone(&ctx.storage);
                move |address: Address| {
                    let storage = Arc::clone(&storage);
                    async move { state::eth_get_transaction_count(&storage, address, None).await }
                }
            })
            .route("eth_getCode", {
                let storage = Arc::clone(&ctx.storage);
                move |address: Address| {
                    let storage = Arc::clone(&storage);
                    async move { state::eth_get_code(&storage, address, None).await }
                }
            })
            // Note: eth_getStorageAt takes (address, slot) params - wrapped in tuple for ajj
            .route("eth_getStorageAt", {
                let storage = Arc::clone(&ctx.storage);
                move |(address, slot): (Address, B256)| {
                    let storage = Arc::clone(&storage);
                    async move { state::eth_get_storage_at(&storage, address, slot, None).await }
                }
            })
            // ================================================================
            // Gas endpoints (hot path)
            // ================================================================
            .route("eth_gasPrice", {
                let storage = Arc::clone(&ctx.storage);
                move || {
                    let storage = Arc::clone(&storage);
                    async move { gas::eth_gas_price(&storage).await }
                }
            })
            .route("eth_maxPriorityFeePerGas", {
                let storage = Arc::clone(&ctx.storage);
                move || {
                    let storage = Arc::clone(&storage);
                    async move { gas::eth_max_priority_fee_per_gas(&storage).await }
                }
            })
            // ================================================================
            // EVM execution endpoints (hot path)
            // ================================================================
            .route("eth_call", {
                let storage = Arc::clone(&ctx.storage);
                move |call: TransactionRequest| {
                    let storage = Arc::clone(&storage);
                    async move { evm::eth_call(&storage, call, None).await }
                }
            })
            .route("eth_estimateGas", {
                let storage = Arc::clone(&ctx.storage);
                move |call: TransactionRequest| {
                    let storage = Arc::clone(&storage);
                    async move { evm::eth_estimate_gas(&storage, call, None).await }
                }
            })
            .route("eth_sendRawTransaction", {
                let storage = Arc::clone(&ctx.storage);
                move |data: Bytes| {
                    let storage = Arc::clone(&storage);
                    async move { evm::eth_send_raw_transaction(&storage, data).await }
                }
            })
            // ================================================================
            // Unsupported endpoints (return errors)
            // ================================================================
            .route("eth_coinbase", || async { handlers::eth_coinbase().await })
            .route("eth_accounts", || async { handlers::eth_accounts().await })
            .route("eth_blobBaseFee", || async {
                handlers::eth_blob_base_fee().await
            })
            .route("eth_getUncleCountByBlockHash", || async {
                handlers::eth_get_uncle_count_by_block_hash().await
            })
            .route("eth_getUncleCountByBlockNumber", || async {
                handlers::eth_get_uncle_count_by_block_number().await
            })
            .route("eth_getUncleByBlockHashAndIndex", || async {
                handlers::eth_get_uncle_by_block_hash_and_index().await
            })
            .route("eth_getUncleByBlockNumberAndIndex", || async {
                handlers::eth_get_uncle_by_block_number_and_index().await
            })
            .route("eth_mining", || async { handlers::eth_mining().await })
            .route("eth_hashrate", || async { handlers::eth_hashrate().await })
            .route("eth_getWork", || async { handlers::eth_get_work().await })
            .route("eth_submitWork", || async {
                handlers::eth_submit_work().await
            })
            .route("eth_submitHashrate", || async {
                handlers::eth_submit_hashrate().await
            })
            .route("eth_sendTransaction", || async {
                handlers::eth_send_transaction().await
            })
            .route("eth_sign", || async { handlers::eth_sign().await })
            .route("eth_signTransaction", || async {
                handlers::eth_sign_transaction().await
            })
            .route("eth_getProof", || async {
                handlers::eth_get_proof().await
            })
            .route("eth_createAccessList", || async {
                handlers::eth_create_access_list().await
            })
            .route("eth_newPendingTransactionFilter", || async {
                handlers::eth_new_pending_transaction_filter().await
            })
            // Store context for future use (when handlers need storage access)
            .with_state(ctx)
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

/// Helper trait for handlers that need storage access.
///
/// This allows handlers to extract storage from the RPC context.
pub trait WithStorage<H: HotKv> {
    /// Get a reference to the unified storage.
    fn storage(&self) -> &UnifiedStorage<H>;
}

impl<H: HotKv> WithStorage<H> for RpcContext<H> {
    fn storage(&self) -> &UnifiedStorage<H> {
        &self.storage
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
