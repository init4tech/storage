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

use crate::handlers::{self, block, receipt, transaction, DEFAULT_CHAIN_ID};
use ajj::Router;
use alloy::{eips::BlockNumberOrTag, primitives::B256};
use signet_hot::HotKv;
use signet_storage::UnifiedStorage;
use std::sync::Arc;

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
    ) -> Router<()> {
        let cold = storage.cold_reader();
        let chain_id = self.chain_id;

        let mut router = Router::<()>::new();

        // ================================================================
        // Stub endpoints (return static values)
        // ================================================================
        router = router.route("eth_chainId", move || {
            async move { handlers::eth_chain_id(chain_id).await }
        });
        router = router.route("eth_protocolVersion", || async {
            handlers::eth_protocol_version().await
        });
        router = router.route("eth_syncing", || async { handlers::eth_syncing().await });

        // ================================================================
        // Block endpoints (cold path - Phase 2)
        // ================================================================
        {
            let cold = cold.clone();
            router = router.route("eth_blockNumber", move || {
                let cold = cold.clone();
                async move { block::eth_block_number(&cold).await }
            });
        }
        // Note: eth_getBlockByHash and eth_getBlockByNumber require 2 params
        // which ajj doesn't directly support with closures. These need to be
        // added via a custom handler wrapper or by using ajj's state mechanism.
        // For now, they're implemented with a default full_txs=false.
        {
            let cold = cold.clone();
            router = router.route("eth_getBlockTransactionCountByHash", move |hash: B256| {
                let cold = cold.clone();
                async move { block::eth_get_block_transaction_count_by_hash(&cold, hash).await }
            });
        }
        {
            let cold = cold.clone();
            router = router.route("eth_getBlockTransactionCountByNumber", move |block_id: BlockNumberOrTag| {
                let cold = cold.clone();
                async move { block::eth_get_block_transaction_count_by_number(&cold, block_id).await }
            });
        }
        {
            let cold = cold.clone();
            router = router.route("eth_getBlockReceipts", move |block_id: BlockNumberOrTag| {
                let cold = cold.clone();
                async move { block::eth_get_block_receipts(&cold, block_id).await }
            });
        }

        // ================================================================
        // Transaction endpoints (cold path - Phase 2)
        // ================================================================
        {
            let cold = cold.clone();
            router = router.route("eth_getTransactionByHash", move |hash: B256| {
                let cold = cold.clone();
                async move { transaction::eth_get_transaction_by_hash(&cold, hash).await }
            });
        }
        {
            let cold = cold.clone();
            router = router.route("eth_getRawTransactionByHash", move |hash: B256| {
                let cold = cold.clone();
                async move { transaction::eth_get_raw_transaction_by_hash(&cold, hash).await }
            });
        }

        // ================================================================
        // Receipt endpoints (cold path - Phase 2)
        // ================================================================
        {
            let cold = cold.clone();
            router = router.route("eth_getTransactionReceipt", move |tx_hash: B256| {
                let cold = cold.clone();
                async move { receipt::eth_get_transaction_receipt(&cold, tx_hash).await }
            });
        }

        // ================================================================
        // Unsupported endpoints (return errors)
        // ================================================================
        router = router.route("eth_coinbase", || async { handlers::eth_coinbase().await });
        router = router.route("eth_accounts", || async { handlers::eth_accounts().await });
        router = router.route("eth_blobBaseFee", || async {
            handlers::eth_blob_base_fee().await
        });
        router = router.route("eth_getUncleCountByBlockHash", || async {
            handlers::eth_get_uncle_count_by_block_hash().await
        });
        router = router.route("eth_getUncleCountByBlockNumber", || async {
            handlers::eth_get_uncle_count_by_block_number().await
        });
        router = router.route("eth_getUncleByBlockHashAndIndex", || async {
            handlers::eth_get_uncle_by_block_hash_and_index().await
        });
        router = router.route("eth_getUncleByBlockNumberAndIndex", || async {
            handlers::eth_get_uncle_by_block_number_and_index().await
        });
        router = router.route("eth_mining", || async { handlers::eth_mining().await });
        router = router.route("eth_hashrate", || async { handlers::eth_hashrate().await });
        router = router.route("eth_getWork", || async { handlers::eth_get_work().await });
        router = router.route("eth_submitWork", || async {
            handlers::eth_submit_work().await
        });
        router = router.route("eth_submitHashrate", || async {
            handlers::eth_submit_hashrate().await
        });
        router = router.route("eth_sendTransaction", || async {
            handlers::eth_send_transaction().await
        });
        router = router.route("eth_sign", || async { handlers::eth_sign().await });
        router = router.route("eth_signTransaction", || async {
            handlers::eth_sign_transaction().await
        });
        router = router.route("eth_getProof", || async {
            handlers::eth_get_proof().await
        });
        router = router.route("eth_createAccessList", || async {
            handlers::eth_create_access_list().await
        });
        router = router.route("eth_newPendingTransactionFilter", || async {
            handlers::eth_new_pending_transaction_filter().await
        });

        router
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
    ) -> axum::Router {
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
