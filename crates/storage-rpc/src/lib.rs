//! JSON-RPC server for Signet storage.
//!
//! This crate provides a JSON-RPC 2.0 server that exposes Ethereum-compatible
//! RPC methods backed by [`UnifiedStorage`].
//!
//! # Overview
//!
//! The RPC server is built using the [ajj](https://docs.rs/ajj) crate for JSON-RPC
//! routing and can be served over HTTP using axum.
//!
//! # Quick Start
//!
//! ```ignore
//! use signet_storage_rpc::RpcRouter;
//! use signet_storage::UnifiedStorage;
//! use std::sync::Arc;
//!
//! // Create unified storage from hot and cold backends
//! let storage = Arc::new(UnifiedStorage::new(hot_db, cold_handle));
//!
//! // Build the RPC router
//! let axum_router = RpcRouter::new()
//!     .with_chain_id(31337)  // Local devnet
//!     .build_axum(storage, "/");
//!
//! // Serve over HTTP
//! let listener = tokio::net::TcpListener::bind("0.0.0.0:8545").await?;
//! axum::serve(listener, axum_router).await?;
//! ```
//!
//! # Supported Methods
//!
//! ## Stub Endpoints (static responses)
//!
//! | Method | Response |
//! |--------|----------|
//! | `eth_chainId` | Configured chain ID (default: 0x7A69) |
//! | `eth_protocolVersion` | "1.0" |
//! | `eth_syncing` | false |
//!
//! ## Unsupported Endpoints (return errors)
//!
//! The following methods return "method not supported" errors because they
//! don't apply to Signet's architecture:
//!
//! - `eth_coinbase`, `eth_accounts` - No wallet functionality
//! - `eth_blobBaseFee` - No blob transactions
//! - `eth_getUncle*` - No uncles in Signet
//! - `eth_mining`, `eth_hashrate`, `eth_getWork`, `eth_submitWork`, `eth_submitHashrate` - No PoW
//! - `eth_sendTransaction`, `eth_sign*` - Read-only node
//! - `eth_getProof`, `eth_createAccessList` - Not implemented
//! - `eth_newPendingTransactionFilter` - No pending transaction pool
//!
//! # Architecture
//!
//! The server uses [`UnifiedStorage`] as its single backend, which provides:
//!
//! - Hot storage for fast state access (accounts, storage, code)
//! - Cold storage for historical data (blocks, transactions, receipts)
//!
//! Handlers are async functions that receive storage context and return
//! results following the Ethereum JSON-RPC specification.
//!
//! [`UnifiedStorage`]: signet_storage::UnifiedStorage

#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    missing_docs,
    unreachable_pub,
    clippy::missing_const_for_fn,
    rustdoc::all
)]
#![cfg_attr(not(test), warn(unused_crate_dependencies))]
#![deny(unused_must_use, rust_2018_idioms)]
#![cfg_attr(docsrs, feature(doc_cfg))]

pub mod error;
pub use error::{ErrorCode, RpcError, RpcResult};

pub mod handlers;

pub mod router;
pub use router::{RpcContext, RpcRouter};

// Re-export key dependencies for convenience
pub use ajj;
pub use signet_storage::UnifiedStorage;

// These are used in handlers and will be more extensively used in Phase 2
use thiserror as _;
use tokio as _;
use tracing as _;
