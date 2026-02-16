//! Unified storage interface for Signet.
//!
//! This crate provides a unified interface for writing execution data to both
//! hot storage (for fast state access) and cold storage (for historical archival).
//!
//! # Overview
//!
//! The [`UnifiedStorage`] struct wraps both a hot storage backend and a cold
//! storage handle, providing a single API for block writes:
//!
//! - Hot storage receives headers and state changes for fast access
//! - Cold storage receives full block data (transactions, receipts, events)
//!
//! # Write Semantics
//!
//! - Hot writes are synchronous (database transactions)
//! - Cold writes are dispatched asynchronously (fire-and-forget)
//! - Hot errors are fatal; cold errors are logged but don't block
//!
//! # Example
//!
//! ```ignore
//! use signet_storage::UnifiedStorage;
//! use signet_storage_types::ExecutedBlockBuilder;
//!
//! // Create unified storage from hot and cold backends
//! let storage = UnifiedStorage::new(hot_db, cold_handle);
//!
//! // Build an executed block
//! let block = ExecutedBlockBuilder::new()
//!     .header(sealed_header)
//!     .bundle(bundle_state)
//!     .transactions(txs)
//!     .receipts(receipts)
//!     .build()
//!     .unwrap();
//!
//! // Write to both storages (takes ownership)
//! storage.append_blocks(vec![block])?;
//! ```
//!
//! # Feature Flags
//!
//! - **`test-utils`**: Propagates `signet-hot/test-utils` and
//!   `signet-cold/test-utils`, enabling in-memory backends and conformance
//!   tests for both storage layers.

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

mod error;
pub use error::{StorageError, StorageResult};

pub mod config;

pub mod builder;

mod unified;
pub use unified::UnifiedStorage;

// Re-export key types for convenience
pub use signet_cold::{ColdStorage, ColdStorageError, ColdStorageHandle, ColdStorageTask};
pub use signet_cold_mdbx::MdbxColdBackend;
pub use signet_hot::{
    HistoryError, HistoryRead, HistoryWrite, HotKv,
    model::{HotKvRead, RevmRead, RevmWrite},
};
pub use signet_hot_mdbx::{DatabaseArguments, DatabaseEnv};
pub use signet_storage_types::{ExecutedBlock, ExecutedBlockBuilder};
pub use tokio_util::sync::CancellationToken;

#[cfg(feature = "sql")]
pub use signet_cold_sql::SqlColdBackend;
