//! Async cold storage engine for historical Ethereum data.
//!
//! This module provides an abstraction over various backend storage systems
//! for historical blockchain data. Unlike hot storage which uses transaction
//! semantics for mutable state, cold storage is optimized for:
//!
//! - **Append-only writes** with block-ordered data
//! - **Efficient bulk reads** by block number or index
//! - **Truncation** (reorg handling) that removes data beyond a certain block
//! - **Index maintenance** for hash-based lookups
//!
//! # Architecture
//!
//! The cold storage engine uses a task-based architecture:
//!
//! - [`ColdStorage`] trait defines the backend interface
//! - [`ColdStorageTask`] processes requests from channels
//! - [`ColdStorageHandle`] provides full read/write access
//! - [`ColdStorageReadHandle`] provides read-only access
//!
//! # Channel Separation
//!
//! Reads and writes use **separate channels**:
//!
//! - **Read channel**: Shared between [`ColdStorageHandle`] and
//!   [`ColdStorageReadHandle`]. Reads are processed concurrently (up to 64 in
//!   flight).
//! - **Write channel**: Exclusive to [`ColdStorageHandle`]. Writes are
//!   processed sequentially to maintain ordering.
//!
//! This design allows read-heavy workloads to proceed without being blocked by
//! write operations, while ensuring write ordering is preserved.
//!
//! # Consistency Model
//!
//! Cold storage is **eventually consistent** with hot storage. Hot storage is
//! always authoritative.
//!
//! ## When Cold May Lag
//!
//! - **Normal operation**: Writes are dispatched asynchronously. Cold may be a
//!   few blocks behind hot during normal block processing.
//! - **Backpressure**: If cold storage cannot keep up, the write channel fills.
//!   Dispatch methods return [`ColdStorageError::Backpressure`].
//! - **Task termination**: If the cold storage task stops, writes cannot be
//!   dispatched. Dispatch methods return [`ColdStorageError::TaskTerminated`].
//!
//! ## When Cold May Have Stale Data
//!
//! - **Failed truncate after reorg**: If a truncate dispatch fails, cold may
//!   temporarily contain blocks that hot has unwound. This is safe because hot
//!   is authoritative, but cold queries may return stale data.
//!
//! ## Recovery Procedures
//!
//! Use these methods on `UnifiedStorage` (from `signet-storage`) to detect and
//! recover from inconsistencies:
//!
//! - **`cold_lag()`**: Returns `Some(first_missing_block)` if cold is behind
//!   hot. Returns `None` if synced.
//! - **`replay_to_cold()`**: Re-sends blocks to cold storage. Use after
//!   detecting a gap or recovering from task failure.
//!
//! # Example
//!
//! ```ignore
//! use tokio_util::sync::CancellationToken;
//! use signet_cold::{ColdStorageTask, mem::MemColdBackend};
//!
//! let cancel = CancellationToken::new();
//! let handle = ColdStorageTask::spawn(MemColdBackend::new(), cancel);
//!
//! // Use the handle to interact with cold storage
//! let header = handle.get_header_by_number(100).await?;
//!
//! // Get a read-only handle for query-only components
//! let reader = handle.reader();
//! let tx = reader.get_tx_by_hash(hash).await?;
//! ```
//!
//! # Future Work: Streaming Writes
//!
//! For bulk data loading (e.g., initial sync or historical backfill), a
//! streaming write interface is planned:
//!
//! ```ignore
//! /// Streaming write session for bulk data loading.
//! ///
//! /// This type enables efficient bulk writes by buffering data and
//! /// batching backend operations. Use for initial sync or historical
//! /// backfill scenarios.
//! pub struct ColdStreamingWrite { /* ... */ }
//!
//! impl ColdStreamingWrite {
//!     /// Create a new streaming write session.
//!     ///
//!     /// # Arguments
//!     ///
//!     /// * `handle` - The cold storage handle to write through
//!     /// * `buffer_capacity` - Number of blocks to buffer before flushing
//!     pub fn new(handle: &ColdStorageHandle, buffer_capacity: usize) -> Self;
//!
//!     /// Push a block to the write buffer.
//!     ///
//!     /// May trigger an automatic flush if the buffer is full.
//!     pub async fn push(&mut self, block: BlockData) -> ColdResult<()>;
//!
//!     /// Flush buffered blocks to storage.
//!     pub async fn flush(&mut self) -> ColdResult<()>;
//!
//!     /// Create a checkpoint at the given block number.
//!     ///
//!     /// Flushes the buffer and records that blocks up to this number
//!     /// have been durably written. Useful for resumable sync.
//!     pub async fn checkpoint(&mut self, block: BlockNumber) -> ColdResult<()>;
//!
//!     /// Finish the streaming session.
//!     ///
//!     /// Flushes any remaining buffered data.
//!     pub async fn finish(self) -> ColdResult<()>;
//! }
//! ```
//!
//! This is a design sketch; no implementation is provided yet.
//!
//! # Feature Flags
//!
//! - **`in-memory`**: Enables the `mem` module, providing an in-memory
//!   [`ColdStorage`] backend for testing.
//! - **`test-utils`**: Enables the `conformance` module with backend
//!   conformance tests. Implies `in-memory`.

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
pub use error::{ColdResult, ColdStorageError};
mod request;
pub use request::{AppendBlockRequest, ColdReadRequest, ColdWriteRequest, Responder};
mod specifier;
pub use alloy::rpc::types::{Filter, Log as RpcLog};
pub use signet_storage_types::{Confirmed, Recovered};
pub use specifier::{
    HeaderSpecifier, ReceiptSpecifier, SignetEventsSpecifier, TransactionSpecifier,
    ZenithHeaderSpecifier,
};

mod cold_receipt;
pub use cold_receipt::ColdReceipt;
mod stream;
pub use stream::{StreamParams, produce_log_stream_default};
mod traits;
pub use traits::{BlockData, ColdStorage, LogStream};

pub mod connect;
pub use connect::ColdConnect;

/// Task module containing the storage task runner and handles.
pub mod task;
pub use task::{ColdStorageHandle, ColdStorageReadHandle, ColdStorageTask};

/// Conformance tests for cold storage backends.
#[cfg(any(test, feature = "test-utils"))]
pub mod conformance;

#[cfg(any(test, feature = "in-memory"))]
pub mod mem;
