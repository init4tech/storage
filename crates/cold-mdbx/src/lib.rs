//! MDBX table definitions and backend for cold storage.
//!
//! This crate provides table definitions for storing historical blockchain data
//! in MDBX. It defines 8 tables:
//!
//! ## Primary Data Tables
//!
//! - [`ColdHeaders`]: Block headers indexed by block number.
//! - [`ColdTransactions`]: Transactions indexed by (block number, tx index).
//! - [`ColdReceipts`]: Receipts indexed by (block number, tx index).
//! - [`ColdSignetEvents`]: Signet events indexed by (block number, event index).
//! - [`ColdZenithHeaders`]: Zenith headers indexed by block number.
//!
//! ## Index Tables
//!
//! - [`ColdBlockHashIndex`]: Maps block hash to block number.
//! - [`ColdTxHashIndex`]: Maps transaction hash to (block number, tx index).
//!
//! # Feature Flags
//!
//! - **`test-utils`**: Propagates `signet-cold/test-utils` for conformance
//!   testing against the MDBX backend.

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
pub use error::MdbxColdError;

mod tables;
pub use tables::{
    ColdBlockHashIndex, ColdHeaders, ColdReceipts, ColdSignetEvents, ColdTransactions,
    ColdTxHashIndex, ColdZenithHeaders,
};

mod backend;
pub use backend::MdbxColdBackend;

pub use signet_hot_mdbx::{DatabaseArguments, DatabaseEnvKind};
