//! SQL backend for cold storage.
//!
//! This crate provides SQL-based implementations of the [`ColdStorage`] trait
//! for storing historical blockchain data in relational databases. All data is
//! stored in fully decomposed SQL columns for rich queryability.
//!
//! # Supported Databases
//!
//! - **PostgreSQL** (feature `postgres`): Production-ready backend using
//!   connection pooling.
//! - **SQLite** (feature `sqlite`): Lightweight backend for testing and
//!   single-binary deployments.
//!
//! # Feature Flags
//!
//! - **`postgres`**: Enables the PostgreSQL backend.
//! - **`sqlite`**: Enables the SQLite backend.
//! - **`test-utils`**: Enables the SQLite backend and propagates
//!   `signet-cold/test-utils` for conformance testing.
//!
//! [`ColdStorage`]: signet_cold::ColdStorage

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
pub use error::SqlColdError;

#[cfg(any(feature = "sqlite", feature = "postgres"))]
mod convert;

#[cfg(feature = "sqlite")]
mod sqlite;
#[cfg(feature = "sqlite")]
pub use sqlite::SqliteColdBackend;

#[cfg(feature = "postgres")]
mod postgres;
#[cfg(feature = "postgres")]
pub use self::postgres::PostgresColdBackend;
