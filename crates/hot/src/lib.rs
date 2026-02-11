//! Hot storage module.
//!
//! Hot storage is designed for fast read and write access to frequently used
//! data. It provides abstractions and implementations for key-value storage
//! backends.
//!
//! # Quick Start
//!
//! ```ignore
//! use signet_hot::{HotKv, HistoryRead, HistoryWrite};
//!
//! fn example<D: HotKv>(db: &D) -> Result<(), signet_hot::db::HotKvError> {
//!     // Read operations
//!     let reader = db.reader()?;
//!     let tip = reader.get_chain_tip()?;
//!     let account = reader.get_account(&address)?;
//!
//!     // Write operations (pass iterator of (&header, &bundle) tuples)
//!     let writer = db.writer()?;
//!     writer.append_blocks(blocks.iter().map(|(h, b)| (h, b)))?;
//!     writer.commit()?;
//!     Ok(())
//! }
//! ```
//!
//! For a concrete implementation, see the `signet-hot-mdbx` crate.
//!
//! # Trait Hierarchy
//!
//! ```text
//! HotKv                    ← Transaction factory
//!   ├─ reader() → HotKvRead    ← Read-only transactions
//!   │              └─ HotDbRead     ← Typed accessors (blanket impl)
//!   │                   └─ HistoryRead  ← History queries (blanket impl)
//!   └─ writer() → HotKvWrite   ← Read-write transactions
//!                  └─ UnsafeDbWrite    ← Low-level writes (blanket impl)
//!                       └─ HistoryWrite ← Safe chain operations (blanket impl)
//! ```
//!
//! ## Serialization
//!
//! Hot storage is opinionated with respect to serialization. Each table defines
//! the key and value types it uses, and these types must implement the
//! appropriate serialization traits. See the [`KeySer`] and [`ValSer`] traits
//! for more information.
//!
//! # Trait Model
//!
//! The hot storage module defines a set of traits to abstract over different
//! hot storage backends. The primary traits are:
//!
//! - [`HotKvRead`]: for transactional read-only access to hot storage.
//! - [`HotKvWrite`]: for transactional read-write access to hot storage.
//! - [`HotKv`]: for creating read and write transactions.
//!
//! These traits provide methods for common operations such as getting,
//! setting, and deleting key-value pairs in hot storage tables. The raw
//! key-value operations use byte slices for maximum flexibility. The
//! [`HistoryRead`] and [`HistoryWrite`] traits provide higher-level
//! abstractions that work with the predefined tables and their associated key
//! and value types.
//!
//! See the [`model`] module documentation for more details on the traits and
//! their usage.
//!
//! ## Tables
//!
//! Hot storage tables are predefined in the [`tables`] module. Each table
//! defines the key and value types it uses, along with serialization logic.
//! The [`Table`] and [`DualKey`] traits define the interface for tables.
//! The [`SingleKey`] trait is a marker for tables with single keys.
//!
//! See the [`Table`] trait documentation for more information on defining and
//! using tables.
//!
//! [`HistoryRead`]: db::HistoryRead
//! [`HistoryWrite`]: db::HistoryWrite
//! [`HotKvRead`]: model::HotKvRead
//! [`HotKvWrite`]: model::HotKvWrite
//! [`HotKv`]: model::HotKv
//! # Feature Flags
//!
//! - **`in-memory`**: Enables the [`mem`] module, providing an in-memory
//!   [`HotKv`] backend for testing.
//! - **`test-utils`**: Enables the [`conformance`] module with backend
//!   conformance tests. Implies `in-memory`.
//!
//! [`DualKey`]: tables::DualKey
//! [`SingleKey`]: tables::SingleKey
//! [`Table`]: tables::Table

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

/// Conformance tests for hot storage backends.
#[cfg(any(test, feature = "test-utils"))]
pub mod conformance;

pub mod db;
pub use db::{HistoryError, HistoryRead, HistoryWrite};

pub mod model;
pub use model::HotKv;

/// Serialization module.
pub mod ser;
pub use ser::{DeserError, KeySer, MAX_FIXED_VAL_SIZE, MAX_KEY_SIZE, ValSer};

/// Predefined tables module.
pub mod tables;

#[cfg(any(test, feature = "in-memory"))]
pub mod mem;
