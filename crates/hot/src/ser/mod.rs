//! Serialization traits and implementations for hot storage.
//!
//! Hot storage requires efficient, deterministic serialization for keys and
//! values. This module provides:
//!
//! - [`KeySer`]: Fixed-size key serialization with preserved ordering.
//! - [`ValSer`]: Variable-size value serialization.
//! - [`DeserError`]: Deserialization error type.
//!
//! ## Key Serialization
//!
//! Keys must have a fixed, known size (up to [`MAX_KEY_SIZE`] bytes) and their
//! byte representation must preserve lexicographic ordering. This enables
//! efficient range queries and cursor navigation.
//!
//! ## Value Serialization
//!
//! Values can be variable-size and self-describing. Optionally, values may
//! declare a fixed size (up to [`MAX_FIXED_VAL_SIZE`] bytes) for backend
//! optimizations.

mod error;
pub use error::DeserError;
mod impls;
mod reth_impls;
mod traits;
pub use traits::{KeySer, MAX_FIXED_VAL_SIZE, MAX_KEY_SIZE, ValSer};
