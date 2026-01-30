//! Cursor traversal traits and typed wrappers for database navigation.
//!
//! This module provides a layered abstraction for database cursor traversal:
//!
//! 1. **Raw traits** (`KvTraverse`, `DualKeyTraverse`) operate on raw bytes
//! 2. **Typed extension traits** (`TableTraverse`, `DualTableTraverse`) add
//!    automatic (de)serialization
//! 3. **Cursor wrappers** (`TableCursor`, `DualTableCursor`) provide convenient
//!    method-based access
//!
//! ## Type Hierarchy
//!
//! | Category | Item | Description |
//! |----------|------|-------------|
//! | **Raw Traits** | | |
//! | | [`KvTraverse`] | Single-key cursor traversal |
//! | | [`KvTraverseMut`] | Single-key traversal + mutation |
//! | | [`DualKeyTraverse`] | Dual-key cursor traversal |
//! | | [`DualKeyTraverseMut`] | Dual-key traversal + mutation |
//! | **Typed Extension Traits** | | |
//! | | [`TableTraverse`] | Typed single-key traversal (blanket impl) |
//! | | [`TableTraverseMut`] | Typed single-key mutation (blanket impl) |
//! | | [`DualTableTraverse`] | Typed dual-key traversal (blanket impl) |
//! | | [`DualTableTraverseMut`] | Typed dual-key mutation (blanket impl) |
//! | **Cursor Wrappers** | | |
//! | | [`TableCursor`] | Typed wrapper for single-key cursors |
//! | | [`DualTableCursor`] | Typed wrapper for dual-key cursors |
//! | **Types** | | |
//! | | [`DualKeyItem`] | Enum avoiding k1 clones in iteration |
//! | | [`RawK2Value`] | Raw (k2, value) pair type alias |
//! | | [`K2Value`] | Typed (k2, value) pair type alias |
//! | | [`RawDualKeyItem`] | Raw dual-key item type alias |
//!
//! ## Implementation Guide
//!
//! ### Required Methods (must implement)
//!
//! **`KvTraverse<E>`:**
//! - `first`, `last`, `exact`, `lower_bound`, `read_next`, `read_prev`
//!
//! **`KvTraverseMut<E>`:**
//! - `delete_current`
//!
//! **`DualKeyTraverse<E>`:**
//! - `first`, `last`, `read_next`, `read_prev`, `exact_dual`, `next_dual_above`
//! - `next_k1`, `next_k2`, `last_of_k1`, `previous_k1`, `previous_k2`,
//!   `iter_items`
//!
//! **`DualKeyTraverseMut<E>`:**
//! - `clear_k1`, `delete_current`
//!
//! ### Optional Methods (can override defaults)
//!
//! **`KvTraverse<E>`:**
//! - `iter` - default uses `first()` + repeated `read_next()`
//! - `iter_from` - default uses `lower_bound()` + repeated `read_next()`
//!
//! **`KvTraverseMut<E>`:**
//! - `delete_range` - default iterates and deletes
//! - `delete_range_inclusive` - default iterates and deletes
//!
//! **`DualKeyTraverse<E>`:**
//! - `iter` - default uses `first()` + repeated `read_next()`
//! - `iter_from` - default uses `next_dual_above()` + repeated `read_next()`
//! - `iter_k2` - default uses `next_dual_above()` + repeated `next_k2()`
//!
//! **`DualKeyTraverseMut<E>`:**
//! - `delete_range` - default iterates and deletes
//!
//! ### Blanket Implementations
//!
//! The typed extension traits have blanket implementations:
//! - `impl<C, T, E> TableTraverse<T, E> for C where C: KvTraverse<E>, T:
//!   SingleKey`
//! - `impl<C, T, E> TableTraverseMut<T, E> for C where C: KvTraverseMut<E>, T:
//!   SingleKey`
//! - `impl<C, T, E> DualTableTraverse<T, E> for C where C: DualKeyTraverse<E>,
//!   T: DualKey`
//! - `impl<C, T, E> DualTableTraverseMut<T, E> for C where C:
//!   DualKeyTraverseMut<E>, T: DualKey`
//!
//! Implementations only need to implement the raw traits to get typed access.

mod cursor;
mod dual_key;
mod dual_table;
mod iter;
mod kv;
mod table;
mod types;

// Re-export types from parent module needed by submodules
use super::{DualKeyValue, HotKvReadError, KeyValue, RawDualKeyValue, RawKeyValue, RawValue};

// Re-export all public items
pub use cursor::{DualTableCursor, TableCursor};
pub use dual_key::{DualKeyTraverse, DualKeyTraverseMut};
pub use dual_table::{DualTableTraverse, DualTableTraverseMut};
pub use kv::{KvTraverse, KvTraverseMut};
pub use table::{TableTraverse, TableTraverseMut};
pub use types::{DualKeyItem, K2Value, RawDualKeyItem, RawK2Value};
