//! Typed cursor wrapper structs.

use super::{
    DualKeyValue, HotKvReadError, KeyValue,
    dual_table::{DualTableTraverse, DualTableTraverseMut},
    kv::{KvTraverse, KvTraverseMut},
    table::{TableTraverse, TableTraverseMut},
    types::K2Value,
};
use crate::tables::{DualKey, SingleKey};
use core::marker::PhantomData;
use std::ops::{Range, RangeInclusive};

// ============================================================================
// TableCursor
// ============================================================================

/// A wrapper struct for typed table traversal.
///
/// This struct wraps a raw cursor and provides type-safe access to table
/// entries. It implements `TableTraverse<T, E>` by delegating to the inner
/// cursor.
#[derive(Debug)]
pub struct TableCursor<C, T, E> {
    inner: C,
    _marker: PhantomData<fn() -> (T, E)>,
}

impl<C, T, E> TableCursor<C, T, E> {
    /// Create a new typed table cursor wrapper.
    pub const fn new(cursor: C) -> Self {
        Self { inner: cursor, _marker: PhantomData }
    }

    /// Get a reference to the inner cursor.
    pub const fn inner(&self) -> &C {
        &self.inner
    }

    /// Get a mutable reference to the inner cursor.
    pub const fn inner_mut(&mut self) -> &mut C {
        &mut self.inner
    }

    /// Consume the wrapper and return the inner cursor.
    pub fn into_inner(self) -> C {
        self.inner
    }
}

impl<C, T, E> TableCursor<C, T, E>
where
    C: KvTraverse<E>,
    T: SingleKey,
    E: HotKvReadError,
{
    /// Get the first key-value pair in the table.
    pub fn first(&mut self) -> Result<Option<KeyValue<T>>, E> {
        TableTraverse::<T, E>::first(&mut self.inner)
    }

    /// Get the last key-value pair in the table.
    pub fn last(&mut self) -> Result<Option<KeyValue<T>>, E> {
        TableTraverse::<T, E>::last(&mut self.inner)
    }

    /// Set the cursor to a specific key and return the EXACT value if it exists.
    pub fn exact(&mut self, key: &T::Key) -> Result<Option<T::Value>, E> {
        TableTraverse::<T, E>::exact(&mut self.inner, key)
    }

    /// Seek to the next key-value pair AT OR ABOVE the specified key.
    pub fn lower_bound(&mut self, key: &T::Key) -> Result<Option<KeyValue<T>>, E> {
        TableTraverse::<T, E>::lower_bound(&mut self.inner, key)
    }

    /// Get the next key-value pair and advance the cursor.
    pub fn read_next(&mut self) -> Result<Option<KeyValue<T>>, E> {
        TableTraverse::<T, E>::read_next(&mut self.inner)
    }

    /// Get the previous key-value pair and move the cursor backward.
    pub fn read_prev(&mut self) -> Result<Option<KeyValue<T>>, E> {
        TableTraverse::<T, E>::read_prev(&mut self.inner)
    }

    /// Position at `key` and return iterator over subsequent entries.
    ///
    /// The iterator starts at the first entry with key >= `key`.
    pub fn iter_from(
        &mut self,
        key: &T::Key,
    ) -> Result<impl Iterator<Item = Result<KeyValue<T>, E>> + '_, E> {
        TableTraverse::<T, E>::iter_from(&mut self.inner, key)
    }

    /// Position at first entry and return iterator over all entries.
    pub fn iter(&mut self) -> Result<impl Iterator<Item = Result<KeyValue<T>, E>> + '_, E> {
        TableTraverse::<T, E>::iter(&mut self.inner)
    }
}

impl<C, T, E> TableCursor<C, T, E>
where
    C: KvTraverseMut<E>,
    T: SingleKey,
    E: HotKvReadError,
{
    /// Delete the current key-value pair.
    pub fn delete_current(&mut self) -> Result<(), E> {
        TableTraverseMut::<T, E>::delete_current(&mut self.inner)
    }

    /// Delete a range of key-value pairs (exclusive end).
    pub fn delete_range(&mut self, range: Range<T::Key>) -> Result<(), E> {
        TableTraverseMut::<T, E>::delete_range(&mut self.inner, range)
    }

    /// Delete a range of key-value pairs (inclusive end).
    pub fn delete_range_inclusive(&mut self, range: RangeInclusive<T::Key>) -> Result<(), E> {
        TableTraverseMut::<T, E>::delete_range_inclusive(&mut self.inner, range)
    }

    /// Delete a range of key-value pairs and return the removed entries.
    pub fn take_range(&mut self, range: RangeInclusive<T::Key>) -> Result<Vec<KeyValue<T>>, E> {
        TableTraverseMut::<T, E>::take_range(&mut self.inner, range)
    }
}

// ============================================================================
// DualTableCursor
// ============================================================================

/// A wrapper struct for typed dual-keyed table traversal.
///
/// This struct wraps a raw cursor and provides type-safe access to dual-keyed
/// table entries. It delegates to the `DualTableTraverse<T, E>` trait
/// implementation on the inner cursor.
#[derive(Debug)]
pub struct DualTableCursor<C, T, E> {
    inner: C,
    _marker: PhantomData<fn() -> (T, E)>,
}

impl<C, T, E> DualTableCursor<C, T, E> {
    /// Create a new typed dual-keyed table cursor wrapper.
    pub const fn new(cursor: C) -> Self {
        Self { inner: cursor, _marker: PhantomData }
    }

    /// Get a reference to the inner cursor.
    pub const fn inner(&self) -> &C {
        &self.inner
    }

    /// Get a mutable reference to the inner cursor.
    pub const fn inner_mut(&mut self) -> &mut C {
        &mut self.inner
    }

    /// Consume the wrapper and return the inner cursor.
    pub fn into_inner(self) -> C {
        self.inner
    }
}

impl<C, T, E> DualTableCursor<C, T, E>
where
    C: DualTableTraverse<T, E>,
    T: DualKey,
    E: HotKvReadError,
{
    /// Return the EXACT value for the specified dual key if it exists.
    pub fn exact_dual(&mut self, key1: &T::Key, key2: &T::Key2) -> Result<Option<T::Value>, E> {
        DualTableTraverse::<T, E>::exact_dual(&mut self.inner, key1, key2)
    }

    /// Seek to the next key-value pair AT or ABOVE the specified dual key.
    pub fn next_dual_above(
        &mut self,
        key1: &T::Key,
        key2: &T::Key2,
    ) -> Result<Option<DualKeyValue<T>>, E> {
        DualTableTraverse::<T, E>::next_dual_above(&mut self.inner, key1, key2)
    }

    /// Seek to the next distinct key1, and return the first key-value pair with that key1.
    pub fn next_k1(&mut self) -> Result<Option<DualKeyValue<T>>, E> {
        DualTableTraverse::<T, E>::next_k1(&mut self.inner)
    }

    /// Seek to the next distinct key2 for the current key1.
    pub fn next_k2(&mut self) -> Result<Option<DualKeyValue<T>>, E> {
        DualTableTraverse::<T, E>::next_k2(&mut self.inner)
    }

    /// Seek to the LAST key2 entry for the specified key1.
    pub fn last_of_k1(&mut self, key1: &T::Key) -> Result<Option<DualKeyValue<T>>, E> {
        DualTableTraverse::<T, E>::last_of_k1(&mut self.inner, key1)
    }

    /// Move to the LAST key2 entry of the PREVIOUS key1.
    pub fn previous_k1(&mut self) -> Result<Option<DualKeyValue<T>>, E> {
        DualTableTraverse::<T, E>::previous_k1(&mut self.inner)
    }

    /// Move to the PREVIOUS key2 entry for the CURRENT key1.
    pub fn previous_k2(&mut self) -> Result<Option<DualKeyValue<T>>, E> {
        DualTableTraverse::<T, E>::previous_k2(&mut self.inner)
    }
}

// Also provide access to first/last/read_next/read_prev methods for dual-keyed cursors
impl<C, T, E> DualTableCursor<C, T, E>
where
    C: DualTableTraverse<T, E>,
    T: DualKey,
    E: HotKvReadError,
{
    /// Get the first key-value pair in the table.
    pub fn first(&mut self) -> Result<Option<DualKeyValue<T>>, E> {
        DualTableTraverse::<T, E>::first(&mut self.inner)
    }

    /// Get the last key-value pair in the table.
    pub fn last(&mut self) -> Result<Option<DualKeyValue<T>>, E> {
        DualTableTraverse::<T, E>::last(&mut self.inner)
    }

    /// Get the next key-value pair and advance the cursor.
    pub fn read_next(&mut self) -> Result<Option<DualKeyValue<T>>, E> {
        DualTableTraverse::<T, E>::read_next(&mut self.inner)
    }

    /// Get the previous key-value pair and move the cursor backward.
    pub fn read_prev(&mut self) -> Result<Option<DualKeyValue<T>>, E> {
        DualTableTraverse::<T, E>::read_prev(&mut self.inner)
    }
}

// Iterator methods for DualTableCursor - require T::Key: PartialEq for k2 iteration
impl<C, T, E> DualTableCursor<C, T, E>
where
    C: DualTableTraverse<T, E>,
    T: DualKey,
    T::Key: PartialEq,
    E: HotKvReadError,
{
    /// Position at (k1, k2) and iterate forward, crossing k1 boundaries.
    ///
    /// The iterator starts at the first entry with (k1, k2) >= the specified
    /// keys and continues through all subsequent entries.
    pub fn iter_from(
        &mut self,
        k1: &T::Key,
        k2: &T::Key2,
    ) -> Result<impl Iterator<Item = Result<DualKeyValue<T>, E>> + '_, E> {
        DualTableTraverse::<T, E>::iter_from(&mut self.inner, k1, k2)
    }

    /// Position at first entry and return iterator over all entries.
    pub fn iter(&mut self) -> Result<impl Iterator<Item = Result<DualKeyValue<T>, E>> + '_, E> {
        DualTableTraverse::<T, E>::iter(&mut self.inner)
    }

    /// Iterate all k2 entries within a single k1.
    ///
    /// The iterator yields `(k2, value)` pairs for the specified k1, starting
    /// from the first k2 value, and stops when k1 changes or the table is
    /// exhausted.
    ///
    /// Note: k1 is not included in the output since the caller already knows
    /// it (they passed it in). This avoids redundant allocations.
    pub fn iter_k2(
        &mut self,
        k1: &T::Key,
    ) -> Result<impl Iterator<Item = Result<K2Value<T>, E>> + '_, E> {
        DualTableTraverse::<T, E>::iter_k2(&mut self.inner, k1)
    }
}

impl<C, T, E> DualTableCursor<C, T, E>
where
    C: DualTableTraverseMut<T, E>,
    T: DualKey,
    E: HotKvReadError,
{
    /// Delete the current key-value pair.
    pub fn delete_current(&mut self) -> Result<(), E> {
        DualTableTraverseMut::<T, E>::delete_current(&mut self.inner)
    }

    /// Delete all K2 entries for the specified K1.
    pub fn clear_k1(&mut self, key1: &T::Key) -> Result<(), E> {
        DualTableTraverseMut::<T, E>::clear_k1(&mut self.inner, key1)
    }

    /// Delete a range of dual-keyed entries (inclusive).
    pub fn delete_range(&mut self, range: RangeInclusive<(T::Key, T::Key2)>) -> Result<(), E> {
        DualTableTraverseMut::<T, E>::delete_range(&mut self.inner, range)
    }

    /// Delete a range of dual-keyed entries and return the removed entries.
    pub fn take_range(
        &mut self,
        range: RangeInclusive<(T::Key, T::Key2)>,
    ) -> Result<Vec<DualKeyValue<T>>, E> {
        DualTableTraverseMut::<T, E>::take_range(&mut self.inner, range)
    }
}
