//! Iterator structs for cursor traversal.

use super::{
    HotKvReadError, RawDualKeyValue, RawKeyValue,
    dual_key::DualKeyTraverse,
    dual_table::DualTableTraverse,
    kv::KvTraverse,
    types::{K2Value, RawK2Value},
};
use crate::tables::DualKey;
use core::marker::PhantomData;
use std::borrow::Cow;

// ============================================================================
// Raw Iterator Structs (for base traits)
// ============================================================================

/// Default forward iterator over raw key-value entries.
///
/// This iterator wraps a cursor implementing `KvTraverse` and yields entries
/// by calling `read_next` on each iteration.
pub(crate) struct RawKvIter<'a, C, E> {
    pub(super) cursor: &'a mut C,
    pub(super) done: bool,
    pub(super) _marker: PhantomData<fn() -> E>,
}

impl<'a, C, E> Iterator for RawKvIter<'a, C, E>
where
    C: KvTraverse<E>,
    E: HotKvReadError,
{
    type Item = Result<RawKeyValue<'a>, E>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        // SAFETY: We're using a mutable borrow of the cursor, so the lifetime
        // of the returned data is correctly tied to this iterator.
        // We transmute the lifetime because the borrow checker can't see
        // that the cursor's internal state keeps the data alive.
        let result = unsafe {
            std::mem::transmute::<
                Result<Option<RawKeyValue<'_>>, E>,
                Result<Option<RawKeyValue<'a>>, E>,
            >(self.cursor.read_next())
        };
        match result {
            Ok(Some(kv)) => Some(Ok(kv)),
            Ok(None) => {
                self.done = true;
                None
            }
            Err(e) => {
                self.done = true;
                Some(Err(e))
            }
        }
    }
}

/// Default forward iterator over raw dual-keyed entries.
///
/// This iterator wraps a cursor implementing `DualKeyTraverse` and yields
/// entries by calling `read_next` on each iteration.
pub(crate) struct RawDualKeyIter<'a, C, E> {
    pub(super) cursor: &'a mut C,
    pub(super) done: bool,
    pub(super) _marker: PhantomData<fn() -> E>,
}

impl<'a, C, E> Iterator for RawDualKeyIter<'a, C, E>
where
    C: DualKeyTraverse<E>,
    E: HotKvReadError,
{
    type Item = Result<RawDualKeyValue<'a>, E>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        // SAFETY: Same rationale as RawKvIter - the cursor's internal state
        // keeps the data alive for the duration of iteration.
        let result = unsafe {
            std::mem::transmute::<
                Result<Option<RawDualKeyValue<'_>>, E>,
                Result<Option<RawDualKeyValue<'a>>, E>,
            >(self.cursor.read_next())
        };
        match result {
            Ok(Some(kv)) => Some(Ok(kv)),
            Ok(None) => {
                self.done = true;
                None
            }
            Err(e) => {
                self.done = true;
                Some(Err(e))
            }
        }
    }
}

/// Default forward iterator over raw k2-value entries within a single k1.
///
/// This iterator wraps a cursor implementing `DualKeyTraverse` and yields
/// `(k2, value)` pairs while k1 remains unchanged. The iterator stops when k1
/// changes or the table is exhausted.
pub(crate) struct RawDualKeyK2Iter<'a, C, E> {
    pub(super) cursor: &'a mut C,
    pub(super) done: bool,
    pub(super) first_entry: Option<(Vec<u8>, Vec<u8>)>,
    pub(super) _marker: PhantomData<fn() -> E>,
}

impl<'a, C, E> Iterator for RawDualKeyK2Iter<'a, C, E>
where
    C: DualKeyTraverse<E>,
    E: HotKvReadError,
{
    type Item = Result<RawK2Value<'a>, E>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        if let Some((k2, v)) = self.first_entry.take() {
            return Some(Ok((Cow::Owned(k2), Cow::Owned(v))));
        }
        // SAFETY: Same rationale as RawKvIter
        let result = unsafe {
            std::mem::transmute::<
                Result<Option<RawDualKeyValue<'_>>, E>,
                Result<Option<RawDualKeyValue<'a>>, E>,
            >(self.cursor.next_k2())
        };
        match result {
            Ok(Some((_k1, k2, v))) => Some(Ok((k2, v))),
            Ok(None) => {
                self.done = true;
                None
            }
            Err(e) => {
                self.done = true;
                Some(Err(e))
            }
        }
    }
}

// ============================================================================
// Typed Iterator Structs (for extension traits)
// ============================================================================

/// Forward iterator over k2-value pairs within a single k1.
///
/// This iterator wraps a cursor and yields `(k2, value)` pairs by calling
/// `next_k2()` on each iteration. The iterator stops when there are no more
/// k2 entries for the current k1 or when an error occurs.
pub(crate) struct DualTableK2Iter<'a, C, T, E>
where
    T: DualKey,
{
    pub(super) cursor: &'a mut C,
    pub(super) done: bool,
    pub(super) first_entry: Option<K2Value<T>>,
    pub(super) _marker: PhantomData<fn() -> (T, E)>,
}

impl<'a, C, T, E> Iterator for DualTableK2Iter<'a, C, T, E>
where
    C: DualKeyTraverse<E>,
    T: DualKey,
    E: HotKvReadError,
{
    type Item = Result<K2Value<T>, E>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        if let Some(entry) = self.first_entry.take() {
            return Some(Ok(entry));
        }
        match DualTableTraverse::<T, E>::next_k2(self.cursor) {
            Ok(Some((_k1, k2, v))) => Some(Ok((k2, v))),
            Ok(None) => {
                self.done = true;
                None
            }
            Err(e) => {
                self.done = true;
                Some(Err(e))
            }
        }
    }
}
