//! Typed dual-key table traversal traits.

use super::{
    DualKeyValue, HotKvReadError,
    dual_key::{DualKeyTraverse, DualKeyTraverseMut},
    iter::DualTableK2Iter,
    types::K2Value,
};
use crate::{ser::KeySer, ser::MAX_KEY_SIZE, tables::DualKey};
use core::marker::PhantomData;
use std::ops::RangeInclusive;

/// A typed cursor wrapper for traversing dual-keyed tables.
///
/// This is an extension trait rather than a wrapper struct because MDBX
/// requires specialized implementations for DUPSORT tables that need access
/// to the table type `T` to handle fixed-size values correctly.
pub trait DualTableTraverse<T: DualKey, E: HotKvReadError>: DualKeyTraverse<E> {
    /// Get the first key-value pair in the table.
    fn first(&mut self) -> Result<Option<DualKeyValue<T>>, E> {
        DualKeyTraverse::first(self)?.map(T::decode_kkv_tuple).transpose().map_err(Into::into)
    }

    /// Get the last key-value pair in the table.
    fn last(&mut self) -> Result<Option<DualKeyValue<T>>, E> {
        DualKeyTraverse::last(self)?.map(T::decode_kkv_tuple).transpose().map_err(Into::into)
    }

    /// Get the next key-value pair and advance the cursor.
    fn read_next(&mut self) -> Result<Option<DualKeyValue<T>>, E> {
        DualKeyTraverse::read_next(self)?.map(T::decode_kkv_tuple).transpose().map_err(Into::into)
    }

    /// Get the previous key-value pair and move the cursor backward.
    fn read_prev(&mut self) -> Result<Option<DualKeyValue<T>>, E> {
        DualKeyTraverse::read_prev(self)?.map(T::decode_kkv_tuple).transpose().map_err(Into::into)
    }

    /// Return the EXACT value for the specified dual key if it exists.
    fn exact_dual(&mut self, key1: &T::Key, key2: &T::Key2) -> Result<Option<T::Value>, E> {
        let Some((k1, k2, v)) = DualTableTraverse::next_dual_above(self, key1, key2)? else {
            return Ok(None);
        };

        if k1 == *key1 && k2 == *key2 { Ok(Some(v)) } else { Ok(None) }
    }

    /// Seek to the next key-value pair AT or ABOVE the specified dual key.
    fn next_dual_above(
        &mut self,
        key1: &T::Key,
        key2: &T::Key2,
    ) -> Result<Option<DualKeyValue<T>>, E>;

    /// Seek to the next distinct key1, and return the first key-value pair with that key1.
    fn next_k1(&mut self) -> Result<Option<DualKeyValue<T>>, E>;

    /// Seek to the next distinct key2 for the current key1.
    fn next_k2(&mut self) -> Result<Option<DualKeyValue<T>>, E>;

    /// Seek to the LAST key2 entry for the specified key1.
    fn last_of_k1(&mut self, key1: &T::Key) -> Result<Option<DualKeyValue<T>>, E>;

    /// Move to the LAST key2 entry of the PREVIOUS key1.
    fn previous_k1(&mut self) -> Result<Option<DualKeyValue<T>>, E>;

    /// Move to the PREVIOUS key2 entry for the CURRENT key1.
    fn previous_k2(&mut self) -> Result<Option<DualKeyValue<T>>, E>;

    /// Position at (k1, k2) and iterate forward, crossing k1 boundaries.
    ///
    /// The iterator starts at the first entry with (k1, k2) >= the specified
    /// keys and continues through all subsequent entries.
    fn iter_from(
        &mut self,
        k1: &T::Key,
        k2: &T::Key2,
    ) -> Result<impl Iterator<Item = Result<DualKeyValue<T>, E>> + '_, E>
    where
        T::Key: PartialEq;

    /// Position at first entry and return iterator over all entries.
    fn iter(&mut self) -> Result<impl Iterator<Item = Result<DualKeyValue<T>, E>> + '_, E>
    where
        T::Key: PartialEq;

    /// Iterate all k2 entries within a single k1.
    ///
    /// The iterator yields `(k2, value)` pairs for the specified k1, starting
    /// from the first k2 value, and stops when k1 changes or the table is
    /// exhausted.
    ///
    /// Note: k1 is not included in the output since the caller already knows
    /// it (they passed it in). This avoids redundant allocations.
    fn iter_k2(
        &mut self,
        k1: &T::Key,
    ) -> Result<impl Iterator<Item = Result<K2Value<T>, E>> + '_, E>
    where
        T::Key: PartialEq;
}

impl<C, T, E> DualTableTraverse<T, E> for C
where
    C: DualKeyTraverse<E>,
    T: DualKey,
    E: HotKvReadError,
{
    fn next_dual_above(
        &mut self,
        key1: &T::Key,
        key2: &T::Key2,
    ) -> Result<Option<DualKeyValue<T>>, E> {
        let mut key1_buf = [0u8; MAX_KEY_SIZE];
        let mut key2_buf = [0u8; MAX_KEY_SIZE];
        let key1_bytes = key1.encode_key(&mut key1_buf);
        let key2_bytes = key2.encode_key(&mut key2_buf);

        DualKeyTraverse::next_dual_above(self, key1_bytes, key2_bytes)?
            .map(T::decode_kkv_tuple)
            .transpose()
            .map_err(Into::into)
    }

    fn next_k1(&mut self) -> Result<Option<DualKeyValue<T>>, E> {
        DualKeyTraverse::next_k1(self)?.map(T::decode_kkv_tuple).transpose().map_err(Into::into)
    }

    fn next_k2(&mut self) -> Result<Option<DualKeyValue<T>>, E> {
        DualKeyTraverse::next_k2(self)?.map(T::decode_kkv_tuple).transpose().map_err(Into::into)
    }

    fn last_of_k1(&mut self, key1: &T::Key) -> Result<Option<DualKeyValue<T>>, E> {
        let mut key1_buf = [0u8; MAX_KEY_SIZE];
        let key1_bytes = key1.encode_key(&mut key1_buf);

        DualKeyTraverse::last_of_k1(self, key1_bytes)?
            .map(T::decode_kkv_tuple)
            .transpose()
            .map_err(Into::into)
    }

    fn previous_k1(&mut self) -> Result<Option<DualKeyValue<T>>, E> {
        DualKeyTraverse::previous_k1(self)?.map(T::decode_kkv_tuple).transpose().map_err(Into::into)
    }

    fn previous_k2(&mut self) -> Result<Option<DualKeyValue<T>>, E> {
        DualKeyTraverse::previous_k2(self)?.map(T::decode_kkv_tuple).transpose().map_err(Into::into)
    }

    fn iter_from(
        &mut self,
        k1: &T::Key,
        k2: &T::Key2,
    ) -> Result<impl Iterator<Item = Result<DualKeyValue<T>, E>> + '_, E>
    where
        T::Key: PartialEq,
    {
        // Position cursor at (k1, k2)
        DualTableTraverse::<T, E>::next_dual_above(self, k1, k2)?;
        // Return iterator that decodes from raw
        Ok(DualKeyTraverse::iter(self)?
            .map(|r| r.and_then(|kkv| T::decode_kkv_tuple(kkv).map_err(Into::into))))
    }

    fn iter(&mut self) -> Result<impl Iterator<Item = Result<DualKeyValue<T>, E>> + '_, E>
    where
        T::Key: PartialEq,
    {
        Ok(DualKeyTraverse::iter(self)?
            .map(|r| r.and_then(|kkv| T::decode_kkv_tuple(kkv).map_err(Into::into))))
    }

    fn iter_k2(
        &mut self,
        k1: &T::Key,
    ) -> Result<impl Iterator<Item = Result<K2Value<T>, E>> + '_, E>
    where
        T::Key: PartialEq,
    {
        // Position cursor at first entry for this k1 using raw interface with empty k2
        let mut key1_buf = [0u8; MAX_KEY_SIZE];
        let key1_bytes = k1.encode_key(&mut key1_buf);
        let entry = DualKeyTraverse::next_dual_above(self, key1_bytes, &[])?;
        let Some((found_k1, _, _)) = entry else {
            return Ok(DualTableK2Iter::<'_, C, T, E> {
                cursor: self,
                done: true,
                _marker: PhantomData,
            });
        };
        // Decode the found k1 to check if it matches
        let decoded_k1 = T::decode_key(found_k1)?;
        // If the found k1 doesn't match, we're done
        let done = decoded_k1 != *k1;
        Ok(DualTableK2Iter::<'_, C, T, E> { cursor: self, done, _marker: PhantomData })
    }
}

/// Extension trait for typed dual table traversal with mutation capabilities.
pub trait DualTableTraverseMut<T: DualKey, E: HotKvReadError>: DualKeyTraverseMut<E> {
    /// Delete all K2 entries for the specified K1.
    fn clear_k1(&mut self, key1: &T::Key) -> Result<(), E> {
        let mut key1_buf = [0u8; MAX_KEY_SIZE];
        let key1_bytes = key1.encode_key(&mut key1_buf);
        DualKeyTraverseMut::clear_k1(self, key1_bytes)
    }

    /// Delete the current dual-keyed entry.
    fn delete_current(&mut self) -> Result<(), E> {
        DualKeyTraverseMut::delete_current(self)
    }

    /// Delete a range of dual-keyed entries (inclusive).
    fn delete_range(&mut self, range: RangeInclusive<(T::Key, T::Key2)>) -> Result<(), E>
    where
        Self: Sized,
    {
        let (start_k1, start_k2) = range.start();
        let (end_k1, end_k2) = range.end();

        // Loop using next_dual_above to handle backends where delete auto-advances
        // the cursor (e.g., MDBX) vs those that don't (e.g., in-memory).
        loop {
            let Some((k1, k2, _)) =
                DualTableTraverse::<T, E>::next_dual_above(self, start_k1, start_k2)?
            else {
                break;
            };

            // Check if entry is past end of range (typed comparison)
            if &k1 > end_k1 || (&k1 == end_k1 && &k2 > end_k2) {
                break;
            }

            DualKeyTraverseMut::delete_current(self)?;
        }

        Ok(())
    }

    /// Delete a range of dual-keyed entries and return the removed entries.
    fn take_range(
        &mut self,
        range: RangeInclusive<(T::Key, T::Key2)>,
    ) -> Result<Vec<DualKeyValue<T>>, E>
    where
        Self: Sized,
    {
        let (start_k1, start_k2) = range.start();
        let (end_k1, end_k2) = range.end();

        let mut result = Vec::new();

        // Loop using next_dual_above to handle backends where delete auto-advances
        // the cursor (e.g., MDBX) vs those that don't (e.g., in-memory).
        loop {
            let Some((k1, k2, value)) =
                DualTableTraverse::<T, E>::next_dual_above(self, start_k1, start_k2)?
            else {
                break;
            };

            // Check if entry is past end of range (typed comparison)
            if &k1 > end_k1 || (&k1 == end_k1 && &k2 > end_k2) {
                break;
            }

            result.push((k1, k2, value));
            DualKeyTraverseMut::delete_current(self)?;
        }

        Ok(result)
    }
}

/// Blanket implementation of `DualTableTraverseMut`.
impl<C, T, E> DualTableTraverseMut<T, E> for C
where
    C: DualKeyTraverseMut<E>,
    T: DualKey,
    E: HotKvReadError,
{
}
