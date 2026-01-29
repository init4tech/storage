//! Cursor traversal traits and typed wrappers for database navigation.

use crate::{
    model::{DualKeyValue, HotKvReadError, KeyValue, RawDualKeyValue, RawKeyValue, RawValue},
    ser::{KeySer, MAX_KEY_SIZE},
    tables::{DualKey, SingleKey},
};
use core::marker::PhantomData;
use std::borrow::Cow;
use std::ops::{Range, RangeInclusive};

/// Raw k2-value pair: (key2, value).
///
/// This is returned by `iter_k2()` on dual-keyed tables. The caller already
/// knows k1 (they passed it in), so we don't return it redundantly.
pub type RawK2Value<'a> = (Cow<'a, [u8]>, RawValue<'a>);

/// Typed k2-value pair for a dual-keyed table.
pub type K2Value<T> = (<T as DualKey>::Key2, <T as crate::tables::Table>::Value);

/// Trait for traversing key-value pairs in the database.
pub trait KvTraverse<E: HotKvReadError> {
    /// Set position to the first key-value pair in the database, and return
    /// the KV pair.
    fn first<'a>(&'a mut self) -> Result<Option<RawKeyValue<'a>>, E>;

    /// Set position to the last key-value pair in the database, and return the
    /// KV pair.
    fn last<'a>(&'a mut self) -> Result<Option<RawKeyValue<'a>>, E>;

    /// Set the cursor to specific key in the database, and return the EXACT KV
    /// pair if it exists.
    fn exact<'a>(&'a mut self, key: &[u8]) -> Result<Option<RawValue<'a>>, E>;

    /// Seek to the next key-value pair AT OR ABOVE the specified key in the
    /// database, and return that KV pair.
    fn lower_bound<'a>(&'a mut self, key: &[u8]) -> Result<Option<RawKeyValue<'a>>, E>;

    /// Get the next key-value pair in the database, and advance the cursor.
    ///
    /// Returning `Ok(None)` indicates the cursor is past the end of the
    /// database.
    fn read_next<'a>(&'a mut self) -> Result<Option<RawKeyValue<'a>>, E>;

    /// Get the previous key-value pair in the database, and move the cursor.
    ///
    /// Returning `Ok(None)` indicates the cursor is before the start of the
    /// database.
    fn read_prev<'a>(&'a mut self) -> Result<Option<RawKeyValue<'a>>, E>;

    /// Position at first entry and return iterator over all entries.
    ///
    /// The default implementation uses `first()` followed by repeated
    /// `read_next()` calls. Implementations may override this to use
    /// more efficient native iterators.
    fn iter(&mut self) -> Result<impl Iterator<Item = Result<RawKeyValue<'_>, E>> + '_, E>
    where
        Self: Sized,
    {
        self.first()?;
        Ok(RawKvIter { cursor: self, done: false, _marker: PhantomData })
    }

    /// Position at `key` and return iterator over subsequent entries.
    ///
    /// The iterator starts at the first entry with key >= `key`.
    /// The default implementation uses `lower_bound()` followed by repeated
    /// `read_next()` calls. Implementations may override this to use
    /// more efficient native iterators.
    fn iter_from<'a>(
        &'a mut self,
        key: &[u8],
    ) -> Result<impl Iterator<Item = Result<RawKeyValue<'a>, E>> + 'a, E>
    where
        Self: Sized,
    {
        self.lower_bound(key)?;
        Ok(RawKvIter { cursor: self, done: false, _marker: PhantomData })
    }
}

/// Trait for traversing key-value pairs in the database with mutation
/// capabilities.
pub trait KvTraverseMut<E: HotKvReadError>: KvTraverse<E> {
    /// Delete the current key-value pair in the database.
    fn delete_current(&mut self) -> Result<(), E>;

    /// Delete a range of key-value pairs in the database (exclusive end).
    fn delete_range(&mut self, range: Range<&[u8]>) -> Result<(), E> {
        let Some((key, _)) = self.lower_bound(range.start)? else {
            return Ok(());
        };
        if key.as_ref() >= range.end {
            return Ok(());
        }
        self.delete_current()?;

        while let Some((key, _)) = self.read_next()? {
            if key.as_ref() >= range.end {
                break;
            }
            self.delete_current()?;
        }
        Ok(())
    }

    /// Delete a range of key-value pairs in the database (inclusive end).
    fn delete_range_inclusive(&mut self, start: &[u8], end: &[u8]) -> Result<(), E> {
        let Some((key, _)) = self.lower_bound(start)? else {
            return Ok(());
        };
        if key.as_ref() > end {
            return Ok(());
        }
        self.delete_current()?;

        while let Some((key, _)) = self.read_next()? {
            if key.as_ref() > end {
                break;
            }
            self.delete_current()?;
        }
        Ok(())
    }
}

/// Trait for traversing dual-keyed key-value pairs in the database.
pub trait DualKeyTraverse<E: HotKvReadError> {
    /// Set position to the first key-value pair in the database, and return
    /// the KV pair with both keys.
    fn first<'a>(&'a mut self) -> Result<Option<RawDualKeyValue<'a>>, E>;

    /// Set position to the last key-value pair in the database, and return the
    /// KV pair with both keys.
    fn last<'a>(&'a mut self) -> Result<Option<RawDualKeyValue<'a>>, E>;

    /// Get the next key-value pair in the database, and advance the cursor.
    ///
    /// Returning `Ok(None)` indicates the cursor is past the end of the
    /// database.
    fn read_next<'a>(&'a mut self) -> Result<Option<RawDualKeyValue<'a>>, E>;

    /// Get the previous key-value pair in the database, and move the cursor.
    ///
    /// Returning `Ok(None)` indicates the cursor is before the start of the
    /// database.
    fn read_prev<'a>(&'a mut self) -> Result<Option<RawDualKeyValue<'a>>, E>;

    /// Set the cursor to specific dual key in the database, and return the
    /// EXACT KV pair if it exists.
    ///
    /// Returning `Ok(None)` indicates the exact dual key does not exist.
    fn exact_dual<'a>(&'a mut self, key1: &[u8], key2: &[u8]) -> Result<Option<RawValue<'a>>, E>;

    /// Seek to the next key-value pair AT or ABOVE the specified dual key in
    /// the database, and return that KV pair.
    ///
    /// Returning `Ok(None)` indicates there are no more key-value pairs above
    /// the specified dual key.
    fn next_dual_above<'a>(
        &'a mut self,
        key1: &[u8],
        key2: &[u8],
    ) -> Result<Option<RawDualKeyValue<'a>>, E>;

    /// Move the cursor to the next distinct key1, and return the first
    /// key-value pair with that key1.
    ///
    /// Returning `Ok(None)` indicates there are no more distinct key1 values.
    fn next_k1<'a>(&'a mut self) -> Result<Option<RawDualKeyValue<'a>>, E>;

    /// Move the cursor to the next distinct key2 for the current key1, and
    /// return the first key-value pair with that key2.
    fn next_k2<'a>(&'a mut self) -> Result<Option<RawDualKeyValue<'a>>, E>;

    /// Seek to the LAST key2 entry for the specified key1.
    ///
    /// This positions the cursor at the last duplicate value for the given key1.
    /// Returning `Ok(None)` indicates the key1 does not exist.
    fn last_of_k1<'a>(&'a mut self, key1: &[u8]) -> Result<Option<RawDualKeyValue<'a>>, E>;

    /// Move the cursor to the LAST key2 entry of the PREVIOUS key1.
    ///
    /// This is the reverse of `next_k1` - it moves backward to the previous distinct
    /// key1 and positions at its last key2 entry.
    /// Returning `Ok(None)` indicates there is no previous key1.
    fn previous_k1<'a>(&'a mut self) -> Result<Option<RawDualKeyValue<'a>>, E>;

    /// Move the cursor to the PREVIOUS key2 entry for the CURRENT key1.
    ///
    /// This is the reverse of `next_k2` - it moves backward within the current key1's
    /// duplicate values.
    /// Returning `Ok(None)` indicates there is no previous key2 for this key1.
    fn previous_k2<'a>(&'a mut self) -> Result<Option<RawDualKeyValue<'a>>, E>;

    /// Position at first entry and return iterator over all entries.
    ///
    /// The default implementation uses `first()` followed by repeated
    /// `read_next()` calls. Implementations may override this to use
    /// more efficient native iterators.
    fn iter(&mut self) -> Result<impl Iterator<Item = Result<RawDualKeyValue<'_>, E>> + '_, E>
    where
        Self: Sized,
    {
        self.first()?;
        Ok(RawDualKeyIter { cursor: self, done: false, _marker: PhantomData })
    }

    /// Position at (k1, k2) and return iterator over subsequent entries.
    ///
    /// The iterator starts at the first entry with (k1, k2) >= the specified
    /// keys and continues through all subsequent entries, crossing k1 boundaries.
    fn iter_from<'a>(
        &'a mut self,
        k1: &[u8],
        k2: &[u8],
    ) -> Result<impl Iterator<Item = Result<RawDualKeyValue<'a>, E>> + 'a, E>
    where
        Self: Sized,
    {
        self.next_dual_above(k1, k2)?;
        Ok(RawDualKeyIter { cursor: self, done: false, _marker: PhantomData })
    }

    /// Iterate all k2 entries within a single k1.
    ///
    /// The iterator yields `(k2, value)` pairs for the specified k1, starting
    /// from the first k2 value, and stops when k1 changes or the table is
    /// exhausted.
    ///
    /// Note: k1 is not included in the output since the caller already knows
    /// it (they passed it in). This avoids redundant allocations.
    fn iter_k2<'a>(
        &'a mut self,
        k1: &[u8],
    ) -> Result<impl Iterator<Item = Result<RawK2Value<'a>, E>> + 'a, E>
    where
        Self: Sized,
    {
        // Position at first entry for this k1 (using empty slice as minimum k2)
        let entry = self.next_dual_above(k1, &[])?;
        let Some((found_k1, _, _)) = entry else {
            return Ok(RawDualKeyK2Iter { cursor: self, done: true, _marker: PhantomData });
        };
        // If the found k1 doesn't match, we're done
        let done = found_k1.as_ref() != k1;
        Ok(RawDualKeyK2Iter { cursor: self, done, _marker: PhantomData })
    }
}

/// Trait for traversing dual-keyed key-value pairs with mutation capabilities.
pub trait DualKeyTraverseMut<E: HotKvReadError>: DualKeyTraverse<E> {
    /// Delete all K2 entries for the specified K1.
    ///
    /// This positions the cursor at the given K1 and removes all associated
    /// K2 entries in a single operation.
    ///
    /// Returns `Ok(())` if the K1 was cleared or didn't exist.
    fn clear_k1(&mut self, key1: &[u8]) -> Result<(), E>;

    /// Delete the current dual-keyed entry.
    fn delete_current(&mut self) -> Result<(), E>;

    /// Delete a range of dual-keyed entries (inclusive).
    ///
    /// Deletes all entries where `(k1, k2) >= (start_k1, start_k2)` and
    /// `(k1, k2) <= (end_k1, end_k2)`.
    fn delete_range(
        &mut self,
        start_k1: &[u8],
        start_k2: &[u8],
        end_k1: &[u8],
        end_k2: &[u8],
    ) -> Result<(), E> {
        let Some((k1, k2, _)) = self.next_dual_above(start_k1, start_k2)? else {
            return Ok(());
        };

        // Check if first entry is past end of range
        if k1.as_ref() > end_k1 || (k1.as_ref() == end_k1 && k2.as_ref() > end_k2) {
            return Ok(());
        }

        self.delete_current()?;

        while let Some((k1, k2, _)) = self.read_next()? {
            // Check if we're past end of range
            if k1.as_ref() > end_k1 || (k1.as_ref() == end_k1 && k2.as_ref() > end_k2) {
                break;
            }
            self.delete_current()?;
        }

        Ok(())
    }
}

// ============================================================================
// Typed Extension Traits
// ============================================================================

/// Extension trait for typed table traversal.
///
/// This trait provides type-safe access to table entries by encoding keys
/// and decoding values according to the table's schema.
pub trait TableTraverse<T: SingleKey, E: HotKvReadError>: KvTraverse<E> {
    /// Get the first key-value pair in the table.
    fn first(&mut self) -> Result<Option<KeyValue<T>>, E> {
        KvTraverse::first(self)?.map(T::decode_kv_tuple).transpose().map_err(Into::into)
    }

    /// Get the last key-value pair in the table.
    fn last(&mut self) -> Result<Option<KeyValue<T>>, E> {
        KvTraverse::last(self)?.map(T::decode_kv_tuple).transpose().map_err(Into::into)
    }

    /// Set the cursor to a specific key and return the EXACT value if it exists.
    fn exact(&mut self, key: &T::Key) -> Result<Option<T::Value>, E> {
        let mut key_buf = [0u8; MAX_KEY_SIZE];
        let key_bytes = key.encode_key(&mut key_buf);

        KvTraverse::exact(self, key_bytes)?.map(T::decode_value).transpose().map_err(Into::into)
    }

    /// Seek to the next key-value pair AT OR ABOVE the specified key.
    fn lower_bound(&mut self, key: &T::Key) -> Result<Option<KeyValue<T>>, E> {
        let mut key_buf = [0u8; MAX_KEY_SIZE];
        let key_bytes = key.encode_key(&mut key_buf);

        KvTraverse::lower_bound(self, key_bytes)?
            .map(T::decode_kv_tuple)
            .transpose()
            .map_err(Into::into)
    }

    /// Get the next key-value pair and advance the cursor.
    fn read_next(&mut self) -> Result<Option<KeyValue<T>>, E> {
        KvTraverse::read_next(self)?.map(T::decode_kv_tuple).transpose().map_err(Into::into)
    }

    /// Get the previous key-value pair and move the cursor backward.
    fn read_prev(&mut self) -> Result<Option<KeyValue<T>>, E> {
        KvTraverse::read_prev(self)?.map(T::decode_kv_tuple).transpose().map_err(Into::into)
    }

    /// Position at `key` and return iterator over subsequent entries.
    ///
    /// The iterator starts at the first entry with key >= `key`.
    fn iter_from(
        &mut self,
        key: &T::Key,
    ) -> Result<impl Iterator<Item = Result<KeyValue<T>, E>> + '_, E>
    where
        Self: Sized,
    {
        // Position the cursor at the key
        TableTraverse::<T, E>::lower_bound(self, key)?;
        // Return iterator that decodes from raw
        Ok(KvTraverse::iter(self)?
            .map(|r| r.and_then(|kv| T::decode_kv_tuple(kv).map_err(Into::into))))
    }

    /// Position at first entry and return iterator over all entries.
    fn iter(&mut self) -> Result<impl Iterator<Item = Result<KeyValue<T>, E>> + '_, E>
    where
        Self: Sized,
    {
        Ok(KvTraverse::iter(self)?
            .map(|r| r.and_then(|kv| T::decode_kv_tuple(kv).map_err(Into::into))))
    }
}

/// Blanket implementation of `TableTraverse` for any cursor that implements `KvTraverse`.
impl<C, T, E> TableTraverse<T, E> for C
where
    C: KvTraverse<E>,
    T: SingleKey,
    E: HotKvReadError,
{
}

/// Extension trait for typed table traversal with mutation capabilities.
pub trait TableTraverseMut<T: SingleKey, E: HotKvReadError>: KvTraverseMut<E> {
    /// Delete the current key-value pair.
    fn delete_current(&mut self) -> Result<(), E> {
        KvTraverseMut::delete_current(self)
    }

    /// Delete a range of key-value pairs (exclusive end).
    fn delete_range(&mut self, range: Range<T::Key>) -> Result<(), E> {
        let mut start_key_buf = [0u8; MAX_KEY_SIZE];
        let mut end_key_buf = [0u8; MAX_KEY_SIZE];
        let start_key_bytes = range.start.encode_key(&mut start_key_buf);
        let end_key_bytes = range.end.encode_key(&mut end_key_buf);

        KvTraverseMut::delete_range(self, start_key_bytes..end_key_bytes)
    }

    /// Delete a range of key-value pairs (inclusive end).
    fn delete_range_inclusive(&mut self, range: RangeInclusive<T::Key>) -> Result<(), E>
    where
        Self: Sized,
    {
        let Some((key, _)) = TableTraverse::<T, E>::lower_bound(self, range.start())? else {
            return Ok(());
        };
        if !range.contains(&key) {
            return Ok(());
        }
        KvTraverseMut::delete_current(self)?;

        while let Some((key, _)) = TableTraverse::<T, E>::read_next(self)? {
            if !range.contains(&key) {
                break;
            }
            KvTraverseMut::delete_current(self)?;
        }
        Ok(())
    }

    /// Delete a range of key-value pairs and return the removed entries.
    fn take_range(&mut self, range: RangeInclusive<T::Key>) -> Result<Vec<KeyValue<T>>, E>
    where
        Self: Sized,
    {
        let mut result = Vec::new();

        let Some((key, value)) = TableTraverse::<T, E>::lower_bound(self, range.start())? else {
            return Ok(result);
        };
        if !range.contains(&key) {
            return Ok(result);
        }
        result.push((key, value));
        KvTraverseMut::delete_current(self)?;

        while let Some((key, value)) = TableTraverse::<T, E>::read_next(self)? {
            if !range.contains(&key) {
                break;
            }
            result.push((key, value));
            KvTraverseMut::delete_current(self)?;
        }

        Ok(result)
    }
}

/// Blanket implementation of [`TableTraverseMut`] for any cursor that implements [`KvTraverseMut`].
impl<C, T, E> TableTraverseMut<T, E> for C
where
    C: KvTraverseMut<E>,
    T: SingleKey,
    E: HotKvReadError,
{
}

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

// ============================================================================
// Raw Iterator Structs (for base traits)
// ============================================================================

/// Default forward iterator over raw key-value entries.
///
/// This iterator wraps a cursor implementing `KvTraverse` and yields entries
/// by calling `read_next` on each iteration.
pub struct RawKvIter<'a, C, E> {
    cursor: &'a mut C,
    done: bool,
    _marker: PhantomData<fn() -> E>,
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
pub struct RawDualKeyIter<'a, C, E> {
    cursor: &'a mut C,
    done: bool,
    _marker: PhantomData<fn() -> E>,
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
pub struct RawDualKeyK2Iter<'a, C, E> {
    cursor: &'a mut C,
    done: bool,
    _marker: PhantomData<fn() -> E>,
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
pub struct DualTableK2Iter<'a, C, T, E>
where
    T: DualKey,
{
    cursor: &'a mut C,
    done: bool,
    _marker: PhantomData<fn() -> (T, E)>,
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

// ============================================================================
// Wrapper Structs
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
