//! Typed single-key table traversal traits.

use super::{HotKvReadError, KeyValue, kv::KvTraverse, kv::KvTraverseMut};
use crate::{ser::KeySer, ser::MAX_KEY_SIZE, tables::SingleKey};
use std::ops::{Range, RangeInclusive};

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
