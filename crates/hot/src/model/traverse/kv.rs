//! Single-key cursor traversal traits.

use super::{HotKvReadError, RawKeyValue, RawValue, iter::RawKvIter};
use core::marker::PhantomData;
use std::ops::Range;

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
        let first_entry = self.first()?.map(|(k, v)| (k.into_owned(), v.into_owned()));
        Ok(RawKvIter {
            cursor: self,
            done: first_entry.is_none(),
            first_entry,
            _marker: PhantomData,
        })
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
        let first_entry = self.lower_bound(key)?.map(|(k, v)| (k.into_owned(), v.into_owned()));
        Ok(RawKvIter {
            cursor: self,
            done: first_entry.is_none(),
            first_entry,
            _marker: PhantomData,
        })
    }
}

/// Trait for traversing key-value pairs in the database with mutation
/// capabilities.
pub trait KvTraverseMut<E: HotKvReadError>: KvTraverse<E> {
    /// Delete the current key-value pair in the database.
    fn delete_current(&mut self) -> Result<(), E>;

    /// Append a key-value pair to the end of the table.
    ///
    /// Key must be greater than all existing keys. If the key is not greater
    /// than the current maximum key, behavior is backend-specific. The backend
    /// may choose to return an error, or silently fall back to a regular put.
    ///
    /// For some backends, this may be more efficient than a regular put.
    fn append(&mut self, key: &[u8], value: &[u8]) -> Result<(), E>;

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
