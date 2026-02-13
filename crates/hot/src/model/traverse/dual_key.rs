//! Dual-key cursor traversal traits.

use super::{
    HotKvReadError, RawDualKeyValue, RawValue,
    iter::{RawDualKeyIter, RawDualKeyK2Iter},
    types::{RawDualKeyItem, RawK2Value},
};
use core::marker::PhantomData;

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
        let first_entry =
            self.first()?.map(|(k1, k2, v)| (k1.into_owned(), k2.into_owned(), v.into_owned()));
        Ok(RawDualKeyIter {
            cursor: self,
            done: first_entry.is_none(),
            first_entry,
            _marker: PhantomData,
        })
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
        let first_entry = self
            .next_dual_above(k1, k2)?
            .map(|(k1, k2, v)| (k1.into_owned(), k2.into_owned(), v.into_owned()));
        Ok(RawDualKeyIter {
            cursor: self,
            done: first_entry.is_none(),
            first_entry,
            _marker: PhantomData,
        })
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
        let Some((found_k1, k2, v)) = entry else {
            return Ok(RawDualKeyK2Iter {
                cursor: self,
                done: true,
                first_entry: None,
                _marker: PhantomData,
            });
        };
        // If the found k1 doesn't match, we're done
        if found_k1.as_ref() != k1 {
            return Ok(RawDualKeyK2Iter {
                cursor: self,
                done: true,
                first_entry: None,
                _marker: PhantomData,
            });
        }
        // Convert to owned before constructing the iterator to release the
        // mutable borrow on self from `next_dual_above`.
        let first = (k2.into_owned(), v.into_owned());
        Ok(RawDualKeyK2Iter {
            cursor: self,
            done: false,
            first_entry: Some(first),
            _marker: PhantomData,
        })
    }

    /// Position at first entry and return iterator yielding [`DualKeyItem`].
    ///
    /// This is more efficient than [`Self::iter()`] as it avoids cloning k1 for
    /// every entry within the same k1 group. The iterator yields
    /// `DualKeyItem::NewK1` when k1 changes and `DualKeyItem::SameK1` for
    /// subsequent entries with the same k1.
    ///
    /// [`DualKeyItem`]: super::DualKeyItem
    fn iter_items(&mut self) -> Result<impl Iterator<Item = Result<RawDualKeyItem<'_>, E>> + '_, E>
    where
        Self: Sized;
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

    /// Append a duplicate entry to a DUPSORT table.
    ///
    /// For some backends, this may be more efficient than a regular put.
    ///
    /// The k2 must be greater than all existing k2s for the given k1.
    ///
    /// If the ordering constraint is violated, behavior is backend-specific.
    /// The backend may choose to return an error, or silently fall back to a
    /// regular put_dual.
    ///
    /// This is more efficient than `put` when inserting duplicates in sorted
    /// order, as optimized backends can skip sub-tree traversal.
    fn append_dual(&mut self, k1: &[u8], k2: &[u8], value: &[u8]) -> Result<(), E>;

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
