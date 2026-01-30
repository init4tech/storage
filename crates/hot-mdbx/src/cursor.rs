//! Cursor wrapper for libmdbx-sys.

use crate::{FixedSizeInfo, MdbxError};
use signet_hot::{
    MAX_FIXED_VAL_SIZE, MAX_KEY_SIZE,
    model::{
        DualKeyItem, DualKeyTraverse, DualKeyTraverseMut, KvTraverse, KvTraverseMut,
        RawDualKeyItem, RawDualKeyValue, RawKeyValue, RawValue,
    },
};
use signet_libmdbx::{DupItem, Ro, Rw, RwSync, TransactionKind, tx::WriteMarker};
use std::{
    borrow::Cow,
    ops::{Deref, DerefMut},
};

/// Read only Cursor.
pub type CursorRo<'a> = Cursor<'a, Ro>;

/// Read write cursor.
pub type CursorRw<'a> = Cursor<'a, Rw>;

/// Synchronized read only cursor.
pub type CursorRoSync<'a> = Cursor<'a, signet_libmdbx::RoSync>;

/// Synchronized read write cursor.
pub type CursorRwSync<'a> = Cursor<'a, RwSync>;

/// Cursor wrapper to access KV items.
///
/// The inner cursor type uses `K::Inner` which is the transaction's internal
/// pointer access type:
/// - For `RO`: `K::Inner = RoGuard`
/// - For `RW`: `K::Inner = RwUnsync`
pub struct Cursor<'a, K: TransactionKind> {
    /// Inner `libmdbx` cursor.
    pub(crate) inner: signet_libmdbx::Cursor<'a, K>,

    /// Fixed size info for this table.
    fsi: FixedSizeInfo,

    /// Scratch buffer for key2 operations in DUPSORT tables.
    /// Sized to hold key2 + fixed value for DUP_FIXED tables.
    buf: [u8; MAX_KEY_SIZE + MAX_FIXED_VAL_SIZE],
}

impl<K: TransactionKind> std::fmt::Debug for Cursor<'_, K> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Cursor").field("fsi", &self.fsi).finish_non_exhaustive()
    }
}

impl<'a, K: TransactionKind> Deref for Cursor<'a, K> {
    type Target = signet_libmdbx::Cursor<'a, K>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<'a, K: TransactionKind> DerefMut for Cursor<'a, K> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl<'a, K: TransactionKind> Cursor<'a, K> {
    /// Creates a new `Cursor` wrapping the given `libmdbx` cursor.
    pub const fn new(inner: signet_libmdbx::Cursor<'a, K>, fsi: FixedSizeInfo) -> Self {
        Self { inner, fsi, buf: [0u8; MAX_KEY_SIZE + MAX_FIXED_VAL_SIZE] }
    }

    /// Returns the fixed size info for this cursor.
    pub const fn fsi(&self) -> FixedSizeInfo {
        self.fsi
    }
}

impl<K> KvTraverse<MdbxError> for Cursor<'_, K>
where
    K: TransactionKind,
{
    fn first<'a>(&'a mut self) -> Result<Option<RawKeyValue<'a>>, MdbxError> {
        self.inner.first().map_err(MdbxError::from)
    }

    fn last<'a>(&'a mut self) -> Result<Option<RawKeyValue<'a>>, MdbxError> {
        self.inner.last().map_err(MdbxError::from)
    }

    fn exact<'a>(&'a mut self, key: &[u8]) -> Result<Option<RawValue<'a>>, MdbxError> {
        self.inner.set(key).map_err(MdbxError::from)
    }

    fn lower_bound<'a>(&'a mut self, key: &[u8]) -> Result<Option<RawKeyValue<'a>>, MdbxError> {
        self.inner.set_range(key).map_err(MdbxError::from)
    }

    fn read_next<'a>(&'a mut self) -> Result<Option<RawKeyValue<'a>>, MdbxError> {
        self.inner.next().map_err(MdbxError::from)
    }

    fn read_prev<'a>(&'a mut self) -> Result<Option<RawKeyValue<'a>>, MdbxError> {
        self.inner.prev().map_err(MdbxError::from)
    }
}

impl<K: TransactionKind + WriteMarker> KvTraverseMut<MdbxError> for Cursor<'_, K> {
    fn delete_current(&mut self) -> Result<(), MdbxError> {
        self.inner.del().map_err(MdbxError::Mdbx)
    }

    fn append(&mut self, key: &[u8], value: &[u8]) -> Result<(), MdbxError> {
        self.inner.append(key, value).map_err(MdbxError::from)
    }
}

impl<K: TransactionKind + WriteMarker> Cursor<'_, K> {
    /// Stores multiple fixed-size entries for a single key.
    ///
    /// # Arguments
    ///
    /// - `key`: The key under which to store the duplicates.
    /// - `entries`: Contiguous buffer containing all elements.
    ///
    /// # Returns
    ///
    /// The number of elements actually written.
    ///
    /// # Errors
    ///
    /// Returns [`MdbxError::NotDupFixed`] if the table is not DUP_FIXED.
    pub fn put_multiple(&mut self, key: &[u8], entries: &[u8]) -> Result<usize, MdbxError> {
        let Some(total_size) = self.fsi.total_size() else {
            return Err(MdbxError::NotDupFixed);
        };

        self.inner.put_multiple(key, entries, total_size).map_err(MdbxError::from)
    }

    /// Stores multiple fixed-size entries for a single key. Overwrites
    /// all existing duplicates.
    ///
    /// # Arguments
    ///
    /// - `key`: The key under which to store the duplicates.
    /// - `entries`: Contiguous buffer containing all elements.
    ///
    /// # Returns
    ///
    /// The number of elements actually written.
    ///
    /// # Errors
    ///
    /// Returns [`MdbxError::NotDupFixed`] if the table is not DUP_FIXED.
    pub fn overwrite_multiple(&mut self, key: &[u8], entries: &[u8]) -> Result<usize, MdbxError> {
        let Some(total_size) = self.fsi.total_size() else {
            return Err(MdbxError::NotDupFixed);
        };

        self.inner.put_multiple_overwrite(key, entries, total_size).map_err(MdbxError::from)
    }
}

/// Splits a [`Cow`] slice at the given index, preserving borrowed status.
///
/// When the input is `Cow::Borrowed`, both outputs will be `Cow::Borrowed`
/// referencing subslices of the original data. When the input is `Cow::Owned`,
/// both outputs will be `Cow::Owned` with newly allocated vectors.
#[inline]
fn split_cow_at(cow: Cow<'_, [u8]>, at: usize) -> (Cow<'_, [u8]>, Cow<'_, [u8]>) {
    match cow {
        Cow::Borrowed(slice) => (Cow::Borrowed(&slice[..at]), Cow::Borrowed(&slice[at..])),
        Cow::Owned(mut vec) => {
            let right = vec.split_off(at);
            (Cow::Owned(vec), Cow::Owned(right))
        }
    }
}

impl<K> DualKeyTraverse<MdbxError> for Cursor<'_, K>
where
    K: TransactionKind,
{
    fn first<'a>(&'a mut self) -> Result<Option<RawDualKeyValue<'a>>, MdbxError> {
        if !self.fsi.is_dupsort() {
            return Err(MdbxError::NotDupSort);
        }

        match self.inner.first::<Cow<'_, [u8]>, Cow<'_, [u8]>>()? {
            Some((k1, v)) => {
                // For DUPSORT, the value contains key2 || actual_value.
                let Some(key2_size) = self.fsi.key2_size() else {
                    return Err(MdbxError::UnknownFixedSize);
                };
                let (k2, val) = split_cow_at(v, key2_size);
                Ok(Some((k1, k2, val)))
            }
            None => Ok(None),
        }
    }

    fn last<'a>(&'a mut self) -> Result<Option<RawDualKeyValue<'a>>, MdbxError> {
        if !self.fsi.is_dupsort() {
            return Err(MdbxError::NotDupSort);
        }

        match self.inner.last::<Cow<'_, [u8]>, Cow<'_, [u8]>>()? {
            Some((k1, v)) => {
                // For DUPSORT, the value contains key2 || actual_value.
                let Some(key2_size) = self.fsi.key2_size() else {
                    return Err(MdbxError::UnknownFixedSize);
                };
                let (k2, val) = split_cow_at(v, key2_size);
                Ok(Some((k1, k2, val)))
            }
            None => Ok(None),
        }
    }

    fn read_next<'a>(&'a mut self) -> Result<Option<RawDualKeyValue<'a>>, MdbxError> {
        if !self.fsi.is_dupsort() {
            return Err(MdbxError::NotDupSort);
        }

        match self.inner.next::<Cow<'_, [u8]>, Cow<'_, [u8]>>()? {
            Some((k1, v)) => {
                // For DUPSORT, the value contains key2 || actual_value.
                let Some(key2_size) = self.fsi.key2_size() else {
                    return Err(MdbxError::UnknownFixedSize);
                };
                let (k2, val) = split_cow_at(v, key2_size);
                Ok(Some((k1, k2, val)))
            }
            None => Ok(None),
        }
    }

    fn read_prev<'a>(&'a mut self) -> Result<Option<RawDualKeyValue<'a>>, MdbxError> {
        if !self.fsi.is_dupsort() {
            return Err(MdbxError::NotDupSort);
        }

        match self.inner.prev::<Cow<'_, [u8]>, Cow<'_, [u8]>>()? {
            Some((k1, v)) => {
                // For DUPSORT, the value contains key2 || actual_value.
                let Some(key2_size) = self.fsi.key2_size() else {
                    return Err(MdbxError::UnknownFixedSize);
                };
                let (k2, val) = split_cow_at(v, key2_size);
                Ok(Some((k1, k2, val)))
            }
            None => Ok(None),
        }
    }

    fn exact_dual<'a>(
        &'a mut self,
        key1: &[u8],
        key2: &[u8],
    ) -> Result<Option<RawValue<'a>>, MdbxError> {
        if !self.fsi.is_dupsort() {
            return Err(MdbxError::NotDupSort);
        }

        // For DUPSORT tables, we use get_both which finds exact (key1, key2) match.
        // The "value" in MDBX DUPSORT is key2 || actual_value, so we return that.
        // Prepare key2 (may need padding for DUP_FIXED)
        let key2_prepared = if let Some(total_size) = self.fsi.total_size() {
            // Copy key2 to scratch buffer and zero-pad to total fixed size
            self.buf[..key2.len()].copy_from_slice(key2);
            self.buf[key2.len()..total_size].fill(0);
            &self.buf[..total_size]
        } else {
            key2
        };
        self.inner.get_both(key1, key2_prepared).map_err(MdbxError::from)
    }

    fn next_dual_above<'a>(
        &'a mut self,
        key1: &[u8],
        key2: &[u8],
    ) -> Result<Option<RawDualKeyValue<'a>>, MdbxError> {
        if !self.fsi.is_dupsort() {
            return Err(MdbxError::NotDupSort);
        }

        let key2_size = self.fsi.key2_size().unwrap_or(key2.len());

        // Use set_range to find the first entry with key1 >= search_key1
        let Some((found_k1, v)) = self.inner.set_range::<Cow<'_, [u8]>, Cow<'_, [u8]>>(key1)?
        else {
            return Ok(None);
        };

        // If found_k1 > search_key1, we have our answer (first entry in next key1)
        if found_k1.as_ref() > key1 {
            let (k2, val) = split_cow_at(v, key2_size);
            return Ok(Some((found_k1, k2, val)));
        }

        // found_k1 == search_key1, so we need to filter by key2 >= search_key2
        // Use get_both_range to find entry with exact key1 and value >= key2
        let key2_prepared = if let Some(total_size) = self.fsi.total_size() {
            // Copy key2 to scratch buffer and zero-pad to total fixed size
            self.buf[..key2.len()].copy_from_slice(key2);
            self.buf[key2.len()..total_size].fill(0);
            &self.buf[..total_size]
        } else {
            key2
        };

        match self.inner.get_both_range::<RawValue<'_>>(key1, key2_prepared)? {
            Some(v) => {
                let (k2, val) = split_cow_at(v, key2_size);
                // key1 must be owned here since we're returning a reference to the input
                Ok(Some((Cow::Owned(key1.to_vec()), k2, val)))
            }
            None => {
                // No entry with key2 >= search_key2 in this key1, try next key1
                match self.inner.next_nodup::<Cow<'_, [u8]>, Cow<'_, [u8]>>()? {
                    Some((k1, v)) => {
                        let (k2, val) = split_cow_at(v, key2_size);
                        Ok(Some((k1, k2, val)))
                    }
                    None => Ok(None),
                }
            }
        }
    }

    fn next_k1<'a>(&'a mut self) -> Result<Option<RawDualKeyValue<'a>>, MdbxError> {
        // Move to the next distinct key1 (skip remaining duplicates for current key1)
        if self.fsi.is_dupsort() {
            match self.inner.next_nodup::<Cow<'_, [u8]>, Cow<'_, [u8]>>()? {
                Some((k1, v)) => {
                    // For DUPSORT, the value contains key2 || actual_value.
                    // Split using the known key2 size.
                    let Some(key2_size) = self.fsi.key2_size() else {
                        return Err(MdbxError::UnknownFixedSize);
                    };
                    let (k2, val) = split_cow_at(v, key2_size);
                    Ok(Some((k1, k2, val)))
                }
                None => Ok(None),
            }
        } else {
            // Not a DUPSORT table - just get next entry
            match self.inner.next()? {
                Some((k, v)) => Ok(Some((k, Cow::Borrowed(&[] as &[u8]), v))),
                None => Ok(None),
            }
        }
    }

    fn next_k2<'a>(&'a mut self) -> Result<Option<RawDualKeyValue<'a>>, MdbxError> {
        // Move to the next duplicate (same key1, next key2)
        if self.fsi.is_dupsort() {
            match self.inner.next_dup::<Cow<'_, [u8]>, Cow<'_, [u8]>>()? {
                Some((k1, v)) => {
                    // For DUPSORT, the value contains key2 || actual_value.
                    // Split using the known key2 size.
                    let Some(key2_size) = self.fsi.key2_size() else {
                        return Err(MdbxError::UnknownFixedSize);
                    };
                    let (k2, val) = split_cow_at(v, key2_size);
                    Ok(Some((k1, k2, val)))
                }
                None => Ok(None),
            }
        } else {
            // Not a DUPSORT table - no concept of "next duplicate"
            Ok(None)
        }
    }

    fn last_of_k1<'a>(&'a mut self, key1: &[u8]) -> Result<Option<RawDualKeyValue<'a>>, MdbxError> {
        if !self.fsi.is_dupsort() {
            return Err(MdbxError::NotDupSort);
        }

        // First, position at key1 (any duplicate)
        let Some(_) = self.inner.set::<Cow<'_, [u8]>>(key1)? else {
            return Ok(None);
        };

        // Then move to the last duplicate for this key1
        let Some(v) = self.inner.last_dup::<Cow<'_, [u8]>>()? else {
            return Ok(None);
        };

        // Split the value into key2 and actual value
        let Some(key2_size) = self.fsi.key2_size() else {
            return Err(MdbxError::UnknownFixedSize);
        };
        let (k2, val) = split_cow_at(v, key2_size);

        // key1 must be owned here since we're returning a reference to the input
        Ok(Some((Cow::Owned(key1.to_vec()), k2, val)))
    }

    fn previous_k1<'a>(&'a mut self) -> Result<Option<RawDualKeyValue<'a>>, MdbxError> {
        if !self.fsi.is_dupsort() {
            return Err(MdbxError::NotDupSort);
        }

        // prev_nodup positions at the last data item of the previous key
        match self.inner.prev_nodup::<Cow<'_, [u8]>, Cow<'_, [u8]>>()? {
            Some((k1, v)) => {
                // For DUPSORT, prev_nodup already positions at the last duplicate
                // of the previous key. Split the value.
                let Some(key2_size) = self.fsi.key2_size() else {
                    return Err(MdbxError::UnknownFixedSize);
                };
                let (k2, val) = split_cow_at(v, key2_size);
                Ok(Some((k1, k2, val)))
            }
            None => Ok(None),
        }
    }

    fn previous_k2<'a>(&'a mut self) -> Result<Option<RawDualKeyValue<'a>>, MdbxError> {
        if !self.fsi.is_dupsort() {
            return Err(MdbxError::NotDupSort);
        }

        // prev_dup positions at the previous duplicate of the current key
        match self.inner.prev_dup::<Cow<'_, [u8]>, Cow<'_, [u8]>>()? {
            Some((k1, v)) => {
                let Some(key2_size) = self.fsi.key2_size() else {
                    return Err(MdbxError::UnknownFixedSize);
                };
                let (k2, val) = split_cow_at(v, key2_size);
                Ok(Some((k1, k2, val)))
            }
            None => Ok(None),
        }
    }

    fn iter_items(
        &mut self,
    ) -> Result<impl Iterator<Item = Result<RawDualKeyItem<'_>, MdbxError>> + '_, MdbxError> {
        if !self.fsi.is_dupsort() {
            return Err(MdbxError::NotDupSort);
        }

        let key2_size = self.fsi.key2_size().ok_or(MdbxError::UnknownFixedSize)?;

        // Use iter_dup_start which yields DupItem::NewKey/SameKey
        let iter_dup = self.inner.iter_dup_start::<Cow<'_, [u8]>, Cow<'_, [u8]>>()?;
        Ok(MdbxDualKeyItemIter { iter_dup, key2_size })
    }
}

/// Iterator adapter that converts mdbx's [`DupItem`] to [`DualKeyItem`].
///
/// This iterator returns owned data to avoid lifetime complexities between
/// the transaction lifetime and the iterator borrow lifetime.
struct MdbxDualKeyItemIter<'tx, 'cur, K: TransactionKind> {
    iter_dup: signet_libmdbx::tx::iter::IterDup<'tx, 'cur, K, Cow<'tx, [u8]>, Cow<'tx, [u8]>>,
    key2_size: usize,
}

impl<'a, K: TransactionKind> Iterator for MdbxDualKeyItemIter<'_, 'a, K> {
    type Item = Result<RawDualKeyItem<'a>, MdbxError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.iter_dup.borrow_next() {
            Ok(Some(item)) => {
                let result = match item {
                    DupItem::NewKey(k1, v) => {
                        let (k2, val) = split_cow_at_owned(v, self.key2_size);
                        DualKeyItem::NewK1(Cow::Owned(k1.into_owned()), k2, val)
                    }
                    DupItem::SameKey(v) => {
                        let (k2, val) = split_cow_at_owned(v, self.key2_size);
                        DualKeyItem::SameK1(k2, val)
                    }
                };
                Some(Ok(result))
            }
            Ok(None) => None,
            Err(e) => Some(Err(MdbxError::from(e))),
        }
    }
}

/// Splits a [`Cow`] at the given index and returns owned [`Cow`]s.
#[inline]
fn split_cow_at_owned(cow: Cow<'_, [u8]>, at: usize) -> (Cow<'static, [u8]>, Cow<'static, [u8]>) {
    let mut vec = cow.into_owned();
    let right = vec.split_off(at);
    (Cow::Owned(vec), Cow::Owned(right))
}

impl<K: TransactionKind + WriteMarker> DualKeyTraverseMut<MdbxError> for Cursor<'_, K> {
    fn delete_current(&mut self) -> Result<(), MdbxError> {
        // For DUPSORT tables, del() deletes only the current duplicate
        self.inner.del().map_err(MdbxError::Mdbx)
    }

    fn clear_k1(&mut self, key1: &[u8]) -> Result<(), MdbxError> {
        if !self.fsi.is_dupsort() {
            return Err(MdbxError::NotDupSort);
        }

        // Position at the K1 - if it doesn't exist, nothing to delete
        if self.inner.set::<()>(key1)?.is_none() {
            return Ok(());
        }
        // Delete all K2 entries for this K1
        self.inner.del_all_dups()?;
        Ok(())
    }

    fn append_dual(&mut self, k1: &[u8], k2: &[u8], value: &[u8]) -> Result<(), MdbxError> {
        // Concatenate k2 || value for DUPSORT using scratch buffer
        let total = k2.len() + value.len();
        self.buf[..k2.len()].copy_from_slice(k2);
        self.buf[k2.len()..total].copy_from_slice(value);
        self.inner.append_dup(k1, &self.buf[..total]).map_err(MdbxError::from)
    }
}
