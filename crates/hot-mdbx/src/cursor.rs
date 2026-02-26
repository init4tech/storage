//! Cursor wrapper for libmdbx-sys.

use crate::{FixedSizeInfo, MdbxError};
use signet_hot::{
    MAX_FIXED_VAL_SIZE, MAX_KEY_SIZE,
    model::{
        DualKeyItem, DualKeyTraverse, DualKeyTraverseMut, KvTraverse, KvTraverseMut,
        RawDualKeyItem, RawDualKeyValue, RawKeyValue, RawValue,
    },
};
use signet_libmdbx::{DupItem, Ro, Rw, RwSync, TransactionKind, tx::WriteMarker, tx::iter};
use std::borrow::Cow;

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

    fn iter(
        &mut self,
    ) -> Result<impl Iterator<Item = Result<RawKeyValue<'_>, MdbxError>> + '_, MdbxError>
    where
        Self: Sized,
    {
        let iter =
            self.inner.iter_start::<Cow<'_, [u8]>, Cow<'_, [u8]>>().map_err(MdbxError::from)?;
        Ok(MdbxKvIter { iter })
    }

    fn iter_from<'a>(
        &'a mut self,
        key: &[u8],
    ) -> Result<impl Iterator<Item = Result<RawKeyValue<'a>, MdbxError>> + 'a, MdbxError>
    where
        Self: Sized,
    {
        let iter =
            self.inner.iter_from::<Cow<'_, [u8]>, Cow<'_, [u8]>>(key).map_err(MdbxError::from)?;
        Ok(MdbxKvIter { iter })
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

/// A key-value pair of borrowed byte slices from the cursor.
type RawCowKv<'a> = (Cow<'a, [u8]>, Cow<'a, [u8]>);

/// Result of splitting a [`Cow`] byte slice into two halves.
type CowSplit<'a> = Result<(Cow<'a, [u8]>, Cow<'a, [u8]>), MdbxError>;

/// Splits a DUPSORT `(key1, dup_value)` pair into `(key1, key2, value)`.
///
/// In DUPSORT tables MDBX stores `key2 || actual_value` as the dup value.
/// This helper extracts `key2_size` from the [`FixedSizeInfo`], splits the
/// value, and reassembles the triple.
fn split_dup_kv(
    fsi: FixedSizeInfo,
    kv: Option<RawCowKv<'_>>,
) -> Result<Option<RawDualKeyValue<'_>>, MdbxError> {
    let Some((k1, v)) = kv else { return Ok(None) };
    let key2_size = fsi.key2_size().ok_or(MdbxError::UnknownFixedSize)?;
    let (k2, val) = split_cow_at(v, key2_size)?;
    Ok(Some((k1, k2, val)))
}

/// Splits a [`Cow`] slice at the given index, preserving borrowed status.
///
/// When the input is `Cow::Borrowed`, both outputs will be `Cow::Borrowed`
/// referencing subslices of the original data. When the input is `Cow::Owned`,
/// both outputs will be `Cow::Owned` with newly allocated vectors.
///
/// Returns [`MdbxError::DupFixedErr`] if `at` exceeds the slice length.
#[inline]
fn split_cow_at(cow: Cow<'_, [u8]>, at: usize) -> CowSplit<'_> {
    debug_assert!(
        at <= cow.len(),
        "'at' ({at}) must not be greater than slice length ({})",
        cow.len()
    );
    Ok(match cow {
        Cow::Borrowed(slice) => (Cow::Borrowed(&slice[..at]), Cow::Borrowed(&slice[at..])),
        Cow::Owned(mut vec) => {
            let right = vec.split_off(at);
            (Cow::Owned(vec), Cow::Owned(right))
        }
    })
}

impl<K> DualKeyTraverse<MdbxError> for Cursor<'_, K>
where
    K: TransactionKind,
{
    fn first<'a>(&'a mut self) -> Result<Option<RawDualKeyValue<'a>>, MdbxError> {
        if !self.fsi.is_dupsort() {
            return Err(MdbxError::NotDupSort);
        }
        split_dup_kv(self.fsi, self.inner.first()?)
    }

    fn last<'a>(&'a mut self) -> Result<Option<RawDualKeyValue<'a>>, MdbxError> {
        if !self.fsi.is_dupsort() {
            return Err(MdbxError::NotDupSort);
        }
        split_dup_kv(self.fsi, self.inner.last()?)
    }

    fn read_next<'a>(&'a mut self) -> Result<Option<RawDualKeyValue<'a>>, MdbxError> {
        if !self.fsi.is_dupsort() {
            return Err(MdbxError::NotDupSort);
        }
        split_dup_kv(self.fsi, self.inner.next()?)
    }

    fn read_prev<'a>(&'a mut self) -> Result<Option<RawDualKeyValue<'a>>, MdbxError> {
        if !self.fsi.is_dupsort() {
            return Err(MdbxError::NotDupSort);
        }
        split_dup_kv(self.fsi, self.inner.prev()?)
    }

    fn exact_dual<'a>(
        &'a mut self,
        key1: &[u8],
        key2: &[u8],
    ) -> Result<Option<RawValue<'a>>, MdbxError> {
        if !self.fsi.is_dupsort() {
            return Err(MdbxError::NotDupSort);
        }

        // For DUPSORT tables, the stored data is key2 || actual_value.
        // We use get_both_range (first value >= search) rather than get_both
        // (exact match), because the search value is just key2 (zero-padded
        // for DUP_FIXED) and an exact match would only succeed when
        // actual_value is empty/zero.
        let key2_prepared = if let Some(total_size) = self.fsi.total_size() {
            self.buf[..key2.len()].copy_from_slice(key2);
            self.buf[key2.len()..total_size].fill(0);
            &self.buf[..total_size]
        } else {
            key2
        };

        let found = self
            .inner
            .get_both_range::<RawValue<'_>>(key1, key2_prepared)
            .map_err(MdbxError::from)?;

        // get_both_range returns the first dup value >= search. The dup
        // value is key2 || actual_value, so verify the key2 prefix matches
        // and strip it before returning.
        match found {
            Some(v) if v.starts_with(key2) => {
                let (_, val) = split_cow_at(v, key2.len())?;
                Ok(Some(val))
            }
            _ => Ok(None),
        }
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
            let (k2, val) = split_cow_at(v, key2_size)?;
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
                let (k2, val) = split_cow_at(v, key2_size)?;
                // key1 must be owned here since we're returning a reference to the input
                Ok(Some((Cow::Owned(key1.to_vec()), k2, val)))
            }
            None => {
                // No entry with key2 >= search_key2 in this key1, try next key1
                match self.inner.next_nodup::<Cow<'_, [u8]>, Cow<'_, [u8]>>()? {
                    Some((k1, v)) => {
                        let (k2, val) = split_cow_at(v, key2_size)?;
                        Ok(Some((k1, k2, val)))
                    }
                    None => Ok(None),
                }
            }
        }
    }

    fn next_k1<'a>(&'a mut self) -> Result<Option<RawDualKeyValue<'a>>, MdbxError> {
        if !self.fsi.is_dupsort() {
            return Err(MdbxError::NotDupSort);
        }
        split_dup_kv(self.fsi, self.inner.next_nodup()?)
    }

    fn next_k2<'a>(&'a mut self) -> Result<Option<RawDualKeyValue<'a>>, MdbxError> {
        if !self.fsi.is_dupsort() {
            return Err(MdbxError::NotDupSort);
        }
        split_dup_kv(self.fsi, self.inner.next_dup()?)
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
        let (k2, val) = split_cow_at(v, key2_size)?;

        // key1 must be owned here since we're returning a reference to the input
        Ok(Some((Cow::Owned(key1.to_vec()), k2, val)))
    }

    fn previous_k1<'a>(&'a mut self) -> Result<Option<RawDualKeyValue<'a>>, MdbxError> {
        if !self.fsi.is_dupsort() {
            return Err(MdbxError::NotDupSort);
        }
        split_dup_kv(self.fsi, self.inner.prev_nodup()?)
    }

    fn previous_k2<'a>(&'a mut self) -> Result<Option<RawDualKeyValue<'a>>, MdbxError> {
        if !self.fsi.is_dupsort() {
            return Err(MdbxError::NotDupSort);
        }
        split_dup_kv(self.fsi, self.inner.prev_dup()?)
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

/// Iterator adapter that wraps a native libmdbx [`iter::Iter`] for
/// [`KvTraverse`].
///
/// Uses the native cursor iteration which captures the first entry in a
/// `pending` field, avoiding data copies for the initial positioning.
struct MdbxKvIter<'tx, 'cur, K: TransactionKind> {
    iter: iter::Iter<'tx, 'cur, K, Cow<'tx, [u8]>, Cow<'tx, [u8]>>,
}

impl<'a, K: TransactionKind> Iterator for MdbxKvIter<'_, 'a, K> {
    type Item = Result<RawKeyValue<'a>, MdbxError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.iter.borrow_next() {
            Ok(Some(kv)) => Some(Ok(kv)),
            Ok(None) => None,
            Err(e) => Some(Err(MdbxError::from(e))),
        }
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
                    DupItem::NewKey(k1, v) => split_cow_at_owned(v, self.key2_size)
                        .map(|(k2, val)| DualKeyItem::NewK1(Cow::Owned(k1.into_owned()), k2, val)),
                    DupItem::SameKey(v) => split_cow_at_owned(v, self.key2_size)
                        .map(|(k2, val)| DualKeyItem::SameK1(k2, val)),
                };
                Some(result)
            }
            Ok(None) => None,
            Err(e) => Some(Err(MdbxError::from(e))),
        }
    }
}

/// Splits a [`Cow`] at the given index and returns owned [`Cow`]s.
///
/// Returns [`MdbxError::DupFixedErr`] if `at` exceeds the slice length.
#[inline]
fn split_cow_at_owned(cow: Cow<'_, [u8]>, at: usize) -> CowSplit<'static> {
    if at > cow.len() {
        return Err(MdbxError::DupFixedErr { expected: at, found: cow.len() });
    }
    let mut vec = cow.into_owned();
    let right = vec.split_off(at);
    Ok((Cow::Owned(vec), Cow::Owned(right)))
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
        // Concatenate k2 || value for DUPSORT
        let total = k2.len() + value.len();
        if total <= self.buf.len() {
            self.buf[..k2.len()].copy_from_slice(k2);
            self.buf[k2.len()..total].copy_from_slice(value);
            self.inner.append_dup(k1, &self.buf[..total]).map_err(MdbxError::from)
        } else {
            let mut combined = Vec::with_capacity(total);
            combined.extend_from_slice(k2);
            combined.extend_from_slice(value);
            self.inner.append_dup(k1, &combined).map_err(MdbxError::from)
        }
    }
}
