//! Transaction wrapper for libmdbx-sys.
use crate::{Cursor, FixedSizeInfo, FsiCache, MdbxError};
use alloy::primitives::B256;
use signet_hot::{
    KeySer, MAX_FIXED_VAL_SIZE, MAX_KEY_SIZE, ValSer,
    model::{DualKeyTraverseMut, DualTableTraverse, HotKvRead, HotKvWrite, KvTraverseMut},
    tables::{DualKey, SingleKey, Table},
};
use signet_libmdbx::{Rw, RwSync, TransactionKind, WriteFlags, tx::WriteMarker};
use std::borrow::Cow;

const TX_BUFFER_SIZE: usize = MAX_KEY_SIZE + MAX_FIXED_VAL_SIZE;

/// Wrapper around [`signet_libmdbx::tx::Tx`], with an additional cache
/// to store [`FixedSizeInfo`] for tables.
///
/// When a DUPSORT table is created, a [`FixedSizeInfo`] is stored in the
/// default metadata table (dbi=0) under a key derived from the table name.
/// This info is then cached in-memory for fast access during subsequent
/// operations.
pub struct Tx<K: TransactionKind> {
    /// Libmdbx-sys transaction wrapped in RefCell for interior mutability.
    inner: signet_libmdbx::tx::Tx<K>,

    /// Cached FixedSizeInfo for tables.
    fsi_cache: FsiCache,
}

impl<K: TransactionKind> std::fmt::Debug for Tx<K> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Tx").field("fsi_cache", &self.fsi_cache).finish_non_exhaustive()
    }
}

impl<K: TransactionKind> Tx<K> {
    /// Creates new `Tx` object with a `RO` or `RW` transaction and optionally enables metrics.
    #[inline]
    pub(crate) const fn new(inner: signet_libmdbx::tx::Tx<K>, fsi_cache: FsiCache) -> Self {
        Self { inner, fsi_cache }
    }

    /// Reads FixedSizeInfo from the metadata table.
    fn read_fsi_from_table(&self, name: &'static str) -> Result<FixedSizeInfo, MdbxError> {
        let mut key = B256::ZERO;
        let to_copy = core::cmp::min(32, name.len());
        key[..to_copy].copy_from_slice(&name.as_bytes()[..to_copy]);

        let db = self.inner.open_db(None)?;

        let data: [u8; 8] = self
            .inner
            .get(db.dbi(), key.as_slice())
            .map_err(MdbxError::from)?
            .ok_or(MdbxError::UnknownTable(name))?;

        FixedSizeInfo::decode_value(&data).map_err(MdbxError::Deser)
    }

    /// Gets cached FixedSizeInfo for a table.
    pub fn get_fsi(&self, name: &'static str) -> Result<FixedSizeInfo, MdbxError> {
        // Fast path: read lock
        if let Some(&fsi) = self.fsi_cache.read().get(name) {
            return Ok(fsi);
        }
        // Slow path: read from table, then write lock
        let fsi = self.read_fsi_from_table(name)?;
        self.fsi_cache.write().insert(name, fsi);
        Ok(fsi)
    }

    /// Gets the database handle (dbi) for the given table name.
    pub fn get_dbi_raw(&self, table: &'static str) -> Result<u32, MdbxError> {
        self.inner.open_db(Some(table)).map(|db| db.dbi()).map_err(MdbxError::Mdbx)
    }

    /// Gets the database handle (dbi) for the given table.
    pub fn get_dbi<T: Table>(&self) -> Result<u32, MdbxError> {
        self.get_dbi_raw(T::NAME)
    }

    /// Gets this transaction ID.
    pub fn id(&self) -> Result<u64, MdbxError> {
        self.inner.id().map_err(MdbxError::Mdbx)
    }

    /// Create [`Cursor`] for raw table name.
    pub fn new_cursor_raw<'a>(&'a self, name: &'static str) -> Result<Cursor<'a, K>, MdbxError> {
        let db = self.inner.open_db(Some(name))?;
        let fsi = self.get_fsi(name)?;

        let cursor = self.inner.cursor(db)?;

        Ok(Cursor::new(cursor, fsi))
    }

    /// Create a [`Cursor`] for the given table.
    pub fn new_cursor<'a, T: Table>(&'a self) -> Result<Cursor<'a, K>, MdbxError> {
        Self::new_cursor_raw(self, T::NAME)
    }
}

impl<K: TransactionKind + WriteMarker> Tx<K> {
    /// Stores FixedSizeInfo in the metadata table.
    fn store_fsi(&self, table: &'static str, fsi: FixedSizeInfo) -> Result<(), MdbxError> {
        let db = self.inner.open_db(None)?;

        let mut key_buf = [0u8; MAX_KEY_SIZE];
        let to_copy = core::cmp::min(32, table.len());
        key_buf[..to_copy].copy_from_slice(&table.as_bytes()[..to_copy]);

        let mut value_buf = [0u8; 8];
        fsi.encode_value_to(&mut value_buf.as_mut_slice());

        self.inner.put(db, key_buf, value_buf, WriteFlags::UPSERT)?;
        self.fsi_cache.write().insert(table, fsi);

        Ok(())
    }
}

impl<K> HotKvRead for Tx<K>
where
    K: TransactionKind,
{
    type Error = MdbxError;

    type Traverse<'a> = Cursor<'a, K>;

    fn raw_traverse<'a>(&'a self, table: &'static str) -> Result<Self::Traverse<'a>, Self::Error> {
        self.new_cursor_raw(table)
    }

    fn raw_get<'a>(
        &'a self,
        table: &'static str,
        key: &[u8],
    ) -> Result<Option<Cow<'a, [u8]>>, Self::Error> {
        let dbi = self.get_dbi_raw(table)?;
        let result: Result<Option<Cow<'_, [u8]>>, _> = self.inner.get(dbi, key.as_ref());
        result.map_err(MdbxError::from)
    }

    fn raw_get_dual<'a>(
        &'a self,
        _table: &'static str,
        _key1: &[u8],
        _key2: &[u8],
    ) -> Result<Option<Cow<'a, [u8]>>, Self::Error> {
        unimplemented!("Use DualTableTraverse for raw_get_dual");
    }

    fn get_dual<T: DualKey>(
        &self,
        key1: &T::Key,
        key2: &T::Key2,
    ) -> Result<Option<T::Value>, Self::Error> {
        let mut cursor = self.new_cursor::<T>()?;

        DualTableTraverse::<T, MdbxError>::exact_dual(&mut cursor, key1, key2)
    }
}
macro_rules! impl_hot_kv_write {
    ($ty:ty) => {
        impl HotKvWrite for Tx<$ty> {
            type TraverseMut<'a> = Cursor<'a, $ty>;

            fn raw_traverse_mut<'a>(
                &'a self,
                table: &'static str,
            ) -> Result<Self::TraverseMut<'a>, Self::Error> {
                self.new_cursor_raw(table)
            }

            fn queue_raw_put(
                &self,
                table: &'static str,
                key: &[u8],
                value: &[u8],
            ) -> Result<(), Self::Error> {
                let db = self.inner.open_db(Some(table))?;
                self.inner.put(db, key, value, WriteFlags::UPSERT).map_err(MdbxError::Mdbx)
            }

            fn queue_raw_put_dual(
                &self,
                table: &'static str,
                key1: &[u8],
                key2: &[u8],
                value: &[u8],
            ) -> Result<(), Self::Error> {
                let db = self.inner.open_db(Some(table))?;
                let fsi = self.get_fsi(table)?;

                // For DUPSORT tables, we must delete any existing entry with the same
                // (key1, key2) before inserting, because MDBX stores key2 as part of
                // the value (key2||actual_value). Without deletion, putting a new value
                // for the same key2 creates a duplicate entry instead of replacing.
                if fsi.is_dupsort() {
                    // Prepare search value (key2, optionally padded for DUP_FIXED)
                    let mut search_buf = [0u8; TX_BUFFER_SIZE];
                    let search_val = if let Some(ts) = fsi.total_size() {
                        search_buf[..key2.len()].copy_from_slice(key2);
                        search_buf[key2.len()..ts].fill(0);
                        &search_buf[..ts]
                    } else {
                        key2
                    };

                    // get_both_range finds entry where key=key1 and value >= search_val
                    // If found and the key2 portion matches, delete it
                    let mut cursor = self.inner.cursor(db).map_err(MdbxError::Mdbx)?;

                    if let Some(found_val) = cursor
                        .get_both_range::<Cow<'_, [u8]>>(key1, search_val)
                        .map_err(MdbxError::from)?
                        && found_val.starts_with(key2)
                    // Check if found value starts with our key2
                    {
                        cursor.del().map_err(MdbxError::Mdbx)?;
                    }
                }

                // For DUPSORT tables, the "value" is key2 concatenated with the actual
                // value.
                // If the value is fixed size, we can write directly into our scratch
                // buffer. Otherwise, we need to allocate
                //
                // NB: DUPSORT and RESERVE are incompatible :(
                if key2.len() + value.len() > TX_BUFFER_SIZE {
                    // Allocate a buffer for the combined value
                    let mut combined = Vec::with_capacity(key2.len() + value.len());
                    combined.extend_from_slice(key2);
                    combined.extend_from_slice(value);
                    return self
                        .inner
                        .put(db, key1, &combined, WriteFlags::UPSERT)
                        .map_err(MdbxError::Mdbx);
                }

                // Use the scratch buffer
                let mut buffer = [0u8; TX_BUFFER_SIZE];
                let buf = &mut buffer[..key2.len() + value.len()];
                buf[..key2.len()].copy_from_slice(key2);
                buf[key2.len()..].copy_from_slice(value);

                self.inner.put(db, key1, buf, WriteFlags::UPSERT).map_err(MdbxError::Mdbx).map(drop)
            }

            fn queue_raw_delete(&self, table: &'static str, key: &[u8]) -> Result<(), Self::Error> {
                let db = self.inner.open_db(Some(table))?;
                self.inner.del(db, key, None).map(drop).map_err(MdbxError::Mdbx)
            }

            fn queue_raw_delete_dual(
                &self,
                table: &'static str,
                key1: &[u8],
                key2: &[u8],
            ) -> Result<(), Self::Error> {
                let db = self.inner.open_db(Some(table))?;
                let fsi = self.get_fsi(table)?;

                // For DUPSORT tables, the "value" is key2 || actual_value.
                // For DUP_FIXED tables, we cannot use del() with a partial value
                // because MDBX requires an exact match. We must use a cursor to
                // find and delete the entry.
                if fsi.is_dupsort() {
                    // Prepare search value (key2, optionally padded for DUP_FIXED)
                    let mut search_buf = [0u8; TX_BUFFER_SIZE];
                    let search_val = if let Some(ts) = fsi.total_size() {
                        search_buf[..key2.len()].copy_from_slice(key2);
                        search_buf[key2.len()..ts].fill(0);
                        &search_buf[..ts]
                    } else {
                        key2
                    };

                    // Use cursor to find and delete the entry
                    let mut cursor = self.inner.cursor(db).map_err(MdbxError::Mdbx)?;

                    // get_both_range finds entry where key=key1 and value >= search_val
                    // If found and the key2 portion matches, delete it
                    if let Some(found_val) = cursor
                        .get_both_range::<Cow<'_, [u8]>>(key1, search_val)
                        .map_err(MdbxError::from)?
                        && found_val.starts_with(key2)
                    {
                        cursor.del().map_err(MdbxError::Mdbx)?;
                    }

                    Ok(())
                } else {
                    // Non-DUPSORT table - just delete by key1
                    self.inner.del(db, key1, Some(key2)).map(drop).map_err(MdbxError::Mdbx)
                }
            }

            fn queue_raw_clear(&self, table: &'static str) -> Result<(), Self::Error> {
                let db = self.inner.open_db(Some(table))?;
                self.inner.clear_db(db).map_err(MdbxError::Mdbx)
            }

            fn queue_raw_create(
                &self,
                table: &'static str,
                dual_key: Option<usize>,
                fixed_val: Option<usize>,
                int_key: bool,
            ) -> Result<(), Self::Error> {
                let mut flags = signet_libmdbx::DatabaseFlags::default();

                let mut fsi = FixedSizeInfo::None;

                if let Some(key2_size) = dual_key {
                    flags.set(signet_libmdbx::DatabaseFlags::DUP_SORT, true);
                    if let Some(value_size) = fixed_val {
                        flags.set(signet_libmdbx::DatabaseFlags::DUP_FIXED, true);
                        fsi = FixedSizeInfo::DupFixed {
                            key2_size,
                            total_size: key2_size + value_size,
                        };
                    } else {
                        // DUPSORT without DUP_FIXED - variable value size
                        fsi = FixedSizeInfo::DupSort { key2_size };
                    }
                }

                if int_key {
                    flags.set(signet_libmdbx::DatabaseFlags::INTEGER_KEY, true);
                }

                self.inner.create_db(Some(table), flags)?;
                self.store_fsi(table, fsi)?;

                Ok(())
            }

            fn queue_put<T: SingleKey>(
                &self,
                key: &T::Key,
                value: &T::Value,
            ) -> Result<(), Self::Error> {
                let db = self.inner.open_db(Some(T::NAME))?;
                let mut key_buf = [0u8; MAX_KEY_SIZE];
                let key_bytes = key.encode_key(&mut key_buf);

                self.inner
                    .with_reservation(
                        db,
                        key_bytes,
                        value.encoded_size(),
                        WriteFlags::UPSERT,
                        |mut reserved| value.encode_value_to(&mut reserved),
                    )
                    .map_err(MdbxError::from)
            }

            fn queue_append<T: SingleKey>(
                &self,
                key: &T::Key,
                value: &T::Value,
            ) -> Result<(), Self::Error> {
                let mut cursor = self.new_cursor::<T>()?;
                let mut key_buf = [0u8; MAX_KEY_SIZE];
                let key_bytes = key.encode_key(&mut key_buf);
                cursor.append(key_bytes, &value.encoded()).map_err(MdbxError::from)
            }

            fn queue_append_dual<T: DualKey>(
                &self,
                k1: &T::Key,
                k2: &T::Key2,
                value: &T::Value,
            ) -> Result<(), Self::Error> {
                let mut cursor = self.new_cursor::<T>()?;
                let mut k1_buf = [0u8; MAX_KEY_SIZE];
                let mut k2_buf = [0u8; MAX_KEY_SIZE];
                let k1_bytes = k1.encode_key(&mut k1_buf);
                let k2_bytes = k2.encode_key(&mut k2_buf);
                let value_bytes = value.encoded();
                cursor.append_dual(k1_bytes, k2_bytes, &value_bytes).map_err(MdbxError::from)
            }

            fn raw_commit(self) -> Result<(), Self::Error> {
                // Take ownership of the inner transaction from the RefCell
                self.inner.commit().map_err(MdbxError::Mdbx)
            }
        }
    };
}

impl_hot_kv_write!(RwSync);
impl_hot_kv_write!(Rw);
