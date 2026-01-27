//! Transaction wrapper for libmdbx-sys.
use crate::{Cursor, FixedSizeInfo, FsiCache, MdbxError};
use alloy::primitives::B256;
use signet_hot::{
    KeySer, MAX_FIXED_VAL_SIZE, MAX_KEY_SIZE, ValSer,
    model::{DualTableTraverse, HotKvRead, HotKvWrite},
    tables::{DualKey, SingleKey, Table},
};
use signet_libmdbx::{Database, DatabaseFlags, RW, TransactionKind, TxUnsync, WriteFlags};
use std::{borrow::Cow, cell::RefCell};

const TX_BUFFER_SIZE: usize = MAX_KEY_SIZE + MAX_FIXED_VAL_SIZE;

/// Wrapper for the libmdbx transaction.
///
/// This wraps [`TxUnsync`] in a [`RefCell`] to provide interior mutability,
/// allowing trait implementations that use `&self` to call methods that
/// require `&mut self` on the underlying transaction.
pub struct Tx<K: TransactionKind> {
    /// Libmdbx-sys transaction wrapped in RefCell for interior mutability.
    inner: RefCell<TxUnsync<K>>,

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
    pub(crate) fn new(inner: TxUnsync<K>, fsi_cache: FsiCache) -> Self {
        Self { inner: RefCell::new(inner), fsi_cache }
    }

    /// Borrows the inner transaction mutably and executes a closure.
    #[inline]
    fn with_tx<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut TxUnsync<K>) -> R,
    {
        let mut guard = self.inner.borrow_mut();
        f(&mut *guard)
    }

    /// Like `with_tx` but for operations returning Cow that need lifetime extension.
    ///
    /// # Safety
    ///
    /// This is safe because:
    /// - MDBX data returned by get() is valid for the transaction lifetime
    /// - The Tx struct owns the transaction via RefCell
    /// - The lifetime 'a is the Tx lifetime, so data outlives the returned Cow
    /// - The RefCell borrow is released before returning, but the data remains
    ///   valid because it's owned by MDBX, not the RefCell guard
    #[inline]
    unsafe fn with_tx_cow<'a, F>(&'a self, f: F) -> Result<Option<Cow<'a, [u8]>>, MdbxError>
    where
        F: FnOnce(&mut TxUnsync<K>) -> Result<Option<Cow<'_, [u8]>>, MdbxError>,
    {
        let mut guard = self.inner.borrow_mut();
        let result = f(&mut *guard);
        // SAFETY: The Cow data comes from MDBX which keeps it valid for the
        // transaction lifetime. The Tx struct owns the transaction, so 'a
        // (the Tx lifetime) is valid.
        result.map(|opt| {
            opt.map(|cow| unsafe { std::mem::transmute::<Cow<'_, [u8]>, Cow<'a, [u8]>>(cow) })
        })
    }

    /// Gets the Database handle for a table (cached internally by signet-libmdbx).
    fn get_db(&self, name: Option<&str>) -> Result<Database, MdbxError> {
        self.with_tx(|tx| tx.open_db(name).map_err(MdbxError::Mdbx))
    }

    /// Reads FixedSizeInfo from the metadata table.
    fn read_fsi_from_table(&self, name: &'static str) -> Result<FixedSizeInfo, MdbxError> {
        let mut key = B256::ZERO;
        let to_copy = core::cmp::min(32, name.len());
        key[..to_copy].copy_from_slice(&name.as_bytes()[..to_copy]);

        let db = self.get_db(None)?;
        // Note: We request Vec<u8> (owned) since we only need it briefly for decoding
        // and don't need zero-copy for this small metadata read.
        let data: Vec<u8> = self
            .with_tx(|tx| tx.get(db.dbi(), key.as_slice()).map_err(MdbxError::from))?
            .ok_or(MdbxError::UnknownTable(name))?;

        // Migration: handle both old (16 byte) and new (8 byte) formats
        let fsi = if data.len() == 16 {
            // Old format: skip dbi (4) + flags (4), read FixedSizeInfo from offset 8
            FixedSizeInfo::decode_value(&data[8..16])
        } else {
            // New format: 8 bytes of FixedSizeInfo
            FixedSizeInfo::decode_value(&data)
        }
        .map_err(MdbxError::Deser)?;

        Ok(fsi)
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
        self.get_db(Some(table)).map(|db| db.dbi())
    }

    /// Gets the database handle (dbi) for the given table.
    pub fn get_dbi<T: Table>(&self) -> Result<u32, MdbxError> {
        self.get_dbi_raw(T::NAME)
    }

    /// Gets this transaction ID.
    pub fn id(&self) -> Result<u64, MdbxError> {
        self.with_tx(|tx| tx.id().map_err(MdbxError::Mdbx))
    }

    /// Create [`Cursor`] for raw table name.
    pub fn new_cursor_raw<'a>(&'a self, name: &'static str) -> Result<Cursor<'a, K>, MdbxError> {
        let db = self.get_db(Some(name))?;
        let fsi = self.get_fsi(name)?;

        // Create cursor with RefCell borrow, then extend its lifetime.
        //
        // SAFETY: The cursor is valid for the transaction lifetime, which is
        // owned by the Tx struct. The RefCell borrow is short-lived, but the
        // underlying MDBX cursor remains valid because:
        // 1. The transaction (TxUnsync) is owned by self.inner
        // 2. MDBX cursors are valid for the transaction's lifetime
        // 3. The Tx struct's lifetime 'a bounds how long the cursor can be used
        let cursor = {
            let guard = self.inner.borrow_mut();
            let cursor = guard.cursor(db)?;
            // Transmute the cursor lifetime from the RefCell borrow to 'a
            unsafe {
                std::mem::transmute::<
                    signet_libmdbx::Cursor<'_, K, K::Inner>,
                    signet_libmdbx::Cursor<'a, K, K::Inner>,
                >(cursor)
            }
        };
        Ok(Cursor::new(cursor, fsi))
    }

    /// Create a [`Cursor`] for the given table.
    pub fn new_cursor<'a, T: Table>(&'a self) -> Result<Cursor<'a, K>, MdbxError> {
        Self::new_cursor_raw(self, T::NAME)
    }
}

impl Tx<RW> {
    /// Stores FixedSizeInfo in the metadata table.
    fn store_fsi(&self, table: &'static str, fsi: FixedSizeInfo) -> Result<(), MdbxError> {
        let db = self.get_db(None)?;

        let mut key_buf = [0u8; MAX_KEY_SIZE];
        let to_copy = core::cmp::min(32, table.len());
        key_buf[..to_copy].copy_from_slice(&table.as_bytes()[..to_copy]);

        let mut value_buf = [0u8; 8];
        fsi.encode_value_to(&mut value_buf.as_mut_slice());

        self.with_tx(|tx| tx.put(db, key_buf, value_buf, WriteFlags::UPSERT))?;
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
        // SAFETY: MDBX data is valid for transaction lifetime
        unsafe {
            self.with_tx_cow(|tx| {
                let result: Result<Option<Cow<'_, [u8]>>, _> = tx.get(dbi, key.as_ref());
                result.map_err(MdbxError::from)
            })
        }
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

impl HotKvWrite for Tx<RW> {
    type TraverseMut<'a> = Cursor<'a, RW>;

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
        let db = self.get_db(Some(table))?;
        self.with_tx(|tx| tx.put(db, key, value, WriteFlags::UPSERT).map_err(MdbxError::Mdbx))
    }

    fn queue_raw_put_dual(
        &self,
        table: &'static str,
        key1: &[u8],
        key2: &[u8],
        value: &[u8],
    ) -> Result<(), Self::Error> {
        let db = self.get_db(Some(table))?;
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
            self.with_tx(|tx| {
                let mut cursor = tx.cursor(db).map_err(MdbxError::Mdbx)?;
                if let Some(found_val) = cursor
                    .get_both_range::<Cow<'_, [u8]>>(key1, search_val)
                    .map_err(MdbxError::from)?
                {
                    // Check if found value starts with our key2
                    if found_val.starts_with(key2) {
                        cursor.del(Default::default()).map_err(MdbxError::Mdbx)?;
                    }
                }
                Ok::<_, MdbxError>(())
            })?;
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
            return self.with_tx(|tx| {
                tx.put(db, key1, &combined, WriteFlags::UPSERT).map_err(MdbxError::Mdbx)
            });
        }

        // Use the scratch buffer
        let mut buffer = [0u8; TX_BUFFER_SIZE];
        let buf = &mut buffer[..key2.len() + value.len()];
        buf[..key2.len()].copy_from_slice(key2);
        buf[key2.len()..].copy_from_slice(value);
        self.with_tx(|tx| tx.put(db, key1, buf, WriteFlags::UPSERT))?;

        Ok(())
    }

    fn queue_raw_delete(&self, table: &'static str, key: &[u8]) -> Result<(), Self::Error> {
        let db = self.get_db(Some(table))?;
        self.with_tx(|tx| tx.del(db, key, None).map(|_| ()).map_err(MdbxError::Mdbx))
    }

    fn queue_raw_delete_dual(
        &self,
        table: &'static str,
        key1: &[u8],
        key2: &[u8],
    ) -> Result<(), Self::Error> {
        let db = self.get_db(Some(table))?;
        let fsi = self.get_fsi(table)?;

        // For DUPSORT tables, the "value" is key2 concatenated with the actual
        // value. If the table is ALSO dupfixed, we need to pad key2 to the
        // fixed size
        if let Some(total_size) = fsi.total_size() {
            // Copy key2 to scratch buffer and zero-pad to total fixed size
            let mut buffer = [0u8; TX_BUFFER_SIZE];
            buffer[..key2.len()].copy_from_slice(key2);
            buffer[key2.len()..total_size].fill(0);
            let k2 = &buffer[..total_size];

            self.with_tx(|tx| tx.del(db, key1, Some(k2)).map(|_| ()).map_err(MdbxError::Mdbx))
        } else {
            self.with_tx(|tx| tx.del(db, key1, Some(key2)).map(|_| ()).map_err(MdbxError::Mdbx))
        }
    }

    fn queue_raw_clear(&self, table: &'static str) -> Result<(), Self::Error> {
        let db = self.get_db(Some(table))?;
        self.with_tx(|tx| tx.clear_db(db).map_err(MdbxError::Mdbx))
    }

    fn queue_raw_create(
        &self,
        table: &'static str,
        dual_key: Option<usize>,
        fixed_val: Option<usize>,
        int_key: bool,
    ) -> Result<(), Self::Error> {
        let mut flags = DatabaseFlags::default();

        let mut fsi = FixedSizeInfo::None;

        if let Some(key2_size) = dual_key {
            flags.set(signet_libmdbx::DatabaseFlags::DUP_SORT, true);
            if let Some(value_size) = fixed_val {
                flags.set(signet_libmdbx::DatabaseFlags::DUP_FIXED, true);
                fsi = FixedSizeInfo::DupFixed { key2_size, total_size: key2_size + value_size };
            } else {
                // DUPSORT without DUP_FIXED - variable value size
                fsi = FixedSizeInfo::DupSort { key2_size };
            }
        }

        if int_key {
            flags.set(signet_libmdbx::DatabaseFlags::INTEGER_KEY, true);
        }

        // no clone. sad.
        let flags2 = DatabaseFlags::from_bits(flags.bits()).unwrap();

        self.with_tx(|tx| tx.create_db(Some(table), flags2))?;
        self.store_fsi(table, fsi)?;

        Ok(())
    }

    fn queue_put<T: SingleKey>(&self, key: &T::Key, value: &T::Value) -> Result<(), Self::Error> {
        let db = self.get_db(Some(T::NAME))?;
        let mut key_buf = [0u8; MAX_KEY_SIZE];
        let key_bytes = key.encode_key(&mut key_buf);

        self.with_tx(|tx| {
            tx.with_reservation(
                db,
                key_bytes,
                value.encoded_size(),
                WriteFlags::UPSERT,
                |mut reserved| value.encode_value_to(&mut reserved),
            )
            .map_err(MdbxError::from)
        })
    }

    fn queue_put_many_dual<'a, 'b, 'c, T, I, J>(&self, groups: I) -> Result<(), Self::Error>
    where
        T: DualKey,
        T::Key: 'a,
        T::Key2: 'b,
        T::Value: 'c,
        I: IntoIterator<Item = (&'a T::Key, J)>,
        J: IntoIterator<Item = (&'b T::Key2, &'c T::Value)>,
    {
        // Compile-time check - optimizer eliminates dead branch per table type
        if !(T::IS_FIXED_VAL && T::DUAL_KEY) {
            // Not a DUP_FIXED table - use default loop implementation
            for (key1, entries) in groups {
                for (key2, value) in entries {
                    self.queue_put_dual::<T>(key1, key2, value)?;
                }
            }
            return Ok(());
        }

        // Sizes known at compile time (monomorphizes per table)
        let dual_key_size = T::DUAL_KEY_SIZE.unwrap();
        let fixed_val_size = T::FIXED_VAL_SIZE.unwrap();
        let entry_size = dual_key_size + fixed_val_size;

        let mut cursor = self.new_cursor::<T>()?;
        let mut key1_buf = [0u8; MAX_KEY_SIZE];
        let mut key2_buf = [0u8; MAX_KEY_SIZE];

        // Calculate max entries per page to bound buffer size
        let page_size = self.with_tx(|tx| tx.env().stat().map(|s| s.page_size() as usize))?;
        let max_entries_per_page = page_size / entry_size;
        let mut buffer = Vec::with_capacity(max_entries_per_page * entry_size);

        // Process each key1 group
        for (key1, entries) in groups {
            let key1_bytes = key1.encode_key(&mut key1_buf);
            buffer.clear();
            let mut entry_count = 0;

            for (key2, value) in entries {
                let key2_bytes = key2.encode_key(&mut key2_buf);
                let value_bytes = value.encoded();
                buffer.extend_from_slice(key2_bytes);
                buffer.extend_from_slice(&value_bytes);
                entry_count += 1;

                // Flush when buffer reaches page capacity
                if entry_count == max_entries_per_page {
                    cursor.put_multiple_fixed(key1_bytes, &buffer, entry_count, false)?;
                    buffer.clear();
                    entry_count = 0;
                }
            }

            // Flush remaining entries for this key1
            if entry_count > 0 {
                cursor.put_multiple_fixed(key1_bytes, &buffer, entry_count, false)?;
            }
        }

        Ok(())
    }

    fn raw_commit(self) -> Result<(), Self::Error> {
        // Take ownership of the inner transaction from the RefCell
        let inner = self.inner.into_inner();
        inner.commit().map_err(MdbxError::Mdbx)
    }
}
