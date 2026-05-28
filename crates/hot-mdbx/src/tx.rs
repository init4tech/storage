//! Transaction wrapper for libmdbx-sys.
use crate::{Cursor, FixedSizeInfo, FsiCache, MdbxError};
use alloy::primitives::B256;
use signet_hot::{
    KeySer, MAX_FIXED_VAL_SIZE, MAX_KEY_SIZE, ValSer,
    model::{DualTableTraverse, HotKvRead, HotKvWrite},
    tables::{DualKey, SingleKey, Table},
};
use signet_libmdbx::{Database, Rw, RwSync, TransactionKind, WriteFlags, tx::WriteMarker};
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

/// Per-shard byte budget for sharded history tables. Derived from MDBX's
/// DUPSORT value cap (~1980 B on 4 KB pages) minus key2 and per-node
/// overhead, with comfortable headroom for roaring encoding variability.
pub(crate) const MAX_SHARD_BYTES: usize = 1500;

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
    pub(crate) fn read_fsi_from_table(
        &self,
        name: &'static str,
    ) -> Result<FixedSizeInfo, MdbxError> {
        let db = self.inner.open_db(None)?;

        let data: [u8; 8] = self
            .inner
            .get(db.dbi(), fsi_name_to_key(name).as_slice())
            .map_err(MdbxError::from)?
            .ok_or(MdbxError::UnknownTable(name))?;

        FixedSizeInfo::decode_value(&data).map_err(MdbxError::Deser)
    }

    /// Gets cached FixedSizeInfo for a table.
    pub fn get_fsi(&self, name: &'static str) -> Result<FixedSizeInfo, MdbxError> {
        // Fast path: lock-free scan over known tables, then locked dynamic map.
        if let Some(fsi) = self.fsi_cache.get(name) {
            return Ok(fsi);
        }
        // Slow path: read from table, then insert into dynamic map.
        let fsi = self.read_fsi_from_table(name)?;
        self.fsi_cache.insert_dynamic(name, fsi);
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
    /// Deletes an existing DUPSORT entry matching `(key1, key2)` if one
    /// exists. Uses `get_both_range` to find the first dup value whose key2
    /// prefix matches, then deletes it.
    fn delete_dup_entry(
        &self,
        db: Database,
        fsi: FixedSizeInfo,
        key1: &[u8],
        key2: &[u8],
    ) -> Result<(), MdbxError> {
        let mut search_buf = [0u8; TX_BUFFER_SIZE];
        let search_val = if let Some(total_size) = fsi.total_size() {
            search_buf[..key2.len()].copy_from_slice(key2);
            search_buf[key2.len()..total_size].fill(0);
            &search_buf[..total_size]
        } else {
            key2
        };

        let mut cursor = self.inner.cursor(db).map_err(MdbxError::Mdbx)?;

        if let Some(found_val) =
            cursor.get_both_range::<Cow<'_, [u8]>>(key1, search_val).map_err(MdbxError::from)?
            && found_val.starts_with(key2)
        {
            cursor.del().map_err(MdbxError::Mdbx)?;
        }

        Ok(())
    }

    /// Stores FixedSizeInfo in the metadata table.
    fn store_fsi(&self, table: &'static str, fsi: FixedSizeInfo) -> Result<(), MdbxError> {
        let db = self.inner.open_db(None)?;

        let mut value_buf = [0u8; 8];
        fsi.encode_value_to(&mut value_buf.as_mut_slice());

        self.inner.put(db, fsi_name_to_key(table).as_slice(), value_buf, WriteFlags::UPSERT)?;
        self.fsi_cache.insert_dynamic(table, fsi);

        Ok(())
    }
}

fn fsi_name_to_key(name: &'static str) -> B256 {
    assert!(
        name.len() <= 32,
        "table name exceeds 32 bytes and would be truncated in the FSI metadata key: {name}"
    );
    let mut key = B256::ZERO;
    key[..name.len()].copy_from_slice(name.as_bytes());
    key
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
        Err(MdbxError::RawDualUnsupported)
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

                if !fsi.is_dupsort() {
                    return Err(MdbxError::NotDupSort);
                }

                // Delete any existing entry with the same (key1, key2)
                // before inserting, because MDBX stores key2 as part of
                // the value (key2||actual_value). Without deletion, putting
                // a new value for the same key2 creates a duplicate entry
                // instead of replacing.
                self.delete_dup_entry(db, fsi, key1, key2)?;

                if key2.len() + value.len() > TX_BUFFER_SIZE {
                    let mut combined = Vec::with_capacity(key2.len() + value.len());
                    combined.extend_from_slice(key2);
                    combined.extend_from_slice(value);
                    return self
                        .inner
                        .put(db, key1, &combined, WriteFlags::UPSERT)
                        .map_err(MdbxError::Mdbx);
                }

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

                if !fsi.is_dupsort() {
                    return Err(MdbxError::NotDupSort);
                }

                self.delete_dup_entry(db, fsi, key1, key2)
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
                let db = self.inner.open_db(Some(T::NAME))?;
                let mut key_buf = [0u8; MAX_KEY_SIZE];
                let key_bytes = key.encode_key(&mut key_buf);
                self.inner
                    .with_reservation(
                        db,
                        key_bytes,
                        value.encoded_size(),
                        WriteFlags::APPEND,
                        |mut reserved| value.encode_value_to(&mut reserved),
                    )
                    .map_err(MdbxError::from)
            }

            fn queue_append_dual<T: DualKey>(
                &self,
                k1: &T::Key,
                k2: &T::Key2,
                value: &T::Value,
            ) -> Result<(), Self::Error> {
                let db = self.inner.open_db(Some(T::NAME))?;
                let mut k1_buf = [0u8; MAX_KEY_SIZE];
                let mut k2_buf = [0u8; MAX_KEY_SIZE];
                let k1_bytes = k1.encode_key(&mut k1_buf);
                let k2_bytes = k2.encode_key(&mut k2_buf);
                let value_size = value.encoded_size();
                let total = k2_bytes.len() + value_size;

                if total <= TX_BUFFER_SIZE {
                    let mut buffer = [0u8; TX_BUFFER_SIZE];
                    buffer[..k2_bytes.len()].copy_from_slice(k2_bytes);
                    let mut val_buf = &mut buffer[k2_bytes.len()..k2_bytes.len() + value_size];
                    value.encode_value_to(&mut val_buf);
                    self.inner.append_dup(db, k1_bytes, &buffer[..total]).map_err(MdbxError::from)
                } else {
                    let mut combined = vec![0u8; total];
                    combined[..k2_bytes.len()].copy_from_slice(k2_bytes);
                    let mut val_buf = &mut combined[k2_bytes.len()..];
                    value.encode_value_to(&mut val_buf);
                    self.inner.append_dup(db, k1_bytes, &combined).map_err(MdbxError::from)
                }
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

macro_rules! impl_history_write {
    ($ty:ty) => {
        impl signet_hot::db::HistoryWrite for Tx<$ty> {
            fn append_account_history(
                &self,
                addr: &alloy::primitives::Address,
                new_blocks: &signet_storage_types::BlockNumberList,
            ) -> Result<(), signet_hot::db::HistoryError<Self::Error>> {
                use signet_hot::{db::HistoryError, model::HotKvWrite, tables};

                let existing_tail = self
                    .get_dual::<tables::AccountsHistory>(addr, &u64::MAX)
                    .map_err(HistoryError::Db)?
                    .unwrap_or_default();

                if !existing_tail.is_empty() {
                    self.queue_delete_dual::<tables::AccountsHistory>(addr, &u64::MAX)
                        .map_err(HistoryError::Db)?;
                }

                let (first, second) =
                    existing_tail.overflowing_extend(new_blocks.iter(), MAX_SHARD_BYTES);

                match second {
                    None => self
                        .queue_put_dual::<tables::AccountsHistory>(addr, &u64::MAX, &first)
                        .map_err(HistoryError::Db),
                    Some(tail) => {
                        let seal_key = first.max().expect("first non-empty after split");
                        self.queue_put_dual::<tables::AccountsHistory>(addr, &seal_key, &first)
                            .map_err(HistoryError::Db)?;
                        self.queue_put_dual::<tables::AccountsHistory>(addr, &u64::MAX, &tail)
                            .map_err(HistoryError::Db)
                    }
                }
            }

            fn append_storage_history(
                &self,
                addr: &alloy::primitives::Address,
                slot: &alloy::primitives::U256,
                new_blocks: &signet_storage_types::BlockNumberList,
            ) -> Result<(), signet_hot::db::HistoryError<Self::Error>> {
                use signet_hot::{db::HistoryError, model::HotKvWrite, tables};
                use signet_storage_types::ShardedKey;

                let tail_key = ShardedKey::new(*slot, u64::MAX);
                let existing_tail = self
                    .get_dual::<tables::StorageHistory>(addr, &tail_key)
                    .map_err(HistoryError::Db)?
                    .unwrap_or_default();

                if !existing_tail.is_empty() {
                    self.queue_delete_dual::<tables::StorageHistory>(addr, &tail_key)
                        .map_err(HistoryError::Db)?;
                }

                let (first, second) =
                    existing_tail.overflowing_extend(new_blocks.iter(), MAX_SHARD_BYTES);

                match second {
                    None => self
                        .queue_put_dual::<tables::StorageHistory>(addr, &tail_key, &first)
                        .map_err(HistoryError::Db),
                    Some(tail) => {
                        let seal_block = first.max().expect("first non-empty after split");
                        let seal_key = ShardedKey::new(*slot, seal_block);
                        self.queue_put_dual::<tables::StorageHistory>(addr, &seal_key, &first)
                            .map_err(HistoryError::Db)?;
                        self.queue_put_dual::<tables::StorageHistory>(addr, &tail_key, &tail)
                            .map_err(HistoryError::Db)
                    }
                }
            }

            fn truncate_account_history_above(
                &self,
                addr: &alloy::primitives::Address,
                above: u64,
            ) -> Result<(), signet_hot::db::HistoryError<Self::Error>> {
                use signet_hot::{
                    db::HistoryError,
                    model::{HotKvRead, HotKvWrite},
                    tables,
                };
                use signet_storage_types::BlockNumberList;

                let mut cursor =
                    self.traverse_dual::<tables::AccountsHistory>().map_err(HistoryError::Db)?;

                let Some((_, mut key2, mut list)) =
                    cursor.last_of_k1(addr).map_err(HistoryError::Db)?
                else {
                    return Ok(());
                };

                let mut deleted_above = false;

                loop {
                    let max_in_shard = list.max().unwrap_or(0);

                    if max_in_shard <= above {
                        if deleted_above && key2 != u64::MAX {
                            self.queue_delete_dual::<tables::AccountsHistory>(addr, &key2)
                                .map_err(HistoryError::Db)?;
                            self.queue_put_dual::<tables::AccountsHistory>(addr, &u64::MAX, &list)
                                .map_err(HistoryError::Db)?;
                        }
                        return Ok(());
                    }

                    self.queue_delete_dual::<tables::AccountsHistory>(addr, &key2)
                        .map_err(HistoryError::Db)?;

                    let kept =
                        BlockNumberList::new_pre_sorted(list.iter().take_while(|&b| b <= above));
                    if !kept.is_empty() {
                        self.queue_put_dual::<tables::AccountsHistory>(addr, &u64::MAX, &kept)
                            .map_err(HistoryError::Db)?;
                        return Ok(());
                    }

                    deleted_above = true;
                    let Some((_, prev_key2, prev_list)) =
                        cursor.previous_k2().map_err(HistoryError::Db)?
                    else {
                        return Ok(());
                    };
                    key2 = prev_key2;
                    list = prev_list;
                }
            }

            fn truncate_storage_history_above(
                &self,
                addr: &alloy::primitives::Address,
                slot: &alloy::primitives::U256,
                above: u64,
            ) -> Result<(), signet_hot::db::HistoryError<Self::Error>> {
                use signet_hot::{
                    db::HistoryError,
                    model::{HotKvRead, HotKvWrite},
                    tables,
                };
                use signet_storage_types::{BlockNumberList, ShardedKey};

                let mut cursor =
                    self.traverse_dual::<tables::StorageHistory>().map_err(HistoryError::Db)?;

                let tail_key = ShardedKey::new(*slot, u64::MAX);

                // Walk backwards from the largest dup for this addr until we
                // find one matching this slot. The cursor may start on a
                // different slot for the same addr.
                let mut cur_entry = cursor.last_of_k1(addr).map_err(HistoryError::Db)?;
                loop {
                    match cur_entry {
                        None => return Ok(()),
                        Some((_, ref sk, _)) if sk.key == *slot => break,
                        Some(_) => {
                            cur_entry = cursor.previous_k2().map_err(HistoryError::Db)?;
                        }
                    }
                }
                let (_, mut sk, mut list) = cur_entry.expect("matched above");

                let mut deleted_above = false;

                loop {
                    let max_in_shard = list.max().unwrap_or(0);

                    if max_in_shard <= above {
                        if deleted_above && sk != tail_key {
                            self.queue_delete_dual::<tables::StorageHistory>(addr, &sk)
                                .map_err(HistoryError::Db)?;
                            self.queue_put_dual::<tables::StorageHistory>(addr, &tail_key, &list)
                                .map_err(HistoryError::Db)?;
                        }
                        return Ok(());
                    }

                    self.queue_delete_dual::<tables::StorageHistory>(addr, &sk)
                        .map_err(HistoryError::Db)?;

                    let kept =
                        BlockNumberList::new_pre_sorted(list.iter().take_while(|&b| b <= above));
                    if !kept.is_empty() {
                        self.queue_put_dual::<tables::StorageHistory>(addr, &tail_key, &kept)
                            .map_err(HistoryError::Db)?;
                        return Ok(());
                    }

                    deleted_above = true;
                    let prev = cursor.previous_k2().map_err(HistoryError::Db)?;
                    match prev {
                        None => return Ok(()),
                        Some((_, ref prev_sk, _)) if prev_sk.key != *slot => return Ok(()),
                        Some((_, prev_sk, prev_list)) => {
                            sk = prev_sk;
                            list = prev_list;
                        }
                    }
                }
            }
        }
    };
}

impl_history_write!(RwSync);
impl_history_write!(Rw);
