use crate::{
    model::{
        DualKeyTraverse, DualKeyTraverseMut, DualTableCursor, HotKvError, HotKvReadError,
        KvTraverse, KvTraverseMut, TableCursor,
        revm::{RevmRead, RevmWrite},
    },
    ser::{KeySer, MAX_KEY_SIZE, ValSer},
    tables::{DualKey, SingleKey, Table},
};
use std::borrow::Cow;

/// Trait for hot storage. This is a KV store with read/write transactions.
///
/// This is the top-level trait for hot storage backends, providing
/// transactional access through read-only and read-write transactions.
///
/// We recommend using [`HistoryRead`] and [`HistoryWrite`] for most use cases,
/// as they provide higher-level abstractions over predefined tables.
///
/// When implementing this trait, consult the [`model`] module documentation for
/// details on the associated types and their requirements.
///
/// [`HistoryRead`]: crate::db::HistoryRead
/// [`HistoryWrite`]: crate::db::HistoryWrite
/// [`model`]: crate::model
#[auto_impl::auto_impl(&, Arc, Box)]
pub trait HotKv {
    /// The read-only transaction type.
    type RoTx: HotKvRead;

    /// The read-write transaction type.
    type RwTx: HotKvWrite;

    /// Create a read-only transaction.
    fn reader(&self) -> Result<Self::RoTx, HotKvError>;

    /// Create a read-only transaction, and wrap it in an adapter for the
    /// revm [`DatabaseRef`] trait. The resulting reader can be used directly
    /// with [`trevm`] and [`revm`].
    ///
    /// [`revm`]: trevm::revm
    /// [`DatabaseRef`]: trevm::revm::database::DatabaseRef
    fn revm_reader(&self) -> Result<RevmRead<Self::RoTx>, HotKvError> {
        self.reader().map(RevmRead::new)
    }

    /// Create a read-write transaction.
    ///
    /// This is allowed to fail with [`Err(HotKvError::WriteLocked)`] if
    /// multiple write transactions are not supported concurrently.
    ///
    /// # Returns
    ///
    /// - `Ok(Some(tx))` if the write transaction was created successfully.
    /// - [`Err(HotKvError::WriteLocked)`] if there is already a write
    ///   transaction in progress.
    /// - [`Err(HotKvError::Inner)`] if there was an error creating the
    ///   transaction.
    ///
    /// [`Err(HotKvError::Inner)`]: HotKvError::Inner
    /// [`Err(HotKvError::WriteLocked)`]: HotKvError::WriteLocked
    fn writer(&self) -> Result<Self::RwTx, HotKvError>;

    /// Create a read-write transaction, and wrap it in an adapter for the
    /// revm [`TryDatabaseCommit`] trait. The resulting writer can be used
    /// directly with [`trevm`] and [`revm`].
    ///
    ///
    /// [`revm`]: trevm::revm
    /// [`TryDatabaseCommit`]: trevm::revm::database::TryDatabaseCommit
    fn revm_writer(&self) -> Result<RevmWrite<Self::RwTx>, HotKvError> {
        self.writer().map(RevmWrite::new)
    }
}

/// Trait for hot storage read transactions.
///
/// This trait provides read-only access to hot storage tables. It should only
/// be imported if accessing custom tables, or when implementing new hot storage
/// backends.
#[auto_impl::auto_impl(&, Arc, Box)]
pub trait HotKvRead {
    /// Error type for read operations.
    type Error: HotKvReadError;

    /// The cursor type for traversing key-value pairs.
    type Traverse<'a>: KvTraverse<Self::Error> + DualKeyTraverse<Self::Error>
    where
        Self: 'a;

    /// Get a raw cursor to traverse the database.
    fn raw_traverse<'a>(&'a self, table: &'static str) -> Result<Self::Traverse<'a>, Self::Error>;

    /// Get a raw value from a specific table.
    ///
    /// The `key` buf must be <= [`MAX_KEY_SIZE`] bytes. Implementations are
    /// allowed to panic if this is not the case.
    ///
    /// If the table is dual-keyed, the output MAY be implementation-defined.
    fn raw_get<'a>(
        &'a self,
        table: &'static str,
        key: &[u8],
    ) -> Result<Option<Cow<'a, [u8]>>, Self::Error>;

    /// Get a raw value from a specific table with dual keys.
    ///
    /// If `key1` is present, but `key2` is not in the table, the output is
    /// implementation-defined. For sorted databases, it SHOULD return the value
    /// of the NEXT populated key. It MAY also return `None`, even if other
    /// subkeys are populated.
    ///
    /// If the table is not dual-keyed, the output MAY be
    /// implementation-defined.
    fn raw_get_dual<'a>(
        &'a self,
        table: &'static str,
        key1: &[u8],
        key2: &[u8],
    ) -> Result<Option<Cow<'a, [u8]>>, Self::Error>;

    /// Traverse a specific table. Returns a typed cursor wrapper.
    fn traverse<'a, T: SingleKey>(
        &'a self,
    ) -> Result<TableCursor<Self::Traverse<'a>, T, Self::Error>, Self::Error> {
        let cursor = self.raw_traverse(T::NAME)?;
        Ok(TableCursor::new(cursor))
    }

    /// Traverse a specific dual-keyed table. Returns a typed dual-keyed
    /// cursor wrapper.
    fn traverse_dual<'a, T: DualKey>(
        &'a self,
    ) -> Result<DualTableCursor<Self::Traverse<'a>, T, Self::Error>, Self::Error> {
        let cursor = self.raw_traverse(T::NAME)?;
        Ok(DualTableCursor::new(cursor))
    }

    /// Get a value from a specific table.
    fn get<T: SingleKey>(&self, key: &T::Key) -> Result<Option<T::Value>, Self::Error> {
        let mut key_buf = [0u8; MAX_KEY_SIZE];
        let key_bytes = key.encode_key(&mut key_buf);
        debug_assert!(
            key_bytes.len() == T::Key::SIZE,
            "Encoded key length does not match expected size"
        );

        let Some(value_bytes) = self.raw_get(T::NAME, key_bytes)? else {
            return Ok(None);
        };
        T::Value::decode_value(&value_bytes).map(Some).map_err(Into::into)
    }

    /// Get a value from a specific dual-keyed table.
    ///
    /// If `key1` is present, but `key2` is not in the table, the output is
    /// implementation-defined. For sorted databases, it SHOULD return the value
    /// of the NEXT populated key. It MAY also return `None`, even if other
    /// subkeys are populated.
    ///
    /// If the table is not dual-keyed, the output MAY be
    /// implementation-defined.
    fn get_dual<T: DualKey>(
        &self,
        key1: &T::Key,
        key2: &T::Key2,
    ) -> Result<Option<T::Value>, Self::Error> {
        let mut key1_buf = [0u8; MAX_KEY_SIZE];
        let mut key2_buf = [0u8; MAX_KEY_SIZE];

        let key1_bytes = key1.encode_key(&mut key1_buf);
        let key2_bytes = key2.encode_key(&mut key2_buf);

        let Some(value_bytes) = self.raw_get_dual(T::NAME, key1_bytes, key2_bytes)? else {
            return Ok(None);
        };
        T::Value::decode_value(&value_bytes).map(Some).map_err(Into::into)
    }
}

/// Trait for hot storage write transactions.
///
/// This extends the [`HotKvRead`] trait with write capabilities.
pub trait HotKvWrite: HotKvRead {
    /// The mutable cursor type for traversing key-value pairs.
    type TraverseMut<'a>: KvTraverseMut<Self::Error> + DualKeyTraverseMut<Self::Error>
    where
        Self: 'a;

    /// Get a raw mutable cursor to traverse the database.
    fn raw_traverse_mut<'a>(
        &'a self,
        table: &'static str,
    ) -> Result<Self::TraverseMut<'a>, Self::Error>;

    /// Queue a raw put operation.
    ///
    /// The `key` buf must be <= [`MAX_KEY_SIZE`] bytes. Implementations are
    /// allowed to panic if this is not the case.
    fn queue_raw_put(
        &self,
        table: &'static str,
        key: &[u8],
        value: &[u8],
    ) -> Result<(), Self::Error>;

    /// Queue a raw put operation for a dual-keyed table.
    ////
    /// The `key1` and `key2` buf must be <= [`MAX_KEY_SIZE`] bytes.
    /// Implementations are allowed to panic if this is not the case.
    fn queue_raw_put_dual(
        &self,
        table: &'static str,
        key1: &[u8],
        key2: &[u8],
        value: &[u8],
    ) -> Result<(), Self::Error>;

    /// Queue a raw delete operation.
    ///
    /// The `key` buf must be <= [`MAX_KEY_SIZE`] bytes. Implementations are
    /// allowed to panic if this is not the case.
    fn queue_raw_delete(&self, table: &'static str, key: &[u8]) -> Result<(), Self::Error>;

    /// Queue a raw delete operation for a dual-keyed table.
    ///
    /// The `key1` and `key2` buf must be <= [`MAX_KEY_SIZE`] bytes.
    /// Implementations are allowed to panic if this is not the case.
    fn queue_raw_delete_dual(
        &self,
        table: &'static str,
        key1: &[u8],
        key2: &[u8],
    ) -> Result<(), Self::Error>;

    /// Queue a raw clear operation for a specific table.
    fn queue_raw_clear(&self, table: &'static str) -> Result<(), Self::Error>;

    /// Queue a raw create operation for a specific table.
    ///
    /// This abstraction supports table specializations:
    /// 1. `dual_key_size` - whether the table is dual-keyed (i.e.,
    ///    `DUPSORT` in LMDB/MDBX). If so, the argument MUST be the
    ///    encoded size of the second key. If not, it MUST be `None`.
    /// 2. `fixed_val_size`: whether the table has fixed-size values.
    ///    If so, the argument MUST be the size of the fixed value.
    ///    If not, it MUST be `None`.
    /// 3. `int_key`: whether the table uses an integer key (u32 or u64).
    ///    If `true`, the backend MAY use optimizations like MDBX's
    ///    `INTEGER_KEY` flag for native-endian key storage.
    ///
    /// Database implementations can use this information for optimizations.
    fn queue_raw_create(
        &self,
        table: &'static str,
        dual_key_size: Option<usize>,
        fixed_val_size: Option<usize>,
        int_key: bool,
    ) -> Result<(), Self::Error>;

    /// Traverse a specific table. Returns a mutable typed cursor wrapper.
    /// If invoked for a dual-keyed table, it will traverse the primary keys
    /// only, and the return value may be implementation-defined.
    fn traverse_mut<'a, T: SingleKey>(
        &'a self,
    ) -> Result<TableCursor<Self::TraverseMut<'a>, T, Self::Error>, Self::Error> {
        let cursor = self.raw_traverse_mut(T::NAME)?;
        Ok(TableCursor::new(cursor))
    }

    /// Traverse a specific dual-keyed table. Returns a mutable typed
    /// dual-keyed cursor wrapper.
    fn traverse_dual_mut<'a, T: DualKey>(
        &'a self,
    ) -> Result<DualTableCursor<Self::TraverseMut<'a>, T, Self::Error>, Self::Error> {
        let cursor = self.raw_traverse_mut(T::NAME)?;
        Ok(DualTableCursor::new(cursor))
    }

    /// Queue a put operation for a specific table.
    fn queue_put<T: SingleKey>(&self, key: &T::Key, value: &T::Value) -> Result<(), Self::Error> {
        let mut key_buf = [0u8; MAX_KEY_SIZE];
        let key_bytes = key.encode_key(&mut key_buf);
        let value_bytes = value.encoded();

        self.queue_raw_put(T::NAME, key_bytes, &value_bytes)
    }

    /// Append a key-value pair. Key must be > all existing keys.
    ///
    /// Default implementation falls back to `queue_put`. Backends that support
    /// optimized append (like MDBX) should override via cursor.append().
    fn queue_append<T: SingleKey>(
        &self,
        key: &T::Key,
        value: &T::Value,
    ) -> Result<(), Self::Error> {
        self.queue_put::<T>(key, value)
    }

    /// Queue a put operation for a specific dual-keyed table.
    fn queue_put_dual<T: DualKey>(
        &self,
        key1: &T::Key,
        key2: &T::Key2,
        value: &T::Value,
    ) -> Result<(), Self::Error> {
        let mut key1_buf = [0u8; MAX_KEY_SIZE];
        let mut key2_buf = [0u8; MAX_KEY_SIZE];
        let key1_bytes = key1.encode_key(&mut key1_buf);
        let key2_bytes = key2.encode_key(&mut key2_buf);
        let value_bytes = value.encoded();

        self.queue_raw_put_dual(T::NAME, key1_bytes, key2_bytes, &value_bytes)
    }

    /// Append a dual-key entry. k2 must be > all existing k2s for k1.
    ///
    /// Default implementation falls back to `queue_put_dual`. Backends that support
    /// optimized append (like MDBX) should override via cursor.append_dual().
    fn queue_append_dual<T: DualKey>(
        &self,
        k1: &T::Key,
        k2: &T::Key2,
        value: &T::Value,
    ) -> Result<(), Self::Error> {
        self.queue_put_dual::<T>(k1, k2, value)
    }

    /// Queue a delete operation for a specific table.
    fn queue_delete<T: SingleKey>(&self, key: &T::Key) -> Result<(), Self::Error> {
        let mut key_buf = [0u8; MAX_KEY_SIZE];
        let key_bytes = key.encode_key(&mut key_buf);

        self.queue_raw_delete(T::NAME, key_bytes)
    }

    /// Queue a delete operation for a specific dual-keyed table.
    fn queue_delete_dual<T: DualKey>(
        &self,
        key1: &T::Key,
        key2: &T::Key2,
    ) -> Result<(), Self::Error> {
        let mut key1_buf = [0u8; MAX_KEY_SIZE];
        let mut key2_buf = [0u8; MAX_KEY_SIZE];
        let key1_bytes = key1.encode_key(&mut key1_buf);
        let key2_bytes = key2.encode_key(&mut key2_buf);

        self.queue_raw_delete_dual(T::NAME, key1_bytes, key2_bytes)
    }

    /// Queue creation of a specific table.
    fn queue_create<T>(&self) -> Result<(), Self::Error>
    where
        T: Table,
    {
        self.queue_raw_create(T::NAME, T::DUAL_KEY_SIZE, T::FIXED_VAL_SIZE, T::INT_KEY)
    }

    /// Queue clearing all entries in a specific table.
    fn queue_clear<T>(&self) -> Result<(), Self::Error>
    where
        T: Table,
    {
        self.queue_raw_clear(T::NAME)
    }

    /// Clear all K2 entries for a specific K1 in a dual-keyed table.
    fn clear_k1_for<T: DualKey>(&self, key1: &T::Key) -> Result<(), Self::Error> {
        let mut cursor = self.traverse_dual_mut::<T>()?;
        cursor.clear_k1(key1)
    }

    /// Commit the queued operations.
    fn raw_commit(self) -> Result<(), Self::Error>;
}
