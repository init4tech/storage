use crate::{model::HotKvWrite, tables};
use ahash::AHashMap;
use alloy::primitives::{Address, B256, U256};
use signet_storage_types::{Account, SealedHeader};
use trevm::revm::bytecode::Bytecode;

/// Bundle state initialization type.
/// Maps address -> (old_account, new_account, storage_changes)
/// where storage_changes maps slot (B256) -> (old_value, new_value)
pub type BundleInit =
    AHashMap<Address, (Option<Account>, Option<Account>, AHashMap<B256, (U256, U256)>)>;

/// Trait for database write operations on standard hot tables.
///
/// This trait is low-level, and usage may leave the database in an
/// inconsistent state if not used carefully. Users should prefer
/// [`HistoryWrite`] or higher-level abstractions when possible.
///
/// [`HistoryWrite`]: crate::db::HistoryWrite
pub trait UnsafeDbWrite: HotKvWrite + super::sealed::Sealed {
    /// Write a block header. This will leave the DB in an inconsistent state
    /// until the corresponding header number is also written. Users should
    /// prefer [`Self::put_header`] instead.
    fn put_header_inconsistent(&self, header: &SealedHeader) -> Result<(), Self::Error> {
        self.queue_put::<tables::Headers>(&header.number, header)
    }

    /// Append a block header. Block number must be > all existing block numbers.
    ///
    /// This will leave the DB in an inconsistent state until the corresponding
    /// header number is also written. Users should prefer [`Self::put_header`]
    /// instead.
    fn append_header(&self, header: &SealedHeader) -> Result<(), Self::Error> {
        self.queue_append::<tables::Headers>(&header.number, header)
    }

    /// Write a block number by its hash. This will leave the DB in an
    /// inconsistent state until the corresponding header is also written.
    /// Users should prefer [`Self::put_header`] instead.
    fn put_header_number_inconsistent(&self, hash: &B256, number: u64) -> Result<(), Self::Error> {
        self.queue_put::<tables::HeaderNumbers>(hash, &number)
    }

    /// Write contract Bytecode by its hash.
    fn put_bytecode(&self, code_hash: &B256, bytecode: &Bytecode) -> Result<(), Self::Error> {
        self.queue_put::<tables::Bytecodes>(code_hash, bytecode)
    }

    /// Write an account by its address.
    fn put_account(&self, address: &Address, account: &Account) -> Result<(), Self::Error> {
        self.queue_put::<tables::PlainAccountState>(address, account)
    }

    /// Append an account by its address. This should generally only be used
    /// when initializing the database (e.g., from genesis).
    fn append_account(&self, address: &Address, account: &Account) -> Result<(), Self::Error> {
        self.queue_append::<tables::PlainAccountState>(address, account)
    }

    /// Write a storage entry by its address and key.
    fn put_storage(&self, address: &Address, key: &U256, entry: &U256) -> Result<(), Self::Error> {
        self.queue_put_dual::<tables::PlainStorageState>(address, key, entry)
    }

    /// Append a storage entry by its address and key. This should generally
    /// only be used when initializing the database (e.g., from genesis).
    fn append_storage(
        &self,
        address: &Address,
        key: &U256,
        entry: &U256,
    ) -> Result<(), Self::Error> {
        self.queue_append_dual::<tables::PlainStorageState>(address, key, entry)
    }

    /// Write a sealed block header (header + number).
    fn put_header(&self, header: &SealedHeader) -> Result<(), Self::Error> {
        self.put_header_inconsistent(header)
            .and_then(|_| self.put_header_number_inconsistent(&header.hash(), header.number))
    }

    /// Delete a header by block number.
    fn delete_header(&self, number: u64) -> Result<(), Self::Error> {
        self.queue_delete::<tables::Headers>(&number)
    }

    /// Delete a header number mapping by hash.
    fn delete_header_number(&self, hash: &B256) -> Result<(), Self::Error> {
        self.queue_delete::<tables::HeaderNumbers>(hash)
    }

    /// Commit the write transaction.
    fn commit(self) -> Result<(), Self::Error>
    where
        Self: Sized,
    {
        HotKvWrite::raw_commit(self)
    }
}

impl<T> UnsafeDbWrite for T where T: HotKvWrite {}
