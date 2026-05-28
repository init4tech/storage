use crate::{model::HotKvRead, tables};
use alloy::primitives::{Address, B256, U256};
use signet_storage_types::{Account, SealedHeader};
use trevm::revm::bytecode::Bytecode;

/// Trait for database read operations on standard hot tables.
///
/// This is a high-level trait that provides convenient methods for reading
/// common data types from predefined hot storage tables. It builds upon the
/// lower-level [`HotKvRead`] trait, which provides raw key-value access.
///
/// Users should prefer this trait unless customizations are needed to the
/// table set.
pub trait HotDbRead: HotKvRead + super::sealed::Sealed {
    /// Read a block header by its number.
    fn get_header(&self, number: u64) -> Result<Option<SealedHeader>, Self::Error> {
        self.get::<tables::Headers>(&number)
    }

    /// Read a block number by its hash.
    fn get_header_number(&self, hash: &B256) -> Result<Option<u64>, Self::Error> {
        self.get::<tables::HeaderNumbers>(hash)
    }

    /// Read contract Bytecode by its hash.
    fn get_bytecode(&self, code_hash: &B256) -> Result<Option<Bytecode>, Self::Error> {
        self.get::<tables::Bytecodes>(code_hash)
    }

    /// Read an account by its address.
    fn get_account(&self, address: &Address) -> Result<Option<Account>, Self::Error> {
        self.get::<tables::PlainAccountState>(address)
    }

    /// Read a storage slot by its address and key.
    fn get_storage(&self, address: &Address, key: &U256) -> Result<Option<U256>, Self::Error> {
        self.get_dual::<tables::PlainStorageState>(address, key)
    }

    /// Read a block header by its hash.
    fn header_by_hash(&self, hash: &B256) -> Result<Option<SealedHeader>, Self::Error> {
        let Some(number) = self.get_header_number(hash)? else {
            return Ok(None);
        };
        self.get_header(number)
    }
}

impl<T> HotDbRead for T where T: HotKvRead {}
