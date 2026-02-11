use crate::{
    db::HistoryRead,
    model::{HotKvError, HotKvRead, HotKvWrite},
    tables::{self, Bytecodes, DualKey, PlainAccountState, SingleKey, Table},
};
use alloy::primitives::{Address, B256, KECCAK256_EMPTY};
use core::fmt;
use signet_storage_types::Account;
use std::borrow::Cow;
use trevm::revm::{
    database::{DBErrorMarker, Database, DatabaseRef, TryDatabaseCommit},
    primitives::{HashMap, StorageKey, StorageValue},
    state::{self, AccountInfo, Bytecode as RevmBytecode},
};

// Error marker implementation
impl DBErrorMarker for HotKvError {}

/// Read-only [`Database`] and [`DatabaseRef`] adapter.
///
/// When `height` is `Some`, reads return state as it was at that block
/// height by consulting history and change set tables. When `None`,
/// reads use the current plain state tables.
pub struct RevmRead<T: HotKvRead> {
    reader: T,
    height: Option<u64>,
}

impl<T: HotKvRead> RevmRead<T> {
    /// Create a new read adapter that reads current state.
    pub const fn new(reader: T) -> Self {
        Self { reader, height: None }
    }

    /// Create a read adapter that reads state at a specific block height.
    pub const fn at_height(reader: T, height: u64) -> Self {
        Self { reader, height: Some(height) }
    }
}

impl<T: HotKvRead> fmt::Debug for RevmRead<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RevmRead").finish()
    }
}

// HotKvRead implementation for RevmRead
impl<U: HotKvRead> HotKvRead for RevmRead<U> {
    type Error = U::Error;

    type Traverse<'a>
        = U::Traverse<'a>
    where
        U: 'a;

    fn raw_traverse<'a>(&'a self, table: &'static str) -> Result<Self::Traverse<'a>, Self::Error> {
        self.reader.raw_traverse(table)
    }

    fn raw_get<'a>(
        &'a self,
        table: &'static str,
        key: &[u8],
    ) -> Result<Option<std::borrow::Cow<'a, [u8]>>, Self::Error> {
        self.reader.raw_get(table, key)
    }

    fn raw_get_dual<'a>(
        &'a self,
        table: &'static str,
        key1: &[u8],
        key2: &[u8],
    ) -> Result<Option<Cow<'a, [u8]>>, Self::Error> {
        self.reader.raw_get_dual(table, key1, key2)
    }

    fn get<T: SingleKey>(&self, key: &T::Key) -> Result<Option<T::Value>, Self::Error> {
        self.reader.get::<T>(key)
    }

    fn get_dual<T: DualKey>(
        &self,
        key1: &T::Key,
        key2: &T::Key2,
    ) -> Result<Option<T::Value>, Self::Error> {
        self.reader.get_dual::<T>(key1, key2)
    }
}

/// Read-write REVM database adapter. This adapter allows committing changes.
/// Despite the naming of [`TryDatabaseCommit::try_commit`], the changes are
/// only persisted when [`Self::persist`] is called. This is because of a
/// mismatch in semantics between the two systems.
pub struct RevmWrite<U: HotKvWrite> {
    writer: U,
}

impl<U: HotKvWrite> RevmWrite<U> {
    /// Create a new write adapter
    pub const fn new(writer: U) -> Self {
        Self { writer }
    }

    /// Persist the changes made in this write transaction.
    pub fn persist(self) -> Result<(), U::Error> {
        self.writer.raw_commit()
    }
}

impl<U: HotKvWrite> fmt::Debug for RevmWrite<U> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RevmWrite").finish()
    }
}

// HotKvWrite implementation for RevmWrite
impl<U: HotKvWrite> HotKvRead for RevmWrite<U> {
    type Error = U::Error;

    type Traverse<'a>
        = U::Traverse<'a>
    where
        U: 'a;

    fn raw_traverse<'a>(&'a self, table: &'static str) -> Result<Self::Traverse<'a>, Self::Error> {
        self.writer.raw_traverse(table)
    }

    fn raw_get<'a>(
        &'a self,
        table: &'static str,
        key: &[u8],
    ) -> Result<Option<std::borrow::Cow<'a, [u8]>>, Self::Error> {
        self.writer.raw_get(table, key)
    }

    fn raw_get_dual<'a>(
        &'a self,
        table: &'static str,
        key1: &[u8],
        key2: &[u8],
    ) -> Result<Option<Cow<'a, [u8]>>, Self::Error> {
        self.writer.raw_get_dual(table, key1, key2)
    }

    fn get<T: SingleKey>(&self, key: &T::Key) -> Result<Option<T::Value>, Self::Error> {
        self.writer.get::<T>(key)
    }

    fn get_dual<T: DualKey>(
        &self,
        key1: &T::Key,
        key2: &T::Key2,
    ) -> Result<Option<T::Value>, Self::Error> {
        self.writer.get_dual::<T>(key1, key2)
    }
}

impl<U: HotKvWrite> HotKvWrite for RevmWrite<U> {
    type TraverseMut<'a>
        = U::TraverseMut<'a>
    where
        U: 'a;

    fn raw_traverse_mut<'a>(
        &'a self,
        table: &'static str,
    ) -> Result<Self::TraverseMut<'a>, Self::Error> {
        self.writer.raw_traverse_mut(table)
    }

    fn queue_raw_put(
        &self,
        table: &'static str,
        key: &[u8],
        value: &[u8],
    ) -> Result<(), Self::Error> {
        self.writer.queue_raw_put(table, key, value)
    }

    fn queue_raw_put_dual(
        &self,
        table: &'static str,
        key1: &[u8],
        key2: &[u8],
        value: &[u8],
    ) -> Result<(), Self::Error> {
        self.writer.queue_raw_put_dual(table, key1, key2, value)
    }

    fn queue_raw_delete(&self, table: &'static str, key: &[u8]) -> Result<(), Self::Error> {
        self.writer.queue_raw_delete(table, key)
    }

    fn queue_raw_delete_dual(
        &self,
        table: &'static str,
        key1: &[u8],
        key2: &[u8],
    ) -> Result<(), Self::Error> {
        self.writer.queue_raw_delete_dual(table, key1, key2)
    }

    fn queue_raw_clear(&self, table: &'static str) -> Result<(), Self::Error> {
        self.writer.queue_raw_clear(table)
    }

    fn queue_raw_create(
        &self,
        table: &'static str,
        dual_key: Option<usize>,
        dual_fixed: Option<usize>,
        int_key: bool,
    ) -> Result<(), Self::Error> {
        self.writer.queue_raw_create(table, dual_key, dual_fixed, int_key)
    }

    fn raw_commit(self) -> Result<(), Self::Error> {
        self.writer.raw_commit()
    }

    fn queue_put<T: SingleKey>(&self, key: &T::Key, value: &T::Value) -> Result<(), Self::Error> {
        self.writer.queue_put::<T>(key, value)
    }

    fn queue_put_dual<T: DualKey>(
        &self,
        key1: &T::Key,
        key2: &T::Key2,
        value: &T::Value,
    ) -> Result<(), Self::Error> {
        self.writer.queue_put_dual::<T>(key1, key2, value)
    }

    fn queue_delete<T: SingleKey>(&self, key: &T::Key) -> Result<(), Self::Error> {
        self.writer.queue_delete::<T>(key)
    }

    fn queue_create<T>(&self) -> Result<(), Self::Error>
    where
        T: Table,
    {
        self.writer.queue_create::<T>()
    }

    fn queue_clear<T>(&self) -> Result<(), Self::Error>
    where
        T: Table,
    {
        self.writer.queue_clear::<T>()
    }
}

// DatabaseRef implementation for RevmRead
impl<T: HotKvRead> DatabaseRef for RevmRead<T>
where
    T::Error: DBErrorMarker,
{
    type Error = T::Error;

    fn basic_ref(&self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        let Some(account) = self.reader.get_account_at_height(&address, self.height)? else {
            return Ok(None);
        };

        let code_hash = account.bytecode_hash.unwrap_or(KECCAK256_EMPTY);
        // Bytecodes are content-addressed (immutable), no height awareness needed
        let code = if code_hash != KECCAK256_EMPTY {
            self.reader.get::<Bytecodes>(&code_hash)?
        } else {
            None
        };

        Ok(Some(AccountInfo {
            balance: account.balance,
            nonce: account.nonce,
            code_hash,
            code,
            account_id: None,
        }))
    }

    fn code_by_hash_ref(&self, code_hash: B256) -> Result<RevmBytecode, Self::Error> {
        Ok(self.reader.get::<Bytecodes>(&code_hash)?.unwrap_or_default())
    }

    fn storage_ref(
        &self,
        address: Address,
        index: StorageKey,
    ) -> Result<StorageValue, Self::Error> {
        Ok(self.reader.get_storage_at_height(&address, &index, self.height)?.unwrap_or_default())
    }

    fn block_hash_ref(&self, _number: u64) -> Result<B256, Self::Error> {
        // This would need to be implemented based on your block hash storage
        // For now, return zero hash
        Ok(B256::ZERO)
    }
}

// Database implementation for RevmRead
impl<T: HotKvRead> Database for RevmRead<T>
where
    T::Error: DBErrorMarker,
{
    type Error = T::Error;

    fn basic(&mut self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        self.basic_ref(address)
    }

    fn code_by_hash(&mut self, code_hash: B256) -> Result<RevmBytecode, Self::Error> {
        self.code_by_hash_ref(code_hash)
    }

    fn storage(
        &mut self,
        address: Address,
        index: StorageKey,
    ) -> Result<StorageValue, Self::Error> {
        self.storage_ref(address, index)
    }

    fn block_hash(&mut self, number: u64) -> Result<B256, Self::Error> {
        self.block_hash_ref(number)
    }
}

// DatabaseRef implementation for RevmWrite (delegates to read operations)
impl<T: HotKvWrite> DatabaseRef for RevmWrite<T>
where
    T::Error: DBErrorMarker,
{
    type Error = T::Error;

    fn basic_ref(&self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        let account_opt = self.writer.get::<PlainAccountState>(&address)?;

        let Some(account) = account_opt else {
            return Ok(None);
        };

        let code_hash = account.bytecode_hash.unwrap_or(KECCAK256_EMPTY);
        let code = if code_hash != KECCAK256_EMPTY {
            self.writer.get::<Bytecodes>(&code_hash)?
        } else {
            None
        };

        Ok(Some(AccountInfo {
            balance: account.balance,
            nonce: account.nonce,
            code_hash,
            code,
            account_id: None,
        }))
    }

    fn code_by_hash_ref(&self, code_hash: B256) -> Result<RevmBytecode, Self::Error> {
        Ok(self.writer.get::<Bytecodes>(&code_hash)?.unwrap_or_default())
    }

    fn storage_ref(
        &self,
        address: Address,
        index: StorageKey,
    ) -> Result<StorageValue, Self::Error> {
        Ok(self.writer.get_dual::<tables::PlainStorageState>(&address, &index)?.unwrap_or_default())
    }

    fn block_hash_ref(&self, _number: u64) -> Result<B256, Self::Error> {
        // This would need to be implemented based on your block hash storage
        // For now, return zero hash
        Ok(B256::ZERO)
    }
}

// Database implementation for RevmWrite
impl<T: HotKvWrite> Database for RevmWrite<T>
where
    T::Error: DBErrorMarker,
{
    type Error = T::Error;

    fn basic(&mut self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        self.basic_ref(address)
    }

    fn code_by_hash(&mut self, code_hash: B256) -> Result<RevmBytecode, Self::Error> {
        self.code_by_hash_ref(code_hash)
    }

    fn storage(
        &mut self,
        address: Address,
        index: StorageKey,
    ) -> Result<StorageValue, Self::Error> {
        self.storage_ref(address, index)
    }

    fn block_hash(&mut self, number: u64) -> Result<B256, Self::Error> {
        self.block_hash_ref(number)
    }
}

// TryDatabaseCommit implementation for RevmWrite
impl<T: HotKvWrite> TryDatabaseCommit for RevmWrite<T>
where
    T::Error: DBErrorMarker,
{
    type Error = T::Error;

    fn try_commit(&mut self, changes: HashMap<Address, state::Account>) -> Result<(), Self::Error> {
        for (address, account) in changes {
            // Handle account info changes
            let account_data = Account {
                nonce: account.info.nonce,
                balance: account.info.balance,
                bytecode_hash: (account.info.code_hash != KECCAK256_EMPTY)
                    .then_some(account.info.code_hash),
            };
            self.writer.queue_put::<PlainAccountState>(&address, &account_data)?;

            // Handle storage changes
            for (key, value) in account.storage {
                self.writer.queue_put_dual::<tables::PlainStorageState>(
                    &address,
                    &key,
                    &value.present_value(),
                )?;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        db::{HistoryRead, UnsafeDbWrite, UnsafeHistoryWrite},
        mem::MemKv,
        model::{HotKv, HotKvRead, HotKvWrite},
        tables::{Bytecodes, PlainAccountState},
    };
    use alloy::primitives::{Address, B256, U256};
    use signet_storage_types::{Account, BlockNumberList};
    use trevm::revm::{
        database::{Database, DatabaseRef, TryDatabaseCommit},
        primitives::{HashMap, StorageKey, StorageValue},
        state::{Account as RevmAccount, AccountInfo, Bytecode},
    };

    /// Create a test account with some data
    fn create_test_account() -> (Address, Account) {
        let address = Address::from_slice(&[0x1; 20]);
        let account = Account {
            nonce: 42,
            balance: U256::from(1000u64),
            bytecode_hash: Some(B256::from_slice(&[0x2; 32])),
        };
        (address, account)
    }

    /// Create test bytecode
    fn create_test_bytecode() -> (B256, Bytecode) {
        let hash = B256::from_slice(&[0x2; 32]);
        let bytecode = RevmBytecode::new_raw(vec![0x60, 0x80, 0x60, 0x40].into());
        (hash, bytecode)
    }

    #[test]
    fn test_database_ref_traits() -> Result<(), Box<dyn std::error::Error>> {
        let mem_kv = MemKv::default();

        let (address, account) = create_test_account();
        let (hash, bytecode) = create_test_bytecode();

        {
            // Setup data using HotKv
            let writer = mem_kv.revm_writer()?;
            writer.queue_put::<PlainAccountState>(&address, &account)?;
            writer.queue_put::<Bytecodes>(&hash, &bytecode)?;
            writer.persist()?;
        }

        {
            // Read using REVM DatabaseRef traits
            let reader = mem_kv.revm_reader()?;

            // Test basic_ref
            let account_info = reader.basic_ref(address)?;
            assert!(account_info.is_some());
            let info = account_info.unwrap();
            assert_eq!(info.nonce, 42);
            assert_eq!(info.balance, U256::from(1000u64));
            assert_eq!(info.code_hash, hash);

            // Test code_by_hash_ref
            let retrieved_code = reader.code_by_hash_ref(hash)?;
            assert_eq!(retrieved_code, bytecode);

            // Test storage_ref (should be zero for non-existent storage)
            let storage_val = reader.storage_ref(address, StorageKey::from(U256::from(123u64)))?;
            assert_eq!(storage_val, U256::ZERO);

            // Test block_hash_ref
            let block_hash = reader.block_hash_ref(123)?;
            assert_eq!(block_hash, B256::ZERO);
        }

        Ok(())
    }

    #[test]
    fn test_database_mutable_traits() -> Result<(), Box<dyn std::error::Error>> {
        let mem_kv = MemKv::default();

        let (address, account) = create_test_account();
        let (hash, bytecode) = create_test_bytecode();

        {
            // Setup data using HotKv
            let writer = mem_kv.revm_writer()?;
            writer.queue_put::<PlainAccountState>(&address, &account)?;
            writer.queue_put::<Bytecodes>(&hash, &bytecode)?;
            writer.persist()?;
        }

        {
            // Read using mutable REVM Database traits
            let mut reader = mem_kv.revm_reader()?;

            // Test basic
            let account_info = reader.basic(address)?;
            assert!(account_info.is_some());
            let info = account_info.unwrap();
            assert_eq!(info.nonce, 42);
            assert_eq!(info.balance, U256::from(1000u64));

            // Test code_by_hash
            let retrieved_code = reader.code_by_hash(hash)?;
            assert_eq!(retrieved_code, bytecode);

            // Test storage
            let storage_val = reader.storage(address, StorageKey::from(U256::from(123u64)))?;
            assert_eq!(storage_val, U256::ZERO);

            // Test block_hash
            let block_hash = reader.block_hash(123)?;
            assert_eq!(block_hash, B256::ZERO);
        }

        Ok(())
    }

    #[test]
    fn test_write_database_traits() -> Result<(), Box<dyn std::error::Error>> {
        let mem_kv = MemKv::default();

        let (address, account) = create_test_account();
        let (hash, bytecode) = create_test_bytecode();

        {
            // Setup initial data
            let writer = mem_kv.revm_writer()?;
            writer.queue_put::<PlainAccountState>(&address, &account)?;
            writer.queue_put::<Bytecodes>(&hash, &bytecode)?;
            writer.persist()?;
        }

        {
            // Test write operations using DatabaseRef and Database traits
            let mut writer = mem_kv.revm_writer()?;

            // Test read operations work on writer
            let account_info = writer.basic_ref(address)?;
            assert!(account_info.is_some());

            let account_info_mut = writer.basic(address)?;
            assert!(account_info_mut.is_some());

            let code = writer.code_by_hash_ref(hash)?;
            assert_eq!(code, bytecode);

            let code_mut = writer.code_by_hash(hash)?;
            assert_eq!(code_mut, bytecode);

            // Don't persist this writer to test that reads work
        }

        Ok(())
    }

    #[test]
    fn test_try_database_commit() -> Result<(), Box<dyn std::error::Error>> {
        let mem_kv = MemKv::default();

        let address = Address::from_slice(&[0x1; 20]);

        {
            let mut writer = mem_kv.revm_writer()?;

            // Create REVM state changes
            let mut changes = HashMap::default();
            let account_info = AccountInfo {
                nonce: 55,
                balance: U256::from(2000u64),
                code_hash: KECCAK256_EMPTY,
                code: None,
                account_id: None,
            };

            let mut storage = HashMap::default();
            storage.insert(
                StorageKey::from(U256::from(100u64)),
                trevm::revm::state::EvmStorageSlot::new(U256::from(200u64), 0),
            );

            let revm_account = RevmAccount {
                info: account_info,
                storage,
                status: trevm::revm::state::AccountStatus::Touched,
                transaction_id: 0,
                original_info: Box::default(),
            };

            changes.insert(address, revm_account);

            // Commit changes using REVM trait
            writer.try_commit(changes)?;
            writer.persist()?;
        }

        {
            // Verify changes were persisted using HotKv traits
            let reader = mem_kv.revm_reader()?;

            let account: Option<Account> = reader.get::<PlainAccountState>(&address)?;
            assert!(account.is_some());
            let acc = account.unwrap();
            assert_eq!(acc.nonce, 55);
            assert_eq!(acc.balance, U256::from(2000u64));
            assert_eq!(acc.bytecode_hash, None);

            let key = U256::from(100);
            let storage_val: Option<StorageValue> =
                reader.get_dual::<tables::PlainStorageState>(&address, &key)?;
            assert_eq!(storage_val, Some(U256::from(200u64)));
        }

        Ok(())
    }

    #[test]
    fn test_mixed_usage_patterns() -> Result<(), Box<dyn std::error::Error>> {
        let mem_kv = MemKv::default();

        let address1 = Address::from_slice(&[0x1; 20]);
        let address2 = Address::from_slice(&[0x2; 20]);

        // Write some data using HotKv
        {
            let writer = mem_kv.revm_writer()?;
            let account = Account { nonce: 10, balance: U256::from(500u64), bytecode_hash: None };
            writer.queue_put::<PlainAccountState>(&address1, &account)?;
            writer.persist()?;
        }

        // Write more data using REVM traits
        {
            let mut writer = mem_kv.revm_writer()?;
            let mut changes = HashMap::default();
            let revm_account = RevmAccount {
                info: AccountInfo {
                    nonce: 20,
                    balance: U256::from(1500u64),
                    code_hash: KECCAK256_EMPTY,
                    code: None,
                    account_id: None,
                },
                storage: HashMap::default(),
                status: trevm::revm::state::AccountStatus::Touched,
                transaction_id: 0,
                original_info: Box::default(),
            };
            changes.insert(address2, revm_account);
            writer.try_commit(changes)?;
            writer.persist()?;
        }

        // Read using mixed approaches
        {
            let reader = mem_kv.revm_reader()?;

            // Read address1 using HotKv
            let account1: Option<Account> = reader.get::<PlainAccountState>(&address1)?;
            assert!(account1.is_some());
            assert_eq!(account1.unwrap().nonce, 10);

            // Read address2 using REVM DatabaseRef
            let account2_info = reader.basic_ref(address2)?;
            assert!(account2_info.is_some());
            assert_eq!(account2_info.unwrap().nonce, 20);
        }

        Ok(())
    }

    #[test]
    fn test_error_handling() -> Result<(), Box<dyn std::error::Error>> {
        let mem_kv = MemKv::default();

        let address = Address::from_slice(&[0x1; 20]);
        let hash = B256::from_slice(&[0x99; 32]);

        let reader = mem_kv.revm_reader()?;

        // Test reading non-existent account
        let account_info = reader.basic_ref(address)?;
        assert!(account_info.is_none());

        // Test reading non-existent code
        let code = reader.code_by_hash_ref(hash)?;
        assert!(code.is_empty());

        // Test reading non-existent storage
        let storage = reader.storage_ref(address, StorageKey::from(U256::from(123u64)))?;
        assert_eq!(storage, U256::ZERO);

        Ok(())
    }

    #[test]
    fn test_concurrent_readers() -> Result<(), Box<dyn std::error::Error>> {
        let mem_kv = MemKv::default();

        let (address, account) = create_test_account();

        // Setup data
        {
            let writer = mem_kv.revm_writer()?;
            writer.queue_put::<PlainAccountState>(&address, &account)?;
            writer.persist()?;
        }

        // Create multiple readers
        let reader1 = mem_kv.revm_reader()?;
        let reader2 = mem_kv.revm_reader()?;

        // Both should read the same data
        let account1 = reader1.basic_ref(address)?;
        let account2 = reader2.basic_ref(address)?;

        assert_eq!(account1, account2);
        assert!(account1.is_some());

        Ok(())
    }

    /// Set up a MemKv with history data for height-aware reading tests.
    ///
    /// Scenario: account A
    ///   - Genesis/current state: nonce=10, balance=1000
    ///   - Block 5 changed account: pre-state was nonce=1, balance=100
    ///   - Block 10 changed account: pre-state was nonce=5, balance=500
    ///   - Current (PlainAccountState): nonce=10, balance=1000
    ///   - History shard: (A, 10) → [5, 10]
    ///
    /// Storage slot 0x42 for address A:
    ///   - Block 5 changed slot: pre-state was 0
    ///   - Block 10 changed slot: pre-state was 100
    ///   - Current (PlainStorageState): 200
    ///   - History shard: (A, ShardedKey(0x42, 10)) → [5, 10]
    fn setup_history_kv() -> (MemKv, Address) {
        let mem_kv = MemKv::default();
        let address = Address::from_slice(&[0x1; 20]);
        let slot = U256::from(0x42u64);

        let writer = mem_kv.writer().unwrap();

        // Current plain state
        let current_account =
            Account { nonce: 10, balance: U256::from(1000u64), bytecode_hash: None };
        writer.put_account(&address, &current_account).unwrap();
        writer.put_storage(&address, &slot, &U256::from(200u64)).unwrap();

        // Account history shard: blocks 5 and 10 touched address
        let history = BlockNumberList::new([5, 10]).unwrap();
        writer.write_account_history(&address, 10, &history).unwrap();

        // Account change sets (pre-states)
        let pre_state_5 = Account { nonce: 1, balance: U256::from(100u64), bytecode_hash: None };
        writer.write_account_prestate(5, address, &pre_state_5).unwrap();

        let pre_state_10 = Account { nonce: 5, balance: U256::from(500u64), bytecode_hash: None };
        writer.write_account_prestate(10, address, &pre_state_10).unwrap();

        // Storage history shard: blocks 5 and 10 touched (address, slot)
        let storage_history = BlockNumberList::new([5, 10]).unwrap();
        writer.write_storage_history(&address, slot, 10, &storage_history).unwrap();

        // Storage change sets (pre-states)
        writer.write_storage_prestate(5, address, &slot, &U256::ZERO).unwrap();
        writer.write_storage_prestate(10, address, &slot, &U256::from(100u64)).unwrap();

        writer.raw_commit().unwrap();

        (mem_kv, address)
    }

    #[test]
    fn test_account_at_height_before_any_changes() {
        let (mem_kv, address) = setup_history_kv();
        let reader = mem_kv.reader().unwrap();

        // Height 3: before block 5 (first change). Should return pre-state of block 5.
        let account = reader.get_account_at_height(&address, Some(3)).unwrap().unwrap();
        assert_eq!(account.nonce, 1);
        assert_eq!(account.balance, U256::from(100u64));
    }

    #[test]
    fn test_account_at_height_between_changes() {
        let (mem_kv, address) = setup_history_kv();
        let reader = mem_kv.reader().unwrap();

        // Height 7: between blocks 5 and 10. Should return pre-state of block 10.
        let account = reader.get_account_at_height(&address, Some(7)).unwrap().unwrap();
        assert_eq!(account.nonce, 5);
        assert_eq!(account.balance, U256::from(500u64));
    }

    #[test]
    fn test_account_at_height_after_all_changes() {
        let (mem_kv, address) = setup_history_kv();
        let reader = mem_kv.reader().unwrap();

        // Height 15: after all changes. Should return current plain state.
        let account = reader.get_account_at_height(&address, Some(15)).unwrap().unwrap();
        assert_eq!(account.nonce, 10);
        assert_eq!(account.balance, U256::from(1000u64));
    }

    #[test]
    fn test_account_at_height_exactly_at_change() {
        let (mem_kv, address) = setup_history_kv();
        let reader = mem_kv.reader().unwrap();

        // Height 5: exactly at block 5. The change AT block 5 is already applied,
        // so earliest block > 5 is 10, returning pre-state of block 10.
        let account = reader.get_account_at_height(&address, Some(5)).unwrap().unwrap();
        assert_eq!(account.nonce, 5);
        assert_eq!(account.balance, U256::from(500u64));
    }

    #[test]
    fn test_account_at_height_exactly_at_last_change() {
        let (mem_kv, address) = setup_history_kv();
        let reader = mem_kv.reader().unwrap();

        // Height 10: exactly at last change block. No blocks > 10 in history,
        // so returns current plain state.
        let account = reader.get_account_at_height(&address, Some(10)).unwrap().unwrap();
        assert_eq!(account.nonce, 10);
        assert_eq!(account.balance, U256::from(1000u64));
    }

    #[test]
    fn test_storage_at_height_before_any_changes() {
        let (mem_kv, address) = setup_history_kv();
        let reader = mem_kv.reader().unwrap();
        let slot = U256::from(0x42u64);

        // Height 3: before block 5. Should return pre-state of block 5 (zero).
        let value = reader.get_storage_at_height(&address, &slot, Some(3)).unwrap();
        assert_eq!(value, Some(U256::ZERO));
    }

    #[test]
    fn test_storage_at_height_between_changes() {
        let (mem_kv, address) = setup_history_kv();
        let reader = mem_kv.reader().unwrap();
        let slot = U256::from(0x42u64);

        // Height 7: between blocks 5 and 10. Should return pre-state of block 10.
        let value = reader.get_storage_at_height(&address, &slot, Some(7)).unwrap();
        assert_eq!(value, Some(U256::from(100u64)));
    }

    #[test]
    fn test_storage_at_height_after_all_changes() {
        let (mem_kv, address) = setup_history_kv();
        let reader = mem_kv.reader().unwrap();
        let slot = U256::from(0x42u64);

        // Height 15: after all changes. Should return current plain state.
        let value = reader.get_storage_at_height(&address, &slot, Some(15)).unwrap();
        assert_eq!(value, Some(U256::from(200u64)));
    }

    #[test]
    fn test_account_at_height_no_history() {
        let (mem_kv, _) = setup_history_kv();
        let reader = mem_kv.reader().unwrap();

        // Unknown address with no history — should return current (None).
        let unknown = Address::from_slice(&[0xFF; 20]);
        let result = reader.get_account_at_height(&unknown, Some(5)).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_storage_at_height_no_history() {
        let (mem_kv, address) = setup_history_kv();
        let reader = mem_kv.reader().unwrap();

        // Unknown slot with no history — should return current (None).
        let unknown_slot = U256::from(0x99u64);
        let result = reader.get_storage_at_height(&address, &unknown_slot, Some(5)).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_revm_read_at_height() {
        let (mem_kv, address) = setup_history_kv();
        let slot = U256::from(0x42u64);

        // Reader at height 3: should see pre-block-5 state
        let reader = mem_kv.revm_reader_at_height(3).unwrap();
        let info = reader.basic_ref(address).unwrap().unwrap();
        assert_eq!(info.nonce, 1);
        assert_eq!(info.balance, U256::from(100u64));

        let storage = reader.storage_ref(address, StorageKey::from(slot)).unwrap();
        assert_eq!(storage, U256::ZERO);
    }

    #[test]
    fn test_revm_read_at_height_current_state() {
        let (mem_kv, address) = setup_history_kv();
        let slot = U256::from(0x42u64);

        // Reader at height 15: after all changes, should see current state
        let reader = mem_kv.revm_reader_at_height(15).unwrap();
        let info = reader.basic_ref(address).unwrap().unwrap();
        assert_eq!(info.nonce, 10);
        assert_eq!(info.balance, U256::from(1000u64));

        let storage = reader.storage_ref(address, StorageKey::from(slot)).unwrap();
        assert_eq!(storage, U256::from(200u64));
    }

    #[test]
    fn test_revm_read_none_height_uses_current() {
        let (mem_kv, address) = setup_history_kv();
        let slot = U256::from(0x42u64);

        // Reader with no height (None) — backward compatible, reads current state
        let reader = mem_kv.revm_reader().unwrap();
        let info = reader.basic_ref(address).unwrap().unwrap();
        assert_eq!(info.nonce, 10);
        assert_eq!(info.balance, U256::from(1000u64));

        let storage = reader.storage_ref(address, StorageKey::from(slot)).unwrap();
        assert_eq!(storage, U256::from(200u64));
    }
}
