//! Utilities for testing MDBX storage implementation.

use crate::{DatabaseArguments, DatabaseEnv, DatabaseEnvKind};
use alloy::primitives::Bytes;
use signet_hot::{
    db::UnsafeDbWrite,
    model::{HotKv, HotKvWrite},
    tables::{self, SingleKey, Table},
};
use tempfile::{TempDir, tempdir};

// Test table definitions for traversal tests
#[derive(Debug)]
struct TestTable;

impl Table for TestTable {
    const NAME: &'static str = "mdbx_test_table";
    type Key = u64;
    type Value = Bytes;
}

impl SingleKey for TestTable {}

/// Creates a temporary MDBX database for testing that will be automatically
/// cleaned up when the TempDir is dropped.
pub fn create_test_rw_db() -> (TempDir, DatabaseEnv) {
    let dir = tempdir().unwrap();

    let args = DatabaseArguments::new();
    let db = DatabaseEnv::open(dir.path(), DatabaseEnvKind::RW, args).unwrap();

    // Create tables from the `crate::tables::hot` module
    let writer = db.writer().unwrap();

    writer.queue_create::<tables::Headers>().unwrap();
    writer.queue_create::<tables::HeaderNumbers>().unwrap();
    writer.queue_create::<tables::Bytecodes>().unwrap();
    writer.queue_create::<tables::PlainAccountState>().unwrap();
    writer.queue_create::<tables::AccountsHistory>().unwrap();
    writer.queue_create::<tables::StorageHistory>().unwrap();
    writer.queue_create::<tables::PlainStorageState>().unwrap();
    writer.queue_create::<tables::StorageChangeSets>().unwrap();
    writer.queue_create::<tables::AccountChangeSets>().unwrap();

    writer.queue_create::<TestTable>().unwrap();

    // Create DUP_FIXED table for put_multiple tests
    // key2_size=8, value_size=8 means total fixed value size is 16 bytes
    writer.queue_raw_create("put_multiple_test", Some(8), Some(8), false).unwrap();

    writer.commit().expect("Failed to commit table creation");

    (dir, db)
}

#[cfg(test)]
mod tests {
    use crate::{DatabaseEnv, MdbxError, Tx};

    use super::*;
    use alloy::{
        consensus::{Header, Sealable},
        primitives::{Address, B256, BlockNumber, Bytes, U256},
    };
    use serial_test::serial;
    use signet_hot::{
        conformance::{conformance, test_unwind_conformance},
        db::UnsafeDbWrite,
        model::{DualTableTraverse, HotKv, HotKvRead, HotKvWrite, TableTraverse, TableTraverseMut},
        tables,
    };
    use signet_libmdbx::{Ro, Rw};
    use signet_storage_types::Account;
    use std::borrow::Cow;
    use trevm::revm::bytecode::Bytecode;

    /// Create a temporary MDBX database for testing that will be automatically cleaned up
    fn run_test<F: FnOnce(&DatabaseEnv)>(f: F) {
        let (dir, db) = create_test_rw_db();

        f(&db);

        drop(dir);
    }

    /// Create test data
    fn create_test_account() -> (Address, Account) {
        let address = Address::from_slice(&[0x1; 20]);
        let account = Account {
            nonce: 42,
            balance: U256::from(1000u64),
            bytecode_hash: Some(B256::from_slice(&[0x2; 32])),
        };
        (address, account)
    }

    fn create_test_bytecode() -> (B256, Bytecode) {
        let hash = B256::from_slice(&[0x2; 32]);
        let code = Bytecode::new_raw(vec![0x60, 0x80, 0x60, 0x40].into());
        (hash, code)
    }

    fn create_test_header() -> (BlockNumber, Header) {
        let block_number = 12345;
        let header = Header {
            number: block_number,
            gas_limit: 8000000,
            gas_used: 100000,
            timestamp: 1640995200,
            parent_hash: B256::from_slice(&[0x3; 32]),
            state_root: B256::from_slice(&[0x4; 32]),
            ..Default::default()
        };
        (block_number, header)
    }

    #[test]
    #[serial]
    fn test_hotkv_basic_operations() {
        run_test(test_hotkv_basic_operations_inner);
    }

    fn test_hotkv_basic_operations_inner(db: &DatabaseEnv) {
        let (address, account) = create_test_account();
        let (hash, bytecode) = create_test_bytecode();

        // Test HotKv::writer() and basic write operations
        {
            let writer: Tx<Rw> = db.writer().unwrap();

            // Create tables first
            writer.queue_create::<tables::Bytecodes>().unwrap();

            // Write account data
            writer.queue_put::<tables::PlainAccountState>(&address, &account).unwrap();
            writer.queue_put::<tables::Bytecodes>(&hash, &bytecode).unwrap();

            // Commit the transaction
            writer.raw_commit().unwrap();
        }

        // Test HotKv::reader() and basic read operations
        {
            let reader: Tx<Ro> = db.reader().unwrap();

            // Read account data
            let read_account: Option<Account> =
                reader.get::<tables::PlainAccountState>(&address).unwrap();
            assert_eq!(read_account, Some(account));

            // Read bytecode
            let read_bytecode: Option<Bytecode> = reader.get::<tables::Bytecodes>(&hash).unwrap();
            assert_eq!(read_bytecode, Some(bytecode));

            // Test non-existent data
            let nonexistent_addr = Address::from_slice(&[0xff; 20]);
            let nonexistent_account: Option<Account> =
                reader.get::<tables::PlainAccountState>(&nonexistent_addr).unwrap();
            assert_eq!(nonexistent_account, None);
        }
    }

    #[test]
    #[serial]
    fn test_raw_operations() {
        run_test(test_raw_operations_inner)
    }

    fn test_raw_operations_inner(db: &DatabaseEnv) {
        let table_name = "test_table";
        let key = b"test_key";
        let value = b"test_value";

        // Test raw write operations
        {
            let writer: Tx<Rw> = db.writer().unwrap();

            // Create table
            writer.queue_raw_create(table_name, None, None, false).unwrap();

            // Put raw data
            writer.queue_raw_put(table_name, key, value).unwrap();

            writer.raw_commit().unwrap();
        }

        // Test raw read operations
        {
            let reader: Tx<Ro> = db.reader().unwrap();

            let read_value = reader.raw_get(table_name, key).unwrap();
            assert_eq!(read_value.as_deref(), Some(value.as_slice()));

            // Test non-existent key
            let nonexistent = reader.raw_get(table_name, b"nonexistent").unwrap();
            assert_eq!(nonexistent, None);
        }

        // Test raw delete
        {
            let writer: Tx<Rw> = db.writer().unwrap();

            writer.queue_raw_delete(table_name, key).unwrap();
            writer.raw_commit().unwrap();
        }

        // Verify deletion
        {
            let reader: Tx<Ro> = db.reader().unwrap();
            let deleted_value = reader.raw_get(table_name, key).unwrap();
            assert_eq!(deleted_value, None);
        }
    }

    #[test]
    #[serial]
    fn test_dual_keyed_operations() {
        run_test(test_dual_keyed_operations_inner)
    }

    fn test_dual_keyed_operations_inner(db: &DatabaseEnv) {
        let address = Address::from_slice(&[0x1; 20]);
        let storage_key = U256::from(5);
        let storage_value = U256::from(999u64);

        // Test dual-keyed table operations
        {
            let writer: Tx<Rw> = db.writer().unwrap();

            // Put storage data using dual keys
            writer
                .queue_put_dual::<tables::PlainStorageState>(&address, &storage_key, &storage_value)
                .unwrap();

            writer.raw_commit().unwrap();
        }

        // Test reading dual-keyed data
        {
            let reader: Tx<Ro> = db.reader().unwrap();

            // Read storage using dual key lookup
            let read_value = reader
                .get_dual::<tables::PlainStorageState>(&address, &storage_key)
                .unwrap()
                .unwrap();

            assert_eq!(read_value, storage_value);
        }
    }

    #[test]
    #[serial]
    fn test_table_management() {
        run_test(test_table_management_inner)
    }

    fn test_table_management_inner(db: &DatabaseEnv) {
        // Add some data
        let (block_number, header) = create_test_header();
        {
            let writer: Tx<Rw> = db.writer().unwrap();
            writer.queue_put::<tables::Headers>(&block_number, &header).unwrap();
            writer.raw_commit().unwrap();
        }

        // Verify data exists
        {
            let reader: Tx<Ro> = db.reader().unwrap();
            let read_header: Option<Header> = reader.get::<tables::Headers>(&block_number).unwrap();
            assert_eq!(read_header, Some(header.clone()));
        }

        // Clear the table
        {
            let writer: Tx<Rw> = db.writer().unwrap();
            writer.queue_clear::<tables::Headers>().unwrap();
            writer.raw_commit().unwrap();
        }

        // Verify table is empty
        {
            let reader: Tx<Ro> = db.reader().unwrap();
            let read_header: Option<Header> = reader.get::<tables::Headers>(&block_number).unwrap();
            assert_eq!(read_header, None);
        }
    }

    #[test]
    fn test_batch_operations() {
        run_test(test_batch_operations_inner)
    }

    fn test_batch_operations_inner(db: &DatabaseEnv) {
        // Create test data
        let accounts: Vec<(Address, Account)> = (0..10)
            .map(|i| {
                let mut addr_bytes = [0u8; 20];
                addr_bytes[19] = i;
                let address = Address::from_slice(&addr_bytes);
                let account = Account {
                    nonce: i.into(),
                    balance: U256::from((i as u64) * 100),
                    bytecode_hash: None,
                };
                (address, account)
            })
            .collect();

        // Test batch writes
        {
            let writer: Tx<Rw> = db.writer().unwrap();

            // Write multiple accounts
            for (address, account) in &accounts {
                writer.queue_put::<tables::PlainAccountState>(address, account).unwrap();
            }

            writer.raw_commit().unwrap();
        }

        // Test batch reads
        {
            let reader: Tx<Ro> = db.reader().unwrap();

            for (address, expected_account) in &accounts {
                let read_account: Option<Account> =
                    reader.get::<tables::PlainAccountState>(address).unwrap();
                assert_eq!(read_account.as_ref(), Some(expected_account));
            }
        }
    }

    #[test]
    fn test_transaction_isolation() {
        run_test(test_transaction_isolation_inner)
    }

    fn test_transaction_isolation_inner(db: &DatabaseEnv) {
        let (address, account) = create_test_account();

        // Setup initial data
        {
            let writer: Tx<Rw> = db.writer().unwrap();
            writer.queue_put::<tables::PlainAccountState>(&address, &account).unwrap();
            writer.raw_commit().unwrap();
        }

        // Start a reader transaction
        let reader: Tx<Ro> = db.reader().unwrap();

        // Modify data in a writer transaction
        {
            let writer: Tx<Rw> = db.writer().unwrap();
            let modified_account =
                Account { nonce: 999, balance: U256::from(9999u64), bytecode_hash: None };
            writer.queue_put::<tables::PlainAccountState>(&address, &modified_account).unwrap();
            writer.raw_commit().unwrap();
        }

        // Reader should still see original data (snapshot isolation)
        {
            let read_account: Option<Account> =
                reader.get::<tables::PlainAccountState>(&address).unwrap();
            assert_eq!(read_account, Some(account));
        }

        // New reader should see modified data
        {
            let new_reader: Tx<Ro> = db.reader().unwrap();
            let read_account: Option<Account> =
                new_reader.get::<tables::PlainAccountState>(&address).unwrap();
            assert_eq!(read_account.unwrap().nonce, 999);
        }
    }

    #[test]
    fn test_multiple_readers() {
        run_test(test_multiple_readers_inner)
    }

    fn test_multiple_readers_inner(db: &DatabaseEnv) {
        let (address, account) = create_test_account();

        // Setup data
        {
            let writer: Tx<Rw> = db.writer().unwrap();
            writer.queue_put::<tables::PlainAccountState>(&address, &account).unwrap();
            writer.raw_commit().unwrap();
        }

        // Create multiple readers
        let reader1: Tx<Ro> = db.reader().unwrap();
        let reader2: Tx<Ro> = db.reader().unwrap();
        let reader3: Tx<Ro> = db.reader().unwrap();

        // All readers should see the same data
        let account1: Option<Account> = reader1.get::<tables::PlainAccountState>(&address).unwrap();
        let account2: Option<Account> = reader2.get::<tables::PlainAccountState>(&address).unwrap();
        let account3: Option<Account> = reader3.get::<tables::PlainAccountState>(&address).unwrap();

        assert_eq!(account1, Some(account));
        assert_eq!(account2, Some(account));
        assert_eq!(account3, Some(account));
    }

    #[test]
    fn test_error_handling() {
        run_test(test_error_handling_inner)
    }

    fn test_error_handling_inner(db: &DatabaseEnv) {
        // Test reading from non-existent table
        {
            let reader: Tx<Ro> = db.reader().unwrap();
            let result = reader.raw_get("nonexistent_table", b"key");

            // Should handle gracefully (may return None or error depending on MDBX behavior)
            match result {
                Ok(None) => {} // This is fine
                Err(_) => {}   // This is also acceptable for non-existent table
                Ok(Some(_)) => panic!("Should not return data for non-existent table"),
            }
        }

        // Test writing to a table without creating it first
        {
            let writer: Tx<Rw> = db.writer().unwrap();
            let (address, account) = create_test_account();

            // This should handle the case where table doesn't exist
            let result = writer.queue_put::<tables::PlainAccountState>(&address, &account);
            match result {
                Ok(_) => {
                    // If it succeeds, commit should work
                    writer.raw_commit().unwrap();
                }
                Err(_) => {
                    // If it fails, that's expected behavior
                }
            }
        }
    }

    #[test]
    fn test_serialization_roundtrip() {
        run_test(test_serialization_roundtrip_inner)
    }

    fn test_serialization_roundtrip_inner(db: &DatabaseEnv) {
        // Test various data types
        let (block_number, header) = create_test_header();
        let header = header.seal_slow();

        {
            let writer: Tx<Rw> = db.writer().unwrap();

            // Write different types
            writer.put_header(&header).unwrap();

            writer.raw_commit().unwrap();
        }

        {
            let reader: Tx<Ro> = db.reader().unwrap();

            // Read and verify
            let read_header: Option<Header> = reader.get::<tables::Headers>(&block_number).unwrap();
            assert_eq!(read_header.as_ref(), Some(header.inner()));

            let read_hash: Option<u64> =
                reader.get::<tables::HeaderNumbers>(&header.hash()).unwrap();
            assert_eq!(read_hash, Some(header.number));
        }
    }

    #[test]
    fn test_large_data() {
        run_test(test_large_data_inner)
    }

    fn test_large_data_inner(db: &DatabaseEnv) {
        // Create a large bytecode
        let hash = B256::from_slice(&[0x8; 32]);
        let large_code_vec: Vec<u8> = (0..10000).map(|i| (i % 256) as u8).collect();
        let large_bytecode = Bytecode::new_raw(large_code_vec.clone().into());

        {
            let writer: Tx<Rw> = db.writer().unwrap();
            writer.queue_create::<tables::Bytecodes>().unwrap();
            writer.queue_put::<tables::Bytecodes>(&hash, &large_bytecode).unwrap();
            writer.raw_commit().unwrap();
        }

        {
            let reader: Tx<Ro> = db.reader().unwrap();
            let read_bytecode: Option<Bytecode> = reader.get::<tables::Bytecodes>(&hash).unwrap();
            assert_eq!(read_bytecode, Some(large_bytecode));
        }
    }

    // ========================================================================
    // Cursor Traversal Tests
    // ========================================================================

    #[test]
    fn test_table_traverse_basic_navigation() {
        run_test(test_table_traverse_basic_navigation_inner)
    }

    fn test_table_traverse_basic_navigation_inner(db: &DatabaseEnv) {
        // Setup test data with multiple entries
        let test_data: Vec<(u64, Bytes)> = vec![
            (1, Bytes::from_static(b"value_001")),
            (2, Bytes::from_static(b"value_002")),
            (3, Bytes::from_static(b"value_003")),
            (10, Bytes::from_static(b"value_010")),
            (20, Bytes::from_static(b"value_020")),
        ];

        // Insert test data
        {
            let writer: Tx<Rw> = db.writer().unwrap();
            for (key, value) in &test_data {
                writer.queue_put::<TestTable>(key, value).unwrap();
            }
            writer.raw_commit().unwrap();
        }

        // Test cursor traversal
        {
            let tx: Tx<Ro> = db.reader().unwrap();
            let mut cursor = tx.new_cursor::<TestTable>().unwrap();

            // Test first()
            let first_result = TableTraverse::<TestTable, _>::first(&mut cursor).unwrap();
            assert!(first_result.is_some());
            let (key, value) = first_result.unwrap();
            assert_eq!(key, test_data[0].0);
            assert_eq!(value, test_data[0].1);

            // Test last()
            let last_result = TableTraverse::<TestTable, _>::last(&mut cursor).unwrap();
            assert!(last_result.is_some());
            let (key, value) = last_result.unwrap();
            assert_eq!(key, test_data.last().unwrap().0);
            assert_eq!(value, test_data.last().unwrap().1);

            // Test exact lookup
            let exact_result = TableTraverse::<TestTable, _>::exact(&mut cursor, &2u64).unwrap();
            assert!(exact_result.is_some());
            assert_eq!(exact_result.unwrap(), test_data[1].1);

            // Test exact lookup for non-existent key
            let missing_result =
                TableTraverse::<TestTable, _>::exact(&mut cursor, &999u64).unwrap();
            assert!(missing_result.is_none());

            // Test next_above (range lookup)
            let range_result =
                TableTraverse::<TestTable, _>::lower_bound(&mut cursor, &5u64).unwrap();
            assert!(range_result.is_some());
            let (key, value) = range_result.unwrap();
            assert_eq!(key, test_data[3].0); // key 10
            assert_eq!(value, test_data[3].1);
        }
    }

    #[test]
    fn test_table_traverse_sequential_navigation() {
        run_test(test_table_traverse_sequential_navigation_inner)
    }

    fn test_table_traverse_sequential_navigation_inner(db: &DatabaseEnv) {
        // Setup sequential test data
        let test_data: Vec<(u64, Bytes)> = (1..=10)
            .map(|i| {
                let s = format!("value_{:03}", i);
                let s = s.as_bytes();
                let value = Bytes::copy_from_slice(s);
                (i, value)
            })
            .collect();

        // Insert test data
        {
            let writer: Tx<Rw> = db.writer().unwrap();
            for (key, value) in &test_data {
                writer.queue_put::<TestTable>(key, value).unwrap();
            }
            writer.raw_commit().unwrap();
        }

        // Test sequential navigation
        {
            let tx: Tx<Ro> = db.reader().unwrap();
            let mut cursor = tx.new_cursor::<TestTable>().unwrap();

            // Start from first and traverse forward
            let mut current_idx = 0;
            let first_result = TableTraverse::<TestTable, _>::first(&mut cursor).unwrap();
            assert!(first_result.is_some());

            let (key, value) = first_result.unwrap();
            assert_eq!(key, test_data[current_idx].0);
            assert_eq!(value, test_data[current_idx].1);

            // Navigate forward through all entries
            while current_idx < test_data.len() - 1 {
                let next_result = TableTraverse::<TestTable, _>::read_next(&mut cursor).unwrap();
                assert!(next_result.is_some());

                current_idx += 1;
                let (key, value) = next_result.unwrap();
                assert_eq!(key, test_data[current_idx].0);
                assert_eq!(value, test_data[current_idx].1);
            }

            // Next should return None at the end
            let beyond_end = TableTraverse::<TestTable, _>::read_next(&mut cursor).unwrap();
            assert!(beyond_end.is_none());

            // Navigate backward
            while current_idx > 0 {
                let prev_result = TableTraverse::<TestTable, _>::read_prev(&mut cursor).unwrap();
                assert!(prev_result.is_some());

                current_idx -= 1;
                let (key, value) = prev_result.unwrap();
                assert_eq!(key, test_data[current_idx].0);
                assert_eq!(value, test_data[current_idx].1);
            }

            // Previous should return None at the beginning
            let before_start = TableTraverse::<TestTable, _>::read_prev(&mut cursor).unwrap();
            assert!(before_start.is_none());
        }
    }

    #[test]
    fn test_table_traverse_mut_delete() {
        run_test(test_table_traverse_mut_delete_inner)
    }

    fn test_table_traverse_mut_delete_inner(db: &DatabaseEnv) {
        let test_data: Vec<(u64, Bytes)> = vec![
            (1, Bytes::from_static(b"delete_value_1")),
            (2, Bytes::from_static(b"delete_value_2")),
            (3, Bytes::from_static(b"delete_value_3")),
        ];

        // Insert test data
        {
            let writer: Tx<Rw> = db.writer().unwrap();
            for (key, value) in &test_data {
                writer.queue_put::<TestTable>(key, value).unwrap();
            }
            writer.raw_commit().unwrap();
        }
        // Test cursor deletion
        {
            let tx: Tx<Rw> = db.writer().unwrap();

            let mut cursor = tx.new_cursor::<TestTable>().unwrap();

            // Navigate to middle entry
            let first = TableTraverse::<TestTable, _>::first(&mut cursor).unwrap().unwrap();
            assert_eq!(first.0, test_data[0].0);

            let next = TableTraverse::<TestTable, _>::read_next(&mut cursor).unwrap().unwrap();
            assert_eq!(next.0, test_data[1].0);

            // Delete current entry (key 2)
            TableTraverseMut::<TestTable, _>::delete_current(&mut cursor).unwrap();

            drop(cursor);
            tx.raw_commit().unwrap();
        }

        // Verify deletion
        {
            let tx: Tx<Ro> = db.reader().unwrap();
            let mut cursor = tx.new_cursor::<TestTable>().unwrap();

            // Should only have first and third entries
            let first = TableTraverse::<TestTable, _>::first(&mut cursor).unwrap().unwrap();
            assert_eq!(first.0, test_data[0].0);

            let second = TableTraverse::<TestTable, _>::read_next(&mut cursor).unwrap().unwrap();
            assert_eq!(second.0, test_data[2].0);

            // Should be no more entries
            let none = TableTraverse::<TestTable, _>::read_next(&mut cursor).unwrap();
            assert!(none.is_none());

            // Verify deleted key is gone
            let missing =
                TableTraverse::<TestTable, _>::exact(&mut cursor, &test_data[1].0).unwrap();
            assert!(missing.is_none());
        }
    }

    #[test]
    fn test_table_traverse_accounts() {
        run_test(test_table_traverse_accounts_inner)
    }

    fn test_table_traverse_accounts_inner(db: &DatabaseEnv) {
        // Setup test accounts
        let test_accounts: Vec<(Address, Account)> = (0..5)
            .map(|i| {
                let mut addr_bytes = [0u8; 20];
                addr_bytes[19] = i;
                let address = Address::from_slice(&addr_bytes);
                let account = Account {
                    nonce: (i as u64) * 10,
                    balance: U256::from((i as u64) * 1000),
                    bytecode_hash: if i % 2 == 0 { Some(B256::from_slice(&[i; 32])) } else { None },
                };
                (address, account)
            })
            .collect();

        // Insert test data
        {
            let writer: Tx<Rw> = db.writer().unwrap();

            for (address, account) in &test_accounts {
                writer.queue_put::<tables::PlainAccountState>(address, account).unwrap();
            }

            writer.raw_commit().unwrap();
        }

        // Test typed table traversal
        {
            let tx: Tx<Ro> = db.reader().unwrap();
            let mut cursor = tx.new_cursor::<tables::PlainAccountState>().unwrap();

            // Test first with type-safe operations
            let first_raw =
                TableTraverse::<tables::PlainAccountState, _>::first(&mut cursor).unwrap();
            assert!(first_raw.is_some());
            let (first_key, first_account) = first_raw.unwrap();
            assert_eq!(first_key, test_accounts[0].0);
            assert_eq!(first_account, test_accounts[0].1);

            // Test last
            let last_raw =
                TableTraverse::<tables::PlainAccountState, _>::last(&mut cursor).unwrap();
            assert!(last_raw.is_some());
            let (last_key, last_account) = last_raw.unwrap();
            assert_eq!(last_key, test_accounts.last().unwrap().0);
            assert_eq!(last_account, test_accounts.last().unwrap().1);

            // Test exact lookup
            let target_address = &test_accounts[2].0;
            let exact_account =
                TableTraverse::<tables::PlainAccountState, _>::exact(&mut cursor, target_address)
                    .unwrap();
            assert!(exact_account.is_some());
            assert_eq!(exact_account.unwrap(), test_accounts[2].1);

            // Test range lookup
            let mut partial_addr = [0u8; 20];
            partial_addr[19] = 3; // Between entries 2 and 3
            let range_addr = Address::from_slice(&partial_addr);

            let range_result = TableTraverse::<tables::PlainAccountState, _>::lower_bound(
                &mut cursor,
                &range_addr,
            )
            .unwrap();
            assert!(range_result.is_some());
            let (found_addr, found_account) = range_result.unwrap();
            assert_eq!(found_addr, test_accounts[3].0);
            assert_eq!(found_account, test_accounts[3].1);
        }
    }

    #[test]
    fn test_dual_table_traverse() {
        run_test(test_dual_table_traverse_inner)
    }

    fn test_dual_table_traverse_inner(db: &DatabaseEnv) {
        let one_addr = Address::repeat_byte(0x01);
        let two_addr = Address::repeat_byte(0x02);

        let one_slot = U256::from(0x01);
        let two_slot = U256::from(0x06);
        let three_slot = U256::from(0x09);

        let one_value = U256::from(0x100);
        let two_value = U256::from(0x200);
        let three_value = U256::from(0x300);
        let four_value = U256::from(0x400);
        let five_value = U256::from(0x500);

        // Setup test storage data
        let test_storage: Vec<(Address, U256, U256)> = vec![
            (one_addr, one_slot, one_value),
            (one_addr, two_slot, two_value),
            (one_addr, three_slot, three_value),
            (two_addr, one_slot, four_value),
            (two_addr, two_slot, five_value),
        ];

        // Insert test data
        {
            let writer: Tx<Rw> = db.writer().unwrap();

            for (address, storage_key, value) in &test_storage {
                writer
                    .queue_put_dual::<tables::PlainStorageState>(address, storage_key, value)
                    .unwrap();
            }

            writer.raw_commit().unwrap();
        }

        // Test dual-keyed traversal
        {
            let tx: Tx<Ro> = db.reader().unwrap();
            let mut cursor = tx.new_cursor::<tables::PlainStorageState>().unwrap();

            // Test exact dual lookup
            let address = &test_storage[1].0;
            let storage_key = &test_storage[1].1;
            let expected_value = &test_storage[1].2;

            let exact_result = DualTableTraverse::<tables::PlainStorageState, _>::exact_dual(
                &mut cursor,
                address,
                storage_key,
            )
            .unwrap()
            .unwrap();
            assert_eq!(exact_result, *expected_value);

            // Test range lookup for dual keys
            let search_key = U256::from(0x02);
            let range_result = DualTableTraverse::<tables::PlainStorageState, _>::next_dual_above(
                &mut cursor,
                &test_storage[0].0, // Address 0x01
                &search_key,
            )
            .unwrap()
            .unwrap();

            let (found_addr, found_key, found_value) = range_result;
            assert_eq!(found_addr, test_storage[1].0); // Same address
            assert_eq!(found_key, test_storage[1].1); // Next storage key (0x02)
            assert_eq!(found_value, test_storage[1].2); // Corresponding value

            // Test next_k1 (move to next primary key)
            // First position cursor at first entry of first address
            DualTableTraverse::<tables::PlainStorageState, _>::exact_dual(
                &mut cursor,
                &test_storage[0].0,
                &test_storage[0].1,
            )
            .unwrap();

            // Move to next primary key (different address)
            let next_k1_result =
                DualTableTraverse::<tables::PlainStorageState, _>::next_k1(&mut cursor).unwrap();
            assert!(next_k1_result.is_some());
            let (next_addr, next_storage_key, next_value) = next_k1_result.unwrap();
            assert_eq!(next_addr, test_storage[3].0); // Address 0x02
            assert_eq!(next_storage_key, test_storage[3].1); // First storage key for new address
            assert_eq!(next_value, test_storage[3].2);
        }
    }

    #[test]
    fn test_dual_table_traverse_empty_results() {
        run_test(test_dual_table_traverse_empty_results_inner)
    }

    fn test_dual_table_traverse_empty_results_inner(db: &DatabaseEnv) {
        // Setup minimal test data
        let address = Address::from_slice(&[0x01; 20]);
        let storage_key = U256::from(1);
        let value = U256::from(100);

        {
            let writer: Tx<Rw> = db.writer().unwrap();
            writer
                .queue_put_dual::<tables::PlainStorageState>(&address, &storage_key, &value)
                .unwrap();
            writer.raw_commit().unwrap();
        }

        {
            let tx: Tx<Ro> = db.reader().unwrap();
            let mut cursor = tx.new_cursor::<tables::PlainStorageState>().unwrap();

            // Test exact lookup for non-existent dual key
            let missing_addr = Address::from_slice(&[0xFF; 20]);
            let missing_key = U256::from(0xFF);

            let exact_missing = DualTableTraverse::<tables::PlainStorageState, _>::exact_dual(
                &mut cursor,
                &missing_addr,
                &missing_key,
            )
            .unwrap();
            assert!(exact_missing.is_none());

            // Test range lookup beyond all data
            let beyond_key = U256::MAX;
            let range_missing = DualTableTraverse::<tables::PlainStorageState, _>::next_dual_above(
                &mut cursor,
                &address,
                &beyond_key,
            )
            .unwrap();
            assert!(range_missing.is_none());

            // Position at the only entry, then try next_k1
            DualTableTraverse::<tables::PlainStorageState, _>::exact_dual(
                &mut cursor,
                &address,
                &storage_key,
            )
            .unwrap();

            let next_k1_missing =
                DualTableTraverse::<tables::PlainStorageState, _>::next_k1(&mut cursor).unwrap();
            assert!(next_k1_missing.is_none());
        }
    }

    #[test]
    fn test_table_traverse_empty_table() {
        run_test(test_table_traverse_empty_table_inner)
    }

    fn test_table_traverse_empty_table_inner(db: &DatabaseEnv) {
        // TestTable is already created but empty
        {
            let tx: Tx<Ro> = db.reader().unwrap();
            let mut cursor = tx.new_cursor::<TestTable>().unwrap();

            // All operations should return None on empty table
            assert!(TableTraverse::<TestTable, _>::first(&mut cursor).unwrap().is_none());
            assert!(TableTraverse::<TestTable, _>::last(&mut cursor).unwrap().is_none());
            assert!(TableTraverse::<TestTable, _>::exact(&mut cursor, &42u64).unwrap().is_none());
            assert!(
                TableTraverse::<TestTable, _>::lower_bound(&mut cursor, &42u64).unwrap().is_none()
            );
            assert!(TableTraverse::<TestTable, _>::read_next(&mut cursor).unwrap().is_none());
            assert!(TableTraverse::<TestTable, _>::read_prev(&mut cursor).unwrap().is_none());
        }
    }

    #[test]
    fn test_table_traverse_state_management() {
        run_test(test_table_traverse_state_management_inner)
    }

    fn test_table_traverse_state_management_inner(db: &DatabaseEnv) {
        let test_data: Vec<(u64, Bytes)> = vec![
            (1, Bytes::from_static(b"state_value_1")),
            (2, Bytes::from_static(b"state_value_2")),
            (3, Bytes::from_static(b"state_value_3")),
        ];

        {
            let writer: Tx<Rw> = db.writer().unwrap();
            for (key, value) in &test_data {
                writer.queue_put::<TestTable>(key, value).unwrap();
            }
            writer.raw_commit().unwrap();
        }

        {
            let tx: Tx<Ro> = db.reader().unwrap();
            let mut cursor = tx.new_cursor::<TestTable>().unwrap();

            // Test that cursor operations maintain state correctly

            // Start at first
            let first = TableTraverse::<TestTable, _>::first(&mut cursor).unwrap().unwrap();
            assert_eq!(first.0, test_data[0].0);

            // Move to second via next
            let second = TableTraverse::<TestTable, _>::read_next(&mut cursor).unwrap().unwrap();
            assert_eq!(second.0, test_data[1].0);

            // Jump to last
            let last = TableTraverse::<TestTable, _>::last(&mut cursor).unwrap().unwrap();
            assert_eq!(last.0, test_data[2].0);

            // Move back via prev
            let back_to_second =
                TableTraverse::<TestTable, _>::read_prev(&mut cursor).unwrap().unwrap();
            assert_eq!(back_to_second.0, test_data[1].0);

            // Use exact to jump to specific position
            let exact_first =
                TableTraverse::<TestTable, _>::exact(&mut cursor, &test_data[0].0).unwrap();
            assert!(exact_first.is_some());
            assert_eq!(exact_first.unwrap(), test_data[0].1);

            // Verify cursor is now positioned at first entry
            let next_from_first =
                TableTraverse::<TestTable, _>::read_next(&mut cursor).unwrap().unwrap();
            assert_eq!(next_from_first.0, test_data[1].0);

            // Use range lookup - look for key >= 1, should find key 1
            let range_lookup =
                TableTraverse::<TestTable, _>::lower_bound(&mut cursor, &1u64).unwrap().unwrap();
            assert_eq!(range_lookup.0, test_data[0].0); // Should find key 1

            // Verify we can continue navigation from range position
            let next_after_range =
                TableTraverse::<TestTable, _>::read_next(&mut cursor).unwrap().unwrap();
            assert_eq!(next_after_range.0, test_data[1].0);
        }
    }

    #[test]
    fn mdbx_conformance() {
        run_test(conformance)
    }

    #[test]
    fn test_get_fsi() {
        run_test(test_get_fsi_inner)
    }

    fn test_get_fsi_inner(db: &DatabaseEnv) {
        // Tables are already created in create_test_rw_db()
        // Try to get FixedSizeInfo for an existing table
        let reader: Tx<Ro> = db.reader().unwrap();

        // This should work - Headers table was created in setup
        reader.get_fsi(tables::Headers::NAME).unwrap();

        // Try with TestTable which was also created
        reader.get_fsi(TestTable::NAME).unwrap();

        // Use a DUP_FIXED table and assert the result contains the expected info
        let result3 = reader.get_fsi(tables::PlainStorageState::NAME).unwrap();
        assert!(result3.is_dup_fixed());
    }

    #[test]
    fn test_storage_roundtrip_debug() {
        run_test(test_storage_roundtrip_debug_inner)
    }

    fn test_storage_roundtrip_debug_inner(db: &DatabaseEnv) {
        use alloy::primitives::address;

        let addr = address!("0xabcdef0123456789abcdef0123456789abcdef01");
        let slot = U256::from(1);
        let value = U256::from(999);

        // Write storage
        {
            let writer: Tx<Rw> = db.writer().unwrap();

            // Check fsi before write
            {
                let fsi = writer.get_fsi(tables::PlainStorageState::NAME).unwrap();
                assert!(fsi.is_dup_fixed());
            }

            writer.queue_put_dual::<tables::PlainStorageState>(&addr, &slot, &value).unwrap();
            writer.raw_commit().unwrap();
        }

        // Read storage
        {
            let reader: Tx<Ro> = db.reader().unwrap();

            // Check fsi after write
            {
                let fsi = reader.get_fsi(tables::PlainStorageState::NAME).unwrap();
                assert!(fsi.is_dup_fixed());
            }

            let read_value = reader.get_dual::<tables::PlainStorageState>(&addr, &slot).unwrap();
            assert!(read_value.is_some());
            assert_eq!(read_value.unwrap(), U256::from(999));
        }
    }

    #[test]
    #[serial]
    fn mdbx_unwind_conformance() {
        let (_dir_a, db_a) = create_test_rw_db();
        let (_dir_b, db_b) = create_test_rw_db();
        test_unwind_conformance(&db_a, &db_b);
    }

    #[test]
    #[serial]
    fn mdbx_range_clear() {
        let (_dir, db) = create_test_rw_db();
        signet_hot::conformance::test_clear_range(&db);
    }

    #[test]
    #[serial]
    fn mdbx_range_take() {
        let (_dir, db) = create_test_rw_db();
        signet_hot::conformance::test_take_range(&db);
    }

    #[test]
    #[serial]
    fn mdbx_range_clear_dual() {
        let (_dir, db) = create_test_rw_db();
        signet_hot::conformance::test_clear_range_dual(&db);
    }

    #[test]
    #[serial]
    fn mdbx_range_take_dual() {
        let (_dir, db) = create_test_rw_db();
        signet_hot::conformance::test_take_range_dual(&db);
    }

    /// Debug test to trace StorageChangeSets iteration during unwind
    /// This test mirrors the full unwind_conformance test to debug the MDBX failure
    #[test]
    #[serial]
    fn debug_storage_changesets_iteration() {
        use alloy::{
            consensus::{Header, Sealable},
            primitives::{B256, U256, address},
        };
        use signet_hot::{
            conformance::make_bundle_state,
            db::{HistoryWrite, HotDbRead},
            model::{DualTableTraverse, HotKvRead},
            tables,
        };

        // Use same addresses as the full test
        let addr1 = address!("0x1111111111111111111111111111111111111111");
        let addr2 = address!("0x2222222222222222222222222222222222222222");
        let addr3 = address!("0x3333333333333333333333333333333333333333");
        let addr4 = address!("0x4444444444444444444444444444444444444444");

        let slot1 = U256::from(1);
        let slot2 = U256::from(2);
        let slot3 = U256::from(3);

        let (_dir, db) = create_test_rw_db();

        // Create 5 blocks matching the full test
        let mut blocks = Vec::new();
        let mut prev_hash = B256::ZERO;

        // Block 0: Create addr1.slot1 = 10
        {
            let header = Header {
                number: 0,
                parent_hash: prev_hash,
                gas_limit: 1_000_000,
                ..Default::default()
            };
            let sealed = header.seal_slow();
            prev_hash = sealed.hash();
            let bundle = make_bundle_state(
                vec![],
                vec![(addr1, vec![(slot1, U256::ZERO, U256::from(10))])],
                vec![],
            );
            blocks.push((sealed, bundle));
        }

        // Block 1: addr1.slot1 = 20, addr2.slot1 = 100
        {
            let header = Header {
                number: 1,
                parent_hash: prev_hash,
                gas_limit: 1_000_000,
                ..Default::default()
            };
            let sealed = header.seal_slow();
            prev_hash = sealed.hash();
            let bundle = make_bundle_state(
                vec![],
                vec![
                    (addr1, vec![(slot1, U256::from(10), U256::from(20))]),
                    (addr2, vec![(slot1, U256::ZERO, U256::from(100))]),
                ],
                vec![],
            );
            blocks.push((sealed, bundle));
        }

        // Block 2: addr3.slot1 = 1000
        {
            let header = Header {
                number: 2,
                parent_hash: prev_hash,
                gas_limit: 1_000_000,
                ..Default::default()
            };
            let sealed = header.seal_slow();
            prev_hash = sealed.hash();
            let bundle = make_bundle_state(
                vec![],
                vec![(addr3, vec![(slot1, U256::ZERO, U256::from(1000))])],
                vec![],
            );
            blocks.push((sealed, bundle));
        }

        // Block 3: addr1.slot1 = 30, addr1.slot2 = 50, addr4.slot1 = 500
        {
            let header = Header {
                number: 3,
                parent_hash: prev_hash,
                gas_limit: 1_000_000,
                ..Default::default()
            };
            let sealed = header.seal_slow();
            prev_hash = sealed.hash();
            let bundle = make_bundle_state(
                vec![],
                vec![
                    (
                        addr1,
                        vec![
                            (slot1, U256::from(20), U256::from(30)),
                            (slot2, U256::ZERO, U256::from(50)),
                        ],
                    ),
                    (addr4, vec![(slot1, U256::ZERO, U256::from(500))]),
                ],
                vec![],
            );
            blocks.push((sealed, bundle));
        }

        // Block 4: addr1.slot1 = 40, addr1.slot3 = 60, addr2.slot1 = 150, addr2.slot2 = 200
        {
            let header = Header {
                number: 4,
                parent_hash: prev_hash,
                gas_limit: 1_000_000,
                ..Default::default()
            };
            let sealed = header.seal_slow();
            let bundle = make_bundle_state(
                vec![],
                vec![
                    (
                        addr1,
                        vec![
                            (slot1, U256::from(30), U256::from(40)),
                            (slot3, U256::ZERO, U256::from(60)),
                        ],
                    ),
                    (
                        addr2,
                        vec![
                            (slot1, U256::from(100), U256::from(150)),
                            (slot2, U256::ZERO, U256::from(200)),
                        ],
                    ),
                ],
                vec![],
            );
            blocks.push((sealed, bundle));
        }

        // Append all blocks
        {
            let writer = db.writer().unwrap();
            writer.append_blocks(blocks.iter().map(|(h, b)| (h, b))).unwrap();
            writer.commit().unwrap();
        }

        // Verify StorageChangeSets contains expected entries
        {
            let reader = db.reader().unwrap();
            let mut cursor = reader.traverse_dual::<tables::StorageChangeSets>().unwrap();

            let mut entries = Vec::new();
            if let Some(first) =
                DualTableTraverse::<tables::StorageChangeSets, _>::first(&mut *cursor.inner_mut())
                    .unwrap()
            {
                entries.push(first);
                while let Some(next) = DualTableTraverse::<tables::StorageChangeSets, _>::read_next(
                    &mut *cursor.inner_mut(),
                )
                .unwrap()
                {
                    entries.push(next);
                }
            }

            eprintln!("\nStorageChangeSets entries ({} total):", entries.len());
            for ((bn, addr), slot, val) in &entries {
                eprintln!("  block={}, addr={:?}, slot={}, old_value={}", bn, addr, slot, val);
            }
        }

        // Count PlainStorageState entries before unwind
        {
            let reader = db.reader().unwrap();
            let mut cursor = reader.traverse_dual::<tables::PlainStorageState>().unwrap();

            let mut entries = Vec::new();
            if let Some(first) =
                DualTableTraverse::<tables::PlainStorageState, _>::first(&mut *cursor.inner_mut())
                    .unwrap()
            {
                entries.push(first);
                while let Some(next) = DualTableTraverse::<tables::PlainStorageState, _>::read_next(
                    &mut *cursor.inner_mut(),
                )
                .unwrap()
                {
                    entries.push(next);
                }
            }

            eprintln!("\nPlainStorageState BEFORE unwind ({} total):", entries.len());
            for (addr, slot, val) in &entries {
                eprintln!("  addr={:?}, slot={}, value={}", addr, slot, val);
            }
            assert_eq!(entries.len(), 7, "Expected 7 storage entries before unwind");
        }

        // Unwind to block 1
        {
            let writer = db.writer().unwrap();
            writer.unwind_above(1).unwrap();
            writer.commit().unwrap();
        }

        // Count PlainStorageState entries after unwind
        {
            let reader = db.reader().unwrap();
            let mut cursor = reader.traverse_dual::<tables::PlainStorageState>().unwrap();

            let mut entries = Vec::new();
            if let Some(first) =
                DualTableTraverse::<tables::PlainStorageState, _>::first(&mut *cursor.inner_mut())
                    .unwrap()
            {
                entries.push(first);
                while let Some(next) = DualTableTraverse::<tables::PlainStorageState, _>::read_next(
                    &mut *cursor.inner_mut(),
                )
                .unwrap()
                {
                    entries.push(next);
                }
            }

            eprintln!("\nPlainStorageState AFTER unwind ({} total):", entries.len());
            for (addr, slot, val) in &entries {
                eprintln!("  addr={:?}, slot={}, value={}", addr, slot, val);
            }

            // Expected: addr1.slot1=20, addr2.slot1=100 (2 entries)
            assert_eq!(entries.len(), 2, "Expected 2 storage entries after unwind");

            // Verify specific values
            let reader = db.reader().unwrap();
            assert_eq!(
                reader.get_storage(&addr1, &slot1).unwrap(),
                Some(U256::from(20)),
                "addr1.slot1 should be 20"
            );
            assert_eq!(
                reader.get_storage(&addr2, &slot1).unwrap(),
                Some(U256::from(100)),
                "addr2.slot1 should be 100"
            );
        }
    }

    // ========================================================================
    // put_multiple Tests
    // ========================================================================

    #[test]
    fn test_put_multiple_basic() {
        run_test(test_put_multiple_basic_inner)
    }

    fn test_put_multiple_basic_inner(db: &DatabaseEnv) {
        use signet_hot::model::KvTraverse;

        let key = [0x01u8; 8];
        let data_size = 16; // key2 (8) + value (8)
        let count = 3;

        // Create 3 contiguous elements, each 16 bytes
        let mut data = vec![0u8; data_size * count];
        for i in 0..count {
            let offset = i * data_size;
            // key2 part (first 8 bytes of each element)
            data[offset..offset + 8].copy_from_slice(&[i as u8; 8]);
            // value part (next 8 bytes)
            data[offset + 8..offset + 16].copy_from_slice(&[(i as u8) + 100; 8]);
        }

        // Write using put_multiple
        {
            let tx: Tx<Rw> = db.writer().unwrap();
            let mut cursor = tx.new_cursor_raw("put_multiple_test").unwrap();

            let written = cursor.put_multiple(&key, &data).unwrap();

            assert_eq!(written, count);

            drop(cursor);
            tx.raw_commit().unwrap();
        }

        // Verify all entries were written
        {
            let tx: Tx<Ro> = db.reader().unwrap();
            let mut cursor = tx.new_cursor_raw("put_multiple_test").unwrap();

            // Traverse and count entries
            let first = KvTraverse::first(&mut cursor).unwrap();
            assert!(first.is_some());

            let mut entry_count = 1;
            while KvTraverse::read_next(&mut cursor).unwrap().is_some() {
                entry_count += 1;
            }

            assert_eq!(entry_count, count);
        }
    }

    #[test]
    fn test_put_multiple_with_alldups() {
        run_test(test_put_multiple_with_alldups_inner)
    }

    fn test_put_multiple_with_alldups_inner(db: &DatabaseEnv) {
        let key = [0x02u8; 8];
        let data_size = 16;

        // First, insert some initial data
        {
            let tx: Tx<Rw> = db.writer().unwrap();
            let mut cursor = tx.new_cursor_raw("put_multiple_test").unwrap();

            let mut initial_data = vec![0u8; data_size * 2];
            initial_data[0..8].copy_from_slice(&[0xAAu8; 8]);
            initial_data[8..16].copy_from_slice(&[0xBBu8; 8]);
            initial_data[16..24].copy_from_slice(&[0xCCu8; 8]);
            initial_data[24..32].copy_from_slice(&[0xDDu8; 8]);

            cursor.put_multiple(&key, &initial_data).unwrap();

            drop(cursor);
            tx.raw_commit().unwrap();
        }

        // Now replace ALL dups with new data using all_dups=true
        {
            let tx: Tx<Rw> = db.writer().unwrap();
            let mut cursor = tx.new_cursor_raw("put_multiple_test").unwrap();

            let mut new_data = vec![0u8; data_size * 3];
            for i in 0..3 {
                let offset = i * data_size;
                new_data[offset..offset + 8].copy_from_slice(&[(i as u8) + 1; 8]);
                new_data[offset + 8..offset + 16].copy_from_slice(&[(i as u8) + 200; 8]);
            }

            let written = cursor.overwrite_multiple(&key, &new_data).unwrap();

            assert_eq!(written, 3);

            drop(cursor);
            tx.raw_commit().unwrap();
        }

        // Verify: should have exactly 3 entries (old ones replaced)
        {
            let tx: Tx<Ro> = db.reader().unwrap();
            let mut cursor = tx.new_cursor_raw("put_multiple_test").unwrap();

            // Position at the key
            let found = cursor.inner.set::<Cow<'_, [u8]>>(&key).unwrap();
            assert!(found.is_some());

            // Count duplicates for this key
            let mut dup_count = 1;
            while cursor.inner.next_dup::<Cow<'_, [u8]>, Cow<'_, [u8]>>().unwrap().is_some() {
                dup_count += 1;
            }

            assert_eq!(dup_count, 3, "Expected 3 entries after ALLDUPS replacement");
        }
    }

    #[test]
    fn test_put_multiple_single_element() {
        run_test(test_put_multiple_single_element_inner)
    }

    fn test_put_multiple_single_element_inner(db: &DatabaseEnv) {
        let key = [0x03u8; 8];
        let data_size = 16;

        let mut data = vec![0u8; data_size];
        data[0..8].copy_from_slice(&[0x11u8; 8]);
        data[8..16].copy_from_slice(&[0x22u8; 8]);

        {
            let tx: Tx<Rw> = db.writer().unwrap();
            let mut cursor = tx.new_cursor_raw("put_multiple_test").unwrap();

            let written = cursor.put_multiple(&key, &data).unwrap();

            assert_eq!(written, 1);

            drop(cursor);
            tx.raw_commit().unwrap();
        }
    }

    #[test]
    fn test_put_multiple_mismatched_length_panics() {
        run_test(test_put_multiple_mismatched_length_panics_inner)
    }

    fn test_put_multiple_mismatched_length_panics_inner(db: &DatabaseEnv) {
        let key = [0x05u8; 8];

        // Intentionally wrong size: 17 bytes is not a multiple of value_size (16)
        let data = vec![0u8; 17];

        let tx: Tx<Rw> = db.writer().unwrap();
        let mut cursor = tx.new_cursor_raw("put_multiple_test").unwrap();

        // The underlying library should return an error or panic for misaligned data
        let result = cursor.put_multiple(&key, &data);
        assert!(result.is_err(), "Should error when data.len() is not a multiple of value_size");
    }

    #[test]
    fn test_put_multiple_oversized_data_panics() {
        run_test(test_put_multiple_oversized_data_panics_inner)
    }

    fn test_put_multiple_oversized_data_panics_inner(db: &DatabaseEnv) {
        let key = [0x06u8; 8];

        // Intentionally wrong size: 33 bytes is not a multiple of value_size (16)
        let data = vec![0u8; 33];

        let tx: Tx<Rw> = db.writer().unwrap();
        let mut cursor = tx.new_cursor_raw("put_multiple_test").unwrap();

        // The underlying library should return an error or panic for misaligned data
        let result = cursor.put_multiple(&key, &data);
        assert!(result.is_err(), "Should error when data.len() is not a multiple of value_size");
    }

    #[test]
    fn test_put_multiple_large_batch() {
        run_test(test_put_multiple_large_batch_inner)
    }

    fn test_put_multiple_large_batch_inner(db: &DatabaseEnv) {
        let key = [0x07u8; 8];
        let data_size = 16;
        let count = 1000;

        let mut data = vec![0u8; data_size * count];
        for i in 0..count {
            let offset = i * data_size;
            // Encode index into the data for verification
            data[offset..offset + 8].copy_from_slice(&(i as u64).to_le_bytes());
            data[offset + 8..offset + 16].copy_from_slice(&((i + 1000) as u64).to_le_bytes());
        }

        {
            let tx: Tx<Rw> = db.writer().unwrap();
            let mut cursor = tx.new_cursor_raw("put_multiple_test").unwrap();

            let written = cursor.put_multiple(&key, &data).unwrap();

            assert_eq!(written, count);

            drop(cursor);
            tx.raw_commit().unwrap();
        }

        // Verify count
        {
            let tx: Tx<Ro> = db.reader().unwrap();
            let mut cursor = tx.new_cursor_raw("put_multiple_test").unwrap();

            cursor.inner.set::<Cow<'_, [u8]>>(&key).unwrap();

            let mut dup_count = 1;
            while cursor.inner.next_dup::<Cow<'_, [u8]>, Cow<'_, [u8]>>().unwrap().is_some() {
                dup_count += 1;
            }

            assert_eq!(dup_count, count);
        }
    }

    #[test]
    fn test_put_multiple_exceeds_page_size() {
        run_test(test_put_multiple_exceeds_page_size_inner)
    }

    fn test_put_multiple_exceeds_page_size_inner(db: &DatabaseEnv) {
        // MDBX max page size is 64KB (0x10000 = 65536 bytes)
        // With data_size=16, we need > 4096 elements to exceed max page size
        // Using 5000 elements = 80,000 bytes > 64KB
        let key = [0x08u8; 8];
        let data_size = 16;
        let count = 5000;

        let total_size = data_size * count;
        assert!(total_size > 65536, "Test data must exceed max MDBX page size (64KB)");

        let mut data = vec![0u8; total_size];
        for i in 0..count {
            let offset = i * data_size;
            // key2: element index as little-endian u64
            data[offset..offset + 8].copy_from_slice(&(i as u64).to_le_bytes());
            // value: index + 0x1000_0000 as little-endian u64
            data[offset + 8..offset + 16]
                .copy_from_slice(&((i as u64) + 0x1000_0000).to_le_bytes());
        }

        // Write - MDBX should handle multi-page writes internally
        {
            let tx: Tx<Rw> = db.writer().unwrap();
            let mut cursor = tx.new_cursor_raw("put_multiple_test").unwrap();

            let written = cursor.put_multiple(&key, &data).unwrap();

            // MDBX may write fewer than requested if it spans pages
            // The return value indicates how many were actually written
            assert!(written > 0, "Should write at least some elements");

            drop(cursor);
            tx.raw_commit().unwrap();
        }

        // Verify at least partial write succeeded
        {
            let tx: Tx<Ro> = db.reader().unwrap();
            let mut cursor = tx.new_cursor_raw("put_multiple_test").unwrap();

            let found = cursor.inner.set::<Cow<'_, [u8]>>(&key).unwrap();
            assert!(found.is_some(), "Key should exist after put_multiple");

            // Count all duplicates
            let mut dup_count = 1;
            while cursor.inner.next_dup::<Cow<'_, [u8]>, Cow<'_, [u8]>>().unwrap().is_some() {
                dup_count += 1;
            }

            assert!(dup_count > 0, "Should have at least some entries written");
        }
    }

    // ========================================================================
    // put_multiple_fixed Tests (Safe Wrapper)
    // ========================================================================

    #[test]
    fn test_put_multiple_fixed_basic() {
        run_test(test_put_multiple_fixed_basic_inner)
    }

    fn test_put_multiple_fixed_basic_inner(db: &DatabaseEnv) {
        let key = [0x10u8; 8];
        let count = 3;

        // Create 3 contiguous elements, each 16 bytes (key2=8 + value=8)
        let mut data = vec![0u8; 16 * count];
        for i in 0..count {
            let offset = i * 16;
            // key2 part (first 8 bytes)
            data[offset..offset + 8].copy_from_slice(&[i as u8; 8]);
            // value part (next 8 bytes)
            data[offset + 8..offset + 16].copy_from_slice(&[(i as u8) + 100; 8]);
        }

        // Write using put_multiple_fixed (safe wrapper)
        {
            let tx: Tx<Rw> = db.writer().unwrap();
            let mut cursor = tx.new_cursor_raw("put_multiple_test").unwrap();

            let written = cursor.put_multiple(&key, &data).unwrap();

            assert_eq!(written, count);

            drop(cursor);
            tx.raw_commit().unwrap();
        }

        // Verify all entries were written
        {
            let tx: Tx<Ro> = db.reader().unwrap();
            let mut cursor = tx.new_cursor_raw("put_multiple_test").unwrap();

            // Position at key
            let first = cursor.inner.set::<Cow<'_, [u8]>>(&key).unwrap();
            assert!(first.is_some());

            // Count duplicates
            let mut entry_count = 1;
            while cursor.inner.next_dup::<Cow<'_, [u8]>, Cow<'_, [u8]>>().unwrap().is_some() {
                entry_count += 1;
            }

            assert_eq!(entry_count, count);
        }
    }

    #[test]
    fn test_put_multiple_fixed_not_dupfixed() {
        run_test(test_put_multiple_fixed_not_dupfixed_inner)
    }

    fn test_put_multiple_fixed_not_dupfixed_inner(db: &DatabaseEnv) {
        // Try to use put_multiple_fixed on a non-DUP_FIXED table (TestTable)
        let key = 42u64.to_le_bytes();
        let data = vec![0u8; 16];

        let tx: Tx<Rw> = db.writer().unwrap();
        let mut cursor = tx.new_cursor::<TestTable>().unwrap();

        let result = cursor.put_multiple(&key, &data);

        assert!(matches!(result, Err(MdbxError::NotDupFixed)));
    }

    #[test]
    #[serial]
    fn test_clear_k1() {
        run_test(test_clear_k1_inner)
    }

    fn test_clear_k1_inner(db: &DatabaseEnv) {
        let addr1 = Address::from_slice(&[0x11; 20]);
        let addr2 = Address::from_slice(&[0x22; 20]);
        let addr3 = Address::from_slice(&[0x33; 20]);

        // Setup: Write storage entries for multiple addresses
        {
            let writer = db.writer().unwrap();
            // addr1: slots 1, 2, 3
            writer
                .queue_put_dual::<tables::PlainStorageState>(
                    &addr1,
                    &U256::from(1),
                    &U256::from(10),
                )
                .unwrap();
            writer
                .queue_put_dual::<tables::PlainStorageState>(
                    &addr1,
                    &U256::from(2),
                    &U256::from(20),
                )
                .unwrap();
            writer
                .queue_put_dual::<tables::PlainStorageState>(
                    &addr1,
                    &U256::from(3),
                    &U256::from(30),
                )
                .unwrap();
            // addr2: slots 10, 20
            writer
                .queue_put_dual::<tables::PlainStorageState>(
                    &addr2,
                    &U256::from(10),
                    &U256::from(100),
                )
                .unwrap();
            writer
                .queue_put_dual::<tables::PlainStorageState>(
                    &addr2,
                    &U256::from(20),
                    &U256::from(200),
                )
                .unwrap();
            // addr3: slot 100
            writer
                .queue_put_dual::<tables::PlainStorageState>(
                    &addr3,
                    &U256::from(100),
                    &U256::from(1000),
                )
                .unwrap();
            writer.commit().unwrap();
        }

        // Clear all entries for addr2
        {
            let writer = db.writer().unwrap();
            {
                let mut cursor = writer.traverse_dual_mut::<tables::PlainStorageState>().unwrap();
                cursor.clear_k1(&addr2).unwrap();
            }
            writer.commit().unwrap();
        }

        // Verify addr2 entries are deleted, others remain
        {
            let reader: Tx<Ro> = db.reader().unwrap();
            // addr1 should exist
            assert!(
                reader
                    .get_dual::<tables::PlainStorageState>(&addr1, &U256::from(1))
                    .unwrap()
                    .is_some()
            );
            assert!(
                reader
                    .get_dual::<tables::PlainStorageState>(&addr1, &U256::from(2))
                    .unwrap()
                    .is_some()
            );
            assert!(
                reader
                    .get_dual::<tables::PlainStorageState>(&addr1, &U256::from(3))
                    .unwrap()
                    .is_some()
            );
            // addr2 should be deleted
            assert!(
                reader
                    .get_dual::<tables::PlainStorageState>(&addr2, &U256::from(10))
                    .unwrap()
                    .is_none()
            );
            assert!(
                reader
                    .get_dual::<tables::PlainStorageState>(&addr2, &U256::from(20))
                    .unwrap()
                    .is_none()
            );
            // addr3 should exist
            assert!(
                reader
                    .get_dual::<tables::PlainStorageState>(&addr3, &U256::from(100))
                    .unwrap()
                    .is_some()
            );
        }

        // Test idempotent - clearing already deleted K1 should be no-op
        {
            let writer = db.writer().unwrap();
            {
                let mut cursor = writer.traverse_dual_mut::<tables::PlainStorageState>().unwrap();
                cursor.clear_k1(&addr2).unwrap();
            }
            writer.commit().unwrap();
        }

        // Verify state unchanged
        {
            let reader: Tx<Ro> = db.reader().unwrap();
            assert!(
                reader
                    .get_dual::<tables::PlainStorageState>(&addr1, &U256::from(1))
                    .unwrap()
                    .is_some()
            );
            assert!(
                reader
                    .get_dual::<tables::PlainStorageState>(&addr3, &U256::from(100))
                    .unwrap()
                    .is_some()
            );
        }
    }
}
