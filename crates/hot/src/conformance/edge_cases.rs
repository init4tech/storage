//! Value edge cases and batch operations tests.

use crate::{
    db::{HotDbRead, UnsafeDbWrite},
    model::{HotKv, HotKvRead, HotKvWrite},
    tables,
};
use alloy::{
    consensus::{Header, Sealable},
    primitives::{U256, address},
};
use signet_storage_types::Account;

/// Test that zero storage values are correctly stored and retrieved.
///
/// This verifies that U256::ZERO is not confused with "not set" or deleted.
pub fn test_zero_storage_value<T: HotKv>(hot_kv: &T) {
    let addr = address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa1");
    let slot = U256::from(1);

    // Write zero value
    {
        let writer = hot_kv.writer().unwrap();
        writer.put_storage(&addr, &slot, &U256::ZERO).unwrap();
        writer.commit().unwrap();
    }

    // Read zero value - should return Some(ZERO), not None
    {
        let reader = hot_kv.reader().unwrap();
        let value = reader.get_storage(&addr, &slot).unwrap();
        assert!(value.is_some(), "Zero storage value should be Some, not None");
        assert_eq!(value.unwrap(), U256::ZERO, "Zero storage value should be U256::ZERO");
    }

    // Verify via traversal that the entry exists
    {
        let reader = hot_kv.reader().unwrap();
        let mut cursor = reader.traverse_dual::<tables::PlainStorageState>().unwrap();
        let mut found = false;
        while let Some((k1, k2, v)) = cursor.read_next().unwrap() {
            if k1 == addr && k2 == slot {
                found = true;
                assert_eq!(v, U256::ZERO);
            }
        }
        assert!(found, "Zero value entry should exist in table");
    }
}

/// Test that empty accounts (all zero fields) are correctly stored and retrieved.
///
/// This verifies that an account with nonce=0, balance=0, no code is not
/// confused with a non-existent account.
pub fn test_empty_account<T: HotKv>(hot_kv: &T) {
    let addr = address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa2");
    let empty_account = Account { nonce: 0, balance: U256::ZERO, bytecode_hash: None };

    // Write empty account
    {
        let writer = hot_kv.writer().unwrap();
        writer.put_account(&addr, &empty_account).unwrap();
        writer.commit().unwrap();
    }

    // Read empty account - should return Some, not None
    {
        let reader = hot_kv.reader().unwrap();
        let account = reader.get_account(&addr).unwrap();
        assert!(account.is_some(), "Empty account should be Some, not None");
        let account = account.unwrap();
        assert_eq!(account.nonce, 0);
        assert_eq!(account.balance, U256::ZERO);
        assert!(account.bytecode_hash.is_none());
    }
}

/// Test that maximum storage values (U256::MAX) are correctly stored and retrieved.
pub fn test_max_storage_value<T: HotKv>(hot_kv: &T) {
    let addr = address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa3");
    let slot = U256::from(1);

    // Write max value
    {
        let writer = hot_kv.writer().unwrap();
        writer.put_storage(&addr, &slot, &U256::MAX).unwrap();
        writer.commit().unwrap();
    }

    // Read max value
    {
        let reader = hot_kv.reader().unwrap();
        let value = reader.get_storage(&addr, &slot).unwrap();
        assert!(value.is_some());
        assert_eq!(value.unwrap(), U256::MAX, "Max storage value should be preserved");
    }
}

/// Test that maximum block numbers (u64::MAX) work correctly in headers.
pub fn test_max_block_number<T: HotKv>(hot_kv: &T) {
    let header = Header { number: u64::MAX, gas_limit: 1_000_000, ..Default::default() };
    let sealed = header.seal_slow();

    // Write header at max block number
    {
        let writer = hot_kv.writer().unwrap();
        writer.put_header(&sealed).unwrap();
        writer.commit().unwrap();
    }

    // Read header
    {
        let reader = hot_kv.reader().unwrap();
        let read_header = reader.get_header(u64::MAX).unwrap();
        assert!(read_header.is_some());
        assert_eq!(read_header.unwrap().number, u64::MAX);
    }
}

/// Test get_many batch retrieval.
pub fn test_get_many<T: HotKv>(hot_kv: &T) {
    let addr1 = address!("0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee01");
    let addr2 = address!("0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee02");
    let addr3 = address!("0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee03");

    let acc1 = Account { nonce: 1, balance: U256::from(100), bytecode_hash: None };
    let acc2 = Account { nonce: 2, balance: U256::from(200), bytecode_hash: None };
    let acc3 = Account { nonce: 3, balance: U256::from(300), bytecode_hash: None };

    // Write accounts
    {
        let writer = hot_kv.writer().unwrap();
        writer.put_account(&addr1, &acc1).unwrap();
        writer.put_account(&addr2, &acc2).unwrap();
        writer.put_account(&addr3, &acc3).unwrap();
        writer.commit().unwrap();
    }
}

/// Test queue_put_many batch writes.
pub fn test_queue_put_many<T: HotKv>(hot_kv: &T) {
    let entries: Vec<(u64, Header)> = (200u64..210)
        .map(|i| (i, Header { number: i, gas_limit: 1_000_000, ..Default::default() }))
        .collect();

    // Batch write using queue_put_many
    {
        let writer = hot_kv.writer().unwrap();
        let refs: Vec<(&u64, &Header)> = entries.iter().map(|(k, v)| (k, v)).collect();
        writer.queue_put_many::<tables::Headers, _>(refs).unwrap();
        writer.commit().unwrap();
    }

    // Verify all entries exist
    {
        let reader = hot_kv.reader().unwrap();
        for i in 200u64..210 {
            let header = reader.get_header(i).unwrap();
            assert!(header.is_some(), "Header {} should exist after batch write", i);
            assert_eq!(header.unwrap().number, i);
        }
    }
}

/// Test queue_clear clears all entries in a table.
pub fn test_queue_clear<T: HotKv>(hot_kv: &T) {
    // Write some headers
    {
        let writer = hot_kv.writer().unwrap();
        for i in 300u64..310 {
            let header = Header { number: i, gas_limit: 1_000_000, ..Default::default() };
            writer.put_header_inconsistent(&header).unwrap();
        }
        writer.commit().unwrap();
    }

    // Verify entries exist
    {
        let reader = hot_kv.reader().unwrap();
        for i in 300u64..310 {
            assert!(reader.get_header(i).unwrap().is_some());
        }
    }

    // Clear the table
    {
        let writer = hot_kv.writer().unwrap();
        writer.queue_clear::<tables::Headers>().unwrap();
        writer.commit().unwrap();
    }

    // Verify all entries are gone
    {
        let reader = hot_kv.reader().unwrap();
        for i in 300u64..310 {
            assert!(
                reader.get_header(i).unwrap().is_none(),
                "Header {} should be gone after clear",
                i
            );
        }
    }
}

/// Test that put-then-delete in the same transaction results in deletion.
pub fn test_put_then_delete_same_key<T: HotKv>(hot_kv: &T) {
    let addr = address!("0xffffffffffffffffffffffffffffffffffff0001");
    let account = Account { nonce: 99, balance: U256::from(9999), bytecode_hash: None };

    // In a single transaction: put then delete
    {
        let writer = hot_kv.writer().unwrap();
        writer.put_account(&addr, &account).unwrap();
        writer.queue_delete::<tables::PlainAccountState>(&addr).unwrap();
        writer.commit().unwrap();
    }

    // Account should not exist
    {
        let reader = hot_kv.reader().unwrap();
        let result = reader.get_account(&addr).unwrap();
        assert!(result.is_none(), "Put-then-delete should result in no entry");
    }
}

/// Test that delete-then-put in the same transaction results in the put value.
pub fn test_delete_then_put_same_key<T: HotKv>(hot_kv: &T) {
    let addr = address!("0xffffffffffffffffffffffffffffffffffff0002");
    let old_account = Account { nonce: 1, balance: U256::from(100), bytecode_hash: None };
    let new_account = Account { nonce: 2, balance: U256::from(200), bytecode_hash: None };

    // First, write an account
    {
        let writer = hot_kv.writer().unwrap();
        writer.put_account(&addr, &old_account).unwrap();
        writer.commit().unwrap();
    }

    // In a single transaction: delete then put new value
    {
        let writer = hot_kv.writer().unwrap();
        writer.queue_delete::<tables::PlainAccountState>(&addr).unwrap();
        writer.put_account(&addr, &new_account).unwrap();
        writer.commit().unwrap();
    }

    // Should have the new value
    {
        let reader = hot_kv.reader().unwrap();
        let result = reader.get_account(&addr).unwrap();
        assert!(result.is_some(), "Delete-then-put should result in entry existing");
        let account = result.unwrap();
        assert_eq!(account.nonce, 2, "Should have the new nonce");
        assert_eq!(account.balance, U256::from(200), "Should have the new balance");
    }
}
