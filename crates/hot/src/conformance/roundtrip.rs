//! Basic CRUD roundtrip tests for hot storage.

use crate::{
    db::{HistoryRead, HotDbRead, UnsafeDbWrite, UnsafeHistoryWrite},
    model::{HotKv, HotKvRead},
    tables,
};
use alloy::{
    consensus::Header,
    primitives::{B256, Bytes, U256, address, b256},
};
use signet_storage_types::{Account, BlockNumberList, SealedHeader};
use trevm::revm::bytecode::Bytecode;

/// Test writing and reading headers via HotDbWrite/HotDbRead
pub fn test_header_roundtrip<T: HotKv>(hot_kv: &T) {
    let header = Header { number: 42, gas_limit: 1_000_000, ..Default::default() };
    let sealed = SealedHeader::new(header.clone());
    let hash = sealed.hash();

    // Write header
    {
        let writer = hot_kv.writer().unwrap();
        writer.put_header(&sealed).unwrap();
        writer.commit().unwrap();
    }

    // Read header by number
    {
        let reader = hot_kv.reader().unwrap();
        let read_header = reader.get_header(42).unwrap();
        assert!(read_header.is_some());
        assert_eq!(read_header.unwrap().number, 42);
    }

    // Read header number by hash
    {
        let reader = hot_kv.reader().unwrap();
        let read_number = reader.get_header_number(&hash).unwrap();
        assert!(read_number.is_some());
        assert_eq!(read_number.unwrap(), 42);
    }

    // Read header by hash
    {
        let reader = hot_kv.reader().unwrap();
        let read_header = reader.header_by_hash(&hash).unwrap();
        assert!(read_header.is_some());
        assert_eq!(read_header.unwrap().number, 42);
    }
}

/// Test writing and reading accounts via HotDbWrite/HotDbRead
pub fn test_account_roundtrip<T: HotKv>(hot_kv: &T) {
    let addr = address!("0x1234567890123456789012345678901234567890");
    let account = Account { nonce: 5, balance: U256::from(1000), bytecode_hash: Some(B256::ZERO) };

    // Write account
    {
        let writer = hot_kv.writer().unwrap();
        writer.put_account(&addr, &account).unwrap();
        writer.commit().unwrap();
    }

    // Read account
    {
        let reader = hot_kv.reader().unwrap();
        let read_account = reader.get_account(&addr).unwrap();
        assert!(read_account.is_some());
        let read_account = read_account.unwrap();
        assert_eq!(read_account.nonce, 5);
        assert_eq!(read_account.balance, U256::from(1000));
    }
}

/// Test writing and reading storage via HotDbWrite/HotDbRead
pub fn test_storage_roundtrip<T: HotKv>(hot_kv: &T) {
    let addr = address!("0xabcdef0123456789abcdef0123456789abcdef01");
    let slot = U256::from(42);
    let value = U256::from(999);

    // Write storage
    {
        let writer = hot_kv.writer().unwrap();
        writer.put_storage(&addr, &slot, &value).unwrap();
        writer.commit().unwrap();
    }

    // Read storage
    {
        let reader = hot_kv.reader().unwrap();
        let read_value = reader.get_storage(&addr, &slot).unwrap();
        assert!(read_value.is_some());
        assert_eq!(read_value.unwrap(), U256::from(999));
    }
}

/// Test that updating a storage slot replaces the value (no duplicates).
///
/// This test verifies that DUPSORT tables properly handle updates by deleting
/// existing entries before inserting new ones.
pub fn test_storage_update_replaces<T: HotKv>(hot_kv: &T) {
    let addr = address!("0x2222222222222222222222222222222222222222");
    let slot = U256::from(1);

    // Write initial value
    {
        let writer = hot_kv.writer().unwrap();
        writer.put_storage(&addr, &slot, &U256::from(10)).unwrap();
        writer.commit().unwrap();
    }

    // Update to new value
    {
        let writer = hot_kv.writer().unwrap();
        writer.put_storage(&addr, &slot, &U256::from(20)).unwrap();
        writer.commit().unwrap();
    }

    // Verify: only ONE entry exists with the NEW value
    let reader = hot_kv.reader().unwrap();
    let mut cursor = reader.traverse_dual::<tables::PlainStorageState>().unwrap();

    let mut count = 0;
    let mut found_value = None;
    while let Some((k, k2, v)) = cursor.read_next().unwrap() {
        if k == addr && k2 == slot {
            count += 1;
            found_value = Some(v);
        }
    }

    assert_eq!(count, 1, "Should have exactly one entry, not duplicates");
    assert_eq!(found_value, Some(U256::from(20)), "Value should be 20");
}

/// Test writing and reading bytecode via HotDbWrite/HotDbRead
pub fn test_bytecode_roundtrip<T: HotKv>(hot_kv: &T) {
    let code = Bytes::from_static(&[0x60, 0x00, 0x60, 0x00, 0xf3]); // Simple EVM bytecode
    let bytecode = Bytecode::new_raw(code);
    let code_hash = bytecode.hash_slow();

    // Write bytecode
    {
        let writer = hot_kv.writer().unwrap();
        writer.put_bytecode(&code_hash, &bytecode).unwrap();
        writer.commit().unwrap();
    }

    // Read bytecode
    {
        let reader = hot_kv.reader().unwrap();
        let read_bytecode = reader.get_bytecode(&code_hash).unwrap();
        assert!(read_bytecode.is_some());
    }
}

/// Test account history via HotHistoryWrite/HotHistoryRead
pub fn test_account_history<T: HotKv>(hot_kv: &T) {
    let addr = address!("0x1111111111111111111111111111111111111111");
    let touched_blocks = BlockNumberList::new([10, 20, 30]).unwrap();
    let latest_height = 100u64;

    // Write account history
    {
        let writer = hot_kv.writer().unwrap();
        writer.write_account_history(&addr, latest_height, &touched_blocks).unwrap();
        writer.commit().unwrap();
    }

    // Read account history
    {
        let reader = hot_kv.reader().unwrap();
        let read_history = reader.get_account_history(&addr, latest_height).unwrap();
        assert!(read_history.is_some());
        let history = read_history.unwrap();
        assert_eq!(history.iter().collect::<Vec<_>>(), vec![10, 20, 30]);
    }
}

/// Test storage history via HotHistoryWrite/HotHistoryRead
pub fn test_storage_history<T: HotKv>(hot_kv: &T) {
    let addr = address!("0x2222222222222222222222222222222222222222");
    let slot = U256::from(42);
    let touched_blocks = BlockNumberList::new([5, 15, 25]).unwrap();
    let highest_block = 50u64;

    // Write storage history
    {
        let writer = hot_kv.writer().unwrap();
        writer.write_storage_history(&addr, slot, highest_block, &touched_blocks).unwrap();
        writer.commit().unwrap();
    }

    // Read storage history
    {
        let reader = hot_kv.reader().unwrap();
        let read_history = reader.get_storage_history(&addr, slot, highest_block).unwrap();
        assert!(read_history.is_some());
        let history = read_history.unwrap();
        assert_eq!(history.iter().collect::<Vec<_>>(), vec![5, 15, 25]);
    }
}

/// Test account change sets via HotHistoryWrite/HotHistoryRead
pub fn test_account_changes<T: HotKv>(hot_kv: &T) {
    let addr = address!("0x3333333333333333333333333333333333333333");
    let pre_state = Account { nonce: 10, balance: U256::from(5000), bytecode_hash: None };
    let block_number = 100u64;

    // Write account change
    {
        let writer = hot_kv.writer().unwrap();
        writer.write_account_prestate(block_number, addr, &pre_state).unwrap();
        writer.commit().unwrap();
    }

    // Read account change
    {
        let reader = hot_kv.reader().unwrap();

        let read_change = reader.get_account_change(block_number, &addr).unwrap();

        assert!(read_change.is_some());
        let change = read_change.unwrap();
        assert_eq!(change.nonce, 10);
        assert_eq!(change.balance, U256::from(5000));
    }
}

/// Test storage change sets via HotHistoryWrite/HotHistoryRead
pub fn test_storage_changes<T: HotKv>(hot_kv: &T) {
    let addr = address!("0x4444444444444444444444444444444444444444");
    let slot = U256::from(153);
    let pre_value = U256::from(12345);
    let block_number = 200u64;

    // Write storage change
    {
        let writer = hot_kv.writer().unwrap();
        writer.write_storage_prestate(block_number, addr, &slot, &pre_value).unwrap();
        writer.commit().unwrap();
    }

    // Read storage change
    {
        let reader = hot_kv.reader().unwrap();
        let read_change = reader.get_storage_change(block_number, &addr, &slot).unwrap();
        assert!(read_change.is_some());
        assert_eq!(read_change.unwrap(), U256::from(12345));
    }
}

/// Test that missing reads return None
pub fn test_missing_reads<T: HotKv>(hot_kv: &T) {
    let missing_addr = address!("0x9999999999999999999999999999999999999999");
    let missing_hash = b256!("0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff");
    let missing_slot = U256::from(99999);

    let reader = hot_kv.reader().unwrap();

    // Missing header
    assert!(reader.get_header(999999).unwrap().is_none());

    // Missing header number
    assert!(reader.get_header_number(&missing_hash).unwrap().is_none());

    // Missing account
    assert!(reader.get_account(&missing_addr).unwrap().is_none());

    // Missing storage
    assert!(reader.get_storage(&missing_addr, &missing_slot).unwrap().is_none());

    // Missing bytecode
    assert!(reader.get_bytecode(&missing_hash).unwrap().is_none());

    // Missing header by hash
    assert!(reader.header_by_hash(&missing_hash).unwrap().is_none());

    // Missing account history
    assert!(reader.get_account_history(&missing_addr, 1000).unwrap().is_none());

    // Missing storage history
    assert!(reader.get_storage_history(&missing_addr, missing_slot, 1000).unwrap().is_none());

    // Missing account change
    assert!(reader.get_account_change(999999, &missing_addr).unwrap().is_none());

    // Missing storage change
    assert!(reader.get_storage_change(999999, &missing_addr, &missing_slot).unwrap().is_none());
}
