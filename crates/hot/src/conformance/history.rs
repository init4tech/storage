//! History and change set tests for hot storage.

use crate::{
    db::{HistoryRead, UnsafeDbWrite, UnsafeHistoryWrite},
    model::{HotKv, HotKvWrite},
    tables,
};
use alloy::primitives::{U256, address};
use signet_storage_types::{Account, BlockNumberList, ShardedKey};

/// Test update_history_indices_inconsistent for account history.
///
/// This test verifies that:
/// 1. Account change sets are correctly indexed into account history
/// 2. Appending to existing history works correctly
/// 3. Old shards are deleted when appending
pub fn test_update_history_indices_account<T: HotKv>(hot_kv: &T) {
    let addr1 = address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    let addr2 = address!("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");

    // Phase 1: Write account change sets for blocks 1-3
    {
        let writer = hot_kv.writer().unwrap();

        // Block 1: addr1 changed
        let pre_acc = Account::default();
        writer.write_account_prestate(1, addr1, &pre_acc).unwrap();

        // Block 2: addr1 and addr2 changed
        let acc1 = Account { nonce: 1, balance: U256::from(100), bytecode_hash: None };
        writer.write_account_prestate(2, addr1, &acc1).unwrap();
        writer.write_account_prestate(2, addr2, &pre_acc).unwrap();

        // Block 3: addr2 changed
        let acc2 = Account { nonce: 1, balance: U256::from(200), bytecode_hash: None };
        writer.write_account_prestate(3, addr2, &acc2).unwrap();

        writer.commit().unwrap();
    }

    // Phase 2: Run update_history_indices_inconsistent for blocks 1-3
    {
        let writer = hot_kv.writer().unwrap();
        writer.update_history_indices_inconsistent(1..=3).unwrap();
        writer.commit().unwrap();
    }

    // Phase 3: Verify account history was created correctly
    {
        let reader = hot_kv.reader().unwrap();

        // addr1 should have history at blocks 1, 2
        let (_, history1) =
            reader.last_account_history(addr1).unwrap().expect("addr1 should have history");
        let blocks1: Vec<u64> = history1.iter().collect();
        assert_eq!(blocks1, vec![1, 2], "addr1 history mismatch");

        // addr2 should have history at blocks 2, 3
        let (_, history2) =
            reader.last_account_history(addr2).unwrap().expect("addr2 should have history");
        let blocks2: Vec<u64> = history2.iter().collect();
        assert_eq!(blocks2, vec![2, 3], "addr2 history mismatch");
    }

    // Phase 4: Write more change sets for blocks 4-5
    {
        let writer = hot_kv.writer().unwrap();

        // Block 4: addr1 changed
        let acc1 = Account { nonce: 2, balance: U256::from(300), bytecode_hash: None };
        writer.write_account_prestate(4, addr1, &acc1).unwrap();

        // Block 5: addr1 changed again
        let acc1_v2 = Account { nonce: 3, balance: U256::from(400), bytecode_hash: None };
        writer.write_account_prestate(5, addr1, &acc1_v2).unwrap();

        writer.commit().unwrap();
    }

    // Phase 5: Run update_history_indices_inconsistent for blocks 4-5
    {
        let writer = hot_kv.writer().unwrap();
        writer.update_history_indices_inconsistent(4..=5).unwrap();
        writer.commit().unwrap();
    }

    // Phase 6: Verify history was appended correctly
    {
        let reader = hot_kv.reader().unwrap();

        // addr1 should now have history at blocks 1, 2, 4, 5
        let (_, history1) =
            reader.last_account_history(addr1).unwrap().expect("addr1 should have history");
        let blocks1: Vec<u64> = history1.iter().collect();
        assert_eq!(blocks1, vec![1, 2, 4, 5], "addr1 history mismatch after append");

        // addr2 should still have history at blocks 2, 3 (unchanged)
        let (_, history2) =
            reader.last_account_history(addr2).unwrap().expect("addr2 should have history");
        let blocks2: Vec<u64> = history2.iter().collect();
        assert_eq!(blocks2, vec![2, 3], "addr2 history should be unchanged");
    }
}

/// Test update_history_indices_inconsistent for storage history.
///
/// This test verifies that:
/// 1. Storage change sets are correctly indexed into storage history
/// 2. Appending to existing history works correctly
/// 3. Old shards are deleted when appending
/// 4. Different slots for the same address are tracked separately
pub fn test_update_history_indices_storage<T: HotKv>(hot_kv: &T) {
    let addr1 = address!("0xcccccccccccccccccccccccccccccccccccccccc");
    let slot1 = U256::from(1);
    let slot2 = U256::from(2);

    // Phase 1: Write storage change sets for blocks 1-3
    {
        let writer = hot_kv.writer().unwrap();

        // Block 1: addr1.slot1 changed
        writer.write_storage_prestate(1, addr1, &slot1, &U256::ZERO).unwrap();

        // Block 2: addr1.slot1 and addr1.slot2 changed
        writer.write_storage_prestate(2, addr1, &slot1, &U256::from(100)).unwrap();
        writer.write_storage_prestate(2, addr1, &slot2, &U256::ZERO).unwrap();

        // Block 3: addr1.slot2 changed
        writer.write_storage_prestate(3, addr1, &slot2, &U256::from(200)).unwrap();

        writer.commit().unwrap();
    }

    // Phase 2: Run update_history_indices_inconsistent for blocks 1-3
    {
        let writer = hot_kv.writer().unwrap();
        writer.update_history_indices_inconsistent(1..=3).unwrap();
        writer.commit().unwrap();
    }

    // Phase 3: Verify storage history was created correctly
    {
        let reader = hot_kv.reader().unwrap();

        // addr1.slot1 should have history at blocks 1, 2
        let (_, history1) = reader
            .last_storage_history(&addr1, &slot1)
            .unwrap()
            .expect("addr1.slot1 should have history");
        let blocks1: Vec<u64> = history1.iter().collect();
        assert_eq!(blocks1, vec![1, 2], "addr1.slot1 history mismatch");

        // addr1.slot2 should have history at blocks 2, 3
        let (_, history2) = reader
            .last_storage_history(&addr1, &slot2)
            .unwrap()
            .expect("addr1.slot2 should have history");
        let blocks2: Vec<u64> = history2.iter().collect();
        assert_eq!(blocks2, vec![2, 3], "addr1.slot2 history mismatch");
    }

    // Phase 4: Write more change sets for blocks 4-5
    {
        let writer = hot_kv.writer().unwrap();

        // Block 4: addr1.slot1 changed
        writer.write_storage_prestate(4, addr1, &slot1, &U256::from(300)).unwrap();

        // Block 5: addr1.slot1 changed again
        writer.write_storage_prestate(5, addr1, &slot1, &U256::from(400)).unwrap();

        writer.commit().unwrap();
    }

    // Phase 5: Run update_history_indices_inconsistent for blocks 4-5
    {
        let writer = hot_kv.writer().unwrap();
        writer.update_history_indices_inconsistent(4..=5).unwrap();
        writer.commit().unwrap();
    }

    // Phase 6: Verify history was appended correctly
    {
        let reader = hot_kv.reader().unwrap();

        // addr1.slot1 should now have history at blocks 1, 2, 4, 5
        let (_, history1) = reader
            .last_storage_history(&addr1, &slot1)
            .unwrap()
            .expect("addr1.slot1 should have history");
        let blocks1: Vec<u64> = history1.iter().collect();
        assert_eq!(blocks1, vec![1, 2, 4, 5], "addr1.slot1 history mismatch after append");

        // addr1.slot2 should still have history at blocks 2, 3 (unchanged)
        let (_, history2) = reader
            .last_storage_history(&addr1, &slot2)
            .unwrap()
            .expect("addr1.slot2 should have history");
        let blocks2: Vec<u64> = history2.iter().collect();
        assert_eq!(blocks2, vec![2, 3], "addr1.slot2 history should be unchanged");
    }
}

/// Test that appending to history correctly removes old entries at same k1,k2.
///
/// This test specifically verifies that when we append new indices to an existing
/// shard, the old shard is properly deleted so we don't end up with duplicate data.
pub fn test_history_append_removes_old_entries<T: HotKv>(hot_kv: &T) {
    let addr = address!("0xdddddddddddddddddddddddddddddddddddddddd");

    // Phase 1: Manually write account history
    {
        let writer = hot_kv.writer().unwrap();
        let initial_history = BlockNumberList::new([10, 20, 30]).unwrap();
        writer.write_account_history(&addr, u64::MAX, &initial_history).unwrap();
        writer.commit().unwrap();
    }

    // Verify initial state
    {
        let reader = hot_kv.reader().unwrap();
        let (key, history) =
            reader.last_account_history(addr).unwrap().expect("should have history");
        assert_eq!(key, u64::MAX);
        let blocks: Vec<u64> = history.iter().collect();
        assert_eq!(blocks, vec![10, 20, 30]);
    }

    // Phase 2: Write account change set for block 40
    {
        let writer = hot_kv.writer().unwrap();
        let acc = Account { nonce: 1, balance: U256::from(100), bytecode_hash: None };
        writer.write_account_prestate(40, addr, &acc).unwrap();
        writer.commit().unwrap();
    }

    // Phase 3: Run update_history_indices_inconsistent
    {
        let writer = hot_kv.writer().unwrap();
        writer.update_history_indices_inconsistent(40..=40).unwrap();
        writer.commit().unwrap();
    }

    // Phase 4: Verify history was correctly appended
    {
        let reader = hot_kv.reader().unwrap();
        let (key, history) =
            reader.last_account_history(addr).unwrap().expect("should have history");
        assert_eq!(key, u64::MAX, "key should still be u64::MAX");
        let blocks: Vec<u64> = history.iter().collect();
        assert_eq!(blocks, vec![10, 20, 30, 40], "history should include appended block");
    }
}

/// Test deleting dual-keyed account history entries.
///
/// This test verifies that:
/// 1. Writing dual-keyed entries works correctly
/// 2. Deleting specific dual-keyed entries removes only that entry
/// 3. Other entries for the same k1 remain intact
/// 4. Traversal after deletion shows the entry is gone
pub fn test_delete_dual_account_history<T: HotKv>(hot_kv: &T) {
    let addr1 = address!("0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee");
    let addr2 = address!("0xffffffffffffffffffffffffffffffffffffffff");

    // Phase 1: Write account history entries for multiple addresses
    {
        let writer = hot_kv.writer().unwrap();

        // Write history for addr1 at two different shard keys
        let history1_a = BlockNumberList::new([1, 2, 3]).unwrap();
        let history1_b = BlockNumberList::new([4, 5, 6]).unwrap();
        writer.write_account_history(&addr1, 100, &history1_a).unwrap();
        writer.write_account_history(&addr1, u64::MAX, &history1_b).unwrap();

        // Write history for addr2
        let history2 = BlockNumberList::new([10, 20, 30]).unwrap();
        writer.write_account_history(&addr2, u64::MAX, &history2).unwrap();

        writer.commit().unwrap();
    }

    // Phase 2: Verify all entries exist
    {
        let reader = hot_kv.reader().unwrap();

        // Check addr1 entries
        let hist1_a = reader.get_account_history(&addr1, 100).unwrap();
        assert!(hist1_a.is_some(), "addr1 shard 100 should exist");
        assert_eq!(hist1_a.unwrap().iter().collect::<Vec<_>>(), vec![1, 2, 3]);

        let hist1_b = reader.get_account_history(&addr1, u64::MAX).unwrap();
        assert!(hist1_b.is_some(), "addr1 shard u64::MAX should exist");
        assert_eq!(hist1_b.unwrap().iter().collect::<Vec<_>>(), vec![4, 5, 6]);

        // Check addr2 entry
        let hist2 = reader.get_account_history(&addr2, u64::MAX).unwrap();
        assert!(hist2.is_some(), "addr2 should exist");
        assert_eq!(hist2.unwrap().iter().collect::<Vec<_>>(), vec![10, 20, 30]);
    }

    // Phase 3: Delete addr1's u64::MAX entry
    {
        let writer = hot_kv.writer().unwrap();
        writer.queue_delete_dual::<tables::AccountsHistory>(&addr1, &u64::MAX).unwrap();
        writer.commit().unwrap();
    }

    // Phase 4: Verify only the deleted entry is gone
    {
        let reader = hot_kv.reader().unwrap();

        // addr1 shard 100 should still exist
        let hist1_a = reader.get_account_history(&addr1, 100).unwrap();
        assert!(hist1_a.is_some(), "addr1 shard 100 should still exist after delete");
        assert_eq!(hist1_a.unwrap().iter().collect::<Vec<_>>(), vec![1, 2, 3]);

        // addr1 shard u64::MAX should be gone
        let hist1_b = reader.get_account_history(&addr1, u64::MAX).unwrap();
        assert!(hist1_b.is_none(), "addr1 shard u64::MAX should be deleted");

        // addr2 should be unaffected
        let hist2 = reader.get_account_history(&addr2, u64::MAX).unwrap();
        assert!(hist2.is_some(), "addr2 should be unaffected by delete");
        assert_eq!(hist2.unwrap().iter().collect::<Vec<_>>(), vec![10, 20, 30]);

        // Verify last_account_history now returns shard 100 for addr1
        let (key, _) =
            reader.last_account_history(addr1).unwrap().expect("addr1 should still have history");
        assert_eq!(key, 100, "last shard for addr1 should now be 100");
    }
}

/// Test deleting dual-keyed storage history entries.
///
/// This test verifies that:
/// 1. Writing storage history entries works correctly
/// 2. Deleting specific (address, slot, shard) entries removes only that entry
/// 3. Other slots for the same address remain intact
/// 4. Traversal after deletion shows the entry is gone
pub fn test_delete_dual_storage_history<T: HotKv>(hot_kv: &T) {
    let addr = address!("0x1111111111111111111111111111111111111111");
    let slot1 = U256::from(100);
    let slot2 = U256::from(200);

    // Phase 1: Write storage history entries for multiple slots
    {
        let writer = hot_kv.writer().unwrap();

        // Write history for slot1
        let history1 = BlockNumberList::new([1, 2, 3]).unwrap();
        writer.write_storage_history(&addr, slot1, u64::MAX, &history1).unwrap();

        // Write history for slot2
        let history2 = BlockNumberList::new([10, 20, 30]).unwrap();
        writer.write_storage_history(&addr, slot2, u64::MAX, &history2).unwrap();

        writer.commit().unwrap();
    }

    // Phase 2: Verify both entries exist
    {
        let reader = hot_kv.reader().unwrap();

        let hist1 = reader.get_storage_history(&addr, slot1, u64::MAX).unwrap();
        assert!(hist1.is_some(), "slot1 should exist");
        assert_eq!(hist1.unwrap().iter().collect::<Vec<_>>(), vec![1, 2, 3]);

        let hist2 = reader.get_storage_history(&addr, slot2, u64::MAX).unwrap();
        assert!(hist2.is_some(), "slot2 should exist");
        assert_eq!(hist2.unwrap().iter().collect::<Vec<_>>(), vec![10, 20, 30]);
    }

    // Phase 3: Delete slot1's entry
    {
        let writer = hot_kv.writer().unwrap();
        let key_to_delete = ShardedKey::new(slot1, u64::MAX);
        writer.queue_delete_dual::<tables::StorageHistory>(&addr, &key_to_delete).unwrap();
        writer.commit().unwrap();
    }

    // Phase 4: Verify only slot1 is gone
    {
        let reader = hot_kv.reader().unwrap();

        // slot1 should be gone
        let hist1 = reader.get_storage_history(&addr, slot1, u64::MAX).unwrap();
        assert!(hist1.is_none(), "slot1 should be deleted");

        // slot2 should be unaffected
        let hist2 = reader.get_storage_history(&addr, slot2, u64::MAX).unwrap();
        assert!(hist2.is_some(), "slot2 should be unaffected");
        assert_eq!(hist2.unwrap().iter().collect::<Vec<_>>(), vec![10, 20, 30]);

        // last_storage_history for slot1 should return None
        let last1 = reader.last_storage_history(&addr, &slot1).unwrap();
        assert!(last1.is_none(), "last_storage_history for slot1 should return None");

        // last_storage_history for slot2 should still work
        let last2 = reader.last_storage_history(&addr, &slot2).unwrap();
        assert!(last2.is_some(), "last_storage_history for slot2 should still work");
    }
}

/// Test deleting and re-adding dual-keyed entries.
///
/// This test verifies that after deleting an entry, we can write a new entry
/// with the same key and it works correctly.
pub fn test_delete_and_rewrite_dual<T: HotKv>(hot_kv: &T) {
    let addr = address!("0x2222222222222222222222222222222222222222");

    // Phase 1: Write initial entry
    {
        let writer = hot_kv.writer().unwrap();
        let history = BlockNumberList::new([1, 2, 3]).unwrap();
        writer.write_account_history(&addr, u64::MAX, &history).unwrap();
        writer.commit().unwrap();
    }

    // Verify initial state
    {
        let reader = hot_kv.reader().unwrap();
        let hist = reader.get_account_history(&addr, u64::MAX).unwrap();
        assert_eq!(hist.unwrap().iter().collect::<Vec<_>>(), vec![1, 2, 3]);
    }

    // Phase 2: Delete the entry
    {
        let writer = hot_kv.writer().unwrap();
        writer.queue_delete_dual::<tables::AccountsHistory>(&addr, &u64::MAX).unwrap();
        writer.commit().unwrap();
    }

    // Verify deleted
    {
        let reader = hot_kv.reader().unwrap();
        let hist = reader.get_account_history(&addr, u64::MAX).unwrap();
        assert!(hist.is_none(), "entry should be deleted");
    }

    // Phase 3: Write new entry with same key but different value
    {
        let writer = hot_kv.writer().unwrap();
        let new_history = BlockNumberList::new([100, 200, 300]).unwrap();
        writer.write_account_history(&addr, u64::MAX, &new_history).unwrap();
        writer.commit().unwrap();
    }

    // Verify new value
    {
        let reader = hot_kv.reader().unwrap();
        let hist = reader.get_account_history(&addr, u64::MAX).unwrap();
        assert!(hist.is_some(), "new entry should exist");
        assert_eq!(hist.unwrap().iter().collect::<Vec<_>>(), vec![100, 200, 300]);
    }
}
