//! History and change set tests for hot storage.

use crate::{
    db::{HistoryRead, HistoryWrite, UnsafeDbWrite},
    model::HotKv,
};
use alloy::primitives::{U256, address};
use signet_storage_types::{Account, BlockNumberList};

/// Test update_history_indices for account history.
///
/// This test verifies that:
/// 1. Account change sets are correctly indexed into account history
/// 2. Appending to existing history works correctly
pub fn test_update_history_indices_account<T: HotKv>(hot_kv: &T)
where
    T::RwTx: HistoryWrite,
{
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

    // Phase 2: Run update_history_indices for blocks 1-3
    {
        let writer = hot_kv.writer().unwrap();
        writer.update_history_indices(1..=3).unwrap();
        writer.commit().unwrap();
    }

    // Phase 3: Verify account history was created correctly
    {
        let reader = hot_kv.reader().unwrap();

        // addr1 should have history at blocks 1, 2
        let history1 =
            reader.blocks_changed_account(&addr1).unwrap().expect("addr1 should have history");
        let blocks1: Vec<u64> = history1.iter().collect();
        assert_eq!(blocks1, vec![1, 2], "addr1 history mismatch");

        // addr2 should have history at blocks 2, 3
        let history2 =
            reader.blocks_changed_account(&addr2).unwrap().expect("addr2 should have history");
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

    // Phase 5: Run update_history_indices for blocks 4-5
    {
        let writer = hot_kv.writer().unwrap();
        writer.update_history_indices(4..=5).unwrap();
        writer.commit().unwrap();
    }

    // Phase 6: Verify history was appended correctly
    {
        let reader = hot_kv.reader().unwrap();

        // addr1 should now have history at blocks 1, 2, 4, 5
        let history1 =
            reader.blocks_changed_account(&addr1).unwrap().expect("addr1 should have history");
        let blocks1: Vec<u64> = history1.iter().collect();
        assert_eq!(blocks1, vec![1, 2, 4, 5], "addr1 history mismatch after append");

        // addr2 should still have history at blocks 2, 3 (unchanged)
        let history2 =
            reader.blocks_changed_account(&addr2).unwrap().expect("addr2 should have history");
        let blocks2: Vec<u64> = history2.iter().collect();
        assert_eq!(blocks2, vec![2, 3], "addr2 history should be unchanged");
    }
}

/// Test update_history_indices for storage history.
///
/// This test verifies that:
/// 1. Storage change sets are correctly indexed into storage history
/// 2. Appending to existing history works correctly
/// 3. Different slots for the same address are tracked separately
pub fn test_update_history_indices_storage<T: HotKv>(hot_kv: &T)
where
    T::RwTx: HistoryWrite,
{
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

    // Phase 2: Run update_history_indices for blocks 1-3
    {
        let writer = hot_kv.writer().unwrap();
        writer.update_history_indices(1..=3).unwrap();
        writer.commit().unwrap();
    }

    // Phase 3: Verify storage history was created correctly
    {
        let reader = hot_kv.reader().unwrap();

        // addr1.slot1 should have history at blocks 1, 2
        let history1 = reader
            .blocks_changed_storage(&addr1, &slot1)
            .unwrap()
            .expect("addr1.slot1 should have history");
        let blocks1: Vec<u64> = history1.iter().collect();
        assert_eq!(blocks1, vec![1, 2], "addr1.slot1 history mismatch");

        // addr1.slot2 should have history at blocks 2, 3
        let history2 = reader
            .blocks_changed_storage(&addr1, &slot2)
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

    // Phase 5: Run update_history_indices for blocks 4-5
    {
        let writer = hot_kv.writer().unwrap();
        writer.update_history_indices(4..=5).unwrap();
        writer.commit().unwrap();
    }

    // Phase 6: Verify history was appended correctly
    {
        let reader = hot_kv.reader().unwrap();

        // addr1.slot1 should now have history at blocks 1, 2, 4, 5
        let history1 = reader
            .blocks_changed_storage(&addr1, &slot1)
            .unwrap()
            .expect("addr1.slot1 should have history");
        let blocks1: Vec<u64> = history1.iter().collect();
        assert_eq!(blocks1, vec![1, 2, 4, 5], "addr1.slot1 history mismatch after append");

        // addr1.slot2 should still have history at blocks 2, 3 (unchanged)
        let history2 = reader
            .blocks_changed_storage(&addr1, &slot2)
            .unwrap()
            .expect("addr1.slot2 should have history");
        let blocks2: Vec<u64> = history2.iter().collect();
        assert_eq!(blocks2, vec![2, 3], "addr1.slot2 history should be unchanged");
    }
}

/// Test that appending to history correctly merges blocks.
///
/// This test verifies that after appending an initial list and then a new
/// block via `update_history_indices`, `blocks_changed_account` returns the
/// expected union of all blocks.
pub fn test_history_append_removes_old_entries<T: HotKv>(hot_kv: &T)
where
    T::RwTx: HistoryWrite,
{
    let addr = address!("0xdddddddddddddddddddddddddddddddddddddddd");

    // Phase 1: Append account history for blocks 10, 20, 30
    {
        let writer = hot_kv.writer().unwrap();
        let initial_history = BlockNumberList::new([10, 20, 30]).unwrap();
        writer.append_account_history(&addr, &initial_history).unwrap();
        writer.commit().unwrap();
    }

    // Verify initial state
    {
        let reader = hot_kv.reader().unwrap();
        let history = reader.blocks_changed_account(&addr).unwrap().expect("should have history");
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

    // Phase 3: Run update_history_indices
    {
        let writer = hot_kv.writer().unwrap();
        writer.update_history_indices(40..=40).unwrap();
        writer.commit().unwrap();
    }

    // Phase 4: Verify history was correctly appended — union is [10, 20, 30, 40]
    {
        let reader = hot_kv.reader().unwrap();
        let history = reader.blocks_changed_account(&addr).unwrap().expect("should have history");
        let blocks: Vec<u64> = history.iter().collect();
        assert_eq!(blocks, vec![10, 20, 30, 40], "history should include appended block");
    }
}

/// Test that truncating account history removes only blocks above the given
/// height and leaves other addresses intact.
///
/// This test verifies that:
/// 1. Appending two disjoint sets of blocks for the same address works
/// 2. `truncate_account_history_above` removes blocks above the cutoff
/// 3. Other addresses are not affected
pub fn test_delete_dual_account_history<T: HotKv>(hot_kv: &T)
where
    T::RwTx: HistoryWrite,
{
    let addr1 = address!("0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee");
    let addr2 = address!("0xffffffffffffffffffffffffffffffffffffffff");

    // Phase 1: Append history for addr1 ([1,2,3] then [4,5,6]) and addr2 ([10,20,30])
    {
        let writer = hot_kv.writer().unwrap();

        let history1_a = BlockNumberList::new([1, 2, 3]).unwrap();
        writer.append_account_history(&addr1, &history1_a).unwrap();

        let history1_b = BlockNumberList::new([4, 5, 6]).unwrap();
        writer.append_account_history(&addr1, &history1_b).unwrap();

        let history2 = BlockNumberList::new([10, 20, 30]).unwrap();
        writer.append_account_history(&addr2, &history2).unwrap();

        writer.commit().unwrap();
    }

    // Phase 2: Verify the full logical union for addr1 is [1,2,3,4,5,6]
    {
        let reader = hot_kv.reader().unwrap();

        let hist1 =
            reader.blocks_changed_account(&addr1).unwrap().expect("addr1 should have history");
        assert_eq!(hist1.iter().collect::<Vec<_>>(), vec![1, 2, 3, 4, 5, 6]);

        let hist2 =
            reader.blocks_changed_account(&addr2).unwrap().expect("addr2 should have history");
        assert_eq!(hist2.iter().collect::<Vec<_>>(), vec![10, 20, 30]);
    }

    // Phase 3: Truncate addr1's history above block 3
    {
        let writer = hot_kv.writer().unwrap();
        writer.truncate_account_history_above(&addr1, 3).unwrap();
        writer.commit().unwrap();
    }

    // Phase 4: Verify only blocks <= 3 remain for addr1; addr2 is unaffected
    {
        let reader = hot_kv.reader().unwrap();

        let hist1 = reader
            .blocks_changed_account(&addr1)
            .unwrap()
            .expect("addr1 should still have history after truncation");
        assert_eq!(hist1.iter().collect::<Vec<_>>(), vec![1, 2, 3]);

        let hist2 =
            reader.blocks_changed_account(&addr2).unwrap().expect("addr2 should be unaffected");
        assert_eq!(hist2.iter().collect::<Vec<_>>(), vec![10, 20, 30]);
    }
}

/// Test that truncating storage history removes only the targeted slot's
/// blocks and leaves other slots intact.
///
/// This test verifies that:
/// 1. Appending storage history for two slots works correctly
/// 2. `truncate_storage_history_above(addr, slot1, 0)` removes all blocks for slot1
/// 3. Other slots for the same address are not affected
pub fn test_delete_dual_storage_history<T: HotKv>(hot_kv: &T)
where
    T::RwTx: HistoryWrite,
{
    let addr = address!("0x1111111111111111111111111111111111111111");
    let slot1 = U256::from(100);
    let slot2 = U256::from(200);

    // Phase 1: Append storage history for both slots
    {
        let writer = hot_kv.writer().unwrap();

        let history1 = BlockNumberList::new([1, 2, 3]).unwrap();
        writer.append_storage_history(&addr, &slot1, &history1).unwrap();

        let history2 = BlockNumberList::new([10, 20, 30]).unwrap();
        writer.append_storage_history(&addr, &slot2, &history2).unwrap();

        writer.commit().unwrap();
    }

    // Phase 2: Verify both slots have history
    {
        let reader = hot_kv.reader().unwrap();

        let hist1 = reader
            .blocks_changed_storage(&addr, &slot1)
            .unwrap()
            .expect("slot1 should have history");
        assert_eq!(hist1.iter().collect::<Vec<_>>(), vec![1, 2, 3]);

        let hist2 = reader
            .blocks_changed_storage(&addr, &slot2)
            .unwrap()
            .expect("slot2 should have history");
        assert_eq!(hist2.iter().collect::<Vec<_>>(), vec![10, 20, 30]);
    }

    // Phase 3: Remove all blocks for slot1 by truncating above 0
    // (all test blocks are > 0, so nothing is kept)
    {
        let writer = hot_kv.writer().unwrap();
        writer.truncate_storage_history_above(&addr, &slot1, 0).unwrap();
        writer.commit().unwrap();
    }

    // Phase 4: Verify slot1 is gone; slot2 is unaffected
    {
        let reader = hot_kv.reader().unwrap();

        let hist1 = reader.blocks_changed_storage(&addr, &slot1).unwrap();
        assert!(hist1.is_none(), "slot1 history should be gone after truncation");

        let hist2 = reader
            .blocks_changed_storage(&addr, &slot2)
            .unwrap()
            .expect("slot2 should be unaffected");
        assert_eq!(hist2.iter().collect::<Vec<_>>(), vec![10, 20, 30]);
    }
}

/// Test deleting and re-adding account history entries.
///
/// This test verifies that after truncating all history for an address, we
/// can append new blocks and read them back correctly.
pub fn test_delete_and_rewrite_dual<T: HotKv>(hot_kv: &T)
where
    T::RwTx: HistoryWrite,
{
    let addr = address!("0x2222222222222222222222222222222222222222");

    // Phase 1: Append initial history [1, 2, 3]
    {
        let writer = hot_kv.writer().unwrap();
        let history = BlockNumberList::new([1, 2, 3]).unwrap();
        writer.append_account_history(&addr, &history).unwrap();
        writer.commit().unwrap();
    }

    // Verify initial state
    {
        let reader = hot_kv.reader().unwrap();
        let hist = reader.blocks_changed_account(&addr).unwrap().expect("should have history");
        assert_eq!(hist.iter().collect::<Vec<_>>(), vec![1, 2, 3]);
    }

    // Phase 2: Remove all history by truncating above 0
    // (all test blocks are > 0, so nothing is kept)
    {
        let writer = hot_kv.writer().unwrap();
        writer.truncate_account_history_above(&addr, 0).unwrap();
        writer.commit().unwrap();
    }

    // Verify deleted
    {
        let reader = hot_kv.reader().unwrap();
        let hist = reader.blocks_changed_account(&addr).unwrap();
        assert!(hist.is_none(), "history should be empty after truncation");
    }

    // Phase 3: Append new history [100, 200, 300]
    {
        let writer = hot_kv.writer().unwrap();
        let new_history = BlockNumberList::new([100, 200, 300]).unwrap();
        writer.append_account_history(&addr, &new_history).unwrap();
        writer.commit().unwrap();
    }

    // Verify new value
    {
        let reader = hot_kv.reader().unwrap();
        let hist = reader.blocks_changed_account(&addr).unwrap().expect("new history should exist");
        assert_eq!(hist.iter().collect::<Vec<_>>(), vec![100, 200, 300]);
    }
}
