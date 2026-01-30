//! Clear/take range operations for single and dual-keyed tables.

use crate::{
    db::{HistoryRead, HotDbRead, UnsafeDbWrite, UnsafeHistoryWrite},
    model::{HotKv, HotKvWrite},
    tables,
};
use alloy::{
    consensus::Header,
    primitives::{U256, address},
};
use signet_storage_types::BlockNumberList;
use trevm::revm::database::states::PlainStorageChangeset;

/// Test clear_range on a single-keyed table.
///
/// This test verifies that:
/// 1. Keys within the range are deleted
/// 2. Keys outside the range remain intact
/// 3. Edge cases like adjacent keys and boundary conditions work correctly
pub fn test_clear_range<T: HotKv>(hot_kv: &T) {
    // Phase 1: Write 15 headers with block numbers 0-14
    {
        let writer = hot_kv.writer().unwrap();
        for i in 0u64..15 {
            let header = Header { number: i, gas_limit: 1_000_000, ..Default::default() };
            writer.put_header_inconsistent(&header).unwrap();
        }
        writer.commit().unwrap();
    }

    // Verify all headers exist
    {
        let reader = hot_kv.reader().unwrap();
        for i in 0u64..15 {
            assert!(reader.get_header(i).unwrap().is_some(), "header {} should exist", i);
        }
    }

    // Phase 2: Clear range 5..=9 (middle range)
    {
        let writer = hot_kv.writer().unwrap();
        writer.traverse_mut::<tables::Headers>().unwrap().delete_range_inclusive(5..=9).unwrap();
        writer.commit().unwrap();
    }

    // Verify: 0-4 and 10-14 should exist, 5-9 should be gone
    {
        let reader = hot_kv.reader().unwrap();

        // Keys before range should exist
        for i in 0u64..5 {
            assert!(reader.get_header(i).unwrap().is_some(), "header {} should still exist", i);
        }

        // Keys in range should be deleted
        for i in 5u64..10 {
            assert!(reader.get_header(i).unwrap().is_none(), "header {} should be deleted", i);
        }

        // Keys after range should exist
        for i in 10u64..15 {
            assert!(reader.get_header(i).unwrap().is_some(), "header {} should still exist", i);
        }
    }

    // Phase 3: Test corner case - clear adjacent keys at the boundary
    {
        let writer = hot_kv.writer().unwrap();
        // Clear keys 3 and 4 (adjacent to the already cleared range)
        writer.traverse_mut::<tables::Headers>().unwrap().delete_range_inclusive(3..=4).unwrap();
        writer.commit().unwrap();
    }

    // Verify: 0-2 and 10-14 should exist, 3-9 should be gone
    {
        let reader = hot_kv.reader().unwrap();

        // Keys 0-2 should exist
        for i in 0u64..3 {
            assert!(reader.get_header(i).unwrap().is_some(), "header {} should still exist", i);
        }

        // Keys 3-9 should all be deleted now
        for i in 3u64..10 {
            assert!(reader.get_header(i).unwrap().is_none(), "header {} should be deleted", i);
        }

        // Keys 10-14 should exist
        for i in 10u64..15 {
            assert!(reader.get_header(i).unwrap().is_some(), "header {} should still exist", i);
        }
    }

    // Phase 4: Test clearing a range that includes the first key
    {
        let writer = hot_kv.writer().unwrap();
        writer.traverse_mut::<tables::Headers>().unwrap().delete_range_inclusive(0..=1).unwrap();
        writer.commit().unwrap();
    }

    {
        let reader = hot_kv.reader().unwrap();
        assert!(reader.get_header(0).unwrap().is_none(), "header 0 should be deleted");
        assert!(reader.get_header(1).unwrap().is_none(), "header 1 should be deleted");
        assert!(reader.get_header(2).unwrap().is_some(), "header 2 should still exist");
    }

    // Phase 5: Test clearing a range that includes the last key
    {
        let writer = hot_kv.writer().unwrap();
        writer.traverse_mut::<tables::Headers>().unwrap().delete_range_inclusive(13..=14).unwrap();
        writer.commit().unwrap();
    }

    {
        let reader = hot_kv.reader().unwrap();
        assert!(reader.get_header(12).unwrap().is_some(), "header 12 should still exist");
        assert!(reader.get_header(13).unwrap().is_none(), "header 13 should be deleted");
        assert!(reader.get_header(14).unwrap().is_none(), "header 14 should be deleted");
    }

    // Phase 6: Test clearing a single key
    {
        let writer = hot_kv.writer().unwrap();
        writer.traverse_mut::<tables::Headers>().unwrap().delete_range_inclusive(11..=11).unwrap();
        writer.commit().unwrap();
    }

    {
        let reader = hot_kv.reader().unwrap();
        assert!(reader.get_header(10).unwrap().is_some(), "header 10 should still exist");
        assert!(reader.get_header(11).unwrap().is_none(), "header 11 should be deleted");
        assert!(reader.get_header(12).unwrap().is_some(), "header 12 should still exist");
    }

    // Phase 7: Test clearing a range where nothing exists (should be no-op)
    {
        let writer = hot_kv.writer().unwrap();
        writer
            .traverse_mut::<tables::Headers>()
            .unwrap()
            .delete_range_inclusive(100..=200)
            .unwrap();
        writer.commit().unwrap();
    }

    // Verify remaining keys are still intact
    {
        let reader = hot_kv.reader().unwrap();
        assert!(reader.get_header(2).unwrap().is_some(), "header 2 should still exist");
        assert!(reader.get_header(10).unwrap().is_some(), "header 10 should still exist");
        assert!(reader.get_header(12).unwrap().is_some(), "header 12 should still exist");
    }
}

/// Test take_range on a single-keyed table.
///
/// Similar to clear_range but also returns the removed keys.
pub fn test_take_range<T: HotKv>(hot_kv: &T) {
    let headers = (0..10u64)
        .map(|i| Header { number: i, gas_limit: 1_000_000, ..Default::default() })
        .collect::<Vec<_>>();

    // Phase 1: Write 10 headers with block numbers 0-9
    {
        let writer = hot_kv.writer().unwrap();
        for header in headers.iter() {
            writer.put_header_inconsistent(header).unwrap();
        }
        writer.commit().unwrap();
    }

    // Phase 2: Take range 3..=6 and verify returned keys
    {
        let writer = hot_kv.writer().unwrap();
        let removed = writer.traverse_mut::<tables::Headers>().unwrap().take_range(3..=6).unwrap();
        writer.commit().unwrap();

        // Should return keys 3, 4, 5, 6 in order
        assert_eq!(removed.len(), 4);

        for i in 0..4 {
            assert_eq!(removed[i].0, (i as u64) + 3);
            assert_eq!(&removed[i].1, &headers[i + 3]);
        }
    }

    // Verify the keys are actually removed
    {
        let reader = hot_kv.reader().unwrap();
        for i in 0u64..3 {
            assert!(reader.get_header(i).unwrap().is_some(), "header {} should exist", i);
        }
        for i in 3u64..7 {
            assert!(reader.get_header(i).unwrap().is_none(), "header {} should be gone", i);
        }
        for i in 7u64..10 {
            assert!(reader.get_header(i).unwrap().is_some(), "header {} should exist", i);
        }
    }

    // Phase 3: Take empty range (nothing to remove)
    {
        let writer = hot_kv.writer().unwrap();
        let removed =
            writer.traverse_mut::<tables::Headers>().unwrap().take_range(100..=200).unwrap();
        writer.commit().unwrap();

        assert!(removed.is_empty(), "should return empty vec for non-existent range");
    }

    // Phase 4: Take single key
    {
        let writer = hot_kv.writer().unwrap();
        let removed = writer.traverse_mut::<tables::Headers>().unwrap().take_range(8..=8).unwrap();
        writer.commit().unwrap();

        assert_eq!(removed.len(), 1);
        assert_eq!(removed[0].0, 8);
        assert_eq!(&removed[0].1, &headers[8]);
    }

    {
        let reader = hot_kv.reader().unwrap();
        assert!(reader.get_header(7).unwrap().is_some());
        assert!(reader.get_header(8).unwrap().is_none());
        assert!(reader.get_header(9).unwrap().is_some());
    }
}

/// Test clear_range_dual on a dual-keyed table.
///
/// This test verifies that:
/// 1. All k2 entries for k1 values within the range are deleted
/// 2. k1 values outside the range remain intact
/// 3. Edge cases work correctly
pub fn test_clear_range_dual<T: HotKv>(hot_kv: &T) {
    let addr1 = address!("0x1000000000000000000000000000000000000001");
    let addr2 = address!("0x2000000000000000000000000000000000000002");
    let addr3 = address!("0x3000000000000000000000000000000000000003");
    let addr4 = address!("0x4000000000000000000000000000000000000004");
    let addr5 = address!("0x5000000000000000000000000000000000000005");

    // Phase 1: Write account history entries for multiple addresses with multiple shards
    {
        let writer = hot_kv.writer().unwrap();

        // addr1: two shards
        let history1_a = BlockNumberList::new([1, 2, 3]).unwrap();
        let history1_b = BlockNumberList::new([4, 5, 6]).unwrap();
        writer.write_account_history(&addr1, 100, &history1_a).unwrap();
        writer.write_account_history(&addr1, u64::MAX, &history1_b).unwrap();

        // addr2: one shard
        let history2 = BlockNumberList::new([10, 20]).unwrap();
        writer.write_account_history(&addr2, u64::MAX, &history2).unwrap();

        // addr3: one shard
        let history3 = BlockNumberList::new([30, 40]).unwrap();
        writer.write_account_history(&addr3, u64::MAX, &history3).unwrap();

        // addr4: two shards
        let history4_a = BlockNumberList::new([50, 60]).unwrap();
        let history4_b = BlockNumberList::new([70, 80]).unwrap();
        writer.write_account_history(&addr4, 200, &history4_a).unwrap();
        writer.write_account_history(&addr4, u64::MAX, &history4_b).unwrap();

        // addr5: one shard
        let history5 = BlockNumberList::new([90, 100]).unwrap();
        writer.write_account_history(&addr5, u64::MAX, &history5).unwrap();

        writer.commit().unwrap();
    }

    // Verify all entries exist
    {
        let reader = hot_kv.reader().unwrap();
        assert!(reader.get_account_history(&addr1, 100).unwrap().is_some());
        assert!(reader.get_account_history(&addr1, u64::MAX).unwrap().is_some());
        assert!(reader.get_account_history(&addr2, u64::MAX).unwrap().is_some());
        assert!(reader.get_account_history(&addr3, u64::MAX).unwrap().is_some());
        assert!(reader.get_account_history(&addr4, 200).unwrap().is_some());
        assert!(reader.get_account_history(&addr4, u64::MAX).unwrap().is_some());
        assert!(reader.get_account_history(&addr5, u64::MAX).unwrap().is_some());
    }

    // Phase 2: Clear range addr2..=addr3 (middle range)
    {
        let writer = hot_kv.writer().unwrap();
        writer
            .traverse_dual_mut::<tables::AccountsHistory>()
            .unwrap()
            .delete_range((addr2, 0)..=(addr3, u64::MAX))
            .unwrap();
        writer.commit().unwrap();
    }

    // Verify: addr1 and addr4, addr5 should exist, addr2 and addr3 should be gone
    {
        let reader = hot_kv.reader().unwrap();

        // addr1 entries should still exist
        assert!(
            reader.get_account_history(&addr1, 100).unwrap().is_some(),
            "addr1 shard 100 should exist"
        );
        assert!(
            reader.get_account_history(&addr1, u64::MAX).unwrap().is_some(),
            "addr1 shard max should exist"
        );

        // addr2 and addr3 should be deleted
        assert!(
            reader.get_account_history(&addr2, u64::MAX).unwrap().is_none(),
            "addr2 should be deleted"
        );
        assert!(
            reader.get_account_history(&addr3, u64::MAX).unwrap().is_none(),
            "addr3 should be deleted"
        );

        // addr4 and addr5 entries should still exist
        assert!(
            reader.get_account_history(&addr4, 200).unwrap().is_some(),
            "addr4 shard 200 should exist"
        );
        assert!(
            reader.get_account_history(&addr4, u64::MAX).unwrap().is_some(),
            "addr4 shard max should exist"
        );
        assert!(
            reader.get_account_history(&addr5, u64::MAX).unwrap().is_some(),
            "addr5 should exist"
        );
    }
}

/// Test take_range_dual on a dual-keyed table.
///
/// Similar to clear_range_dual but also returns the removed (k1, k2) pairs.
pub fn test_take_range_dual<T: HotKv>(hot_kv: &T) {
    let addr1 = address!("0xa000000000000000000000000000000000000001");
    let addr2 = address!("0xb000000000000000000000000000000000000002");
    let addr3 = address!("0xc000000000000000000000000000000000000003");

    // Phase 1: Write account history entries
    {
        let writer = hot_kv.writer().unwrap();

        // addr1: two shards
        let history1_a = BlockNumberList::new([1, 2]).unwrap();
        let history1_b = BlockNumberList::new([3, 4]).unwrap();
        writer.write_account_history(&addr1, 50, &history1_a).unwrap();
        writer.write_account_history(&addr1, u64::MAX, &history1_b).unwrap();

        // addr2: one shard
        let history2 = BlockNumberList::new([10, 20]).unwrap();
        writer.write_account_history(&addr2, u64::MAX, &history2).unwrap();

        // addr3: one shard
        let history3 = BlockNumberList::new([30, 40]).unwrap();
        writer.write_account_history(&addr3, u64::MAX, &history3).unwrap();

        writer.commit().unwrap();
    }

    // Phase 2: Take range addr1..=addr2 and verify returned pairs
    {
        let writer = hot_kv.writer().unwrap();
        let removed = writer
            .traverse_dual_mut::<tables::AccountsHistory>()
            .unwrap()
            .take_range((addr1, 0)..=(addr2, u64::MAX))
            .unwrap();
        writer.commit().unwrap();

        // Should return (addr1, 50), (addr1, max), (addr2, max)
        assert_eq!(removed.len(), 3, "should have removed 3 entries");
        assert_eq!(removed[0].0, addr1);
        assert_eq!(removed[0].1, 50);
        assert_eq!(removed[1].0, addr1);
        assert_eq!(removed[1].1, u64::MAX);
        assert_eq!(removed[2].0, addr2);
        assert_eq!(removed[2].1, u64::MAX);
    }

    // Verify only addr3 remains
    {
        let reader = hot_kv.reader().unwrap();
        assert!(reader.get_account_history(&addr1, 50).unwrap().is_none());
        assert!(reader.get_account_history(&addr1, u64::MAX).unwrap().is_none());
        assert!(reader.get_account_history(&addr2, u64::MAX).unwrap().is_none());
        assert!(reader.get_account_history(&addr3, u64::MAX).unwrap().is_some());
    }

    // Phase 3: Take empty range
    {
        let writer = hot_kv.writer().unwrap();
        let removed = writer
            .traverse_dual_mut::<tables::AccountsHistory>()
            .unwrap()
            .take_range(
                (address!("0xf000000000000000000000000000000000000000"), 0)
                    ..=(address!("0xff00000000000000000000000000000000000000"), u64::MAX),
            )
            .unwrap();
        writer.commit().unwrap();

        assert!(removed.is_empty(), "should return empty vec for non-existent range");
    }
}

/// Test that storage wipe via changeset uses `clear_k1`.
///
/// This test verifies that:
/// 1. Multiple storage slots can be written for an address
/// 2. `write_changed_storage` with `wipe_storage: true` clears all slots
/// 3. After wipe, all slots return None
pub fn test_write_changed_storage_wipe<T: HotKv>(hot_kv: &T) {
    let addr = address!("0x1111111111111111111111111111111111111111");

    // Setup: write multiple storage slots for an address
    {
        let writer = hot_kv.writer().unwrap();
        for i in 0..10u64 {
            writer.put_storage(&addr, &U256::from(i), &U256::from(i * 100)).unwrap();
        }
        writer.commit().unwrap();
    }

    // Verify storage exists
    {
        let reader = hot_kv.reader().unwrap();
        for i in 0..10u64 {
            let val = reader.get_storage(&addr, &U256::from(i)).unwrap();
            assert_eq!(val, Some(U256::from(i * 100)));
        }
    }

    // Apply wipe via write_changed_storage
    {
        let writer = hot_kv.writer().unwrap();
        let changeset =
            PlainStorageChangeset { address: addr, wipe_storage: true, storage: vec![] };
        writer.write_changed_storage(&changeset).unwrap();
        writer.commit().unwrap();
    }

    // Verify all storage is cleared
    {
        let reader = hot_kv.reader().unwrap();
        for i in 0..10u64 {
            let val = reader.get_storage(&addr, &U256::from(i)).unwrap();
            assert!(val.is_none(), "slot {i} should be cleared");
        }
    }
}

/// Test the `clear_k1_for<T>` trait method.
///
/// This test verifies that:
/// 1. Storage slots can be written for an address
/// 2. `clear_k1_for` removes all k2 entries for the given k1
/// 3. After clearing, all slots return None
pub fn test_clear_k1_for<T: HotKv>(hot_kv: &T) {
    let addr = address!("0x2222222222222222222222222222222222222222");

    // Setup: write storage slots
    {
        let writer = hot_kv.writer().unwrap();
        for i in 0..5u64 {
            writer.put_storage(&addr, &U256::from(i), &U256::from(i + 1)).unwrap();
        }
        writer.commit().unwrap();
    }

    // Clear using clear_k1_for
    {
        let writer = hot_kv.writer().unwrap();
        writer.clear_k1_for::<tables::PlainStorageState>(&addr).unwrap();
        writer.commit().unwrap();
    }

    // Verify cleared
    {
        let reader = hot_kv.reader().unwrap();
        for i in 0..5u64 {
            assert!(reader.get_storage(&addr, &U256::from(i)).unwrap().is_none());
        }
    }
}
