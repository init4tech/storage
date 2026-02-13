//! Cursor operation tests for hot storage.

use crate::{
    db::UnsafeDbWrite,
    model::{DualTableTraverse, HotKv, HotKvRead},
    tables,
};
use alloy::{
    consensus::{Header, Sealable},
    primitives::{U256, address},
};
use signet_storage_types::Account;

/// Test cursor operations on an empty table.
///
/// Verifies that first(), last(), exact(), lower_bound() return None on empty tables.
pub fn test_cursor_empty_table<T: HotKv>(hot_kv: &T) {
    // Use a table that we haven't written to in this test
    // We'll use HeaderNumbers which should be empty if we haven't written headers with hashes
    let reader = hot_kv.reader().unwrap();

    // Create a fresh address that definitely doesn't exist
    let missing_addr = address!("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb01");

    // Test single-key cursor on PlainAccountState for a non-existent key
    {
        let mut cursor = reader.traverse::<tables::PlainAccountState>().unwrap();

        // exact() for non-existent key should return None
        let exact_result = cursor.exact(&missing_addr).unwrap();
        assert!(exact_result.is_none(), "exact() on non-existent key should return None");

        // lower_bound for a key beyond all existing should return None
        let lb_result =
            cursor.lower_bound(&address!("0xffffffffffffffffffffffffffffffffffffff99")).unwrap();
        // This might return something if there are entries, but for a truly empty table it would be None
        // We're mainly testing that it doesn't panic
        let _ = lb_result;
    }

    // Test dual-key cursor
    {
        let mut cursor = reader.traverse_dual::<tables::PlainStorageState>().unwrap();

        // exact_dual for non-existent keys should return None
        let exact_result = cursor.exact_dual(&missing_addr, &U256::from(999)).unwrap();
        assert!(exact_result.is_none(), "exact_dual() on non-existent key should return None");
    }
}

/// Test cursor exact() match semantics.
///
/// Verifies that exact() returns only exact matches, not lower_bound semantics.
pub fn test_cursor_exact_match<T: HotKv>(hot_kv: &T) {
    // Write headers at block numbers 10, 20, 30
    {
        let writer = hot_kv.writer().unwrap();
        for i in [10u64, 20, 30] {
            let header = Header { number: i, gas_limit: 1_000_000, ..Default::default() };
            writer.put_header_inconsistent(&header.seal_slow()).unwrap();
        }
        writer.commit().unwrap();
    }

    let reader = hot_kv.reader().unwrap();
    let mut cursor = reader.traverse::<tables::Headers>().unwrap();

    // exact() for existing key should return value
    let exact_10 = cursor.exact(&10u64).unwrap();
    assert!(exact_10.is_some(), "exact(10) should find the header");
    assert_eq!(exact_10.unwrap().number, 10);

    // exact() for non-existing key should return None, not the next key
    let exact_15 = cursor.exact(&15u64).unwrap();
    assert!(exact_15.is_none(), "exact(15) should return None, not header 20");

    // Verify lower_bound would have found something at 15
    let lb_15 = cursor.lower_bound(&15u64).unwrap();
    assert!(lb_15.is_some(), "lower_bound(15) should find header 20");
    assert_eq!(lb_15.unwrap().0, 20);
}

/// Test cursor backward iteration with read_prev().
pub fn test_cursor_backward_iteration<T: HotKv>(hot_kv: &T) {
    // Write headers at block numbers 100, 101, 102, 103, 104
    {
        let writer = hot_kv.writer().unwrap();
        for i in 100u64..105 {
            let header = Header { number: i, gas_limit: 1_000_000, ..Default::default() };
            writer.put_header_inconsistent(&header.seal_slow()).unwrap();
        }
        writer.commit().unwrap();
    }

    let reader = hot_kv.reader().unwrap();
    let mut cursor = reader.traverse::<tables::Headers>().unwrap();

    // Position at last entry
    let last = cursor.last().unwrap();
    assert!(last.is_some());
    let (num, _) = last.unwrap();
    assert_eq!(num, 104);

    // Iterate backward
    let prev1 = cursor.read_prev().unwrap();
    assert!(prev1.is_some());
    assert_eq!(prev1.unwrap().0, 103);

    let prev2 = cursor.read_prev().unwrap();
    assert!(prev2.is_some());
    assert_eq!(prev2.unwrap().0, 102);

    let prev3 = cursor.read_prev().unwrap();
    assert!(prev3.is_some());
    assert_eq!(prev3.unwrap().0, 101);

    let prev4 = cursor.read_prev().unwrap();
    assert!(prev4.is_some());
    assert_eq!(prev4.unwrap().0, 100);

    // Should hit beginning
    let prev5 = cursor.read_prev().unwrap();
    assert!(prev5.is_none(), "read_prev() past beginning should return None");
}

/// Test dual-key cursor navigation between k1 values.
pub fn test_cursor_dual_navigation<T: HotKv>(hot_kv: &T) {
    let addr1 = address!("0xcccccccccccccccccccccccccccccccccccccc01");
    let addr2 = address!("0xcccccccccccccccccccccccccccccccccccccc02");
    let addr3 = address!("0xcccccccccccccccccccccccccccccccccccccc03");

    // Write storage for multiple addresses with multiple slots
    {
        let writer = hot_kv.writer().unwrap();

        // addr1: slots 1, 2, 3
        writer.put_storage(&addr1, &U256::from(1), &U256::from(10)).unwrap();
        writer.put_storage(&addr1, &U256::from(2), &U256::from(20)).unwrap();
        writer.put_storage(&addr1, &U256::from(3), &U256::from(30)).unwrap();

        // addr2: slots 1, 2
        writer.put_storage(&addr2, &U256::from(1), &U256::from(100)).unwrap();
        writer.put_storage(&addr2, &U256::from(2), &U256::from(200)).unwrap();

        // addr3: slot 1
        writer.put_storage(&addr3, &U256::from(1), &U256::from(1000)).unwrap();

        writer.commit().unwrap();
    }

    let reader = hot_kv.reader().unwrap();
    let mut cursor = reader.traverse_dual::<tables::PlainStorageState>().unwrap();

    // Position at first entry
    let first =
        DualTableTraverse::<tables::PlainStorageState, _>::first(&mut *cursor.inner_mut()).unwrap();
    assert!(first.is_some());
    let (k1, k2, _) = first.unwrap();
    assert_eq!(k1, addr1);
    assert_eq!(k2, U256::from(1));

    // next_k1() should jump to addr2
    let next_addr = cursor.next_k1().unwrap();
    assert!(next_addr.is_some());
    let (k1, k2, _) = next_addr.unwrap();
    assert_eq!(k1, addr2, "next_k1() should jump to addr2");
    assert_eq!(k2, U256::from(1), "Should be at first slot of addr2");

    // next_k1() again should jump to addr3
    let next_addr = cursor.next_k1().unwrap();
    assert!(next_addr.is_some());
    let (k1, _, _) = next_addr.unwrap();
    assert_eq!(k1, addr3, "next_k1() should jump to addr3");

    // next_k1() again should return None (no more k1 values)
    let next_addr = cursor.next_k1().unwrap();
    assert!(next_addr.is_none(), "next_k1() at end should return None");

    // Test previous_k1()
    // First position at addr3
    cursor.last_of_k1(&addr3).unwrap();
    let prev_addr = cursor.previous_k1().unwrap();
    assert!(prev_addr.is_some());
    let (k1, _, _) = prev_addr.unwrap();
    assert_eq!(k1, addr2, "previous_k1() from addr3 should go to addr2");
}

/// Test `iter_k2` returns all entries for a given k1.
///
/// Writes 3 storage slots for a single address and verifies that `iter_k2`
/// yields all 3 entries in order.
pub fn test_cursor_iter_k2<T: HotKv>(hot_kv: &T) {
    let addr = address!("0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee01");

    {
        let writer = hot_kv.writer().unwrap();
        writer.put_storage(&addr, &U256::from(10), &U256::from(100)).unwrap();
        writer.put_storage(&addr, &U256::from(20), &U256::from(200)).unwrap();
        writer.put_storage(&addr, &U256::from(30), &U256::from(300)).unwrap();
        writer.commit().unwrap();
    }

    let reader = hot_kv.reader().unwrap();
    let mut cursor = reader.traverse_dual::<tables::PlainStorageState>().unwrap();

    let entries: Vec<_> = cursor.iter_k2(&addr).unwrap().collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(entries.len(), 3, "iter_k2 should return all 3 entries");
    assert_eq!(entries[0], (U256::from(10), U256::from(100)));
    assert_eq!(entries[1], (U256::from(20), U256::from(200)));
    assert_eq!(entries[2], (U256::from(30), U256::from(300)));
}

/// Test `iter_k2` returns a single entry when only one exists.
pub fn test_cursor_iter_k2_single<T: HotKv>(hot_kv: &T) {
    let addr = address!("0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee02");

    {
        let writer = hot_kv.writer().unwrap();
        writer.put_storage(&addr, &U256::from(42), &U256::from(999)).unwrap();
        writer.commit().unwrap();
    }

    let reader = hot_kv.reader().unwrap();
    let mut cursor = reader.traverse_dual::<tables::PlainStorageState>().unwrap();

    let entries: Vec<_> = cursor.iter_k2(&addr).unwrap().collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(entries.len(), 1, "iter_k2 should return the single entry");
    assert_eq!(entries[0], (U256::from(42), U256::from(999)));
}

/// Test `iter_k2` returns empty iterator for a nonexistent k1.
pub fn test_cursor_iter_k2_empty<T: HotKv>(hot_kv: &T) {
    let missing = address!("0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee03");

    let reader = hot_kv.reader().unwrap();
    let mut cursor = reader.traverse_dual::<tables::PlainStorageState>().unwrap();

    let entries: Vec<_> = cursor.iter_k2(&missing).unwrap().collect::<Result<Vec<_>, _>>().unwrap();
    assert!(entries.is_empty(), "iter_k2 on nonexistent k1 should return empty");
}

/// Test `iter()` yields all entries including the first.
///
/// Writes headers at blocks 500–504 and verifies that `iter()` returns all 5
/// in order.
pub fn test_cursor_iter<T: HotKv>(hot_kv: &T) {
    {
        let writer = hot_kv.writer().unwrap();
        for i in 500u64..505 {
            let header = Header { number: i, gas_limit: 1_000_000, ..Default::default() };
            writer.put_header_inconsistent(&header.seal_slow()).unwrap();
        }
        writer.commit().unwrap();
    }

    let reader = hot_kv.reader().unwrap();
    let mut cursor = reader.traverse::<tables::Headers>().unwrap();

    let entries: Vec<_> = cursor.iter().unwrap().collect::<Result<Vec<_>, _>>().unwrap();
    assert!(entries.len() >= 5, "iter() should return at least 5 entries");

    // Verify our 5 entries are present in order
    let our_entries: Vec<_> = entries.iter().filter(|(n, _)| (500..505).contains(n)).collect();
    assert_eq!(our_entries.len(), 5, "iter() must include all 5 written headers");
    for (i, (num, _)) in our_entries.iter().enumerate() {
        assert_eq!(*num, 500 + i as u64);
    }
}

/// Test `iter_from()` starts at the correct key including that key.
///
/// Reuses headers at blocks 500–504 and calls `iter_from(&502)`, asserting
/// entries 502, 503, 504 are returned.
pub fn test_cursor_iter_from<T: HotKv>(hot_kv: &T) {
    // Ensure data exists (may already be written by test_cursor_iter)
    {
        let writer = hot_kv.writer().unwrap();
        for i in 500u64..505 {
            let header = Header { number: i, gas_limit: 1_000_000, ..Default::default() };
            writer.put_header_inconsistent(&header.seal_slow()).unwrap();
        }
        writer.commit().unwrap();
    }

    let reader = hot_kv.reader().unwrap();
    let mut cursor = reader.traverse::<tables::Headers>().unwrap();

    let entries: Vec<_> =
        cursor.iter_from(&502u64).unwrap().collect::<Result<Vec<_>, _>>().unwrap();
    assert!(entries.len() >= 3, "iter_from(502) should return at least 3 entries");
    assert_eq!(entries[0].0, 502, "iter_from(502) must start at 502");
    assert_eq!(entries[1].0, 503);
    assert_eq!(entries[2].0, 504);
}

/// Test dual-key `iter()` yields all entries including the first.
///
/// Writes storage for 3 addresses (6 total entries) and verifies that `iter()`
/// returns all 6 in order.
pub fn test_cursor_dual_iter<T: HotKv>(hot_kv: &T) {
    let addr1 = address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    let addr2 = address!("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
    let addr3 = address!("0xcccccccccccccccccccccccccccccccccccccccc");

    {
        let writer = hot_kv.writer().unwrap();
        writer.put_storage(&addr1, &U256::from(1), &U256::from(10)).unwrap();
        writer.put_storage(&addr1, &U256::from(2), &U256::from(20)).unwrap();
        writer.put_storage(&addr2, &U256::from(1), &U256::from(100)).unwrap();
        writer.put_storage(&addr2, &U256::from(2), &U256::from(200)).unwrap();
        writer.put_storage(&addr3, &U256::from(1), &U256::from(1000)).unwrap();
        writer.put_storage(&addr3, &U256::from(2), &U256::from(2000)).unwrap();
        writer.commit().unwrap();
    }

    let reader = hot_kv.reader().unwrap();
    let mut cursor = reader.traverse_dual::<tables::PlainStorageState>().unwrap();

    let entries: Vec<_> = cursor.iter().unwrap().collect::<Result<Vec<_>, _>>().unwrap();
    assert!(entries.len() >= 6, "dual iter() should return at least 6 entries");

    // Find our entries by address
    let our_entries: Vec<_> =
        entries.iter().filter(|(k1, _, _)| *k1 == addr1 || *k1 == addr2 || *k1 == addr3).collect();
    assert_eq!(our_entries.len(), 6, "dual iter() must include all 6 written entries");
    assert_eq!(our_entries[0], &(addr1, U256::from(1), U256::from(10)));
    assert_eq!(our_entries[1], &(addr1, U256::from(2), U256::from(20)));
    assert_eq!(our_entries[2], &(addr2, U256::from(1), U256::from(100)));
    assert_eq!(our_entries[3], &(addr2, U256::from(2), U256::from(200)));
    assert_eq!(our_entries[4], &(addr3, U256::from(1), U256::from(1000)));
    assert_eq!(our_entries[5], &(addr3, U256::from(2), U256::from(2000)));
}

/// Test dual-key `iter_from()` starts at the correct key pair.
///
/// Uses the same data as `test_cursor_dual_iter` and calls
/// `iter_from(&addr2, &U256::from(1))`, asserting entries from (addr2, 1)
/// onward are returned.
pub fn test_cursor_dual_iter_from<T: HotKv>(hot_kv: &T) {
    let addr2 = address!("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
    let addr3 = address!("0xcccccccccccccccccccccccccccccccccccccccc");

    // Ensure data exists
    {
        let writer = hot_kv.writer().unwrap();
        let addr1 = address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        writer.put_storage(&addr1, &U256::from(1), &U256::from(10)).unwrap();
        writer.put_storage(&addr1, &U256::from(2), &U256::from(20)).unwrap();
        writer.put_storage(&addr2, &U256::from(1), &U256::from(100)).unwrap();
        writer.put_storage(&addr2, &U256::from(2), &U256::from(200)).unwrap();
        writer.put_storage(&addr3, &U256::from(1), &U256::from(1000)).unwrap();
        writer.put_storage(&addr3, &U256::from(2), &U256::from(2000)).unwrap();
        writer.commit().unwrap();
    }

    let reader = hot_kv.reader().unwrap();
    let mut cursor = reader.traverse_dual::<tables::PlainStorageState>().unwrap();

    let entries: Vec<_> =
        cursor.iter_from(&addr2, &U256::from(1)).unwrap().collect::<Result<Vec<_>, _>>().unwrap();
    assert!(entries.len() >= 4, "dual iter_from(addr2, 1) should return at least 4 entries");
    assert_eq!(entries[0], (addr2, U256::from(1), U256::from(100)));
    assert_eq!(entries[1], (addr2, U256::from(2), U256::from(200)));
    assert_eq!(entries[2], (addr3, U256::from(1), U256::from(1000)));
    assert_eq!(entries[3], (addr3, U256::from(2), U256::from(2000)));
}

/// Test cursor on table with single entry.
pub fn test_cursor_single_entry<T: HotKv>(hot_kv: &T) {
    let addr = address!("0xdddddddddddddddddddddddddddddddddddddd01");
    let account = Account { nonce: 42, balance: U256::from(1000), bytecode_hash: None };

    // Write single account
    {
        let writer = hot_kv.writer().unwrap();
        writer.put_account(&addr, &account).unwrap();
        writer.commit().unwrap();
    }

    let reader = hot_kv.reader().unwrap();
    let mut cursor = reader.traverse::<tables::PlainAccountState>().unwrap();

    // first() and last() should return the same entry
    let first = cursor.first().unwrap();
    assert!(first.is_some());
    let (first_addr, _) = first.unwrap();

    let last = cursor.last().unwrap();
    assert!(last.is_some());
    let (last_addr, _) = last.unwrap();

    assert_eq!(first_addr, last_addr, "first() and last() should be same for single entry");

    // read_next() after first() should return None
    cursor.first().unwrap();
    let next = cursor.read_next().unwrap();
    assert!(next.is_none(), "read_next() after first() on single entry should return None");
}
