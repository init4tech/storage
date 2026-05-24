//! MDBX-specific structural test: prove that signet-hot-mdbx splits the
//! AccountsHistory / StorageHistory tables into multiple shards when the
//! input exceeds MAX_SHARD_BYTES.

use alloy::primitives::{Address, U256};
use serial_test::serial;
use signet_hot::{
    HotKv,
    db::{HistoryRead, HistoryWrite, UnsafeDbWrite},
    model::HotKvRead,
    tables,
};
use signet_hot_mdbx::test_utils::create_test_rw_db;
use signet_storage_types::{BlockNumberList, ShardedKey};

/// 200 sparse blocks: one per distinct 16-bit roaring container.
///
/// Each block `i * 0x1_0000` falls in container `i`, so the roaring bitmap
/// cannot use run-length or bitmap compression across containers. The first
/// 100 blocks encode to ~1400 B (just under the 1500 B shard budget), and
/// the second 100 blocks encode to another ~1400 B. Appending all 200 in
/// two writes of 100 triggers the split: merged size ~2800 B > 1500 B.
///
/// The two writes mirror the `overflowing_extend_at_realistic_budget_respects_per_shard_size`
/// unit test in `signet-storage-types`.
fn sparse_blocks() -> Vec<u64> {
    (0..200u64).map(|i| i * 0x1_0000).collect()
}

#[test]
#[serial]
fn account_history_splits_on_oversized_input() {
    let (_dir, db) = create_test_rw_db();
    let addr = Address::from_slice(&[0x1; 20]);
    let blocks = sparse_blocks();
    // Split into two writes of 100 blocks each. Each batch individually fits
    // within MAX_SHARD_BYTES (~1400 B < 1500 B), but merging both (~2800 B)
    // forces the MDBX backend to create at least two dup entries.
    let first_half = BlockNumberList::new(blocks[..100].iter().copied()).unwrap();
    let second_half = BlockNumberList::new(blocks[100..].iter().copied()).unwrap();

    // Write in two transactions to match the incremental history-append pattern.
    {
        let writer = db.writer().unwrap();
        writer.append_account_history(&addr, &first_half).unwrap();
        writer.commit().unwrap();
    }
    {
        let writer = db.writer().unwrap();
        writer.append_account_history(&addr, &second_half).unwrap();
        writer.commit().unwrap();
    }

    let reader = db.reader().unwrap();

    // Logical: round-trip succeeds and returns the same blocks.
    let recovered = reader.blocks_changed_account(&addr).unwrap().unwrap();
    assert_eq!(recovered.iter().collect::<Vec<_>>(), blocks);

    // Structural: count dup entries directly via the raw cursor. ≥2 expected.
    let dup_count =
        reader.traverse_dual::<tables::AccountsHistory>().unwrap().iter_k2(&addr).unwrap().count();
    assert!(
        dup_count >= 2,
        "expected MDBX to split oversized input into >=2 shards, got {dup_count}"
    );

    // Structural: the tail shard's subkey is u64::MAX.
    let (_, tail_key, _) = reader
        .traverse_dual::<tables::AccountsHistory>()
        .unwrap()
        .last_of_k1(&addr)
        .unwrap()
        .expect("tail shard must exist");
    assert_eq!(tail_key, u64::MAX, "tail shard must be at u64::MAX");
}

#[test]
#[serial]
fn storage_history_splits_on_oversized_input() {
    let (_dir, db) = create_test_rw_db();
    let addr = Address::from_slice(&[0x2; 20]);
    let slot = U256::from(0xCAFEu64);
    let blocks = sparse_blocks();
    let first_half = BlockNumberList::new(blocks[..100].iter().copied()).unwrap();
    let second_half = BlockNumberList::new(blocks[100..].iter().copied()).unwrap();

    // Write in two transactions.
    {
        let writer = db.writer().unwrap();
        writer.append_storage_history(&addr, &slot, &first_half).unwrap();
        writer.commit().unwrap();
    }
    {
        let writer = db.writer().unwrap();
        writer.append_storage_history(&addr, &slot, &second_half).unwrap();
        writer.commit().unwrap();
    }

    let reader = db.reader().unwrap();

    // Logical: round-trip succeeds and returns the same blocks.
    let recovered = reader.blocks_changed_storage(&addr, &slot).unwrap().unwrap();
    assert_eq!(recovered.iter().collect::<Vec<_>>(), blocks);

    // Structural: count dup entries for this (addr, slot).
    let count = reader
        .traverse_dual::<tables::StorageHistory>()
        .unwrap()
        .iter_k2(&addr)
        .unwrap()
        .filter(|r: &Result<(ShardedKey<U256>, _), _>| {
            r.as_ref().is_ok_and(|(sk, _)| sk.key == slot)
        })
        .count();
    assert!(count >= 2, "expected >=2 shards for (addr, slot), got {count}");

    // Structural: the tail shard's subkey is ShardedKey(slot, u64::MAX).
    let (_, tail_sk, _) = reader
        .traverse_dual::<tables::StorageHistory>()
        .unwrap()
        .last_of_k1(&addr)
        .unwrap()
        .expect("tail shard must exist");
    assert_eq!(tail_sk, ShardedKey::new(slot, u64::MAX));
}
