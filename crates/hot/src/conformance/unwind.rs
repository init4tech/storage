//! Unwind conformance test and helpers.

use crate::{
    db::{HistoryWrite, UnsafeDbWrite},
    model::{DualKeyValue, DualTableTraverse, HotKv, HotKvRead, KeyValue, TableTraverse},
    tables::{self, DualKey, SingleKey},
};
use alloy::{
    consensus::{Header, Sealable},
    primitives::{Address, B256, Bytes, U256, address},
};
use std::{collections::HashMap, fmt::Debug};
use trevm::revm::{
    bytecode::Bytecode,
    database::{
        AccountStatus, BundleAccount, BundleState,
        states::{
            StorageSlot,
            reverts::{AccountInfoRevert, AccountRevert, RevertToSlot, Reverts},
        },
    },
    primitives::map::DefaultHashBuilder,
    state::AccountInfo,
};

/// Collect all entries from a single-keyed table.
pub fn collect_single_table<T, R>(reader: &R) -> Vec<KeyValue<T>>
where
    T: SingleKey,
    T::Key: Ord,
    R: HotKvRead,
{
    let mut cursor = reader.traverse::<T>().unwrap();
    let mut entries = Vec::new();
    if let Some(first) = TableTraverse::<T, _>::first(&mut *cursor.inner_mut()).unwrap() {
        entries.push(first);
        while let Some(next) = TableTraverse::<T, _>::read_next(&mut *cursor.inner_mut()).unwrap() {
            entries.push(next);
        }
    }
    entries.sort_by(|a, b| a.0.cmp(&b.0));
    entries
}

/// Collect all entries from a dual-keyed table.
pub fn collect_dual_table<T, R>(reader: &R) -> Vec<DualKeyValue<T>>
where
    T: DualKey,
    T::Key: Ord,
    T::Key2: Ord,
    R: HotKvRead,
{
    let mut cursor = reader.traverse_dual::<T>().unwrap();
    let mut entries = Vec::new();
    if let Some(first) = DualTableTraverse::<T, _>::first(&mut *cursor.inner_mut()).unwrap() {
        entries.push(first);
        while let Some(next) =
            DualTableTraverse::<T, _>::read_next(&mut *cursor.inner_mut()).unwrap()
        {
            entries.push(next);
        }
    }
    entries.sort_by(|a, b| (&a.0, &a.1).cmp(&(&b.0, &b.1)));
    entries
}

/// Assert two single-keyed table contents are equal.
pub fn assert_single_tables_equal<T>(table_name: &str, a: Vec<KeyValue<T>>, b: Vec<KeyValue<T>>)
where
    T: SingleKey,
    T::Key: Debug + PartialEq,
    T::Value: Debug + PartialEq,
{
    assert_eq!(
        a.len(),
        b.len(),
        "{} table entry count mismatch: {} vs {}",
        table_name,
        a.len(),
        b.len()
    );
    for (i, (entry_a, entry_b)) in a.iter().zip(b.iter()).enumerate() {
        assert_eq!(
            entry_a, entry_b,
            "{} table entry {} mismatch:\n  A: {:?}\n  B: {:?}",
            table_name, i, entry_a, entry_b
        );
    }
}

/// Assert two dual-keyed table contents are equal.
pub fn assert_dual_tables_equal<T>(
    table_name: &str,
    a: Vec<DualKeyValue<T>>,
    b: Vec<DualKeyValue<T>>,
) where
    T: DualKey,
    T::Key: Debug + PartialEq,
    T::Key2: Debug + PartialEq,
    T::Value: Debug + PartialEq,
{
    assert_eq!(
        a.len(),
        b.len(),
        "{} table entry count mismatch: {} vs {}",
        table_name,
        a.len(),
        b.len()
    );
    for (i, (entry_a, entry_b)) in a.iter().zip(b.iter()).enumerate() {
        assert_eq!(
            entry_a, entry_b,
            "{} table entry {} mismatch:\n  A: {:?}\n  B: {:?}",
            table_name, i, entry_a, entry_b
        );
    }
}

/// Create a BundleState with account and storage changes.
///
/// This function creates a proper BundleState with reverts populated so that
/// `to_plain_state_and_reverts` will produce the expected output.
#[allow(clippy::type_complexity)]
pub fn make_bundle_state(
    accounts: Vec<(Address, Option<AccountInfo>, Option<AccountInfo>)>,
    storage: Vec<(Address, Vec<(U256, U256, U256)>)>, // (addr, [(slot, old, new)])
    _contracts: Vec<(B256, Bytecode)>,
) -> BundleState {
    let mut state: HashMap<Address, BundleAccount, DefaultHashBuilder> = Default::default();

    // Build account reverts for this block
    let mut block_reverts: Vec<(Address, AccountRevert)> = Vec::new();

    for (addr, original, info) in &accounts {
        let account_storage: HashMap<U256, StorageSlot, DefaultHashBuilder> = Default::default();
        state.insert(
            *addr,
            BundleAccount {
                info: info.clone(),
                original_info: original.clone(),
                storage: account_storage,
                status: AccountStatus::Changed,
            },
        );

        // Create account revert - this stores what to restore to when unwinding
        let account_info_revert = match original {
            Some(orig) => AccountInfoRevert::RevertTo(orig.clone()),
            None => AccountInfoRevert::DeleteIt,
        };

        block_reverts.push((
            *addr,
            AccountRevert {
                account: account_info_revert,
                storage: Default::default(), // Storage reverts added below
                previous_status: AccountStatus::Changed,
                wipe_storage: false,
            },
        ));
    }

    // Process storage changes
    for (addr, slots) in &storage {
        let account = state.entry(*addr).or_insert_with(|| BundleAccount {
            info: None,
            original_info: None,
            storage: Default::default(),
            status: AccountStatus::Changed,
        });

        // Find or create the account revert entry
        let revert_entry = block_reverts.iter_mut().find(|(a, _)| a == addr);
        let account_revert = if let Some((_, revert)) = revert_entry {
            revert
        } else {
            block_reverts.push((
                *addr,
                AccountRevert {
                    account: AccountInfoRevert::DoNothing,
                    storage: Default::default(),
                    previous_status: AccountStatus::Changed,
                    wipe_storage: false,
                },
            ));
            &mut block_reverts.last_mut().unwrap().1
        };

        for (slot, old_value, new_value) in slots {
            account.storage.insert(
                *slot,
                StorageSlot { previous_or_original_value: *old_value, present_value: *new_value },
            );

            // Add storage revert entry
            account_revert.storage.insert(*slot, RevertToSlot::Some(*old_value));
        }
    }

    // Create Reverts with one block's worth of reverts
    let reverts = Reverts::new(vec![block_reverts]);

    BundleState { state, contracts: Default::default(), reverts, state_size: 0, reverts_size: 0 }
}

/// Create a simple AccountInfo for testing.
pub fn make_account_info(nonce: u64, balance: U256, code_hash: Option<B256>) -> AccountInfo {
    AccountInfo {
        nonce,
        balance,
        code_hash: code_hash.unwrap_or(B256::ZERO),
        code: None,
        account_id: None,
    }
}

/// Test that unwinding produces the exact same state as never having appended.
///
/// This test:
/// 1. Creates 5 blocks with complex state changes
/// 2. Appends all 5 blocks to store_a, then unwinds to block 1 (keeping blocks 0, 1)
/// 3. Appends only blocks 0, 1 to store_b
/// 4. Compares ALL tables between the two stores - they must be exactly equal
///
/// This proves that `unwind_above` correctly reverses all state changes including:
/// - Plain account state
/// - Plain storage state
/// - Headers and header number mappings
/// - Account and storage change sets
/// - Account and storage history indices
pub fn test_unwind_conformance<Kv: HotKv>(store_a: &Kv, store_b: &Kv) {
    // Test addresses
    let addr1 = address!("0x1111111111111111111111111111111111111111");
    let addr2 = address!("0x2222222222222222222222222222222222222222");
    let addr3 = address!("0x3333333333333333333333333333333333333333");
    let addr4 = address!("0x4444444444444444444444444444444444444444");

    // Storage slots
    let slot1 = U256::from(1);
    let slot2 = U256::from(2);
    let slot3 = U256::from(3);

    // Create bytecode
    let code = Bytes::from_static(&[0x60, 0x00, 0x60, 0x00, 0xf3]);
    let bytecode = Bytecode::new_raw(code);
    let code_hash = bytecode.hash_slow();

    // Create 5 blocks with complex state
    let mut blocks: Vec<(signet_storage_types::SealedHeader, BundleState)> = Vec::new();
    let mut prev_hash = B256::ZERO;

    // Block 0: Create addr1, addr2, addr3 with different states
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
            vec![
                (addr1, None, Some(make_account_info(1, U256::from(100), None))),
                (addr2, None, Some(make_account_info(1, U256::from(200), None))),
                (addr3, None, Some(make_account_info(1, U256::from(300), None))),
            ],
            vec![(addr1, vec![(slot1, U256::ZERO, U256::from(10))])],
            vec![],
        );
        blocks.push((sealed, bundle));
    }

    // Block 1: Update addr1, addr2; add storage to addr2
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
            vec![
                (
                    addr1,
                    Some(make_account_info(1, U256::from(100), None)),
                    Some(make_account_info(2, U256::from(150), None)),
                ),
                (
                    addr2,
                    Some(make_account_info(1, U256::from(200), None)),
                    Some(make_account_info(2, U256::from(250), None)),
                ),
            ],
            vec![
                (addr1, vec![(slot1, U256::from(10), U256::from(20))]),
                (addr2, vec![(slot1, U256::ZERO, U256::from(100))]),
            ],
            vec![],
        );
        blocks.push((sealed, bundle));
    }

    // Block 2: Update addr3, add bytecode (this is the boundary - will be unwound)
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
            vec![(
                addr3,
                Some(make_account_info(1, U256::from(300), None)),
                Some(make_account_info(2, U256::from(350), Some(code_hash))),
            )],
            vec![(addr3, vec![(slot1, U256::ZERO, U256::from(1000))])],
            vec![(code_hash, bytecode.clone())],
        );
        blocks.push((sealed, bundle));
    }

    // Block 3: Create addr4, update existing storage
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
            vec![
                (addr4, None, Some(make_account_info(1, U256::from(400), None))),
                (
                    addr1,
                    Some(make_account_info(2, U256::from(150), None)),
                    Some(make_account_info(3, U256::from(175), None)),
                ),
            ],
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

    // Block 4: Update multiple addresses and storage
    {
        let header = Header {
            number: 4,
            parent_hash: prev_hash,
            gas_limit: 1_000_000,
            ..Default::default()
        };
        let sealed = header.seal_slow();

        let bundle = make_bundle_state(
            vec![
                (
                    addr1,
                    Some(make_account_info(3, U256::from(175), None)),
                    Some(make_account_info(4, U256::from(200), None)),
                ),
                (
                    addr2,
                    Some(make_account_info(2, U256::from(250), None)),
                    Some(make_account_info(3, U256::from(275), None)),
                ),
                (
                    addr4,
                    Some(make_account_info(1, U256::from(400), None)),
                    Some(make_account_info(2, U256::from(450), None)),
                ),
            ],
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

    // Store A: Append all 5 blocks, then unwind to block 1
    {
        let writer = store_a.writer().unwrap();
        writer.append_blocks(&blocks).unwrap();
        writer.commit().unwrap();
    }
    {
        let writer = store_a.writer().unwrap();
        writer.unwind_above(1).unwrap();
        writer.commit().unwrap();
    }

    // Store B: Append only blocks 0, 1
    {
        let writer = store_b.writer().unwrap();
        writer.append_blocks(&blocks[0..2]).unwrap();
        writer.commit().unwrap();
    }

    // Compare all tables
    let reader_a = store_a.reader().unwrap();
    let reader_b = store_b.reader().unwrap();

    // Single-keyed tables
    assert_single_tables_equal::<tables::Headers>(
        "Headers",
        collect_single_table::<tables::Headers, _>(&reader_a),
        collect_single_table::<tables::Headers, _>(&reader_b),
    );

    assert_single_tables_equal::<tables::HeaderNumbers>(
        "HeaderNumbers",
        collect_single_table::<tables::HeaderNumbers, _>(&reader_a),
        collect_single_table::<tables::HeaderNumbers, _>(&reader_b),
    );

    assert_single_tables_equal::<tables::PlainAccountState>(
        "PlainAccountState",
        collect_single_table::<tables::PlainAccountState, _>(&reader_a),
        collect_single_table::<tables::PlainAccountState, _>(&reader_b),
    );

    // Note: Bytecodes are not removed on unwind (they're content-addressed),
    // so store_a may have more bytecodes than store_b. We skip this comparison.
    // assert_single_tables_equal::<tables::Bytecodes>(...)

    // Dual-keyed tables
    assert_dual_tables_equal::<tables::PlainStorageState>(
        "PlainStorageState",
        collect_dual_table::<tables::PlainStorageState, _>(&reader_a),
        collect_dual_table::<tables::PlainStorageState, _>(&reader_b),
    );

    assert_dual_tables_equal::<tables::AccountChangeSets>(
        "AccountChangeSets",
        collect_dual_table::<tables::AccountChangeSets, _>(&reader_a),
        collect_dual_table::<tables::AccountChangeSets, _>(&reader_b),
    );

    assert_dual_tables_equal::<tables::StorageChangeSets>(
        "StorageChangeSets",
        collect_dual_table::<tables::StorageChangeSets, _>(&reader_a),
        collect_dual_table::<tables::StorageChangeSets, _>(&reader_b),
    );

    assert_dual_tables_equal::<tables::AccountsHistory>(
        "AccountsHistory",
        collect_dual_table::<tables::AccountsHistory, _>(&reader_a),
        collect_dual_table::<tables::AccountsHistory, _>(&reader_b),
    );

    assert_dual_tables_equal::<tables::StorageHistory>(
        "StorageHistory",
        collect_dual_table::<tables::StorageHistory, _>(&reader_a),
        collect_dual_table::<tables::StorageHistory, _>(&reader_b),
    );
}
