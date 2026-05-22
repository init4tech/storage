//! Logical history reads and writes.
//!
//! These traits replace the shard-leaking surface in `db::read` and
//! `db::inconsistent`. [`HistoryRead`] is blanket-impled on [`HotKvRead`] and
//! cannot be overridden — the KV-table layout is mandated by the abstraction.
//! [`HistoryWrite`] is required per-backend; each backend chooses its
//! splitting policy (MDBX uses [`signet_storage_types::merge_and_split`];
//! MemKv writes a single dup entry per addr).

use crate::{
    db::{HistoryError, HotDbRead, UnsafeDbWrite},
    model::HotKvRead,
    tables,
};
use ahash::{AHashMap, AHashSet};
use alloy::{
    consensus::Sealable,
    genesis::{Genesis, GenesisAccount},
    primitives::{Address, B256, BlockNumber, U256, address},
};
use itertools::Itertools;
use signet_storage_types::{Account, BlockNumberList, EthereumHardfork, SealedHeader, ShardedKey};
use std::ops::RangeInclusive;
use trevm::revm::{
    database::{
        BundleState, OriginalValuesKnown,
        states::{PlainStateReverts, PlainStorageChangeset, PlainStorageRevert, StateChangeset},
    },
    state::{AccountInfo, Bytecode},
};

/// Maximum address value (all bits set to 1).
const ADDRESS_MAX: Address = address!("0xffffffffffffffffffffffffffffffffffffffff");

/// Logical reads against history + changeset tables.
///
/// Default-impl-only. Backends cannot override — the blanket impl below
/// occupies the slot, and orphan rules prevent downstream impls. This is
/// structural enforcement of "the KV-table access pattern is mandated by
/// the abstraction".
pub trait HistoryRead: HotDbRead {
    /// All block numbers where `addr` was touched. `None` if no history.
    fn blocks_changed_account(
        &self,
        addr: &Address,
    ) -> Result<Option<BlockNumberList>, Self::Error> {
        let mut cursor = self.traverse_dual::<tables::AccountsHistory>()?;
        let mut iter = cursor.iter_k2(addr)?;
        let Some(first) = iter.next().transpose()? else {
            return Ok(None);
        };
        // first is (u64, BlockNumberList) — the shard key and its list
        let (_, mut merged) = first;
        for entry in iter {
            let (_, list) = entry?;
            merged.append(list.iter()).expect("history blocks strictly increasing");
        }
        Ok(Some(merged))
    }

    /// All block numbers where `(addr, slot)` was touched. `None` if none.
    fn blocks_changed_storage(
        &self,
        addr: &Address,
        slot: &U256,
    ) -> Result<Option<BlockNumberList>, Self::Error> {
        let target = ShardedKey::new(*slot, 0u64);
        let mut cursor = self.traverse_dual::<tables::StorageHistory>()?;
        let Some((found_addr, sk, list)) = cursor.next_dual_above(addr, &target)? else {
            return Ok(None);
        };
        if found_addr != *addr || sk.key != *slot {
            return Ok(None);
        }
        let mut merged = list;
        while let Some((next_addr, next_sk, next_list)) = cursor.read_next()? {
            if next_addr != *addr || next_sk.key != *slot {
                break;
            }
            merged.append(next_list.iter()).expect("history blocks strictly increasing");
        }
        Ok(Some(merged))
    }

    /// Smallest block `> height` where `addr` was touched.
    ///
    /// `None` means the account was not changed after `height`; the caller
    /// should consult the current plain state.
    fn block_account_changed_after(
        &self,
        addr: &Address,
        height: u64,
    ) -> Result<Option<u64>, Self::Error> {
        let Some(target) = height.checked_add(1) else {
            return Ok(None);
        };
        let mut cursor = self.traverse_dual::<tables::AccountsHistory>()?;
        let Some((found_addr, _, list)) = cursor.next_dual_above(addr, &target)? else {
            return Ok(None);
        };
        if found_addr != *addr {
            return Ok(None);
        }
        let rank = list.rank(height);
        Ok(list.select(rank))
    }

    /// Smallest block `> height` where `(addr, slot)` was touched.
    fn block_storage_changed_after(
        &self,
        addr: &Address,
        slot: &U256,
        height: u64,
    ) -> Result<Option<u64>, Self::Error> {
        let Some(target_block) = height.checked_add(1) else {
            return Ok(None);
        };
        let target = ShardedKey::new(*slot, target_block);
        let mut cursor = self.traverse_dual::<tables::StorageHistory>()?;
        let Some((found_addr, sk, list)) = cursor.next_dual_above(addr, &target)? else {
            return Ok(None);
        };
        if found_addr != *addr || sk.key != *slot {
            return Ok(None);
        }
        let rank = list.rank(height);
        Ok(list.select(rank))
    }

    /// Account pre-state recorded at `block`, or `None` if `addr` was not
    /// changed in `block`.
    fn get_account_change(
        &self,
        block: u64,
        addr: &Address,
    ) -> Result<Option<Account>, Self::Error> {
        self.get_dual::<tables::AccountChangeSets>(&block, addr)
    }

    /// Storage pre-state recorded at `block` for `(addr, slot)`.
    fn get_storage_change(
        &self,
        block: u64,
        addr: &Address,
        slot: &U256,
    ) -> Result<Option<U256>, Self::Error> {
        self.get_dual::<tables::StorageChangeSets>(&(block, *addr), slot)
    }

    /// Get account state, optionally at a specific historical block height.
    ///
    /// When `height` is `Some`, reconstructs the account state at that block
    /// height by consulting history and change set tables. When `None`, returns
    /// the current value from `PlainAccountState`.
    fn get_account_at_height(
        &self,
        addr: &Address,
        height: Option<u64>,
    ) -> Result<Option<Account>, Self::Error> {
        let Some(h) = height else {
            return self.get_account(addr);
        };
        match self.block_account_changed_after(addr, h)? {
            None => self.get_account(addr),
            Some(first) => self.get_account_change(first, addr),
        }
    }

    /// Get storage slot value, optionally at a specific historical block height.
    fn get_storage_at_height(
        &self,
        addr: &Address,
        slot: &U256,
        height: Option<u64>,
    ) -> Result<Option<U256>, Self::Error> {
        let Some(h) = height else {
            return self.get_storage(addr, slot);
        };
        match self.block_storage_changed_after(addr, slot, h)? {
            None => self.get_storage(addr, slot),
            Some(first) => self.get_storage_change(first, addr, slot),
        }
    }

    /// Get the last (highest) header in the database.
    /// Returns None if the database is empty.
    fn last_header(&self) -> Result<Option<SealedHeader>, Self::Error> {
        let mut cursor = self.traverse::<tables::Headers>()?;
        Ok(cursor.last()?.map(|(_, header)| header))
    }

    /// Get the last (highest) block number in the database.
    /// Returns None if the database is empty.
    fn last_block_number(&self) -> Result<Option<u64>, Self::Error> {
        let mut cursor = self.traverse::<tables::Headers>()?;
        Ok(cursor.last()?.map(|(number, _)| number))
    }

    /// Get the first (lowest) header in the database.
    /// Returns None if the database is empty.
    fn first_header(&self) -> Result<Option<SealedHeader>, Self::Error> {
        let mut cursor = self.traverse::<tables::Headers>()?;
        Ok(cursor.first()?.map(|(_, header)| header))
    }

    /// Get the current chain tip (highest block number and hash).
    /// Returns None if the database is empty.
    fn get_chain_tip(&self) -> Result<Option<(u64, B256)>, Self::Error> {
        let mut cursor = self.traverse::<tables::Headers>()?;
        let Some((number, header)) = cursor.last()? else {
            return Ok(None);
        };
        let hash = header.hash();
        Ok(Some((number, hash)))
    }

    /// Get the execution range (first and last block numbers with headers).
    /// Returns None if the database is empty.
    fn get_execution_range(&self) -> Result<Option<(u64, u64)>, Self::Error> {
        let mut cursor = self.traverse::<tables::Headers>()?;
        let Some((first, _)) = cursor.first()? else {
            return Ok(None);
        };
        let Some((last, _)) = cursor.last()? else {
            return Ok(None);
        };
        Ok(Some((first, last)))
    }

    /// Check if a specific block number exists in history.
    fn has_block(&self, number: u64) -> Result<bool, Self::Error> {
        self.get_header(number).map(|opt| opt.is_some())
    }

    /// Get headers in a range (inclusive).
    fn get_headers_range(&self, start: u64, end: u64) -> Result<Vec<SealedHeader>, Self::Error> {
        self.traverse::<tables::Headers>()?
            .iter_from(&start)?
            .take_while(|r| r.as_ref().is_ok_and(|(num, _)| *num <= end))
            .map(|r| r.map(|(_, header)| header))
            .collect()
    }

    /// Validate that `height` is within the stored block range.
    ///
    /// Returns `Ok(())` if `height` is `None` (current state) or within the
    /// range of stored blocks. Returns an error if the database has no
    /// blocks or if the height is out of range.
    fn check_height(&self, height: Option<u64>) -> Result<(), HistoryError<Self::Error>> {
        let Some(height) = height else { return Ok(()) };
        let Some((first, last)) = self.get_execution_range().map_err(HistoryError::Db)? else {
            return Err(HistoryError::NoBlocks);
        };
        if height < first || height > last {
            return Err(HistoryError::HeightOutOfRange { height, first, last });
        }
        Ok(())
    }

    /// Get account state at a height, with range validation.
    ///
    /// Validates that `height` is within the stored block range before
    /// delegating to [`Self::get_account_at_height`].
    fn get_account_at_height_checked(
        &self,
        addr: &Address,
        height: Option<u64>,
    ) -> Result<Option<Account>, HistoryError<Self::Error>> {
        self.check_height(height)?;
        self.get_account_at_height(addr, height).map_err(HistoryError::Db)
    }

    /// Get storage slot value at a height, with range validation.
    ///
    /// Validates that `height` is within the stored block range before
    /// delegating to [`Self::get_storage_at_height`].
    fn get_storage_at_height_checked(
        &self,
        addr: &Address,
        slot: &U256,
        height: Option<u64>,
    ) -> Result<Option<U256>, HistoryError<Self::Error>> {
        self.check_height(height)?;
        self.get_storage_at_height(addr, slot, height).map_err(HistoryError::Db)
    }
}

impl<T> HistoryRead for T where T: HotKvRead {}

/// Logical writes against history + changeset tables. Required per backend.
///
/// Backends that implement this trait choose their own shard-splitting policy.
/// The default `update_history_indices` bulk operation is expressed in terms of
/// the four required primitives and works for any backend.
pub trait HistoryWrite: UnsafeDbWrite + HistoryRead {
    /// Merge `new_blocks` into `addr`'s account history.
    ///
    /// Preconditions: `new_blocks` is sorted ascending and every entry is
    /// strictly greater than any block already stored for `addr`.
    fn append_account_history(
        &self,
        addr: &Address,
        new_blocks: &BlockNumberList,
    ) -> Result<(), HistoryError<Self::Error>>;

    /// Merge `new_blocks` into `(addr, slot)`'s storage history.
    fn append_storage_history(
        &self,
        addr: &Address,
        slot: &U256,
        new_blocks: &BlockNumberList,
    ) -> Result<(), HistoryError<Self::Error>>;

    /// Remove all blocks `> above` from `addr`'s account history.
    /// If nothing remains, delete the entry.
    fn truncate_account_history_above(
        &self,
        addr: &Address,
        above: u64,
    ) -> Result<(), HistoryError<Self::Error>>;

    /// Remove all blocks `> above` from `(addr, slot)`'s storage history.
    fn truncate_storage_history_above(
        &self,
        addr: &Address,
        slot: &U256,
        above: u64,
    ) -> Result<(), HistoryError<Self::Error>>;

    // ---- default-impl bulk operations (in terms of the four required) ----

    /// Build per-address block lists from changesets in `range` and call
    /// [`Self::append_account_history`] / [`Self::append_storage_history`] per
    /// entry.
    fn update_history_indices(
        &self,
        range: RangeInclusive<BlockNumber>,
    ) -> Result<(), HistoryError<Self::Error>> {
        // Account stage: collect (addr, block_number) pairs from changesets
        let account_indices: AHashMap<Address, Vec<u64>> = self
            .traverse_dual::<tables::AccountChangeSets>()
            .map_err(HistoryError::Db)?
            .iter_from(range.start(), &Address::ZERO)
            .map_err(HistoryError::Db)?
            .process_results(|iter| {
                iter.take_while(|(num, _, _)| range.contains(num))
                    .map(|(num, addr, _)| (addr, num))
                    .into_group_map_by(|(addr, _)| *addr)
                    .into_iter()
                    .map(|(addr, pairs)| (addr, pairs.into_iter().map(|(_, n)| n).collect()))
                    .collect()
            })
            .map_err(HistoryError::Db)?;

        for (addr, blocks) in account_indices {
            let list = BlockNumberList::new_pre_sorted(blocks);
            self.append_account_history(&addr, &list)?;
        }

        // Storage stage: collect ((addr, slot), block_number) pairs from changesets
        let storage_indices: AHashMap<(Address, U256), Vec<u64>> = self
            .traverse_dual::<tables::StorageChangeSets>()
            .map_err(HistoryError::Db)?
            .iter_from(&(*range.start(), Address::ZERO), &U256::ZERO)
            .map_err(HistoryError::Db)?
            .process_results(|iter| {
                iter.take_while(|(num_addr, _, _)| range.contains(&num_addr.0))
                    .map(|(num_addr, slot, _)| ((num_addr.1, slot), num_addr.0))
                    .into_group_map_by(|(k, _)| *k)
                    .into_iter()
                    .map(|(k, pairs)| (k, pairs.into_iter().map(|(_, n)| n).collect()))
                    .collect()
            })
            .map_err(HistoryError::Db)?;

        for ((addr, slot), blocks) in storage_indices {
            let list = BlockNumberList::new_pre_sorted(blocks);
            self.append_storage_history(&addr, &slot, &list)?;
        }

        Ok(())
    }

    /// Validate that a range of headers forms a valid chain extension.
    ///
    /// Headers must be in order and each must extend the previous.
    /// The first header must extend the current database tip (or be the first
    /// block if the database is empty).
    ///
    /// Returns `Ok(())` if valid, or an error describing the inconsistency.
    fn validate_chain_extension<'a, I>(&self, headers: I) -> Result<(), HistoryError<Self::Error>>
    where
        I: IntoIterator<Item = &'a SealedHeader>,
    {
        let mut iter = headers.into_iter();
        let first = iter.next().ok_or(HistoryError::EmptyRange)?;

        // Validate first header against current DB tip
        match self.get_chain_tip().map_err(HistoryError::Db)? {
            None => {
                // Empty DB - first block is valid as genesis
            }
            Some((tip_number, tip_hash)) => {
                let expected_number = tip_number + 1;
                if first.number != expected_number {
                    return Err(HistoryError::NonContiguousBlock {
                        expected: expected_number,
                        got: first.number,
                    });
                }
                if first.parent_hash != tip_hash {
                    return Err(HistoryError::ParentHashMismatch {
                        expected: tip_hash,
                        got: first.parent_hash,
                    });
                }
            }
        }

        // Validate each subsequent header extends the previous using fold
        iter.try_fold(first, |prev, curr| {
            let expected_number = prev.number + 1;
            if curr.number != expected_number {
                return Err(HistoryError::NonContiguousBlock {
                    expected: expected_number,
                    got: curr.number,
                });
            }

            let expected_hash = prev.hash();
            if curr.parent_hash != expected_hash {
                return Err(HistoryError::ParentHashMismatch {
                    expected: expected_hash,
                    got: curr.parent_hash,
                });
            }

            Ok(curr)
        })?;

        Ok(())
    }

    /// Append a range of blocks and their associated state to the database.
    fn append_blocks<'a>(
        &self,
        blocks: impl IntoIterator<Item = (&'a SealedHeader, &'a BundleState)>,
    ) -> Result<(), HistoryError<Self::Error>> {
        let mut iter = blocks.into_iter();

        let Some((first_header, first_bundle)) = iter.next() else {
            return Err(HistoryError::EmptyRange);
        };

        // Validate first header against DB tip
        match self.get_chain_tip().map_err(HistoryError::Db)? {
            None => { /* Empty DB - first block is valid as genesis */ }
            Some((tip_number, tip_hash)) => {
                let expected_number = tip_number + 1;
                if first_header.number != expected_number {
                    return Err(HistoryError::NonContiguousBlock {
                        expected: expected_number,
                        got: first_header.number,
                    });
                }
                if first_header.parent_hash != tip_hash {
                    return Err(HistoryError::ParentHashMismatch {
                        expected: tip_hash,
                        got: first_header.parent_hash,
                    });
                }
            }
        }

        // Write first block and track range
        self.append_block_inconsistent(first_header, first_bundle)?;
        let first_num = first_header.number;
        let mut last_num = first_num;
        let mut prev = first_header;

        // Process remaining: validate chain continuity and write in one pass
        for (header, bundle) in iter {
            let expected_number = prev.number + 1;
            if header.number != expected_number {
                return Err(HistoryError::NonContiguousBlock {
                    expected: expected_number,
                    got: header.number,
                });
            }
            let expected_hash = prev.hash();
            if header.parent_hash != expected_hash {
                return Err(HistoryError::ParentHashMismatch {
                    expected: expected_hash,
                    got: header.parent_hash,
                });
            }

            self.append_block_inconsistent(header, bundle)?;
            last_num = header.number;
            prev = header;
        }

        self.update_history_indices(first_num..=last_num)
    }

    /// Unwind all data above the given block number.
    ///
    /// This completely reverts the database state to what it was at block
    /// `block`, including:
    /// - Plain account state
    /// - Plain storage state
    /// - Headers and header number mappings
    /// - Account and storage change sets
    /// - Account and storage history indices
    fn unwind_above(&self, block: BlockNumber) -> Result<(), HistoryError<Self::Error>> {
        let first_block = block + 1;
        let Some(last_block) = self.last_block_number()? else {
            return Ok(());
        };

        if first_block > last_block {
            return Ok(());
        }

        // ═══════════════════════════════════════════════════════════════════
        // 1. STREAM AccountChangeSets → restore + filter history in one pass
        // ═══════════════════════════════════════════════════════════════════
        // TODO: estimate capacity from block range size for better allocation
        let mut seen_accounts: AHashSet<Address> = AHashSet::new();
        let mut account_cursor = self.traverse_dual::<tables::AccountChangeSets>()?;

        // Position at first entry
        let mut current = account_cursor.next_dual_above(&first_block, &Address::ZERO)?;

        while let Some((block_num, address, old_account)) = current {
            if block_num > last_block {
                break;
            }

            // First occurrence = process both plain state and history
            if seen_accounts.insert(address) {
                // Restore plain state
                if old_account.is_empty() {
                    self.queue_delete::<tables::PlainAccountState>(&address)?;
                } else {
                    self.put_account(&address, &old_account)?;
                }

                // Truncate history above `block` (logical, no shard surgery)
                self.truncate_account_history_above(&address, block)?;
            }

            current = account_cursor.read_next()?;
        }

        // ═══════════════════════════════════════════════════════════════════
        // 2. STREAM StorageChangeSets → restore + filter history in one pass
        // ═══════════════════════════════════════════════════════════════════
        // TODO: estimate capacity from block range size for better allocation
        let mut seen_storage: AHashSet<(Address, U256)> = AHashSet::new();
        let mut storage_cursor = self.traverse_dual::<tables::StorageChangeSets>()?;

        // Position at first entry
        let mut current_storage =
            storage_cursor.next_dual_above(&(first_block, Address::ZERO), &U256::ZERO)?;

        while let Some(((block_num, address), slot, old_value)) = current_storage {
            if block_num > last_block {
                break;
            }

            if seen_storage.insert((address, slot)) {
                // Restore plain state
                if old_value.is_zero() {
                    self.queue_delete_dual::<tables::PlainStorageState>(&address, &slot)?;
                } else {
                    self.put_storage(&address, &slot, &old_value)?;
                }

                // Truncate history above `block` (logical, no shard surgery)
                self.truncate_storage_history_above(&address, &slot, block)?;
            }

            current_storage = storage_cursor.read_next()?;
        }

        // ═══════════════════════════════════════════════════════════════════
        // 3. DELETE changeset ranges
        // ═══════════════════════════════════════════════════════════════════
        self.traverse_dual_mut::<tables::AccountChangeSets>()?
            .delete_range((first_block, Address::ZERO)..=(last_block, ADDRESS_MAX))?;
        self.traverse_dual_mut::<tables::StorageChangeSets>()?.delete_range(
            ((first_block, Address::ZERO), U256::ZERO)..=((last_block, ADDRESS_MAX), U256::MAX),
        )?;

        // ═══════════════════════════════════════════════════════════════════
        // 4. STREAM Headers → delete HeaderNumbers, then clear Headers
        // ═══════════════════════════════════════════════════════════════════
        let mut header_cursor = self.traverse::<tables::Headers>()?;

        // Position at first entry and process it
        let first_entry = header_cursor.lower_bound(&first_block)?;
        if let Some((block_num, header)) = first_entry
            && block_num <= last_block
        {
            self.delete_header_number(&header.hash())?;

            // Continue with remaining entries
            while let Some((block_num, header)) = header_cursor.read_next()? {
                if block_num > last_block {
                    break;
                }
                self.delete_header_number(&header.hash())?;
            }
        }
        self.traverse_mut::<tables::Headers>()?.delete_range_inclusive(first_block..=last_block)?;

        Ok(())
    }

    /// Load genesis data into the database.
    ///
    /// This operation is only valid on an empty database.
    fn load_genesis(
        &self,
        genesis: &Genesis,
        genesis_hardforks: &EthereumHardfork,
    ) -> Result<(), HistoryError<Self::Error>> {
        // Check that the database is empty
        if self.get_chain_tip().map_err(HistoryError::Db)?.is_some() {
            return Err(HistoryError::DbNotEmpty);
        }

        // Seal the genesis header, record its number, and create a blocknumber
        // list.
        let header = signet_storage_types::genesis_header(genesis, genesis_hardforks).seal_slow();
        let genesis_number = header.number;
        let genesis_history = BlockNumberList::new_pre_sorted([genesis_number]);

        // Append the header, with empty state
        self.append_blocks([(&header, &BundleState::default())])?;

        // Keep track of written bytecode hashes to avoid duplicates.
        let mut written_bytecode_hashes: AHashSet<B256> = AHashSet::new();

        // For each account in the genesis allocation, append account.
        // The accounts are pre-sorted by the BTreeMap in Genesis.
        genesis.alloc.iter().try_for_each(|(address, account)| {
            let GenesisAccount { nonce, balance, code, storage, .. } = account;

            // Insert bytecode if present. Check against the set to avoid
            // duplicate writes. We still have to compute the hash though.
            let bytecode_hash = code
                .as_ref()
                .map(|code_bytes| -> Result<_, HistoryError<Self::Error>> {
                    let hash = alloy::primitives::keccak256(code_bytes);
                    // Short-circuit if already written
                    if !written_bytecode_hashes.insert(hash) {
                        return Ok(hash);
                    }
                    self.put_bytecode(&hash, &Bytecode::new_raw(code_bytes.clone()))?;
                    Ok(hash)
                })
                .transpose()?;

            // Append the account.
            self.append_account(
                address,
                &Account { nonce: nonce.unwrap_or_default(), balance: *balance, bytecode_hash },
            )?;

            // Record account history at genesis
            self.append_account_history(address, &genesis_history)?;

            // Insert storage entries and history
            storage.iter().flatten().try_for_each(|(slot, value)| {
                let slot = U256::from_be_bytes(**slot);
                // We can append directly since the slots are sorted and the
                // db is empty.
                self.append_storage(address, &slot, &U256::from_be_bytes(**value))?;
                // Record storage history at genesis
                self.append_storage_history(address, &slot, &genesis_history)?;
                Ok::<(), HistoryError<Self::Error>>(())
            })?;
            Ok(())
        })
    }

    /// Write an account change (pre-state) for an account at a specific block.
    fn write_account_prestate(
        &self,
        block_number: u64,
        address: Address,
        pre_state: &Account,
    ) -> Result<(), Self::Error> {
        self.queue_put_dual::<tables::AccountChangeSets>(&block_number, &address, pre_state)
    }

    /// Append an account prestate entry.
    ///
    /// Entries must be appended in sorted order by (block_number, address).
    /// Within a single block, addresses must be sorted.
    fn append_account_prestate(
        &self,
        block_number: u64,
        address: Address,
        pre_state: &Account,
    ) -> Result<(), Self::Error> {
        self.queue_append_dual::<tables::AccountChangeSets>(&block_number, &address, pre_state)
    }

    /// Write a storage change (before state) for an account at a specific block.
    fn write_storage_prestate(
        &self,
        block_number: u64,
        address: Address,
        slot: &U256,
        prestate: &U256,
    ) -> Result<(), Self::Error> {
        self.queue_put_dual::<tables::StorageChangeSets>(&(block_number, address), slot, prestate)
    }

    /// Append a storage prestate entry.
    ///
    /// Entries must be appended in sorted order by ((block_number, address), slot).
    /// Within a single (block, address), slots must be sorted.
    fn append_storage_prestate(
        &self,
        block_number: u64,
        address: Address,
        slot: &U256,
        prestate: &U256,
    ) -> Result<(), Self::Error> {
        self.queue_append_dual::<tables::StorageChangeSets>(
            &(block_number, address),
            slot,
            prestate,
        )
    }

    /// Write a pre-state for every storage key that exists for an account at a
    /// specific block.
    ///
    /// Note: This uses `write_storage_prestate` (regular put) instead of
    /// `append_storage_prestate` because the slots may interleave with other
    /// writes to the same K1 from different code paths.
    fn write_wipe(&self, block_number: u64, address: &Address) -> Result<(), Self::Error> {
        let mut cursor = self.traverse_dual::<tables::PlainStorageState>()?;

        for entry in cursor.iter_k2(address)? {
            let (slot, value) = entry?;
            self.write_storage_prestate(block_number, *address, &slot, &value)?;
        }
        Ok(())
    }

    /// Write pre-sorted revert data for a single block.
    ///
    /// # Panics (debug builds only)
    ///
    /// Panics if `accounts` is not sorted by address or `storage` is not sorted
    /// by address.
    fn write_plain_revert_sorted(
        &self,
        block_number: u64,
        accounts: &[&(Address, Option<AccountInfo>)],
        storage: &[&PlainStorageRevert],
    ) -> Result<(), Self::Error> {
        #[cfg(debug_assertions)]
        {
            debug_assert!(
                accounts.windows(2).all(|w| w[0].0 <= w[1].0),
                "accounts must be sorted by address"
            );
            debug_assert!(
                storage.windows(2).all(|w| w[0].address <= w[1].address),
                "storage must be sorted by address"
            );
        }

        for (address, info) in accounts {
            let account = info.as_ref().map(Account::from).unwrap_or_default();

            // bytecode_hash is None when code_hash == KECCAK256_EMPTY,
            // which doesn't need to be stored.
            if let Some((bytecode, code_hash)) =
                info.as_ref().and_then(|info| info.code.clone()).zip(account.bytecode_hash)
            {
                self.put_bytecode(&code_hash, &bytecode)?;
            }

            self.append_account_prestate(block_number, *address, &account)?;
        }

        for entry in storage {
            if entry.wiped {
                self.write_wipe(block_number, &entry.address)?;
                continue;
            }
            // Use write (put) instead of append because storage_revert slots
            // are not guaranteed to be sorted.
            for (key, old_value) in entry.storage_revert.iter() {
                self.write_storage_prestate(
                    block_number,
                    entry.address,
                    key,
                    &old_value.to_previous_value(),
                )?;
            }
        }

        Ok(())
    }

    /// Write multiple blocks' plain state revert information.
    ///
    /// Sorts accounts and storage in parallel before writing to enable
    /// efficient append operations.
    fn write_plain_reverts(
        &self,
        first_block_number: u64,
        PlainStateReverts { accounts, storage }: &PlainStateReverts,
    ) -> Result<(), Self::Error> {
        use rayon::prelude::*;

        // Sort accounts and storage in parallel using rayon::join
        let (sorted_accounts, sorted_storage) = rayon::join(
            || {
                accounts
                    .par_iter()
                    .map(|block_accounts| {
                        let mut sorted: Vec<_> = block_accounts.iter().collect();
                        sorted.sort_by_key(|(addr, _)| *addr);
                        sorted
                    })
                    .collect::<Vec<_>>()
            },
            || {
                storage
                    .par_iter()
                    .map(|block_storage| {
                        let mut sorted: Vec<_> = block_storage.iter().collect();
                        sorted.sort_by_key(|entry| entry.address);
                        sorted
                    })
                    .collect::<Vec<_>>()
            },
        );

        // Write sequentially (DB writes must be ordered)
        sorted_accounts.iter().zip(sorted_storage.iter()).enumerate().try_for_each(
            |(idx, (acc, sto))| {
                self.write_plain_revert_sorted(first_block_number + idx as u64, acc, sto)
            },
        )
    }

    /// Write changed accounts from a [`StateChangeset`].
    fn write_changed_account(
        &self,
        address: &Address,
        account: &Option<AccountInfo>,
    ) -> Result<(), Self::Error> {
        let Some(info) = account.as_ref() else {
            // Account removal
            return self.queue_delete::<tables::PlainAccountState>(address);
        };

        let account = Account::from(info.clone());
        // bytecode_hash is None when code_hash == KECCAK256_EMPTY,
        // which doesn't need to be stored.
        if let Some((bytecode, code_hash)) = info.code.clone().zip(account.bytecode_hash) {
            self.put_bytecode(&code_hash, &bytecode)?;
        }
        self.put_account(address, &account)
    }

    /// Write changed storage from a [`StateChangeset`].
    fn write_changed_storage(
        &self,
        PlainStorageChangeset { address, wipe_storage, storage }: &PlainStorageChangeset,
    ) -> Result<(), Self::Error> {
        if *wipe_storage {
            return self.clear_k1_for::<tables::PlainStorageState>(address);
        }

        storage.iter().try_for_each(|(key, value)| self.put_storage(address, key, value))
    }

    /// Write changed contract bytecode from a [`StateChangeset`].
    fn write_changed_contracts(
        &self,
        code_hash: &B256,
        bytecode: &Bytecode,
    ) -> Result<(), Self::Error> {
        self.put_bytecode(code_hash, bytecode)
    }

    /// Write a state changeset for a specific block.
    fn write_state_changes(
        &self,
        StateChangeset { accounts, storage, contracts }: &StateChangeset,
    ) -> Result<(), Self::Error> {
        contracts.iter().try_for_each(|(code_hash, bytecode)| {
            self.write_changed_contracts(code_hash, bytecode)
        })?;
        accounts
            .iter()
            .try_for_each(|(address, account)| self.write_changed_account(address, account))?;
        storage
            .iter()
            .try_for_each(|storage_changeset| self.write_changed_storage(storage_changeset))?;
        Ok(())
    }

    /// Get all changed accounts with the list of block numbers in the given
    /// range.
    ///
    /// Iterates over entries starting from the first block in the range,
    /// collecting changes while the block number remains in range.
    // TODO: estimate capacity from block range size for better allocation
    fn changed_accounts_with_range(
        &self,
        range: RangeInclusive<BlockNumber>,
    ) -> Result<AHashMap<Address, Vec<u64>>, Self::Error> {
        self.traverse_dual::<tables::AccountChangeSets>()?
            .iter_from(range.start(), &Address::ZERO)?
            .process_results(|iter| {
                iter.take_while(|(num, _, _)| range.contains(num))
                    .map(|(num, addr, _)| (addr, num))
                    .into_group_map_by(|(addr, _)| *addr)
                    .into_iter()
                    .map(|(addr, pairs)| (addr, pairs.into_iter().map(|(_, num)| num).collect()))
                    .collect()
            })
    }

    /// Get all changed storages with the list of block numbers in the given
    /// range.
    ///
    /// Iterates over entries starting from the first block in the range,
    /// collecting changes while the block number remains in range.
    // TODO: estimate capacity from block range size for better allocation
    #[allow(clippy::type_complexity)]
    fn changed_storages_with_range(
        &self,
        range: RangeInclusive<BlockNumber>,
    ) -> Result<AHashMap<(Address, U256), Vec<u64>>, Self::Error> {
        self.traverse_dual::<tables::StorageChangeSets>()?
            .iter_from(&(*range.start(), Address::ZERO), &U256::ZERO)?
            .process_results(|iter| {
                iter.take_while(|(num_addr, _, _)| range.contains(&num_addr.0))
                    .map(|(num_addr, slot, _)| ((num_addr.1, slot), num_addr.0))
                    .into_group_map_by(|(key, _)| *key)
                    .into_iter()
                    .map(|(key, pairs)| (key, pairs.into_iter().map(|(_, num)| num).collect()))
                    .collect()
            })
    }

    /// Append a block's header and state changes in an inconsistent manner.
    ///
    /// This may leave the database in an inconsistent state. Users should
    /// prefer higher-level abstractions when possible.
    ///
    /// 1. It MUST be checked that the header is the child of the current chain
    ///    tip before calling this method.
    /// 2. After calling this method, the caller MUST call
    ///    `update_history_indices`.
    fn append_block_inconsistent(
        &self,
        header: &SealedHeader,
        state_changes: &BundleState,
    ) -> Result<(), Self::Error> {
        self.append_header(header)?;
        self.put_header_number_inconsistent(&header.hash(), header.number)?;

        let (state_changes, reverts) =
            state_changes.to_plain_state_and_reverts(OriginalValuesKnown::No);

        self.write_state_changes(&state_changes)?;
        self.write_plain_reverts(header.number, &reverts)
    }

    /// Append multiple blocks' headers and state changes in an inconsistent
    /// manner.
    ///
    /// This may leave the database in an inconsistent state. Users should
    /// prefer higher-level abstractions when possible.
    /// 1. It MUST be checked that the first header is the child of the current
    ///    chain tip before calling this method.
    /// 2. After calling this method, the caller MUST call
    ///    `update_history_indices`.
    fn append_blocks_inconsistent<'a>(
        &self,
        blocks: impl IntoIterator<Item = (&'a SealedHeader, &'a BundleState)>,
    ) -> Result<(), Self::Error> {
        blocks
            .into_iter()
            .try_for_each(|(header, state)| self.append_block_inconsistent(header, state))
    }
}
