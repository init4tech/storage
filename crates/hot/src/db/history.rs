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
use ahash::AHashMap;
use alloy::primitives::{Address, BlockNumber, U256};
use itertools::Itertools;
use signet_storage_types::{Account, BlockNumberList, ShardedKey};
use std::ops::RangeInclusive;

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
}
