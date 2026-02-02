use crate::{
    db::{HistoryError, HistoryRead},
    model::HotKvWrite,
    tables,
};
use ahash::AHashMap;
use alloy::{
    consensus::Header,
    primitives::{Address, B256, BlockNumber, U256},
};
use itertools::Itertools;
use signet_storage_types::{Account, BlockNumberList, SealedHeader, ShardedKey};
use std::ops::RangeInclusive;
use trevm::revm::{
    bytecode::Bytecode,
    database::{
        BundleState, OriginalValuesKnown,
        states::{PlainStateReverts, PlainStorageChangeset, PlainStorageRevert, StateChangeset},
    },
    state::AccountInfo,
};

/// Bundle state initialization type.
/// Maps address -> (old_account, new_account, storage_changes)
/// where storage_changes maps slot (B256) -> (old_value, new_value)
pub type BundleInit =
    AHashMap<Address, (Option<Account>, Option<Account>, AHashMap<B256, (U256, U256)>)>;

/// Trait for database write operations on standard hot tables.
///
/// This trait is low-level, and usage may leave the database in an
/// inconsistent state if not used carefully. Users should prefer
/// [`HotHistoryWrite`] or higher-level abstractions when possible.
///
/// [`HotHistoryWrite`]: crate::db::HistoryWrite
pub trait UnsafeDbWrite: HotKvWrite + super::sealed::Sealed {
    /// Write a block header. This will leave the DB in an inconsistent state
    /// until the corresponding header number is also written. Users should
    /// prefer [`Self::put_header`] instead.
    fn put_header_inconsistent(&self, header: &Header) -> Result<(), Self::Error> {
        self.queue_put::<tables::Headers>(&header.number, header)
    }

    /// Append a block header. Block number must be > all existing block numbers.
    ///
    /// This will leave the DB in an inconsistent state until the corresponding
    /// header number is also written. Users should prefer [`Self::put_header`]
    /// instead.
    fn append_header(&self, header: &Header) -> Result<(), Self::Error> {
        self.queue_append::<tables::Headers>(&header.number, header)
    }

    /// Write a block number by its hash. This will leave the DB in an
    /// inconsistent state until the corresponding header is also written.
    /// Users should prefer [`Self::put_header`] instead.
    fn put_header_number_inconsistent(&self, hash: &B256, number: u64) -> Result<(), Self::Error> {
        self.queue_put::<tables::HeaderNumbers>(hash, &number)
    }

    /// Write contract Bytecode by its hash.
    fn put_bytecode(&self, code_hash: &B256, bytecode: &Bytecode) -> Result<(), Self::Error> {
        self.queue_put::<tables::Bytecodes>(code_hash, bytecode)
    }

    /// Write an account by its address.
    fn put_account(&self, address: &Address, account: &Account) -> Result<(), Self::Error> {
        self.queue_put::<tables::PlainAccountState>(address, account)
    }

    /// Append an account by its address. This should generally only be used
    /// when initializing the database (e.g., from genesis).
    fn append_account(&self, address: &Address, account: &Account) -> Result<(), Self::Error> {
        self.queue_append::<tables::PlainAccountState>(address, account)
    }

    /// Write a storage entry by its address and key.
    fn put_storage(&self, address: &Address, key: &U256, entry: &U256) -> Result<(), Self::Error> {
        self.queue_put_dual::<tables::PlainStorageState>(address, key, entry)
    }

    /// Append a storage entry by its address and key. This should generally
    /// only be used when initializing the database (e.g., from genesis).
    fn append_storage(
        &self,
        address: &Address,
        key: &U256,
        entry: &U256,
    ) -> Result<(), Self::Error> {
        self.queue_append_dual::<tables::PlainStorageState>(address, key, entry)
    }

    /// Write a sealed block header (header + number).
    fn put_header(&self, header: &SealedHeader) -> Result<(), Self::Error> {
        self.put_header_inconsistent(header.as_ref())
            .and_then(|_| self.put_header_number_inconsistent(&header.hash(), header.number))
    }

    /// Delete a header by block number.
    fn delete_header(&self, number: u64) -> Result<(), Self::Error> {
        self.queue_delete::<tables::Headers>(&number)
    }

    /// Delete a header number mapping by hash.
    fn delete_header_number(&self, hash: &B256) -> Result<(), Self::Error> {
        self.queue_delete::<tables::HeaderNumbers>(hash)
    }

    /// Commit the write transaction.
    fn commit(self) -> Result<(), Self::Error>
    where
        Self: Sized,
    {
        HotKvWrite::raw_commit(self)
    }
}

impl<T> UnsafeDbWrite for T where T: HotKvWrite {}

/// Trait for history write operations.
///
/// These tables maintain historical information about accounts and storage
/// changes, and their contents can be used to reconstruct past states or
/// roll back changes.
pub trait UnsafeHistoryWrite: UnsafeDbWrite + HistoryRead {
    /// Maintain a list of block numbers where an account was touched.
    ///
    /// Accounts are keyed
    fn write_account_history(
        &self,
        address: &Address,
        latest_height: u64,
        touched: &BlockNumberList,
    ) -> Result<(), Self::Error> {
        self.queue_put_dual::<tables::AccountsHistory>(address, &latest_height, touched)
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

    /// Write storage history, by highest block number and touched block
    /// numbers.
    fn write_storage_history(
        &self,
        address: &Address,
        slot: U256,
        highest_block_number: u64,
        touched: &BlockNumberList,
    ) -> Result<(), Self::Error> {
        let sharded_key = ShardedKey::new(slot, highest_block_number);
        self.queue_put_dual::<tables::StorageHistory>(address, &sharded_key, touched)
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

            if let Some(bytecode) = info.as_ref().and_then(|info| info.code.clone()) {
                let code_hash = account.bytecode_hash.expect("info has bytecode; hash must exist");
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
        if let Some(bytecode) = info.code.clone() {
            let code_hash = account.bytecode_hash.expect("info has bytecode; hash must exist");
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

    /// Append account history indices for multiple accounts.
    fn append_account_history_index(
        &self,
        index_updates: impl IntoIterator<Item = (Address, impl IntoIterator<Item = u64>)>,
    ) -> Result<(), HistoryError<Self::Error>> {
        for (acct, indices) in index_updates {
            let existing = self.last_account_history(acct)?;
            append_to_sharded_history(
                existing,
                indices,
                |key| self.queue_delete_dual::<tables::AccountsHistory>(&acct, &key),
                |height, list| self.write_account_history(&acct, height, list),
            )?;
        }
        Ok(())
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

    /// Append storage history indices for multiple (address, slot) pairs.
    fn append_storage_history_index(
        &self,
        index_updates: impl IntoIterator<Item = ((Address, U256), impl IntoIterator<Item = u64>)>,
    ) -> Result<(), HistoryError<Self::Error>> {
        for ((addr, slot), indices) in index_updates {
            let existing = self.last_storage_history(&addr, &slot)?;
            append_to_sharded_history(
                existing,
                indices,
                |key| self.queue_delete_dual::<tables::StorageHistory>(&addr, &key),
                |height, list| self.write_storage_history(&addr, slot, height, list),
            )?;
        }
        Ok(())
    }

    /// Update the history indices for accounts and storage in the given block
    /// range.
    fn update_history_indices_inconsistent(
        &self,
        range: RangeInclusive<BlockNumber>,
    ) -> Result<(), HistoryError<Self::Error>> {
        // account history stage
        {
            let indices = self.changed_accounts_with_range(range.clone())?;
            self.append_account_history_index(indices)?;
        }

        // storage history stage
        {
            let indices = self.changed_storages_with_range(range)?;
            self.append_storage_history_index(indices)?;
        }

        Ok(())
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
        self.append_header(header.as_ref())?;
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
    fn append_blocks_inconsistent(
        &self,
        blocks: &[(SealedHeader, BundleState)],
    ) -> Result<(), Self::Error> {
        blocks.iter().try_for_each(|(header, state)| self.append_block_inconsistent(header, state))
    }
}

impl<T> UnsafeHistoryWrite for T where T: UnsafeDbWrite + HotKvWrite {}

/// Append indices to a sharded history entry, handling shard splitting.
///
/// This helper handles the common pattern of:
/// 1. Appending new block numbers to an existing shard
/// 2. Deleting the old shard if it exists
/// 3. Splitting into multiple shards if the result exceeds the shard size
///
/// # Arguments
/// - `existing`: The current last shard (key, list) if any
/// - `indices`: New block numbers to append
/// - `delete_old`: Called to delete the old shard key before writing new ones
/// - `write_shard`: Called for each resulting shard (highest_block, list)
fn append_to_sharded_history<K, E, D, W>(
    existing: Option<(K, BlockNumberList)>,
    indices: impl IntoIterator<Item = u64>,
    mut delete_old: D,
    mut write_shard: W,
) -> Result<(), HistoryError<E>>
where
    E: std::error::Error,
    D: FnMut(K) -> Result<(), E>,
    W: FnMut(u64, &BlockNumberList) -> Result<(), E>,
{
    let (old_key, last_shard) =
        existing.map_or_else(|| (None, BlockNumberList::default()), |(k, list)| (Some(k), list));
    let mut last_shard = last_shard;

    last_shard.append(indices).map_err(HistoryError::IntList)?;

    // Delete the existing shard before writing new ones to avoid duplicates
    if let Some(key) = old_key {
        delete_old(key).map_err(HistoryError::Db)?;
    }

    // Fast path: all indices fit in one shard
    if last_shard.len() <= ShardedKey::SHARD_COUNT as u64 {
        return write_shard(u64::MAX, &last_shard).map_err(HistoryError::Db);
    }

    // Slow path: rechunk into multiple shards
    // Reuse a single buffer to avoid allocating a new Vec per chunk
    let mut chunk_buf = Vec::with_capacity(ShardedKey::SHARD_COUNT);
    let mut iter = last_shard.iter().peekable();

    while iter.peek().is_some() {
        chunk_buf.clear();
        chunk_buf.extend(iter.by_ref().take(ShardedKey::SHARD_COUNT));

        let highest = if iter.peek().is_some() {
            *chunk_buf.last().expect("chunk_buf is non-empty")
        } else {
            // Insert last list with `u64::MAX`
            u64::MAX
        };

        let shard = BlockNumberList::new_pre_sorted(chunk_buf.iter().copied());
        write_shard(highest, &shard).map_err(HistoryError::Db)?;
    }
    Ok(())
}
