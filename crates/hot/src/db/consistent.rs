use crate::{
    db::{HistoryError, UnsafeDbWrite, UnsafeHistoryWrite},
    tables,
};
use ahash::AHashSet;
use alloy::{
    consensus::Sealable,
    genesis::{Genesis, GenesisAccount},
    primitives::{Address, BlockNumber, U256, address},
};
use signet_storage_types::{Account, BlockNumberList, EthereumHardfork, SealedHeader};
use trevm::revm::{database::BundleState, state::Bytecode};

/// Maximum address value (all bits set to 1).
const ADDRESS_MAX: Address = address!("0xffffffffffffffffffffffffffffffffffffffff");

/// Trait for database write operations on hot history tables. This trait
/// maintains a consistent state of the database.
pub trait HistoryWrite: UnsafeDbWrite + UnsafeHistoryWrite {
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
    fn append_blocks(
        &self,
        blocks: &[(SealedHeader, BundleState)],
    ) -> Result<(), HistoryError<Self::Error>> {
        self.validate_chain_extension(blocks.iter().map(|(h, _)| h))?;

        let Some(first_num) = blocks.first().map(|(h, _)| h.number) else { return Ok(()) };
        let last_num = blocks.last().map(|(h, _)| h.number).expect("non-empty; qed");
        self.append_blocks_inconsistent(blocks)?;

        self.update_history_indices_inconsistent(first_num..=last_num)
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

                // Filter history index
                if let Some((shard_key, list)) = self.last_account_history(address)? {
                    self.queue_delete_dual::<tables::AccountsHistory>(&address, &shard_key)?;
                    let mut filtered = list.iter().take_while(|&bn| bn <= block).peekable();
                    if filtered.peek().is_some() {
                        self.write_account_history(
                            &address,
                            u64::MAX,
                            &BlockNumberList::new_pre_sorted(filtered),
                        )?;
                    }
                }
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

                // Filter history index
                if let Some((shard_key, list)) = self.last_storage_history(&address, &slot)? {
                    self.queue_delete_dual::<tables::StorageHistory>(&address, &shard_key)?;
                    let mut filtered = list.iter().take_while(|&bn| bn <= block).peekable();
                    if filtered.peek().is_some() {
                        self.write_storage_history(
                            &address,
                            slot,
                            u64::MAX,
                            &BlockNumberList::new_pre_sorted(filtered),
                        )?;
                    }
                }
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
            self.delete_header_number(&header.hash_slow())?;

            // Continue with remaining entries
            while let Some((block_num, header)) = header_cursor.read_next()? {
                if block_num > last_block {
                    break;
                }
                self.delete_header_number(&header.hash_slow())?;
            }
        }
        self.traverse_mut::<tables::Headers>()?.delete_range_inclusive(first_block..=last_block)?;

        Ok(())
    }

    /// Load genesis data into the database.
    ///
    /// This operation is only valid on an empty database.
    fn laod_genesis(
        &self,
        genesis: &Genesis,
        genesis_hardforks: &EthereumHardfork,
    ) -> Result<(), HistoryError<Self::Error>> {
        // Check that the database is empty
        if self.get_chain_tip().map_err(HistoryError::Db)?.is_some() {
            return Err(HistoryError::DbNotEmpty);
        }
        let header = signet_storage_types::genesis_header(genesis, genesis_hardforks).seal_slow();
        self.append_blocks(&[(header, BundleState::default())])?;

        // For each account in the genesis allocation, insert account and
        // storage state
        for (address, account) in genesis.alloc.iter() {
            let GenesisAccount { nonce, balance, code, storage, .. } = account;

            // Insert bytecode if present
            let bytecode_hash = if let Some(code_bytes) = code {
                let hash = alloy::primitives::keccak256(code_bytes);
                self.put_bytecode(&hash, &Bytecode::new_raw(code_bytes.clone()))?;
                Some(hash)
            } else {
                None
            };

            // Insert account state
            self.put_account(
                &address,
                &Account { nonce: nonce.unwrap_or_default(), balance: *balance, bytecode_hash },
            )?;

            // Insert storage entries
            for (slot, value) in storage.iter().flatten() {
                self.put_storage(
                    address,
                    &U256::from_be_bytes(**slot),
                    &U256::from_be_bytes(**value),
                )?;
            }
        }
        Ok(())
    }
}

impl<T> HistoryWrite for T where T: UnsafeDbWrite + UnsafeHistoryWrite {}
