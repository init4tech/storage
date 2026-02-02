//! MDBX backend implementation for [`ColdStorage`].
//!
//! This module provides an MDBX-based implementation of the cold storage
//! backend. It uses the table definitions from this crate and the MDBX
//! database environment from `signet-hot-mdbx`.

use crate::{
    ColdBlockHashIndex, ColdHeaders, ColdMetadata, ColdReceipts, ColdSignetEvents,
    ColdTransactions, ColdTxHashIndex, ColdZenithHeaders, MdbxColdError, MetadataKey,
};
use alloy::{consensus::Header, primitives::BlockNumber};
use signet_cold::{
    BlockData, BlockTag, ColdResult, ColdStorage, HeaderSpecifier, ReceiptSpecifier,
    SignetEventsSpecifier, TransactionSpecifier, ZenithHeaderSpecifier,
};
use signet_hot::{
    KeySer, MAX_KEY_SIZE, ValSer,
    model::{DualTableTraverse, HotKvWrite, KvTraverse, TableTraverse},
    tables::Table,
};
use signet_hot_mdbx::{DatabaseArguments, DatabaseEnv, DatabaseEnvKind};
use signet_storage_types::{DbSignetEvent, DbZenithHeader, Receipt, TransactionSigned, TxLocation};
use std::path::Path;

/// MDBX-based cold storage backend.
///
/// This backend stores historical blockchain data in an MDBX database.
/// It implements the [`ColdStorage`] trait for use with the cold storage
/// task runner.
#[derive(Debug)]
pub struct MdbxColdBackend {
    /// The MDBX environment.
    env: DatabaseEnv,
}

impl MdbxColdBackend {
    /// Open an existing MDBX cold storage database in read-only mode.
    pub fn open_ro(path: &Path) -> Result<Self, MdbxColdError> {
        let env = DatabaseArguments::new().open_ro(path)?;
        Ok(Self { env })
    }

    /// Open or create an MDBX cold storage database in read-write mode.
    pub fn open_rw(path: &Path) -> Result<Self, MdbxColdError> {
        let env = DatabaseArguments::new().open_rw(path)?;
        let backend = Self { env };
        backend.create_tables()?;
        Ok(backend)
    }

    /// Open an MDBX cold storage database with custom arguments.
    pub fn open(
        path: &Path,
        kind: DatabaseEnvKind,
        args: DatabaseArguments,
    ) -> Result<Self, MdbxColdError> {
        let env = DatabaseEnv::open(path, kind, args)?;
        let backend = Self { env };
        if kind.is_rw() {
            backend.create_tables()?;
        }
        Ok(backend)
    }

    /// Create all required tables if they don't exist.
    fn create_tables(&self) -> Result<(), MdbxColdError> {
        let tx = self.env.tx_rw()?;

        // Create single-key tables
        tx.queue_raw_create(
            ColdHeaders::NAME,
            ColdHeaders::DUAL_KEY_SIZE,
            ColdHeaders::FIXED_VAL_SIZE,
            ColdHeaders::INT_KEY,
        )?;
        tx.queue_raw_create(
            ColdZenithHeaders::NAME,
            ColdZenithHeaders::DUAL_KEY_SIZE,
            ColdZenithHeaders::FIXED_VAL_SIZE,
            ColdZenithHeaders::INT_KEY,
        )?;
        tx.queue_raw_create(
            ColdBlockHashIndex::NAME,
            ColdBlockHashIndex::DUAL_KEY_SIZE,
            ColdBlockHashIndex::FIXED_VAL_SIZE,
            ColdBlockHashIndex::INT_KEY,
        )?;
        tx.queue_raw_create(
            ColdTxHashIndex::NAME,
            ColdTxHashIndex::DUAL_KEY_SIZE,
            ColdTxHashIndex::FIXED_VAL_SIZE,
            ColdTxHashIndex::INT_KEY,
        )?;
        tx.queue_raw_create(
            ColdMetadata::NAME,
            ColdMetadata::DUAL_KEY_SIZE,
            ColdMetadata::FIXED_VAL_SIZE,
            ColdMetadata::INT_KEY,
        )?;

        // Create dual-key (DUPSORT) tables
        tx.queue_raw_create(
            ColdTransactions::NAME,
            ColdTransactions::DUAL_KEY_SIZE,
            ColdTransactions::FIXED_VAL_SIZE,
            ColdTransactions::INT_KEY,
        )?;
        tx.queue_raw_create(
            ColdReceipts::NAME,
            ColdReceipts::DUAL_KEY_SIZE,
            ColdReceipts::FIXED_VAL_SIZE,
            ColdReceipts::INT_KEY,
        )?;
        tx.queue_raw_create(
            ColdSignetEvents::NAME,
            ColdSignetEvents::DUAL_KEY_SIZE,
            ColdSignetEvents::FIXED_VAL_SIZE,
            ColdSignetEvents::INT_KEY,
        )?;

        tx.raw_commit()?;
        Ok(())
    }

    /// Resolve a block tag to a block number.
    fn resolve_tag(&self, tag: BlockTag) -> Result<Option<BlockNumber>, MdbxColdError> {
        let tx = self.env.tx()?;
        let metadata_key = match tag {
            BlockTag::Latest => MetadataKey::LatestBlock,
            BlockTag::Finalized => MetadataKey::FinalizedBlock,
            BlockTag::Safe => MetadataKey::SafeBlock,
            BlockTag::Earliest => MetadataKey::EarliestBlock,
        };
        Ok(TableTraverse::<ColdMetadata, _>::exact(
            &mut tx.new_cursor::<ColdMetadata>()?,
            &metadata_key,
        )?)
    }

    /// Get the block number for a block hash.
    fn get_block_by_hash(
        &self,
        hash: alloy::primitives::B256,
    ) -> Result<Option<BlockNumber>, MdbxColdError> {
        let tx = self.env.tx()?;
        Ok(TableTraverse::<ColdBlockHashIndex, _>::exact(
            &mut tx.new_cursor::<ColdBlockHashIndex>()?,
            &hash,
        )?)
    }

    /// Get transaction location by hash.
    fn get_tx_location(
        &self,
        hash: alloy::primitives::B256,
    ) -> Result<Option<TxLocation>, MdbxColdError> {
        let tx = self.env.tx()?;
        Ok(TableTraverse::<ColdTxHashIndex, _>::exact(
            &mut tx.new_cursor::<ColdTxHashIndex>()?,
            &hash,
        )?)
    }

    /// Internal implementation of get_header that returns MdbxColdError.
    fn get_header_inner(&self, spec: HeaderSpecifier) -> Result<Option<Header>, MdbxColdError> {
        let block_num = match spec {
            HeaderSpecifier::Number(n) => Some(n),
            HeaderSpecifier::Hash(h) => self.get_block_by_hash(h)?,
            HeaderSpecifier::Tag(tag) => self.resolve_tag(tag)?,
        };

        let Some(block_num) = block_num else {
            return Ok(None);
        };

        let tx = self.env.tx()?;
        Ok(TableTraverse::<ColdHeaders, _>::exact(
            &mut tx.new_cursor::<ColdHeaders>()?,
            &block_num,
        )?)
    }

    /// Internal implementation of get_transaction that returns MdbxColdError.
    fn get_transaction_inner(
        &self,
        spec: TransactionSpecifier,
    ) -> Result<Option<TransactionSigned>, MdbxColdError> {
        let (block, index) = match spec {
            TransactionSpecifier::Hash(h) => {
                let Some(loc) = self.get_tx_location(h)? else {
                    return Ok(None);
                };
                (loc.block, loc.index)
            }
            TransactionSpecifier::BlockAndIndex { block, index } => (block, index),
            TransactionSpecifier::BlockHashAndIndex { block_hash, index } => {
                let Some(block) = self.get_block_by_hash(block_hash)? else {
                    return Ok(None);
                };
                (block, index)
            }
        };

        let tx = self.env.tx()?;
        Ok(DualTableTraverse::<ColdTransactions, _>::exact_dual(
            &mut tx.new_cursor::<ColdTransactions>()?,
            &block,
            &index,
        )?)
    }

    /// Internal implementation of get_transactions_in_block.
    fn get_transactions_in_block_inner(
        &self,
        block: BlockNumber,
    ) -> Result<Vec<TransactionSigned>, MdbxColdError> {
        let tx = self.env.tx()?;
        let mut cursor = tx.new_cursor::<ColdTransactions>()?;

        let mut transactions = Vec::new();
        for item in DualTableTraverse::<ColdTransactions, _>::iter_k2(&mut cursor, &block)? {
            let (_, tx_signed) = item?;
            transactions.push(tx_signed);
        }

        Ok(transactions)
    }

    /// Internal implementation of get_transaction_count.
    fn get_transaction_count_inner(&self, block: BlockNumber) -> Result<u64, MdbxColdError> {
        let tx = self.env.tx()?;
        let mut cursor = tx.new_cursor::<ColdTransactions>()?;

        let mut count = 0u64;
        for item in DualTableTraverse::<ColdTransactions, _>::iter_k2(&mut cursor, &block)? {
            let _ = item?;
            count += 1;
        }

        Ok(count)
    }

    /// Internal implementation of get_receipt.
    fn get_receipt_inner(&self, spec: ReceiptSpecifier) -> Result<Option<Receipt>, MdbxColdError> {
        let (block, index) = match spec {
            ReceiptSpecifier::TxHash(h) => {
                let Some(loc) = self.get_tx_location(h)? else {
                    return Ok(None);
                };
                (loc.block, loc.index)
            }
            ReceiptSpecifier::BlockAndIndex { block, index } => (block, index),
        };

        let tx = self.env.tx()?;
        Ok(DualTableTraverse::<ColdReceipts, _>::exact_dual(
            &mut tx.new_cursor::<ColdReceipts>()?,
            &block,
            &index,
        )?)
    }

    /// Internal implementation of get_receipts_in_block.
    fn get_receipts_in_block_inner(
        &self,
        block: BlockNumber,
    ) -> Result<Vec<Receipt>, MdbxColdError> {
        let tx = self.env.tx()?;
        let mut cursor = tx.new_cursor::<ColdReceipts>()?;

        let mut receipts = Vec::new();
        for item in DualTableTraverse::<ColdReceipts, _>::iter_k2(&mut cursor, &block)? {
            let (_, receipt) = item?;
            receipts.push(receipt);
        }

        Ok(receipts)
    }

    /// Internal implementation of get_signet_events.
    fn get_signet_events_inner(
        &self,
        spec: SignetEventsSpecifier,
    ) -> Result<Vec<DbSignetEvent>, MdbxColdError> {
        let tx = self.env.tx()?;

        match spec {
            SignetEventsSpecifier::Block(block) => {
                let mut cursor = tx.new_cursor::<ColdSignetEvents>()?;
                let mut events = Vec::new();
                for item in DualTableTraverse::<ColdSignetEvents, _>::iter_k2(&mut cursor, &block)?
                {
                    let (_, event) = item?;
                    events.push(event);
                }
                Ok(events)
            }
            SignetEventsSpecifier::BlockRange { start, end } => {
                let mut events = Vec::new();
                for block in start..=end {
                    let mut cursor = tx.new_cursor::<ColdSignetEvents>()?;
                    for item in
                        DualTableTraverse::<ColdSignetEvents, _>::iter_k2(&mut cursor, &block)?
                    {
                        let (_, event) = item?;
                        events.push(event);
                    }
                }
                Ok(events)
            }
        }
    }

    /// Internal implementation of get_zenith_header.
    fn get_zenith_header_inner(
        &self,
        spec: ZenithHeaderSpecifier,
    ) -> Result<Option<DbZenithHeader>, MdbxColdError> {
        let block = match spec {
            ZenithHeaderSpecifier::Number(n) => n,
            ZenithHeaderSpecifier::Range { start, .. } => start,
        };

        let tx = self.env.tx()?;
        Ok(TableTraverse::<ColdZenithHeaders, _>::exact(
            &mut tx.new_cursor::<ColdZenithHeaders>()?,
            &block,
        )?)
    }

    /// Internal implementation of get_zenith_headers.
    fn get_zenith_headers_inner(
        &self,
        spec: ZenithHeaderSpecifier,
    ) -> Result<Vec<DbZenithHeader>, MdbxColdError> {
        let tx = self.env.tx()?;

        match spec {
            ZenithHeaderSpecifier::Number(n) => {
                let header: Option<DbZenithHeader> = TableTraverse::<ColdZenithHeaders, _>::exact(
                    &mut tx.new_cursor::<ColdZenithHeaders>()?,
                    &n,
                )?;
                Ok(header.into_iter().collect())
            }
            ZenithHeaderSpecifier::Range { start, end } => {
                let mut cursor = tx.new_cursor::<ColdZenithHeaders>()?;
                let mut headers = Vec::new();

                // Position at start
                let mut key_buf = [0u8; MAX_KEY_SIZE];
                let key_bytes = start.encode_key(&mut key_buf);

                if let Some((key, value)) = KvTraverse::<_>::lower_bound(&mut cursor, key_bytes)? {
                    let block_num = BlockNumber::decode_key(&key)?;
                    if block_num <= end {
                        headers.push(DbZenithHeader::decode_value(&value)?);
                    }

                    while let Some((key, value)) = KvTraverse::<_>::read_next(&mut cursor)? {
                        let block_num = BlockNumber::decode_key(&key)?;
                        if block_num > end {
                            break;
                        }
                        headers.push(DbZenithHeader::decode_value(&value)?);
                    }
                }

                Ok(headers)
            }
        }
    }

    /// Internal implementation of get_latest_block.
    fn get_latest_block_inner(&self) -> Result<Option<BlockNumber>, MdbxColdError> {
        let tx = self.env.tx()?;
        Ok(TableTraverse::<ColdMetadata, _>::exact(
            &mut tx.new_cursor::<ColdMetadata>()?,
            &MetadataKey::LatestBlock,
        )?)
    }

    /// Internal implementation of append_block.
    fn append_block_inner(&self, data: BlockData) -> Result<(), MdbxColdError> {
        let tx = self.env.tx_rw()?;
        let block = data.block_number();

        // Store header
        tx.queue_put::<ColdHeaders>(&block, &data.header)?;

        // Store header hash index
        let header_hash = data.header.hash_slow();
        tx.queue_put::<ColdBlockHashIndex>(&header_hash, &block)?;

        // Store transactions and their hash index
        for (idx, tx_signed) in data.transactions.iter().enumerate() {
            let tx_idx = idx as u64;
            tx.queue_put_dual::<ColdTransactions>(&block, &tx_idx, tx_signed)?;

            // Store tx hash index
            let tx_hash = *tx_signed.hash();
            let location = TxLocation::new(block, tx_idx);
            tx.queue_put::<ColdTxHashIndex>(&tx_hash, &location)?;
        }

        // Store receipts
        for (idx, receipt) in data.receipts.iter().enumerate() {
            let receipt_idx = idx as u64;
            tx.queue_put_dual::<ColdReceipts>(&block, &receipt_idx, receipt)?;
        }

        // Store signet events
        for (idx, event) in data.signet_events.iter().enumerate() {
            let event_idx = idx as u64;
            tx.queue_put_dual::<ColdSignetEvents>(&block, &event_idx, event)?;
        }

        // Store zenith header if present
        if let Some(zh) = &data.zenith_header {
            tx.queue_put::<ColdZenithHeaders>(&block, zh)?;
        }

        // Update latest block if this is higher
        let current_latest: Option<BlockNumber> = TableTraverse::<ColdMetadata, _>::exact(
            &mut tx.new_cursor::<ColdMetadata>()?,
            &MetadataKey::LatestBlock,
        )?;
        let new_latest = current_latest.map_or(block, |prev: BlockNumber| prev.max(block));
        tx.queue_put::<ColdMetadata>(&MetadataKey::LatestBlock, &new_latest)?;

        // Update earliest block if not set or if this is lower
        let current_earliest: Option<BlockNumber> = TableTraverse::<ColdMetadata, _>::exact(
            &mut tx.new_cursor::<ColdMetadata>()?,
            &MetadataKey::EarliestBlock,
        )?;
        let new_earliest = current_earliest.map_or(block, |prev: BlockNumber| prev.min(block));
        tx.queue_put::<ColdMetadata>(&MetadataKey::EarliestBlock, &new_earliest)?;

        tx.raw_commit()?;
        Ok(())
    }

    /// Internal implementation of truncate_above.
    fn truncate_above_inner(&self, block: BlockNumber) -> Result<(), MdbxColdError> {
        let tx = self.env.tx_rw()?;

        // Collect headers to remove for hash index cleanup
        let mut headers_to_remove: Vec<(BlockNumber, Header)> = Vec::new();
        {
            let mut cursor = tx.new_cursor::<ColdHeaders>()?;

            // Position after the block we want to keep
            let mut key_buf = [0u8; MAX_KEY_SIZE];
            let start_block = block + 1;
            let key_bytes = start_block.encode_key(&mut key_buf);

            if let Some((key, value)) = KvTraverse::<_>::lower_bound(&mut cursor, key_bytes)? {
                let block_num = BlockNumber::decode_key(&key)?;
                let header = Header::decode_value(&value)?;
                headers_to_remove.push((block_num, header));

                while let Some((key, value)) = KvTraverse::<_>::read_next(&mut cursor)? {
                    let block_num = BlockNumber::decode_key(&key)?;
                    let header = Header::decode_value(&value)?;
                    headers_to_remove.push((block_num, header));
                }
            }
        }

        // Collect transactions to remove for hash index cleanup
        let mut tx_hashes_to_remove: Vec<alloy::primitives::B256> = Vec::new();
        for (block_num, _) in &headers_to_remove {
            let mut cursor = tx.new_cursor::<ColdTransactions>()?;
            for item in DualTableTraverse::<ColdTransactions, _>::iter_k2(&mut cursor, block_num)? {
                let (_, tx_signed): (u64, TransactionSigned) = item?;
                tx_hashes_to_remove.push(*tx_signed.hash());
            }
        }

        // Delete headers and their hash index entries
        for (block_num, header) in &headers_to_remove {
            tx.queue_delete::<ColdHeaders>(block_num)?;
            tx.queue_delete::<ColdBlockHashIndex>(&header.hash_slow())?;
        }

        // Delete transaction hash index entries
        for tx_hash in &tx_hashes_to_remove {
            tx.queue_delete::<ColdTxHashIndex>(tx_hash)?;
        }

        // Delete transactions, receipts, and signet events for removed blocks
        for (block_num, _) in &headers_to_remove {
            // Delete all transactions for this block
            tx.clear_k1_for::<ColdTransactions>(block_num)?;

            // Delete all receipts for this block
            tx.clear_k1_for::<ColdReceipts>(block_num)?;

            // Delete all signet events for this block
            tx.clear_k1_for::<ColdSignetEvents>(block_num)?;

            // Delete zenith header for this block
            tx.queue_delete::<ColdZenithHeaders>(block_num)?;
        }

        // Update latest block metadata
        if !headers_to_remove.is_empty() {
            // Find the new latest block
            let mut cursor = tx.new_cursor::<ColdHeaders>()?;
            if let Some((key, _)) = KvTraverse::<_>::last(&mut cursor)? {
                let new_latest = BlockNumber::decode_key(&key)?;
                tx.queue_put::<ColdMetadata>(&MetadataKey::LatestBlock, &new_latest)?;
            } else {
                // No blocks left, remove latest block metadata
                tx.queue_delete::<ColdMetadata>(&MetadataKey::LatestBlock)?;
            }
        }

        tx.raw_commit()?;
        Ok(())
    }
}

impl ColdStorage for MdbxColdBackend {
    async fn get_header(&self, spec: HeaderSpecifier) -> ColdResult<Option<Header>> {
        Ok(self.get_header_inner(spec)?)
    }

    async fn get_headers(&self, specs: Vec<HeaderSpecifier>) -> ColdResult<Vec<Option<Header>>> {
        let mut results = Vec::with_capacity(specs.len());
        for spec in specs {
            results.push(self.get_header_inner(spec)?);
        }
        Ok(results)
    }

    async fn get_transaction(
        &self,
        spec: TransactionSpecifier,
    ) -> ColdResult<Option<TransactionSigned>> {
        Ok(self.get_transaction_inner(spec)?)
    }

    async fn get_transactions_in_block(
        &self,
        block: BlockNumber,
    ) -> ColdResult<Vec<TransactionSigned>> {
        Ok(self.get_transactions_in_block_inner(block)?)
    }

    async fn get_transaction_count(&self, block: BlockNumber) -> ColdResult<u64> {
        Ok(self.get_transaction_count_inner(block)?)
    }

    async fn get_receipt(&self, spec: ReceiptSpecifier) -> ColdResult<Option<Receipt>> {
        Ok(self.get_receipt_inner(spec)?)
    }

    async fn get_receipts_in_block(&self, block: BlockNumber) -> ColdResult<Vec<Receipt>> {
        Ok(self.get_receipts_in_block_inner(block)?)
    }

    async fn get_signet_events(
        &self,
        spec: SignetEventsSpecifier,
    ) -> ColdResult<Vec<DbSignetEvent>> {
        Ok(self.get_signet_events_inner(spec)?)
    }

    async fn get_zenith_header(
        &self,
        spec: ZenithHeaderSpecifier,
    ) -> ColdResult<Option<DbZenithHeader>> {
        Ok(self.get_zenith_header_inner(spec)?)
    }

    async fn get_zenith_headers(
        &self,
        spec: ZenithHeaderSpecifier,
    ) -> ColdResult<Vec<DbZenithHeader>> {
        Ok(self.get_zenith_headers_inner(spec)?)
    }

    async fn get_latest_block(&self) -> ColdResult<Option<BlockNumber>> {
        Ok(self.get_latest_block_inner()?)
    }

    async fn append_block(&self, data: BlockData) -> ColdResult<()> {
        Ok(self.append_block_inner(data)?)
    }

    async fn append_blocks(&self, data: Vec<BlockData>) -> ColdResult<()> {
        for block_data in data {
            self.append_block_inner(block_data)?;
        }
        Ok(())
    }

    async fn truncate_above(&self, block: BlockNumber) -> ColdResult<()> {
        Ok(self.truncate_above_inner(block)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use signet_cold::conformance::conformance;
    use tempfile::tempdir;

    #[tokio::test]
    async fn mdbx_backend_conformance() {
        let dir = tempdir().unwrap();
        let backend = MdbxColdBackend::open_rw(dir.path()).unwrap();
        conformance(&backend).await.unwrap();
    }
}
