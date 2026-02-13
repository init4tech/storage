//! MDBX backend implementation for [`ColdStorage`].
//!
//! This module provides an MDBX-based implementation of the cold storage
//! backend. It uses the table definitions from this crate and the MDBX
//! database environment from `signet-hot-mdbx`.

use crate::{
    ColdBlockHashIndex, ColdHeaders, ColdReceipts, ColdSignetEvents, ColdTransactions,
    ColdTxHashIndex, ColdZenithHeaders, MdbxColdError,
};
use alloy::primitives::BlockNumber;
use signet_cold::{
    BlockData, ColdReceipt, ColdResult, ColdStorage, Confirmed, Filter, HeaderSpecifier,
    ReceiptSpecifier, SignetEventsSpecifier, TransactionSpecifier, ZenithHeaderSpecifier,
};
use signet_hot::{
    KeySer, MAX_KEY_SIZE, ValSer,
    model::{HotKvRead, HotKvWrite, KvTraverse},
    tables::Table,
};
use signet_hot_mdbx::{DatabaseArguments, DatabaseEnv, DatabaseEnvKind};
use signet_storage_types::{
    ConfirmationMeta, DbSignetEvent, DbZenithHeader, IndexedReceipt, SealedHeader,
    TransactionSigned, TxLocation,
};
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

    fn create_tables(&self) -> Result<(), MdbxColdError> {
        let tx = self.env.tx_rw()?;

        for (name, dual_key_size, fixed_val_size, int_key) in [
            (
                ColdHeaders::NAME,
                ColdHeaders::DUAL_KEY_SIZE,
                ColdHeaders::FIXED_VAL_SIZE,
                ColdHeaders::INT_KEY,
            ),
            (
                ColdZenithHeaders::NAME,
                ColdZenithHeaders::DUAL_KEY_SIZE,
                ColdZenithHeaders::FIXED_VAL_SIZE,
                ColdZenithHeaders::INT_KEY,
            ),
            (
                ColdBlockHashIndex::NAME,
                ColdBlockHashIndex::DUAL_KEY_SIZE,
                ColdBlockHashIndex::FIXED_VAL_SIZE,
                ColdBlockHashIndex::INT_KEY,
            ),
            (
                ColdTxHashIndex::NAME,
                ColdTxHashIndex::DUAL_KEY_SIZE,
                ColdTxHashIndex::FIXED_VAL_SIZE,
                ColdTxHashIndex::INT_KEY,
            ),
            (
                ColdTransactions::NAME,
                ColdTransactions::DUAL_KEY_SIZE,
                ColdTransactions::FIXED_VAL_SIZE,
                ColdTransactions::INT_KEY,
            ),
            (
                ColdReceipts::NAME,
                ColdReceipts::DUAL_KEY_SIZE,
                ColdReceipts::FIXED_VAL_SIZE,
                ColdReceipts::INT_KEY,
            ),
            (
                ColdSignetEvents::NAME,
                ColdSignetEvents::DUAL_KEY_SIZE,
                ColdSignetEvents::FIXED_VAL_SIZE,
                ColdSignetEvents::INT_KEY,
            ),
        ] {
            tx.queue_raw_create(name, dual_key_size, fixed_val_size, int_key)?;
        }

        tx.raw_commit()?;
        Ok(())
    }

    fn get_header_inner(
        &self,
        spec: HeaderSpecifier,
    ) -> Result<Option<SealedHeader>, MdbxColdError> {
        let tx = self.env.tx()?;
        let block_num = match spec {
            HeaderSpecifier::Number(n) => n,
            HeaderSpecifier::Hash(h) => {
                let Some(n) = tx.traverse::<ColdBlockHashIndex>()?.exact(&h)? else {
                    return Ok(None);
                };
                n
            }
        };
        Ok(tx.traverse::<ColdHeaders>()?.exact(&block_num)?)
    }

    fn get_headers_inner(
        &self,
        specs: Vec<HeaderSpecifier>,
    ) -> Result<Vec<Option<SealedHeader>>, MdbxColdError> {
        let tx = self.env.tx()?;
        specs
            .into_iter()
            .map(|spec| {
                let block_num = match spec {
                    HeaderSpecifier::Number(n) => Some(n),
                    HeaderSpecifier::Hash(h) => tx.traverse::<ColdBlockHashIndex>()?.exact(&h)?,
                };
                block_num
                    .map(|n| tx.traverse::<ColdHeaders>()?.exact(&n))
                    .transpose()
                    .map(Option::flatten)
            })
            .collect::<Result<_, _>>()
            .map_err(Into::into)
    }

    fn get_transaction_inner(
        &self,
        spec: TransactionSpecifier,
    ) -> Result<Option<Confirmed<TransactionSigned>>, MdbxColdError> {
        let tx = self.env.tx()?;
        let (block, index) = match spec {
            TransactionSpecifier::Hash(h) => {
                let Some(loc) = tx.traverse::<ColdTxHashIndex>()?.exact(&h)? else {
                    return Ok(None);
                };
                (loc.block, loc.index)
            }
            TransactionSpecifier::BlockAndIndex { block, index } => (block, index),
            TransactionSpecifier::BlockHashAndIndex { block_hash, index } => {
                let Some(block) = tx.traverse::<ColdBlockHashIndex>()?.exact(&block_hash)? else {
                    return Ok(None);
                };
                (block, index)
            }
        };
        let Some(signed_tx) = tx.traverse_dual::<ColdTransactions>()?.exact_dual(&block, &index)?
        else {
            return Ok(None);
        };
        let Some(sealed) = tx.traverse::<ColdHeaders>()?.exact(&block)? else {
            return Ok(None);
        };
        let meta = ConfirmationMeta::new(block, sealed.hash(), index);
        Ok(Some(Confirmed::new(signed_tx, meta)))
    }

    fn get_receipt_inner(
        &self,
        spec: ReceiptSpecifier,
    ) -> Result<Option<ColdReceipt>, MdbxColdError> {
        let tx = self.env.tx()?;
        let (block, index) = match spec {
            ReceiptSpecifier::TxHash(h) => {
                let Some(loc) = tx.traverse::<ColdTxHashIndex>()?.exact(&h)? else {
                    return Ok(None);
                };
                (loc.block, loc.index)
            }
            ReceiptSpecifier::BlockAndIndex { block, index } => (block, index),
        };
        let Some(sealed) = tx.traverse::<ColdHeaders>()?.exact(&block)? else {
            return Ok(None);
        };
        let Some(ir) = tx.traverse_dual::<ColdReceipts>()?.exact_dual(&block, &index)? else {
            return Ok(None);
        };
        Ok(Some(ColdReceipt::new(ir, &sealed, index)))
    }

    fn get_zenith_header_by_number(
        &self,
        block: BlockNumber,
    ) -> Result<Option<DbZenithHeader>, MdbxColdError> {
        let tx = self.env.tx()?;
        Ok(tx.traverse::<ColdZenithHeaders>()?.exact(&block)?)
    }

    fn collect_transactions_in_block(
        &self,
        block: BlockNumber,
    ) -> Result<Vec<TransactionSigned>, MdbxColdError> {
        let tx = self.env.tx()?;
        tx.traverse_dual::<ColdTransactions>()?
            .iter_k2(&block)?
            .map(|item| item.map(|(_, v)| v))
            .collect::<Result<_, _>>()
            .map_err(Into::into)
    }

    fn count_transactions_in_block(&self, block: BlockNumber) -> Result<u64, MdbxColdError> {
        let tx = self.env.tx()?;
        let mut count = 0u64;
        for item in tx.traverse_dual::<ColdTransactions>()?.iter_k2(&block)? {
            item?;
            count += 1;
        }
        Ok(count)
    }

    fn collect_receipts_in_block(
        &self,
        block: BlockNumber,
    ) -> Result<Vec<ColdReceipt>, MdbxColdError> {
        let tx = self.env.tx()?;
        let Some(sealed) = tx.traverse::<ColdHeaders>()?.exact(&block)? else {
            return Ok(Vec::new());
        };
        tx.traverse_dual::<ColdReceipts>()?
            .iter_k2(&block)?
            .map(|item| {
                let (idx, ir) = item?;
                Ok::<_, MdbxColdError>(ColdReceipt::new(ir, &sealed, idx))
            })
            .collect()
    }

    fn collect_signet_events_in_block(
        &self,
        block: BlockNumber,
    ) -> Result<Vec<DbSignetEvent>, MdbxColdError> {
        let tx = self.env.tx()?;
        tx.traverse_dual::<ColdSignetEvents>()?
            .iter_k2(&block)?
            .map(|item| item.map(|(_, v)| v))
            .collect::<Result<_, _>>()
            .map_err(Into::into)
    }

    fn collect_signet_events_in_range(
        &self,
        start: BlockNumber,
        end: BlockNumber,
    ) -> Result<Vec<DbSignetEvent>, MdbxColdError> {
        let tx = self.env.tx()?;
        let mut events = Vec::new();
        for block in start..=end {
            for item in tx.traverse_dual::<ColdSignetEvents>()?.iter_k2(&block)? {
                events.push(item?.1);
            }
        }
        Ok(events)
    }

    fn collect_zenith_headers_in_range(
        &self,
        start: BlockNumber,
        end: BlockNumber,
    ) -> Result<Vec<DbZenithHeader>, MdbxColdError> {
        let tx = self.env.tx()?;
        let mut cursor = tx.new_cursor::<ColdZenithHeaders>()?;
        let mut headers = Vec::new();

        let mut key_buf = [0u8; MAX_KEY_SIZE];
        let key_bytes = start.encode_key(&mut key_buf);

        let Some((key, value)) = cursor.lower_bound(key_bytes)? else {
            return Ok(headers);
        };

        let block_num = BlockNumber::decode_key(&key)?;
        if block_num <= end {
            headers.push(DbZenithHeader::decode_value(&value)?);
        }

        while let Some((key, value)) = cursor.read_next()? {
            let block_num = BlockNumber::decode_key(&key)?;
            if block_num > end {
                break;
            }
            headers.push(DbZenithHeader::decode_value(&value)?);
        }

        Ok(headers)
    }

    fn append_block_inner(&self, data: BlockData) -> Result<(), MdbxColdError> {
        let tx = self.env.tx_rw()?;
        let block = data.block_number();

        // Store the sealed header (hash already cached)
        tx.queue_put::<ColdHeaders>(&block, &data.header)?;
        tx.queue_put::<ColdBlockHashIndex>(&data.header.hash(), &block)?;

        // Store transactions and build hash index
        let tx_hashes: Vec<_> = data
            .transactions
            .iter()
            .enumerate()
            .map(|(idx, tx_signed)| {
                let tx_idx = idx as u64;
                tx.queue_put_dual::<ColdTransactions>(&block, &tx_idx, tx_signed)?;
                tx.queue_put::<ColdTxHashIndex>(tx_signed.hash(), &TxLocation::new(block, tx_idx))?;
                Ok(*tx_signed.hash())
            })
            .collect::<Result<_, MdbxColdError>>()?;

        // Compute and store IndexedReceipts with precomputed metadata
        let mut first_log_index = 0u64;
        let mut prior_cumulative_gas = 0u64;
        for (idx, (receipt, tx_hash)) in data.receipts.into_iter().zip(tx_hashes).enumerate() {
            let gas_used = receipt.inner.cumulative_gas_used - prior_cumulative_gas;
            prior_cumulative_gas = receipt.inner.cumulative_gas_used;
            let ir = IndexedReceipt { receipt, tx_hash, first_log_index, gas_used };
            first_log_index += ir.receipt.inner.logs.len() as u64;
            tx.queue_put_dual::<ColdReceipts>(&block, &(idx as u64), &ir)?;
        }

        for (idx, event) in data.signet_events.iter().enumerate() {
            tx.queue_put_dual::<ColdSignetEvents>(&block, &(idx as u64), event)?;
        }

        if let Some(zh) = &data.zenith_header {
            tx.queue_put::<ColdZenithHeaders>(&block, zh)?;
        }

        tx.raw_commit()?;
        Ok(())
    }

    fn truncate_above_inner(&self, block: BlockNumber) -> Result<(), MdbxColdError> {
        let tx = self.env.tx_rw()?;

        // Collect sealed headers above the cutoff
        let headers_to_remove = {
            let mut cursor = tx.new_cursor::<ColdHeaders>()?;
            let mut headers: Vec<(BlockNumber, SealedHeader)> = Vec::new();

            let start_block = block + 1;
            let mut key_buf = [0u8; MAX_KEY_SIZE];
            let key_bytes = start_block.encode_key(&mut key_buf);

            if let Some((key, value)) = cursor.lower_bound(key_bytes)? {
                headers.push((BlockNumber::decode_key(&key)?, SealedHeader::decode_value(&value)?));

                while let Some((key, value)) = cursor.read_next()? {
                    headers.push((
                        BlockNumber::decode_key(&key)?,
                        SealedHeader::decode_value(&value)?,
                    ));
                }
            }
            headers
        };

        if headers_to_remove.is_empty() {
            return Ok(());
        }

        // Delete each block's data
        for (block_num, sealed) in &headers_to_remove {
            // Delete transaction hash indices
            for item in tx.traverse_dual::<ColdTransactions>()?.iter_k2(block_num)? {
                let (_, tx_signed) = item?;
                tx.queue_delete::<ColdTxHashIndex>(tx_signed.hash())?;
            }

            tx.queue_delete::<ColdHeaders>(block_num)?;
            tx.queue_delete::<ColdBlockHashIndex>(&sealed.hash())?;
            tx.clear_k1_for::<ColdTransactions>(block_num)?;
            tx.clear_k1_for::<ColdReceipts>(block_num)?;
            tx.clear_k1_for::<ColdSignetEvents>(block_num)?;
            tx.queue_delete::<ColdZenithHeaders>(block_num)?;
        }

        tx.raw_commit()?;
        Ok(())
    }

    fn get_logs_inner(&self, filter: Filter) -> Result<Vec<signet_cold::RpcLog>, MdbxColdError> {
        let tx = self.env.tx()?;
        let mut results = Vec::new();

        let from = filter.get_from_block().unwrap_or(0);
        let to = filter.get_to_block().unwrap_or(u64::MAX);
        for block_num in from..=to {
            let Some(sealed) = tx.traverse::<ColdHeaders>()?.exact(&block_num)? else {
                continue;
            };
            let block_hash = sealed.hash();
            let block_timestamp = sealed.timestamp;

            for item in tx.traverse_dual::<ColdReceipts>()?.iter_k2(&block_num)? {
                let (tx_idx, ir) = item?;
                results.extend(
                    ir.receipt
                        .inner
                        .logs
                        .iter()
                        .enumerate()
                        .filter(|(_, log)| filter.matches(log))
                        .map(|(log_idx, log)| signet_cold::RpcLog {
                            inner: log.clone(),
                            block_hash: Some(block_hash),
                            block_number: Some(block_num),
                            block_timestamp: Some(block_timestamp),
                            transaction_hash: Some(ir.tx_hash),
                            transaction_index: Some(tx_idx),
                            log_index: Some(ir.first_log_index + log_idx as u64),
                            removed: false,
                        }),
                );
            }
        }

        Ok(results)
    }
}

impl ColdStorage for MdbxColdBackend {
    async fn get_header(&self, spec: HeaderSpecifier) -> ColdResult<Option<SealedHeader>> {
        Ok(self.get_header_inner(spec)?)
    }

    async fn get_headers(
        &self,
        specs: Vec<HeaderSpecifier>,
    ) -> ColdResult<Vec<Option<SealedHeader>>> {
        Ok(self.get_headers_inner(specs)?)
    }

    async fn get_transaction(
        &self,
        spec: TransactionSpecifier,
    ) -> ColdResult<Option<Confirmed<TransactionSigned>>> {
        Ok(self.get_transaction_inner(spec)?)
    }

    async fn get_transactions_in_block(
        &self,
        block: BlockNumber,
    ) -> ColdResult<Vec<TransactionSigned>> {
        Ok(self.collect_transactions_in_block(block)?)
    }

    async fn get_transaction_count(&self, block: BlockNumber) -> ColdResult<u64> {
        Ok(self.count_transactions_in_block(block)?)
    }

    async fn get_receipt(&self, spec: ReceiptSpecifier) -> ColdResult<Option<ColdReceipt>> {
        Ok(self.get_receipt_inner(spec)?)
    }

    async fn get_receipts_in_block(&self, block: BlockNumber) -> ColdResult<Vec<ColdReceipt>> {
        Ok(self.collect_receipts_in_block(block)?)
    }

    async fn get_signet_events(
        &self,
        spec: SignetEventsSpecifier,
    ) -> ColdResult<Vec<DbSignetEvent>> {
        let events = match spec {
            SignetEventsSpecifier::Block(block) => self.collect_signet_events_in_block(block)?,
            SignetEventsSpecifier::BlockRange { start, end } => {
                self.collect_signet_events_in_range(start, end)?
            }
        };
        Ok(events)
    }

    async fn get_zenith_header(
        &self,
        spec: ZenithHeaderSpecifier,
    ) -> ColdResult<Option<DbZenithHeader>> {
        let block = match spec {
            ZenithHeaderSpecifier::Number(n) => n,
            ZenithHeaderSpecifier::Range { start, .. } => start,
        };
        Ok(self.get_zenith_header_by_number(block)?)
    }

    async fn get_zenith_headers(
        &self,
        spec: ZenithHeaderSpecifier,
    ) -> ColdResult<Vec<DbZenithHeader>> {
        let headers = match spec {
            ZenithHeaderSpecifier::Number(n) => {
                self.get_zenith_header_by_number(n)?.into_iter().collect()
            }
            ZenithHeaderSpecifier::Range { start, end } => {
                self.collect_zenith_headers_in_range(start, end)?
            }
        };
        Ok(headers)
    }

    async fn get_logs(&self, filter: Filter) -> ColdResult<Vec<signet_cold::RpcLog>> {
        Ok(self.get_logs_inner(filter)?)
    }

    async fn get_latest_block(&self) -> ColdResult<Option<BlockNumber>> {
        let tx = self.env.tx().map_err(MdbxColdError::from)?;
        let mut cursor = tx.new_cursor::<ColdHeaders>().map_err(MdbxColdError::from)?;
        let latest = cursor
            .last()
            .map_err(MdbxColdError::from)?
            .map(|(key, _)| BlockNumber::decode_key(&key))
            .transpose()
            .map_err(MdbxColdError::from)?;
        Ok(latest)
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

#[cfg(all(test, feature = "test-utils"))]
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
