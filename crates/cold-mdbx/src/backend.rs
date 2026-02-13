//! MDBX backend implementation for [`ColdStorage`].
//!
//! This module provides an MDBX-based implementation of the cold storage
//! backend. It uses the table definitions from this crate and the MDBX
//! database environment from `signet-hot-mdbx`.

use crate::{
    ColdBlockHashIndex, ColdHeaders, ColdReceipts, ColdSignetEvents, ColdTransactions,
    ColdTxHashIndex, ColdZenithHeaders, MdbxColdError,
};
use alloy::{consensus::Header, primitives::BlockNumber};
use signet_cold::{
    BlockData, ColdResult, ColdStorage, Confirmed, HeaderSpecifier, ReceiptContext,
    ReceiptSpecifier, SignetEventsSpecifier, TransactionSpecifier, ZenithHeaderSpecifier,
};
use signet_hot::{
    KeySer, MAX_KEY_SIZE, ValSer,
    model::{DualTableTraverse, HotKvWrite, KvTraverse, TableTraverse},
    tables::Table,
};
use signet_hot_mdbx::{DatabaseArguments, DatabaseEnv, DatabaseEnvKind};
use signet_storage_types::{
    ConfirmationMeta, DbSignetEvent, DbZenithHeader, Receipt, TransactionSigned, TxLocation,
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

    fn get_header_inner(&self, spec: HeaderSpecifier) -> Result<Option<Header>, MdbxColdError> {
        let tx = self.env.tx()?;
        let block_num = match spec {
            HeaderSpecifier::Number(n) => n,
            HeaderSpecifier::Hash(h) => {
                let Some(n) = TableTraverse::<ColdBlockHashIndex, _>::exact(
                    &mut tx.new_cursor::<ColdBlockHashIndex>()?,
                    &h,
                )?
                else {
                    return Ok(None);
                };
                n
            }
        };
        Ok(TableTraverse::<ColdHeaders, _>::exact(
            &mut tx.new_cursor::<ColdHeaders>()?,
            &block_num,
        )?)
    }

    fn get_headers_inner(
        &self,
        specs: Vec<HeaderSpecifier>,
    ) -> Result<Vec<Option<Header>>, MdbxColdError> {
        let tx = self.env.tx()?;
        specs
            .into_iter()
            .map(|spec| {
                let block_num = match spec {
                    HeaderSpecifier::Number(n) => Some(n),
                    HeaderSpecifier::Hash(h) => TableTraverse::<ColdBlockHashIndex, _>::exact(
                        &mut tx.new_cursor::<ColdBlockHashIndex>()?,
                        &h,
                    )?,
                };
                block_num
                    .map(|n| {
                        TableTraverse::<ColdHeaders, _>::exact(
                            &mut tx.new_cursor::<ColdHeaders>()?,
                            &n,
                        )
                    })
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
                let Some(loc) = TableTraverse::<ColdTxHashIndex, _>::exact(
                    &mut tx.new_cursor::<ColdTxHashIndex>()?,
                    &h,
                )?
                else {
                    return Ok(None);
                };
                (loc.block, loc.index)
            }
            TransactionSpecifier::BlockAndIndex { block, index } => (block, index),
            TransactionSpecifier::BlockHashAndIndex { block_hash, index } => {
                let Some(block) = TableTraverse::<ColdBlockHashIndex, _>::exact(
                    &mut tx.new_cursor::<ColdBlockHashIndex>()?,
                    &block_hash,
                )?
                else {
                    return Ok(None);
                };
                (block, index)
            }
        };
        let Some(signed_tx) = DualTableTraverse::<ColdTransactions, _>::exact_dual(
            &mut tx.new_cursor::<ColdTransactions>()?,
            &block,
            &index,
        )?
        else {
            return Ok(None);
        };
        let Some(header) =
            TableTraverse::<ColdHeaders, _>::exact(&mut tx.new_cursor::<ColdHeaders>()?, &block)?
        else {
            return Ok(None);
        };
        let meta = ConfirmationMeta::new(block, header.hash_slow(), index);
        Ok(Some(Confirmed::new(signed_tx, meta)))
    }

    fn get_receipt_inner(
        &self,
        spec: ReceiptSpecifier,
    ) -> Result<Option<Confirmed<Receipt>>, MdbxColdError> {
        Ok(self.get_receipt_with_context_inner(spec)?.map(|ctx| ctx.receipt))
    }

    fn get_zenith_header_by_number(
        &self,
        block: BlockNumber,
    ) -> Result<Option<DbZenithHeader>, MdbxColdError> {
        let tx = self.env.tx()?;
        Ok(TableTraverse::<ColdZenithHeaders, _>::exact(
            &mut tx.new_cursor::<ColdZenithHeaders>()?,
            &block,
        )?)
    }

    fn collect_transactions_in_block(
        &self,
        block: BlockNumber,
    ) -> Result<Vec<TransactionSigned>, MdbxColdError> {
        let tx = self.env.tx()?;
        let mut cursor = tx.new_cursor::<ColdTransactions>()?;
        DualTableTraverse::<ColdTransactions, _>::iter_k2(&mut cursor, &block)?
            .map(|item| item.map(|(_, v)| v))
            .collect::<Result<_, _>>()
            .map_err(Into::into)
    }

    fn count_transactions_in_block(&self, block: BlockNumber) -> Result<u64, MdbxColdError> {
        let tx = self.env.tx()?;
        let mut cursor = tx.new_cursor::<ColdTransactions>()?;
        let mut count = 0u64;
        for item in DualTableTraverse::<ColdTransactions, _>::iter_k2(&mut cursor, &block)? {
            item?;
            count += 1;
        }
        Ok(count)
    }

    fn collect_receipts_in_block(&self, block: BlockNumber) -> Result<Vec<Receipt>, MdbxColdError> {
        let tx = self.env.tx()?;
        let mut cursor = tx.new_cursor::<ColdReceipts>()?;
        DualTableTraverse::<ColdReceipts, _>::iter_k2(&mut cursor, &block)?
            .map(|item| item.map(|(_, v)| v))
            .collect::<Result<_, _>>()
            .map_err(Into::into)
    }

    fn collect_signet_events_in_block(
        &self,
        block: BlockNumber,
    ) -> Result<Vec<DbSignetEvent>, MdbxColdError> {
        let tx = self.env.tx()?;
        let mut cursor = tx.new_cursor::<ColdSignetEvents>()?;
        DualTableTraverse::<ColdSignetEvents, _>::iter_k2(&mut cursor, &block)?
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
            let mut cursor = tx.new_cursor::<ColdSignetEvents>()?;
            for item in DualTableTraverse::<ColdSignetEvents, _>::iter_k2(&mut cursor, &block)? {
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

        let Some((key, value)) = KvTraverse::<_>::lower_bound(&mut cursor, key_bytes)? else {
            return Ok(headers);
        };

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

        Ok(headers)
    }

    fn append_block_inner(&self, data: BlockData) -> Result<(), MdbxColdError> {
        let tx = self.env.tx_rw()?;
        let block = data.block_number();

        tx.queue_put::<ColdHeaders>(&block, &data.header)?;
        tx.queue_put::<ColdBlockHashIndex>(&data.header.hash_slow(), &block)?;

        for (idx, tx_signed) in data.transactions.iter().enumerate() {
            let tx_idx = idx as u64;
            tx.queue_put_dual::<ColdTransactions>(&block, &tx_idx, tx_signed)?;
            tx.queue_put::<ColdTxHashIndex>(tx_signed.hash(), &TxLocation::new(block, tx_idx))?;
        }

        for (idx, receipt) in data.receipts.iter().enumerate() {
            tx.queue_put_dual::<ColdReceipts>(&block, &(idx as u64), receipt)?;
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

        // Collect headers above the cutoff
        let headers_to_remove = {
            let mut cursor = tx.new_cursor::<ColdHeaders>()?;
            let mut headers: Vec<(BlockNumber, Header)> = Vec::new();

            let start_block = block + 1;
            let mut key_buf = [0u8; MAX_KEY_SIZE];
            let key_bytes = start_block.encode_key(&mut key_buf);

            if let Some((key, value)) = KvTraverse::<_>::lower_bound(&mut cursor, key_bytes)? {
                headers.push((BlockNumber::decode_key(&key)?, Header::decode_value(&value)?));

                while let Some((key, value)) = KvTraverse::<_>::read_next(&mut cursor)? {
                    headers.push((BlockNumber::decode_key(&key)?, Header::decode_value(&value)?));
                }
            }
            headers
        };

        if headers_to_remove.is_empty() {
            return Ok(());
        }

        // Delete each block's data
        for (block_num, header) in &headers_to_remove {
            // Delete transaction hash indices
            {
                let mut tx_cursor = tx.new_cursor::<ColdTransactions>()?;
                for item in
                    DualTableTraverse::<ColdTransactions, _>::iter_k2(&mut tx_cursor, block_num)?
                {
                    let (_, tx_signed): (u64, TransactionSigned) = item?;
                    tx.queue_delete::<ColdTxHashIndex>(tx_signed.hash())?;
                }
            }

            tx.queue_delete::<ColdHeaders>(block_num)?;
            tx.queue_delete::<ColdBlockHashIndex>(&header.hash_slow())?;
            tx.clear_k1_for::<ColdTransactions>(block_num)?;
            tx.clear_k1_for::<ColdReceipts>(block_num)?;
            tx.clear_k1_for::<ColdSignetEvents>(block_num)?;
            tx.queue_delete::<ColdZenithHeaders>(block_num)?;
        }

        tx.raw_commit()?;
        Ok(())
    }

    fn get_logs_inner(
        &self,
        filter: signet_cold::LogFilter,
    ) -> Result<Vec<signet_cold::RichLog>, MdbxColdError> {
        let tx = self.env.tx()?;
        let mut results = Vec::new();

        for block_num in filter.from_block..=filter.to_block {
            let Some(header) = TableTraverse::<ColdHeaders, _>::exact(
                &mut tx.new_cursor::<ColdHeaders>()?,
                &block_num,
            )?
            else {
                continue;
            };
            let block_hash = header.hash_slow();
            let mut block_log_index = 0u64;

            let mut receipt_cursor = tx.new_cursor::<ColdReceipts>()?;
            for item in
                DualTableTraverse::<ColdReceipts, _>::iter_k2(&mut receipt_cursor, &block_num)?
            {
                let (tx_idx, receipt): (u64, Receipt) = item?;
                let tx_hash = DualTableTraverse::<ColdTransactions, _>::exact_dual(
                    &mut tx.new_cursor::<ColdTransactions>()?,
                    &block_num,
                    &tx_idx,
                )?
                .map(|t: TransactionSigned| *t.hash())
                .unwrap_or_default();

                let base_log_index = block_log_index;
                block_log_index += receipt.inner.logs.len() as u64;

                results.extend(
                    receipt
                        .inner
                        .logs
                        .iter()
                        .enumerate()
                        .filter(|(_, log)| filter.matches_log(log))
                        .map(|(log_idx, log)| signet_cold::RichLog {
                            log: log.clone(),
                            block_number: block_num,
                            block_hash,
                            tx_hash,
                            tx_index: tx_idx,
                            block_log_index: base_log_index + log_idx as u64,
                            tx_log_index: log_idx as u64,
                        }),
                );
            }
        }

        Ok(results)
    }

    fn get_receipt_with_context_inner(
        &self,
        spec: ReceiptSpecifier,
    ) -> Result<Option<ReceiptContext>, MdbxColdError> {
        let tx = self.env.tx()?;
        let (block, index) = match spec {
            ReceiptSpecifier::TxHash(h) => {
                let Some(loc) = TableTraverse::<ColdTxHashIndex, _>::exact(
                    &mut tx.new_cursor::<ColdTxHashIndex>()?,
                    &h,
                )?
                else {
                    return Ok(None);
                };
                (loc.block, loc.index)
            }
            ReceiptSpecifier::BlockAndIndex { block, index } => (block, index),
        };
        let Some(header) =
            TableTraverse::<ColdHeaders, _>::exact(&mut tx.new_cursor::<ColdHeaders>()?, &block)?
        else {
            return Ok(None);
        };
        let Some(receipt) = DualTableTraverse::<ColdReceipts, _>::exact_dual(
            &mut tx.new_cursor::<ColdReceipts>()?,
            &block,
            &index,
        )?
        else {
            return Ok(None);
        };
        let Some(transaction) = DualTableTraverse::<ColdTransactions, _>::exact_dual(
            &mut tx.new_cursor::<ColdTransactions>()?,
            &block,
            &index,
        )?
        else {
            return Ok(None);
        };

        let mut first_log_index = 0u64;
        let mut prior_cumulative_gas = 0u64;
        for i in 0..index {
            if let Some(r) = DualTableTraverse::<ColdReceipts, _>::exact_dual(
                &mut tx.new_cursor::<ColdReceipts>()?,
                &block,
                &i,
            )? {
                prior_cumulative_gas = r.inner.cumulative_gas_used;
                first_log_index += r.inner.logs.len() as u64;
            }
        }

        let meta = ConfirmationMeta::new(block, header.hash_slow(), index);
        let confirmed_receipt = Confirmed::new(receipt, meta);
        Ok(Some(ReceiptContext::new(
            header,
            transaction,
            confirmed_receipt,
            prior_cumulative_gas,
            first_log_index,
        )))
    }
}

impl ColdStorage for MdbxColdBackend {
    async fn get_header(&self, spec: HeaderSpecifier) -> ColdResult<Option<Header>> {
        Ok(self.get_header_inner(spec)?)
    }

    async fn get_headers(&self, specs: Vec<HeaderSpecifier>) -> ColdResult<Vec<Option<Header>>> {
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

    async fn get_receipt(&self, spec: ReceiptSpecifier) -> ColdResult<Option<Confirmed<Receipt>>> {
        Ok(self.get_receipt_inner(spec)?)
    }

    async fn get_receipts_in_block(&self, block: BlockNumber) -> ColdResult<Vec<Receipt>> {
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

    async fn get_logs(
        &self,
        filter: signet_cold::LogFilter,
    ) -> ColdResult<Vec<signet_cold::RichLog>> {
        Ok(self.get_logs_inner(filter)?)
    }

    async fn get_latest_block(&self) -> ColdResult<Option<BlockNumber>> {
        let tx = self.env.tx().map_err(MdbxColdError::from)?;
        let mut cursor = tx.new_cursor::<ColdHeaders>().map_err(MdbxColdError::from)?;
        let latest = KvTraverse::<_>::last(&mut cursor)
            .map_err(MdbxColdError::from)?
            .map(|(key, _)| BlockNumber::decode_key(&key))
            .transpose()
            .map_err(MdbxColdError::from)?;
        Ok(latest)
    }

    async fn get_receipt_with_context(
        &self,
        spec: ReceiptSpecifier,
    ) -> ColdResult<Option<ReceiptContext>> {
        Ok(self.get_receipt_with_context_inner(spec)?)
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
