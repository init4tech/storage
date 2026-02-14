//! In-memory cold storage backend for testing.
//!
//! This backend stores all data in memory using standard Rust collections.
//! It is primarily intended for testing and development.

use crate::{
    BlockData, ColdReceipt, ColdResult, ColdStorage, ColdStorageError, Confirmed, Filter,
    HeaderSpecifier, ReceiptSpecifier, RpcLog, SignetEventsSpecifier, TransactionSpecifier,
    ZenithHeaderSpecifier,
};
use alloy::primitives::{B256, BlockNumber};
use signet_storage_types::{
    ConfirmationMeta, DbSignetEvent, DbZenithHeader, IndexedReceipt, RecoveredTx, SealedHeader,
};
use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};
use tokio::sync::RwLock;

/// Inner storage state.
#[derive(Default)]
struct MemColdBackendInner {
    /// Sealed headers indexed by block number (hash cached on insert).
    headers: BTreeMap<BlockNumber, SealedHeader>,
    /// Header hash to block number index.
    header_hashes: HashMap<B256, BlockNumber>,

    /// Transactions indexed by block number.
    transactions: BTreeMap<BlockNumber, Vec<RecoveredTx>>,
    /// Transaction hash to (block number, tx index) index.
    tx_hashes: HashMap<B256, (BlockNumber, u64)>,

    /// Indexed receipts (with precomputed tx_hash and first_log_index).
    receipts: BTreeMap<BlockNumber, Vec<IndexedReceipt>>,
    /// Transaction hash to (block number, receipt index) index for receipts.
    receipt_tx_hashes: HashMap<B256, (BlockNumber, u64)>,

    /// Signet events indexed by block number.
    signet_events: BTreeMap<BlockNumber, Vec<DbSignetEvent>>,

    /// Zenith headers indexed by block number.
    zenith_headers: BTreeMap<BlockNumber, DbZenithHeader>,
}

/// In-memory cold storage backend.
///
/// This backend is thread-safe and suitable for concurrent access.
/// All operations are protected by an async read-write lock.
pub struct MemColdBackend {
    inner: Arc<RwLock<MemColdBackendInner>>,
}

impl Default for MemColdBackend {
    fn default() -> Self {
        Self { inner: Arc::new(RwLock::new(MemColdBackendInner::default())) }
    }
}

impl MemColdBackend {
    /// Create a new empty in-memory backend.
    pub fn new() -> Self {
        Self::default()
    }
}

impl std::fmt::Debug for MemColdBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemColdBackend").finish_non_exhaustive()
    }
}

impl MemColdBackendInner {
    /// Build [`ConfirmationMeta`] for a given block and transaction index.
    ///
    /// Uses the cached hash from the [`SealedHeader`] rather than
    /// recomputing via `hash_slow()`.
    fn confirmation_meta(&self, block: BlockNumber, index: u64) -> Option<ConfirmationMeta> {
        self.headers.get(&block).map(|h| ConfirmationMeta::new(block, h.hash(), index))
    }
}

impl ColdStorage for MemColdBackend {
    async fn get_header(&self, spec: HeaderSpecifier) -> ColdResult<Option<SealedHeader>> {
        let inner = self.inner.read().await;
        match spec {
            HeaderSpecifier::Number(n) => Ok(inner.headers.get(&n).cloned()),
            HeaderSpecifier::Hash(h) => {
                Ok(inner.header_hashes.get(&h).and_then(|n| inner.headers.get(n)).cloned())
            }
        }
    }

    async fn get_headers(
        &self,
        specs: Vec<HeaderSpecifier>,
    ) -> ColdResult<Vec<Option<SealedHeader>>> {
        let mut results = Vec::with_capacity(specs.len());
        for spec in specs {
            results.push(self.get_header(spec).await?);
        }
        Ok(results)
    }

    async fn get_transaction(
        &self,
        spec: TransactionSpecifier,
    ) -> ColdResult<Option<Confirmed<RecoveredTx>>> {
        let inner = self.inner.read().await;
        let (block, index) = match spec {
            TransactionSpecifier::Hash(h) => {
                let Some(loc) = inner.tx_hashes.get(&h).copied() else { return Ok(None) };
                loc
            }
            TransactionSpecifier::BlockAndIndex { block, index } => (block, index),
            TransactionSpecifier::BlockHashAndIndex { block_hash, index } => {
                let Some(block) = inner.header_hashes.get(&block_hash).copied() else {
                    return Ok(None);
                };
                (block, index)
            }
        };
        let tx = inner.transactions.get(&block).and_then(|txs| txs.get(index as usize).cloned());
        Ok(tx.zip(inner.confirmation_meta(block, index)).map(|(tx, meta)| Confirmed::new(tx, meta)))
    }

    async fn get_transactions_in_block(&self, block: BlockNumber) -> ColdResult<Vec<RecoveredTx>> {
        let inner = self.inner.read().await;
        Ok(inner.transactions.get(&block).cloned().unwrap_or_default())
    }

    async fn get_transaction_count(&self, block: BlockNumber) -> ColdResult<u64> {
        let inner = self.inner.read().await;
        Ok(inner.transactions.get(&block).map(|txs| txs.len() as u64).unwrap_or(0))
    }

    async fn get_receipt(&self, spec: ReceiptSpecifier) -> ColdResult<Option<ColdReceipt>> {
        let inner = self.inner.read().await;
        let (block, index) = match spec {
            ReceiptSpecifier::TxHash(h) => {
                let Some(loc) = inner.receipt_tx_hashes.get(&h).copied() else {
                    return Ok(None);
                };
                loc
            }
            ReceiptSpecifier::BlockAndIndex { block, index } => (block, index),
        };
        let ir = inner.receipts.get(&block).and_then(|rs| rs.get(index as usize)).cloned();
        let header = inner.headers.get(&block);
        Ok(ir.zip(header).map(|(ir, h)| ColdReceipt::new(ir, h, index)))
    }

    async fn get_receipts_in_block(&self, block: BlockNumber) -> ColdResult<Vec<ColdReceipt>> {
        let inner = self.inner.read().await;
        let Some(header) = inner.headers.get(&block) else {
            return Ok(Vec::new());
        };
        Ok(inner
            .receipts
            .get(&block)
            .map(|receipts| {
                receipts
                    .iter()
                    .enumerate()
                    .map(|(idx, ir)| ColdReceipt::new(ir.clone(), header, idx as u64))
                    .collect()
            })
            .unwrap_or_default())
    }

    async fn get_signet_events(
        &self,
        spec: SignetEventsSpecifier,
    ) -> ColdResult<Vec<DbSignetEvent>> {
        let inner = self.inner.read().await;
        Ok(match spec {
            SignetEventsSpecifier::Block(block) => {
                inner.signet_events.get(&block).cloned().unwrap_or_default()
            }
            SignetEventsSpecifier::BlockRange { start, end } => inner
                .signet_events
                .range(start..=end)
                .flat_map(|(_, e)| e.iter().cloned())
                .collect(),
        })
    }

    async fn get_zenith_header(
        &self,
        spec: ZenithHeaderSpecifier,
    ) -> ColdResult<Option<DbZenithHeader>> {
        let inner = self.inner.read().await;
        Ok(match spec {
            ZenithHeaderSpecifier::Number(n) => inner.zenith_headers.get(&n).cloned(),
            ZenithHeaderSpecifier::Range { start, .. } => inner.zenith_headers.get(&start).cloned(),
        })
    }

    async fn get_zenith_headers(
        &self,
        spec: ZenithHeaderSpecifier,
    ) -> ColdResult<Vec<DbZenithHeader>> {
        let inner = self.inner.read().await;
        Ok(match spec {
            ZenithHeaderSpecifier::Number(n) => {
                inner.zenith_headers.get(&n).cloned().into_iter().collect()
            }
            ZenithHeaderSpecifier::Range { start, end } => {
                inner.zenith_headers.range(start..=end).map(|(_, h)| *h).collect()
            }
        })
    }

    async fn get_logs(&self, filter: Filter, max_logs: usize) -> ColdResult<Vec<RpcLog>> {
        let inner = self.inner.read().await;
        let mut results = Vec::new();

        let from = filter.get_from_block().unwrap_or(0);
        let to = filter.get_to_block().unwrap_or(u64::MAX);
        for (&block_num, receipts) in inner.receipts.range(from..=to) {
            let (block_hash, block_timestamp) =
                inner.headers.get(&block_num).map(|h| (h.hash(), h.timestamp)).unwrap_or_default();

            for (tx_idx, ir) in receipts.iter().enumerate() {
                for (log_idx, log) in ir.receipt.inner.logs.iter().enumerate() {
                    if !filter.matches(log) {
                        continue;
                    }
                    results.push(RpcLog {
                        inner: log.clone(),
                        block_hash: Some(block_hash),
                        block_number: Some(block_num),
                        block_timestamp: Some(block_timestamp),
                        transaction_hash: Some(ir.tx_hash),
                        transaction_index: Some(tx_idx as u64),
                        log_index: Some(ir.first_log_index + log_idx as u64),
                        removed: false,
                    });
                    if results.len() > max_logs {
                        return Err(ColdStorageError::TooManyLogs { limit: max_logs });
                    }
                }
            }
        }

        Ok(results)
    }

    async fn get_block_hash(&self, block: BlockNumber) -> ColdResult<Option<B256>> {
        let inner = self.inner.read().await;
        Ok(inner.headers.get(&block).map(|h| h.hash()))
    }

    async fn get_logs_block(
        &self,
        filter: &Filter,
        block_num: BlockNumber,
        remaining: usize,
    ) -> ColdResult<Vec<RpcLog>> {
        let inner = self.inner.read().await;
        let Some(receipts) = inner.receipts.get(&block_num) else {
            return Ok(Vec::new());
        };
        let (block_hash, block_timestamp) =
            inner.headers.get(&block_num).map(|h| (h.hash(), h.timestamp)).unwrap_or_default();

        let logs: Vec<RpcLog> = receipts
            .iter()
            .enumerate()
            .flat_map(|(tx_idx, ir)| {
                let tx_hash = ir.tx_hash;
                let first_log_index = ir.first_log_index;
                ir.receipt
                    .inner
                    .logs
                    .iter()
                    .enumerate()
                    .filter(move |(_, log)| filter.matches(log))
                    .map(move |(log_idx, log)| RpcLog {
                        inner: log.clone(),
                        block_hash: Some(block_hash),
                        block_number: Some(block_num),
                        block_timestamp: Some(block_timestamp),
                        transaction_hash: Some(tx_hash),
                        transaction_index: Some(tx_idx as u64),
                        log_index: Some(first_log_index + log_idx as u64),
                        removed: false,
                    })
            })
            .collect();

        if logs.len() > remaining {
            return Err(ColdStorageError::TooManyLogs { limit: remaining });
        }
        Ok(logs)
    }

    async fn get_latest_block(&self) -> ColdResult<Option<BlockNumber>> {
        let inner = self.inner.read().await;
        Ok(inner.headers.last_key_value().map(|(k, _)| *k))
    }

    async fn append_block(&self, data: BlockData) -> ColdResult<()> {
        let mut inner = self.inner.write().await;

        let block = data.block_number();

        // Store the sealed header (hash already cached)
        let header_hash = data.header.hash();
        inner.headers.insert(block, data.header);
        inner.header_hashes.insert(header_hash, block);

        // Build tx hash and sender index, store both tx and receipt hash mappings
        let tx_meta: Vec<_> =
            data.transactions.iter().map(|tx| (*tx.tx_hash(), tx.signer())).collect();
        for (idx, &(tx_hash, _)) in tx_meta.iter().enumerate() {
            let loc = (block, idx as u64);
            inner.tx_hashes.insert(tx_hash, loc);
            inner.receipt_tx_hashes.insert(tx_hash, loc);
        }
        inner.transactions.insert(block, data.transactions);

        // Compute IndexedReceipt with precomputed metadata
        let mut first_log_index = 0u64;
        let mut prior_cumulative_gas = 0u64;
        let indexed_receipts = data
            .receipts
            .into_iter()
            .zip(tx_meta)
            .map(|(receipt, (tx_hash, sender))| {
                let gas_used = receipt.inner.cumulative_gas_used - prior_cumulative_gas;
                prior_cumulative_gas = receipt.inner.cumulative_gas_used;
                let ir = IndexedReceipt { receipt, tx_hash, first_log_index, gas_used, sender };
                first_log_index += ir.receipt.inner.logs.len() as u64;
                ir
            })
            .collect();
        inner.receipts.insert(block, indexed_receipts);

        // Store signet events
        inner.signet_events.insert(block, data.signet_events);

        // Store zenith header if present
        if let Some(zh) = data.zenith_header {
            inner.zenith_headers.insert(block, zh);
        }

        Ok(())
    }

    async fn append_blocks(&self, data: Vec<BlockData>) -> ColdResult<()> {
        for block_data in data {
            self.append_block(block_data).await?;
        }
        Ok(())
    }

    async fn truncate_above(&self, block: BlockNumber) -> ColdResult<()> {
        let mut inner = self.inner.write().await;

        // Collect keys to remove
        let to_remove: Vec<_> = inner.headers.range((block + 1)..).map(|(k, _)| *k).collect();

        for k in &to_remove {
            if let Some(sealed) = inner.headers.remove(k) {
                inner.header_hashes.remove(&sealed.hash());
            }
            if let Some(txs) = inner.transactions.remove(k) {
                for tx in txs {
                    inner.tx_hashes.remove(tx.hash());
                }
            }
            if inner.receipts.remove(k).is_some() {
                inner.receipt_tx_hashes.retain(|_, (b, _)| *b <= block);
            }
            inner.signet_events.remove(k);
            inner.zenith_headers.remove(k);
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use crate::conformance::conformance;

    #[tokio::test]
    async fn mem_backend_conformance() {
        let backend = MemColdBackend::new();
        conformance(backend).await.unwrap();
    }
}
