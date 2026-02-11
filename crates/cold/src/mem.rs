//! In-memory cold storage backend for testing.
//!
//! This backend stores all data in memory using standard Rust collections.
//! It is primarily intended for testing and development.

use crate::{
    BlockData, BlockTag, ColdResult, ColdStorage, Confirmed, HeaderSpecifier, ReceiptSpecifier,
    SignetEventsSpecifier, TransactionSpecifier, ZenithHeaderSpecifier,
};
use alloy::{
    consensus::{Header, Sealable},
    primitives::{B256, BlockNumber},
};
use signet_storage_types::{
    ConfirmationMeta, DbSignetEvent, DbZenithHeader, Receipt, SealedHeader, TransactionSigned,
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
    transactions: BTreeMap<BlockNumber, Vec<TransactionSigned>>,
    /// Transaction hash to (block number, tx index) index.
    tx_hashes: HashMap<B256, (BlockNumber, u64)>,

    /// Receipts indexed by block number.
    receipts: BTreeMap<BlockNumber, Vec<Receipt>>,
    /// Transaction hash to (block number, receipt index) index for receipts.
    receipt_tx_hashes: HashMap<B256, (BlockNumber, u64)>,

    /// Signet events indexed by block number.
    signet_events: BTreeMap<BlockNumber, Vec<DbSignetEvent>>,

    /// Zenith headers indexed by block number.
    zenith_headers: BTreeMap<BlockNumber, DbZenithHeader>,

    /// The latest (highest) block number in storage.
    latest_block: Option<BlockNumber>,
}

/// In-memory cold storage backend.
///
/// This backend is thread-safe and suitable for concurrent access.
/// All operations are protected by an async read-write lock.
#[derive(Default)]
pub struct MemColdBackend {
    inner: Arc<RwLock<MemColdBackendInner>>,
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
    async fn get_header(&self, spec: HeaderSpecifier) -> ColdResult<Option<Header>> {
        let inner = self.inner.read().await;
        match spec {
            HeaderSpecifier::Number(n) => Ok(inner.headers.get(&n).map(|s| Header::clone(s))),
            HeaderSpecifier::Hash(h) => {
                let block = inner.header_hashes.get(&h).copied();
                Ok(block.and_then(|n| inner.headers.get(&n).map(|s| Header::clone(s))))
            }
            HeaderSpecifier::Tag(tag) => match tag {
                BlockTag::Latest | BlockTag::Finalized | BlockTag::Safe => Ok(inner
                    .latest_block
                    .and_then(|n| inner.headers.get(&n).map(|s| Header::clone(s)))),
                BlockTag::Earliest => {
                    Ok(inner.headers.first_key_value().map(|(_, s)| Header::clone(s)))
                }
            },
        }
    }

    async fn get_headers(&self, specs: Vec<HeaderSpecifier>) -> ColdResult<Vec<Option<Header>>> {
        let mut results = Vec::with_capacity(specs.len());
        for spec in specs {
            results.push(self.get_header(spec).await?);
        }
        Ok(results)
    }

    async fn get_transaction(
        &self,
        spec: TransactionSpecifier,
    ) -> ColdResult<Option<Confirmed<TransactionSigned>>> {
        let inner = self.inner.read().await;
        let (block, index) = match spec {
            TransactionSpecifier::Hash(h) => match inner.tx_hashes.get(&h).copied() {
                Some(loc) => loc,
                None => return Ok(None),
            },
            TransactionSpecifier::BlockAndIndex { block, index } => (block, index),
            TransactionSpecifier::BlockHashAndIndex { block_hash, index } => {
                match inner.header_hashes.get(&block_hash).copied() {
                    Some(block) => (block, index),
                    None => return Ok(None),
                }
            }
        };
        let tx = inner.transactions.get(&block).and_then(|txs| txs.get(index as usize).cloned());
        Ok(tx.zip(inner.confirmation_meta(block, index)).map(|(tx, meta)| Confirmed::new(tx, meta)))
    }

    async fn get_transactions_in_block(
        &self,
        block: BlockNumber,
    ) -> ColdResult<Vec<TransactionSigned>> {
        let inner = self.inner.read().await;
        Ok(inner.transactions.get(&block).cloned().unwrap_or_default())
    }

    async fn get_transaction_count(&self, block: BlockNumber) -> ColdResult<u64> {
        let inner = self.inner.read().await;
        Ok(inner.transactions.get(&block).map(|txs| txs.len() as u64).unwrap_or(0))
    }

    async fn get_receipt(&self, spec: ReceiptSpecifier) -> ColdResult<Option<Confirmed<Receipt>>> {
        let inner = self.inner.read().await;
        let (block, index) = match spec {
            ReceiptSpecifier::TxHash(h) => match inner.receipt_tx_hashes.get(&h).copied() {
                Some(loc) => loc,
                None => return Ok(None),
            },
            ReceiptSpecifier::BlockAndIndex { block, index } => (block, index),
        };
        let receipt = inner.receipts.get(&block).and_then(|rs| rs.get(index as usize).cloned());
        Ok(receipt
            .zip(inner.confirmation_meta(block, index))
            .map(|(r, meta)| Confirmed::new(r, meta)))
    }

    async fn get_receipts_in_block(&self, block: BlockNumber) -> ColdResult<Vec<Receipt>> {
        let inner = self.inner.read().await;
        Ok(inner.receipts.get(&block).cloned().unwrap_or_default())
    }

    async fn get_signet_events(
        &self,
        spec: SignetEventsSpecifier,
    ) -> ColdResult<Vec<DbSignetEvent>> {
        let inner = self.inner.read().await;
        match spec {
            SignetEventsSpecifier::Block(block) => {
                Ok(inner.signet_events.get(&block).cloned().unwrap_or_default())
            }
            SignetEventsSpecifier::BlockRange { start, end } => {
                let mut results = Vec::new();
                for (_, events) in inner.signet_events.range(start..=end) {
                    results.extend(events.iter().cloned());
                }
                Ok(results)
            }
        }
    }

    async fn get_zenith_header(
        &self,
        spec: ZenithHeaderSpecifier,
    ) -> ColdResult<Option<DbZenithHeader>> {
        let inner = self.inner.read().await;
        match spec {
            ZenithHeaderSpecifier::Number(n) => Ok(inner.zenith_headers.get(&n).cloned()),
            ZenithHeaderSpecifier::Range { start, .. } => {
                // For single lookup via range, return first in range
                Ok(inner.zenith_headers.get(&start).cloned())
            }
        }
    }

    async fn get_zenith_headers(
        &self,
        spec: ZenithHeaderSpecifier,
    ) -> ColdResult<Vec<DbZenithHeader>> {
        let inner = self.inner.read().await;
        match spec {
            ZenithHeaderSpecifier::Number(n) => {
                Ok(inner.zenith_headers.get(&n).cloned().into_iter().collect())
            }
            ZenithHeaderSpecifier::Range { start, end } => {
                Ok(inner.zenith_headers.range(start..=end).map(|(_, h)| *h).collect())
            }
        }
    }

    async fn get_latest_block(&self) -> ColdResult<Option<BlockNumber>> {
        let inner = self.inner.read().await;
        Ok(inner.latest_block)
    }

    async fn append_block(&self, data: BlockData) -> ColdResult<()> {
        let mut inner = self.inner.write().await;

        let block = data.block_number();

        // Seal the header (computes hash once) and store
        let sealed = data.header.seal_slow();
        let header_hash = sealed.hash();
        inner.headers.insert(block, sealed);
        inner.header_hashes.insert(header_hash, block);

        // Build tx hash list for indexing before moving transactions
        let tx_hashes: Vec<_> = data.transactions.iter().map(|tx| *tx.hash()).collect();

        // Store transactions and index by hash
        for (idx, tx_hash) in tx_hashes.iter().enumerate() {
            inner.tx_hashes.insert(*tx_hash, (block, idx as u64));
        }

        inner.transactions.insert(block, data.transactions);

        // Store receipts and index by tx hash
        for (idx, tx_hash) in tx_hashes.iter().enumerate() {
            inner.receipt_tx_hashes.insert(*tx_hash, (block, idx as u64));
        }
        inner.receipts.insert(block, data.receipts);

        // Store signet events
        inner.signet_events.insert(block, data.signet_events);

        // Store zenith header if present
        if let Some(zh) = data.zenith_header {
            inner.zenith_headers.insert(block, zh);
        }

        // Update latest block
        inner.latest_block = Some(inner.latest_block.map_or(block, |prev| prev.max(block)));

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

        // Remove headers above block
        for k in &to_remove {
            if let Some(sealed) = inner.headers.remove(k) {
                inner.header_hashes.remove(&sealed.hash());
            }
        }

        // Remove transactions above block
        for k in &to_remove {
            if let Some(txs) = inner.transactions.remove(k) {
                for tx in txs {
                    inner.tx_hashes.remove(tx.hash());
                }
            }
        }

        // Remove receipts above block
        for k in &to_remove {
            if inner.receipts.remove(k).is_some() {
                // Also remove from receipt_tx_hashes
                inner.receipt_tx_hashes.retain(|_, (b, _)| *b <= block);
            }
        }

        // Remove signet events above block
        for k in &to_remove {
            inner.signet_events.remove(k);
        }

        // Remove zenith headers above block
        for k in &to_remove {
            inner.zenith_headers.remove(k);
        }

        // Update latest block
        inner.latest_block = inner.headers.last_key_value().map(|(k, _)| *k);

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
        conformance(&backend).await.unwrap();
    }
}
