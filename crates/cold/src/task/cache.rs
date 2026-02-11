//! LRU cache for cold storage lookups.
//!
//! Caches recently accessed transactions, receipts, and headers to avoid
//! repeated backend reads for frequently queried items.

use crate::Confirmed;
use alloy::{consensus::Header, primitives::BlockNumber};
use lru::LruCache;
use signet_storage_types::{Receipt, TransactionSigned};
use std::num::NonZeroUsize;

/// Default capacity for each LRU cache map.
const DEFAULT_CACHE_CAPACITY: usize = 128;

/// LRU caches for transaction, receipt, and header lookups.
///
/// Keys are `(BlockNumber, tx_index)` for transactions and receipts,
/// and `BlockNumber` for headers.
pub(crate) struct ColdCache {
    transactions: LruCache<(BlockNumber, u64), Confirmed<TransactionSigned>>,
    receipts: LruCache<(BlockNumber, u64), Confirmed<Receipt>>,
    headers: LruCache<BlockNumber, Header>,
}

impl ColdCache {
    /// Create a new cache with the default capacity (128 per map).
    pub(crate) fn new() -> Self {
        Self::new_with_cap(DEFAULT_CACHE_CAPACITY)
    }

    /// Create a new cache with the given per-map capacity.
    pub(crate) fn new_with_cap(cap: usize) -> Self {
        let cap = NonZeroUsize::new(cap).expect("cache capacity must be non-zero");
        Self {
            transactions: LruCache::new(cap),
            receipts: LruCache::new(cap),
            headers: LruCache::new(cap),
        }
    }

    /// Look up a cached transaction by `(block_number, tx_index)`.
    pub(crate) fn get_tx(
        &mut self,
        key: &(BlockNumber, u64),
    ) -> Option<Confirmed<TransactionSigned>> {
        self.transactions.get(key).cloned()
    }

    /// Insert a transaction into the cache.
    pub(crate) fn put_tx(&mut self, key: (BlockNumber, u64), val: Confirmed<TransactionSigned>) {
        self.transactions.put(key, val);
    }

    /// Look up a cached receipt by `(block_number, tx_index)`.
    pub(crate) fn get_receipt(&mut self, key: &(BlockNumber, u64)) -> Option<Confirmed<Receipt>> {
        self.receipts.get(key).cloned()
    }

    /// Insert a receipt into the cache.
    pub(crate) fn put_receipt(&mut self, key: (BlockNumber, u64), val: Confirmed<Receipt>) {
        self.receipts.put(key, val);
    }

    /// Look up a cached header by block number.
    pub(crate) fn get_header(&mut self, block: &BlockNumber) -> Option<Header> {
        self.headers.get(block).cloned()
    }

    /// Insert a header into the cache.
    pub(crate) fn put_header(&mut self, block: BlockNumber, header: Header) {
        self.headers.put(block, header);
    }

    /// Invalidate all entries with a block number above the given cutoff.
    pub(crate) fn invalidate_above(&mut self, block: u64) {
        let tx_keys: Vec<_> =
            self.transactions.iter().filter(|((bn, _), _)| *bn > block).map(|(k, _)| *k).collect();
        for key in tx_keys {
            self.transactions.pop(&key);
        }

        let rx_keys: Vec<_> =
            self.receipts.iter().filter(|((bn, _), _)| *bn > block).map(|(k, _)| *k).collect();
        for key in rx_keys {
            self.receipts.pop(&key);
        }

        let hdr_keys: Vec<_> =
            self.headers.iter().filter(|(bn, _)| **bn > block).map(|(k, _)| *k).collect();
        for key in hdr_keys {
            self.headers.pop(&key);
        }
    }
}
