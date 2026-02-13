//! LRU cache for cold storage lookups.
//!
//! Caches recently accessed transactions, receipts, and headers to avoid
//! repeated backend reads for frequently queried items.

use crate::{ColdReceipt, Confirmed};
use alloy::primitives::BlockNumber;
use lru::LruCache;
use signet_storage_types::{SealedHeader, TransactionSigned};
use std::num::NonZeroUsize;

/// Default capacity for each LRU cache map.
const DEFAULT_CACHE_CAPACITY: usize = 128;

/// Evict all entries from an LRU cache whose keys satisfy the predicate.
fn evict_where<K: Copy + Eq + std::hash::Hash, V>(
    cache: &mut LruCache<K, V>,
    predicate: impl Fn(&K) -> bool,
) {
    let keys: Vec<_> = cache.iter().filter(|(k, _)| predicate(k)).map(|(k, _)| *k).collect();
    keys.into_iter().for_each(|k| {
        cache.pop(&k);
    });
}

/// LRU caches for transaction, receipt, and header lookups.
///
/// Keys are `(BlockNumber, tx_index)` for transactions and receipts,
/// and `BlockNumber` for headers.
pub(crate) struct ColdCache {
    transactions: LruCache<(BlockNumber, u64), Confirmed<TransactionSigned>>,
    receipts: LruCache<(BlockNumber, u64), ColdReceipt>,
    headers: LruCache<BlockNumber, SealedHeader>,
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
    pub(crate) fn get_receipt(&mut self, key: &(BlockNumber, u64)) -> Option<ColdReceipt> {
        self.receipts.get(key).cloned()
    }

    /// Insert a receipt into the cache.
    pub(crate) fn put_receipt(&mut self, key: (BlockNumber, u64), val: ColdReceipt) {
        self.receipts.put(key, val);
    }

    /// Look up a cached header by block number.
    pub(crate) fn get_header(&mut self, block: &BlockNumber) -> Option<SealedHeader> {
        self.headers.get(block).cloned()
    }

    /// Insert a header into the cache.
    pub(crate) fn put_header(&mut self, block: BlockNumber, header: SealedHeader) {
        self.headers.put(block, header);
    }

    /// Invalidate all entries with a block number above the given cutoff.
    pub(crate) fn invalidate_above(&mut self, block: u64) {
        evict_where(&mut self.transactions, |(bn, _)| *bn > block);
        evict_where(&mut self.receipts, |(bn, _)| *bn > block);
        evict_where(&mut self.headers, |bn| *bn > block);
    }
}
