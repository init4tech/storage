/// Sometimes data can be too big to be saved for a single key. This helps out by dividing the data
/// into different shards. Example:
///
/// `Address | 200` -> data is from block 0 to 200.
///
/// `Address | 300` -> data is from block 201 to 300.
#[derive(Debug, Default, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct ShardedKey<T> {
    /// The key for this type.
    pub key: T,
    /// Highest block number to which `value` is related to.
    pub highest_block_number: u64,
}

impl ShardedKey<()> {
    /// Soft cap on the number of indices in one shard.
    ///
    /// This is a sanity ceiling used alongside [`Self::MAX_SHARD_BYTES`];
    /// shard splitting is driven by encoded size, not by this count.
    pub const SHARD_COUNT: usize = 2000;

    /// Maximum encoded byte size of a single shard's [`BlockNumberList`].
    ///
    /// [`BlockNumberList`]: crate::BlockNumberList
    ///
    /// The MDBX `DUPSORT` value limit is ~1980 bytes on 4 KB pages
    /// (Linux production). Each stored dup value is `key2 || encoded list`,
    /// so this budget reserves headroom for `ShardedKey<U256>` (40 bytes),
    /// the 2-byte length prefix on `BlockNumberList`, and per-node overhead
    /// inside MDBX. Exceeding this triggers `MDBX_BAD_VALSIZE` at write time
    /// (ENG-2287).
    pub const MAX_SHARD_BYTES: usize = 1500;
}

impl<T> ShardedKey<T> {
    /// Creates a new `ShardedKey<T>`.
    pub const fn new(key: T, highest_block_number: u64) -> Self {
        Self { key, highest_block_number }
    }

    /// Creates a new key with the highest block number set to maximum.
    /// This is useful when we want to search the last value for a given key.
    pub const fn last(key: T) -> Self {
        Self { key, highest_block_number: u64::MAX }
    }
}
