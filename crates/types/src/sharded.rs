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

impl<T> AsRef<Self> for ShardedKey<T> {
    fn as_ref(&self) -> &Self {
        self
    }
}

impl ShardedKey<()> {
    /// Number of indices in one shard.
    pub const SHARD_COUNT: usize = 2000;
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
