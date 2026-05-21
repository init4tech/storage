use bytes::BufMut;
use core::fmt;
use roaring::RoaringTreemap;

/// List with block numbers.
pub type BlockNumberList = IntegerList;

/// Primitives error type.
#[derive(Debug, Clone, Copy, thiserror::Error)]
pub enum IntegerListError {
    /// The provided input is unsorted.
    #[error("the provided input is unsorted")]
    UnsortedInput,

    /// Failed to deserialize data into type.
    #[error("failed to deserialize data into type")]
    FailedToDeserialize,

    /// The provided integer is too small. See
    /// [`roaring::RoaringTreemap::try_push`].
    #[error("the provided integer is too small")]
    IntegerTooSmall,
}

/// A data structure that uses Roaring Bitmaps to efficiently store a list of integers.
///
/// This structure provides excellent compression while allowing direct access to individual
/// elements without the need for full decompression.
///
/// Key features:
/// - Efficient compression: the underlying Roaring Bitmaps significantly reduce memory usage.
/// - Direct access: elements can be accessed or queried without needing to decode the entire list.
/// - [`RoaringTreemap`] backing: internally backed by [`RoaringTreemap`], which supports 64-bit
///   integers.
#[derive(Clone, PartialEq, Default)]
pub struct IntegerList(RoaringTreemap);

impl fmt::Debug for IntegerList {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("IntegerList")?;
        f.debug_list().entries(self.0.iter()).finish()
    }
}

impl IntegerList {
    /// Maximum encoded byte size targeted when splitting a list into
    /// shards for storage in size-constrained backends.
    ///
    /// Currently sized for MDBX's `DUPSORT` value limit on 4 KB pages
    /// (~1980 bytes). The budget reserves headroom for a paired secondary
    /// key (up to 40 bytes), per-node metadata, and the rare case of a
    /// trailing index that lands in a fresh roaring container. See
    /// `append_to_sharded_history` in `signet-hot` (ENG-2287).
    pub const MAX_ENCODED_BYTES: usize = 1500;

    /// Maximum number of indices that always serialise within
    /// [`Self::MAX_ENCODED_BYTES`], regardless of how the values are
    /// distributed across roaring containers.
    ///
    /// # Encoding overhead (roaring 0.11)
    ///
    /// A [`RoaringTreemap`] serialises as an 8-byte count of inner
    /// [`roaring::RoaringBitmap`] entries, followed by `(u32 hi32, encoded
    /// RoaringBitmap)` pairs. Each `RoaringBitmap` partitions its 32-bit
    /// values into 16-bit array, bitmap, or run *containers* (4096-entry
    /// crossover from array to bitmap). The portable serialised format of
    /// a `RoaringBitmap` carries:
    ///
    /// - 8 bytes of cookie / container count / option flags
    /// - 4 bytes per container (key + cardinality descriptor)
    /// - 4 bytes per container (offset table entry)
    /// - the container payload itself (2 bytes per element for array, 8 KB
    ///   for bitmap, variable for run)
    ///
    /// The worst case is therefore one index per `RoaringBitmap` with each
    /// landing in its own array container: ~22 bytes per index plus the
    /// 8-byte treemap header. Empirically:
    ///
    /// ```text
    ///   N=  1 (1 bitmap, 1 cont, 1 elem) -> 30 bytes
    ///   N= 67 (67 bitmaps, ...)          -> 1482 bytes
    ///   N= 68 (68 bitmaps, ...)          -> 1504 bytes  (over)
    /// ```
    ///
    /// `BlockNumberList`s built from u64 block numbers that stay below
    /// 2 ^ 32 (i.e. real-world Ethereum block numbers) share a single
    /// `RoaringBitmap` and therefore encode at ~10 bytes per index in the
    /// pathological "one element per 16-bit container" pattern, fitting
    /// ~148 indices in [`Self::MAX_ENCODED_BYTES`]. This constant uses the
    /// stricter, distribution-agnostic bound so callers can rely on it for
    /// any u64 input.
    pub const SAFE_INDICES_PER_SHARD: usize = 67;

    /// Creates a new empty [`IntegerList`].
    pub fn empty() -> Self {
        Self(RoaringTreemap::new())
    }

    /// Creates an [`IntegerList`] from a list of integers.
    ///
    /// Returns an error if the list is not pre-sorted.
    pub fn new(list: impl IntoIterator<Item = u64>) -> Result<Self, IntegerListError> {
        RoaringTreemap::from_sorted_iter(list)
            .map(Self)
            .map_err(|_| IntegerListError::UnsortedInput)
    }

    /// Creates an [`IntegerList`] from a pre-sorted list of integers.
    ///
    /// # Panics
    ///
    /// Panics if the list is not pre-sorted.
    #[inline]
    #[track_caller]
    pub fn new_pre_sorted(list: impl IntoIterator<Item = u64>) -> Self {
        Self::new(list).expect("IntegerList must be pre-sorted")
    }

    /// Appends a list of integers to the current list.
    ///
    /// Returns an error if the list is not pre-sorted, with all entries strictly greater than
    /// existing ones. Any entries of `list` which were added while iterating prior to failure are
    /// retained in the `IntegerList`.
    ///
    /// Returns `Ok` with the number of elements appended to the list on success.
    pub fn append(&mut self, list: impl IntoIterator<Item = u64>) -> Result<u64, IntegerListError> {
        self.0.append(list).map_err(|_| IntegerListError::UnsortedInput)
    }

    /// Pushes a new integer to the list.
    pub fn push(&mut self, value: u64) -> Result<(), IntegerListError> {
        self.0.try_push(value).map_err(|_| IntegerListError::IntegerTooSmall)
    }

    /// Removes a single value from the list. Returns `true` if it was
    /// present.
    pub fn remove(&mut self, value: u64) -> bool {
        self.0.remove(value)
    }

    /// Clears the list.
    pub fn clear(&mut self) {
        self.0.clear();
    }

    /// Serializes an [`IntegerList`] into a sequence of bytes.
    ///
    /// # Panics
    ///
    /// Panics on any serialization error.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut vec = Vec::with_capacity(self.0.serialized_size());
        self.0.serialize_into(&mut vec).expect("not able to encode IntegerList to vec");
        vec
    }

    /// Serializes an [`IntegerList`] into a sequence of bytes.
    ///
    /// # Panics
    ///
    /// Panics on any serialization error.
    pub fn to_mut_bytes<B: BufMut>(&self, buf: &mut B) {
        self.0.serialize_into(buf.writer()).expect("not able to encode IntegerList to buffer");
    }

    /// Deserializes a sequence of bytes into a proper [`IntegerList`].
    pub fn from_bytes(data: &[u8]) -> Result<Self, IntegerListError> {
        RoaringTreemap::deserialize_from(data)
            .map(Self)
            .map_err(|_| IntegerListError::FailedToDeserialize)
    }

    /// Returns an iterator over the integers in the list, in ascending order.
    pub fn iter(&self) -> impl Iterator<Item = u64> + Clone + '_ {
        self.0.iter()
    }

    /// Returns the number of integers in the list.
    pub fn len(&self) -> u64 {
        self.0.len()
    }

    /// Returns `true` if the list contains no integers.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns `true` if the list contains the given value.
    pub fn contains(&self, value: u64) -> bool {
        self.0.contains(value)
    }

    /// Returns the smallest value in the list, or `None` if empty.
    pub fn min(&self) -> Option<u64> {
        self.0.min()
    }

    /// Returns the largest value in the list, or `None` if empty.
    pub fn max(&self) -> Option<u64> {
        self.0.max()
    }

    /// Returns the number of integers that are `<= value`.
    pub fn rank(&self, value: u64) -> u64 {
        self.0.rank(value)
    }

    /// Returns the `n`th integer in the list (0-indexed).
    pub fn select(&self, n: u64) -> Option<u64> {
        self.0.select(n)
    }

    /// Returns the serialized size of the list in bytes.
    pub fn serialized_size(&self) -> usize {
        self.0.serialized_size()
    }

    /// Serializes the list into the given writer.
    pub fn serialize_into<W: std::io::Write>(&self, writer: W) -> std::io::Result<()> {
        self.0.serialize_into(writer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn size(blocks: impl IntoIterator<Item = u64>) -> usize {
        IntegerList::new_pre_sorted(blocks).serialized_size()
    }

    /// A dense run of indices in a single 16-bit container encodes as a
    /// short header plus ~2 bytes per element (array container payload).
    #[test]
    fn dense_run_encodes_compactly() {
        assert_eq!(size(0..1), 30);
        assert_eq!(size(0..10), 48);
        assert!(size(0..1_000) < 2_100);
    }

    /// Each new 16-bit container costs 8 bytes of metadata (4 for the
    /// descriptor entry, 4 for the offset entry) plus 2 bytes for the
    /// element itself — so ~10 bytes per index when every index lands in
    /// its own container within a single [`RoaringBitmap`].
    #[test]
    fn sparse_within_single_bitmap_costs_ten_bytes_per_index() {
        // (i << 16) lands index i in container i of bitmap 0.
        let by_container = |n: u64| -> Vec<u64> { (0..n).map(|i| i << 16).collect() };
        assert_eq!(size(by_container(1)), 30);
        // ~10 bytes per additional index.
        assert_eq!(size(by_container(100)), 1020);
        assert_eq!(size(by_container(147)), 1490);
        // N=148 lands exactly on the budget; N=149 exceeds it.
        assert_eq!(size(by_container(148)), IntegerList::MAX_ENCODED_BYTES);
        assert!(size(by_container(149)) > IntegerList::MAX_ENCODED_BYTES);
    }

    /// Each new [`RoaringBitmap`] (distinct upper-32-bit key) adds ~22
    /// bytes — this is the worst case across any u64 distribution.
    #[test]
    fn sparse_across_bitmaps_costs_twenty_two_bytes_per_index() {
        // (i << 32) places each index in its own bitmap.
        let by_bitmap = |n: u64| -> Vec<u64> { (0..n).map(|i| i << 32).collect() };
        assert_eq!(size(by_bitmap(1)), 30);
        assert_eq!(size(by_bitmap(10)), 228);
        assert_eq!(size(by_bitmap(50)), 1108);
        // The boundary determines SAFE_INDICES_PER_SHARD.
        assert!(size(by_bitmap(67)) <= IntegerList::MAX_ENCODED_BYTES);
        assert!(size(by_bitmap(68)) > IntegerList::MAX_ENCODED_BYTES);
    }

    /// The published [`IntegerList::SAFE_INDICES_PER_SHARD`] must fit in
    /// [`IntegerList::MAX_ENCODED_BYTES`] under the worst-case
    /// distribution (one index per [`RoaringBitmap`] — the most
    /// metadata-heavy u64 layout).
    #[test]
    fn safe_indices_per_shard_fits_worst_case() {
        let worst_case: Vec<u64> =
            (0..IntegerList::SAFE_INDICES_PER_SHARD as u64).map(|i| i << 32).collect();
        let bytes = size(worst_case);
        assert!(
            bytes <= IntegerList::MAX_ENCODED_BYTES,
            "SAFE_INDICES_PER_SHARD={} encodes to {} bytes, over MAX_ENCODED_BYTES={}",
            IntegerList::SAFE_INDICES_PER_SHARD,
            bytes,
            IntegerList::MAX_ENCODED_BYTES,
        );
    }

    /// The next index past [`IntegerList::SAFE_INDICES_PER_SHARD`] must
    /// be *able* to overflow under the worst case — otherwise the bound
    /// is needlessly conservative and should be raised.
    #[test]
    fn safe_indices_per_shard_is_tight() {
        let just_over: Vec<u64> =
            (0..(IntegerList::SAFE_INDICES_PER_SHARD as u64 + 1)).map(|i| i << 32).collect();
        assert!(
            size(just_over) > IntegerList::MAX_ENCODED_BYTES,
            "SAFE_INDICES_PER_SHARD is loose — N+1 worst-case indices still fit; raise the bound",
        );
    }
}
