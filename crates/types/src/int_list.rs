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

    /// Remove and return the largest value, or `None` if the list is empty.
    pub fn pop_max(&mut self) -> Option<u64> {
        let m = self.0.max()?;
        self.0.remove(m);
        Some(m)
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

    /// Append `additions` to `self`, splitting off a tail iff the merged
    /// list exceeds `max_bytes` after roaring encoding.
    ///
    /// Returns `(first, second)`:
    /// - `first`: the lower-block-number portion (always returned).
    /// - `second`: `Some(tail)` iff a split occurred; contains the higher
    ///   block numbers. The caller is responsible for choosing subkeys
    ///   for the two shards (e.g., `first.max()` and `u64::MAX`).
    ///
    /// Every block yielded by `additions` must be strictly greater than
    /// every block already in `self`. If not, the underlying
    /// [`IntegerList::push`] will panic.
    ///
    /// Allocation: zero on the no-split fast path beyond roaring container
    /// growth. One `IntegerList` allocation when a split occurs.
    pub fn merge_and_split(
        mut self,
        additions: impl IntoIterator<Item = u64>,
        max_bytes: usize,
    ) -> (Self, Option<Self>) {
        let mut tail: Option<Self> = None;

        for block in additions {
            if let Some(t) = tail.as_mut() {
                t.push(block).expect("strictly increasing");
                continue;
            }
            self.push(block).expect("strictly increasing");
            if self.serialized_size() > max_bytes {
                let popped = self.pop_max().expect("just pushed");
                debug_assert_eq!(popped, block);
                let mut t = Self::empty();
                t.push(block).expect("first push always succeeds");
                tail = Some(t);
            }
        }

        (self, tail)
    }
}

#[cfg(test)]
mod tests {
    use super::IntegerList;

    #[test]
    fn pop_max_returns_and_removes_largest() {
        let mut list = IntegerList::new([3u64, 7, 9, 12]).unwrap();
        assert_eq!(list.pop_max(), Some(12));
        assert_eq!(list.max(), Some(9));
        assert_eq!(list.len(), 3);
    }

    #[test]
    fn pop_max_on_empty_returns_none() {
        let mut list = IntegerList::empty();
        assert_eq!(list.pop_max(), None);
        assert!(list.is_empty());
    }

    #[test]
    fn pop_max_drains_to_empty() {
        let mut list = IntegerList::new([42u64]).unwrap();
        assert_eq!(list.pop_max(), Some(42));
        assert!(list.is_empty());
        assert_eq!(list.pop_max(), None);
    }

    #[test]
    fn merge_and_split_no_split_when_under_budget() {
        let existing = IntegerList::new([1u64, 2, 3]).unwrap();
        // 1500 B is generous for 5 dense values
        let (first, second) = existing.merge_and_split([4u64, 5], 1500);
        assert_eq!(first.iter().collect::<Vec<_>>(), vec![1, 2, 3, 4, 5]);
        assert!(second.is_none());
    }

    #[test]
    fn merge_and_split_no_additions_returns_existing() {
        let existing = IntegerList::new([10u64, 20]).unwrap();
        let (first, second) = existing.merge_and_split(std::iter::empty(), 1500);
        assert_eq!(first.iter().collect::<Vec<_>>(), vec![10, 20]);
        assert!(second.is_none());
    }

    #[test]
    fn merge_and_split_splits_when_over_budget() {
        // Provoke a split with a deliberately small budget. Use 100 contiguous
        // values starting at 0. Roaring-encoded that's a single run container
        // around 14 B, so we need a tiny budget to force a split. Set budget
        // small enough that ~50 entries push us over.
        let existing = IntegerList::new(0u64..50).unwrap();

        // Compute the budget so existing alone fits but existing + additions
        // doesn't.
        let existing_size = existing.serialized_size();
        let combined = IntegerList::new(0u64..100).unwrap().serialized_size();
        assert!(combined > existing_size, "test setup broken: combined didn't grow");
        let budget = existing_size + (combined - existing_size) / 2;

        let (first, second) = existing.merge_and_split(50u64..100, budget);
        let second = second.expect("split should have occurred");

        // first ∪ second == 0..100, with second's min > first's max.
        let mut all: Vec<u64> = first.iter().collect();
        all.extend(second.iter());
        assert_eq!(all, (0u64..100).collect::<Vec<_>>());
        assert!(first.max().unwrap() < second.min().unwrap());

        // Both halves fit in budget.
        assert!(first.serialized_size() <= budget);
        assert!(second.serialized_size() <= budget);
    }

    #[test]
    fn merge_and_split_split_preserves_strict_ordering() {
        // Sparse blocks (one per distinct 16-bit container) make serialized
        // size grow proportionally to count, so we can budget-tune to force
        // a split while keeping additions within budget.
        let existing_blocks: Vec<u64> = (0..50u64).map(|i| i * 0x1_0000).collect();
        let addition_blocks: Vec<u64> = (50..70u64).map(|i| i * 0x1_0000).collect();
        let combined_blocks: Vec<u64> = (0..70u64).map(|i| i * 0x1_0000).collect();

        let existing = IntegerList::new(existing_blocks).unwrap();
        let additions_size =
            IntegerList::new(addition_blocks.iter().copied()).unwrap().serialized_size();
        let combined_size = IntegerList::new(combined_blocks).unwrap().serialized_size();
        let existing_size = existing.serialized_size();
        // Budget that fits both existing and additions alone, but not combined.
        let budget = existing_size.max(additions_size) + 16;
        assert!(combined_size > budget, "test setup broken: combined fits in budget");

        let (first, second) = existing.merge_and_split(addition_blocks, budget);
        let second = second.expect("split should have occurred");

        let first_max = first.max().unwrap();
        let second_min = second.min().unwrap();
        assert!(first_max < second_min, "first.max={first_max} second.min={second_min}",);
    }

    /// Dense-pack worst case: many contiguous blocks in a single 16-bit
    /// container should encode efficiently (run-length or array). Even at
    /// hundreds of contiguous blocks, we stay comfortably under 1500 B.
    #[test]
    fn worst_case_dense_pack_fits_in_dupsort_budget() {
        // 650 contiguous values empirically encode to ~1300 B; 750 was over budget.
        let list = IntegerList::new(0u64..650).unwrap();
        let size = list.serialized_size();
        assert!(size <= 1500, "dense pack of 650 blocks encoded as {size} B, expected <= 1500");
    }

    /// Sparse worst case: 100 blocks each in a distinct 16-bit container.
    /// Each container is array-encoded with a single element plus header.
    /// This is the realistic worst case for a long-lived hot address touched
    /// once per ~64k-block window.
    #[test]
    fn worst_case_sparse_distinct_containers_fits_in_dupsort_budget() {
        let blocks: Vec<u64> = (0..100u64).map(|i| i * 0x1_0000).collect();
        let list = IntegerList::new(blocks).unwrap();
        let size = list.serialized_size();
        assert!(size <= 1500, "100 sparse blocks encoded as {size} B, expected <= 1500");
    }

    /// merge_and_split with the realistic budget produces shards each within
    /// the budget, even for the worst-case sparse input.
    #[test]
    fn merge_and_split_at_realistic_budget_respects_per_shard_size() {
        // Build 200 sparse blocks (200 distinct containers). Splitter should
        // split this into ~2 shards each under 1500 B.
        let blocks: Vec<u64> = (0..200u64).map(|i| i * 0x1_0000).collect();
        let half = blocks.len() / 2;
        let existing = IntegerList::new(blocks[..half].iter().copied()).unwrap();
        let additions: Vec<u64> = blocks[half..].to_vec();

        assert!(existing.serialized_size() <= 1500);
        assert!(IntegerList::new(additions.iter().copied()).unwrap().serialized_size() <= 1500);

        let (first, second) = existing.merge_and_split(additions, 1500);
        assert!(first.serialized_size() <= 1500);
        if let Some(second) = &second {
            assert!(second.serialized_size() <= 1500);
        }

        // Round-trip: union of (first, second) equals the original input.
        let mut roundtrip: Vec<u64> = first.iter().collect();
        if let Some(s) = second {
            roundtrip.extend(s.iter());
        }
        assert_eq!(roundtrip, blocks);
    }
}
