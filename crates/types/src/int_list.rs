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

    /// Returns an iterator over the integers in the list.
    pub fn iter(&self) -> roaring::treemap::Iter<'_> {
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
