use bytes::BufMut;
use core::fmt;
use roaring::RoaringTreemap;

/// List with transaction numbers.
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
pub struct IntegerList(pub RoaringTreemap);

impl std::ops::Deref for IntegerList {
    type Target = RoaringTreemap;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

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
        Self::new(list).expect("IntegerList must be pre-sorted and non-empty")
    }

    /// Appends a list of integers to the current list.
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
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut vec = Vec::with_capacity(self.0.serialized_size());
        self.0.serialize_into(&mut vec).expect("not able to encode IntegerList");
        vec
    }

    /// Serializes an [`IntegerList`] into a sequence of bytes.
    pub fn to_mut_bytes<B: bytes::BufMut>(&self, buf: &mut B) {
        self.0.serialize_into(buf.writer()).unwrap();
    }

    /// Deserializes a sequence of bytes into a proper [`IntegerList`].
    pub fn from_bytes(data: &[u8]) -> Result<Self, IntegerListError> {
        RoaringTreemap::deserialize_from(data)
            .map(Self)
            .map_err(|_| IntegerListError::FailedToDeserialize)
    }
}
