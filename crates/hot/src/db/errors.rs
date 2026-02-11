use alloy::primitives::B256;
use signet_storage_types::IntegerListError;

/// A result type for history operations.
pub type HistoryResult<T, E> = Result<T, HistoryError<E>>;

/// Error type for history operations.
///
/// This error is returned by methods that append or unwind history,
/// and includes both chain consistency errors and database errors.
#[derive(Debug, thiserror::Error)]
pub enum HistoryError<E: std::error::Error> {
    /// Block number doesn't extend the chain contiguously.
    #[error("non-contiguous block: expected {expected}, got {got}")]
    NonContiguousBlock {
        /// The expected block number (current tip + 1).
        expected: u64,
        /// The actual block number provided.
        got: u64,
    },
    /// Parent hash doesn't match current tip or previous block in range.

    #[error("parent hash mismatch: expected {expected}, got {got}")]
    ParentHashMismatch {
        /// The expected parent hash.
        expected: B256,
        /// The actual parent hash provided.
        got: B256,
    },

    /// Invoked `load_genesis` on a non-empty database.
    #[error("cannot initialize genesis on a non-empty database")]
    DbNotEmpty,

    /// Empty header range provided to a method that requires at least one
    /// header.
    #[error("empty header range provided")]
    EmptyRange,

    /// No blocks exist in the database.
    #[error("no blocks in database")]
    NoBlocks,

    /// The requested height is outside the stored block range.
    #[error("height {height} outside stored range {first}..={last}")]
    HeightOutOfRange {
        /// The requested height.
        height: u64,
        /// The first stored block number.
        first: u64,
        /// The last stored block number.
        last: u64,
    },

    /// Database error.
    #[error("{0}")]
    Db(#[from] E),

    /// Integer List error.
    #[error(transparent)]
    IntList(IntegerListError),
}

impl<E: std::error::Error> HistoryError<E> {
    /// Helper to create a database error
    pub const fn intlist(err: IntegerListError) -> Self {
        HistoryError::IntList(err)
    }

    /// Map the database error variant to a different error type.
    ///
    /// All non-[`Db`](Self::Db) variants are converted directly. The `Db`
    /// variant is transformed using the provided closure.
    pub fn map_db<F: std::error::Error>(self, f: impl FnOnce(E) -> F) -> HistoryError<F> {
        match self {
            Self::Db(e) => HistoryError::Db(f(e)),
            Self::IntList(e) => HistoryError::IntList(e),
            Self::NonContiguousBlock { expected, got } => {
                HistoryError::NonContiguousBlock { expected, got }
            }
            Self::ParentHashMismatch { expected, got } => {
                HistoryError::ParentHashMismatch { expected, got }
            }
            Self::DbNotEmpty => HistoryError::DbNotEmpty,
            Self::EmptyRange => HistoryError::EmptyRange,
            Self::NoBlocks => HistoryError::NoBlocks,
            Self::HeightOutOfRange { height, first, last } => {
                HistoryError::HeightOutOfRange { height, first, last }
            }
        }
    }
}
