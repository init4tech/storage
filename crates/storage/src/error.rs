//! Error types for unified storage operations.

use signet_cold::ColdStorageError;
use signet_hot::{HistoryError, model::HotKvError};

/// Error type for unified storage operations.
#[derive(Debug, thiserror::Error)]
pub enum StorageError<E: std::error::Error> {
    /// Error creating a hot storage transaction.
    #[error("hot storage transaction error: {0}")]
    HotTx(#[source] HotKvError),
    /// Error during hot storage operations.
    #[error("hot storage error: {0}")]
    Hot(#[source] HistoryError<E>),
    /// Error from cold storage operations.
    #[error("cold storage error: {0}")]
    Cold(#[source] ColdStorageError),
}

impl<E: std::error::Error> From<HotKvError> for StorageError<E> {
    fn from(err: HotKvError) -> Self {
        Self::HotTx(err)
    }
}

impl<E: std::error::Error> From<HistoryError<E>> for StorageError<E> {
    fn from(err: HistoryError<E>) -> Self {
        Self::Hot(err)
    }
}

impl<E: std::error::Error> From<ColdStorageError> for StorageError<E> {
    fn from(err: ColdStorageError) -> Self {
        Self::Cold(err)
    }
}

/// Result type alias for unified storage operations.
pub type StorageResult<T, E> = Result<T, StorageError<E>>;
