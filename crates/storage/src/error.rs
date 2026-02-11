//! Error types for unified storage operations.

use signet_cold::ColdStorageError;
use signet_hot::{HistoryError, model::HotKvError};

/// Error type for unified storage operations.
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    /// Error during hot storage operations.
    #[error("hot storage error: {0}")]
    Hot(#[source] HistoryError<HotKvError>),
    /// Error from cold storage operations.
    #[error("cold storage error: {0}")]
    Cold(#[source] ColdStorageError),
}

impl From<HistoryError<HotKvError>> for StorageError {
    fn from(err: HistoryError<HotKvError>) -> Self {
        Self::Hot(err)
    }
}

impl From<ColdStorageError> for StorageError {
    fn from(err: ColdStorageError) -> Self {
        Self::Cold(err)
    }
}

impl From<HotKvError> for StorageError {
    fn from(err: HotKvError) -> Self {
        Self::Hot(HistoryError::Db(err))
    }
}

/// Result type alias for unified storage operations.
pub type StorageResult<T> = Result<T, StorageError>;
