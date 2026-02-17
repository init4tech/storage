//! Error types for unified storage operations.

use crate::config::ConfigError;
use signet_cold::ColdStorageError;
use signet_cold_mdbx::MdbxColdError;
use signet_hot::{HistoryError, model::HotKvError};
use signet_hot_mdbx::MdbxError;

/// Error type for unified storage operations.
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    /// Error during hot storage operations.
    #[error("hot storage error: {0}")]
    Hot(#[source] HistoryError<HotKvError>),
    /// Error from cold storage operations.
    #[error("cold storage error: {0}")]
    Cold(#[source] ColdStorageError),
    /// Configuration error.
    #[error("configuration error: {0}")]
    Config(String),
    /// MDBX hot storage error.
    #[error("MDBX hot storage error: {0}")]
    MdbxHot(#[from] MdbxError),
    /// MDBX cold storage error.
    #[error("MDBX cold storage error: {0}")]
    MdbxCold(#[from] MdbxColdError),
    /// SQL cold storage error.
    #[cfg(any(feature = "postgres", feature = "sqlite"))]
    #[error("SQL cold storage error: {0}")]
    SqlCold(#[from] signet_cold_sql::SqlColdError),
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

impl From<ConfigError> for StorageError {
    fn from(err: ConfigError) -> Self {
        Self::Config(err.to_string())
    }
}

/// Result type alias for unified storage operations.
pub type StorageResult<T> = Result<T, StorageError>;
