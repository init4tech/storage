//! Error types for cold MDBX storage.

use signet_hot::ser::DeserError;

/// Errors that can occur in cold MDBX storage operations.
#[derive(Debug, thiserror::Error)]
pub enum MdbxColdError {
    /// A serialization/deserialization error occurred.
    #[error("serialization error: {0}")]
    Ser(#[from] DeserError),

    /// An MDBX error occurred.
    #[error("mdbx error: {0}")]
    Mdbx(#[from] signet_hot_mdbx::MdbxError),

    /// Database is read-only.
    #[error("database is read-only")]
    ReadOnly,

    /// Too many logs matched the filter.
    #[error("too many logs: limit is {0}")]
    TooManyLogs(usize),
}

impl From<MdbxColdError> for signet_cold::ColdStorageError {
    fn from(error: MdbxColdError) -> Self {
        match error {
            MdbxColdError::TooManyLogs(limit) => Self::TooManyLogs { limit },
            other => Self::Backend(Box::new(other)),
        }
    }
}
