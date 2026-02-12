//! Error types for cold SQL storage.

/// Errors that can occur in cold SQL storage operations.
#[derive(Debug, thiserror::Error)]
pub enum SqlColdError {
    /// A sqlx database error occurred.
    #[error("sqlx error: {0}")]
    Sqlx(#[from] sqlx::Error),

    /// A data conversion error occurred.
    #[error("conversion error: {0}")]
    Convert(String),
}

impl From<SqlColdError> for signet_cold::ColdStorageError {
    fn from(error: SqlColdError) -> Self {
        Self::Backend(Box::new(error))
    }
}
