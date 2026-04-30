//! Error types for cold SQL storage.

/// Postgres SQLSTATE for `query_canceled` — emitted by the server when
/// `statement_timeout` trips a query.
const PG_SQLSTATE_QUERY_CANCELED: &str = "57014";

/// Errors that can occur in cold SQL storage operations.
#[derive(Debug, thiserror::Error)]
pub enum SqlColdError {
    /// The query was cancelled by Postgres `statement_timeout`
    /// (SQLSTATE 57014).
    ///
    /// The configured deadline (read or write timeout) is not threaded
    /// through to this conversion — the variant is detected at the
    /// `From<sqlx::Error>` boundary, which has no method context. The
    /// handle surfaces this as
    /// [`signet_cold::ColdStorageError::DeadlineExceeded`], which is
    /// what callers and metrics should match on.
    #[error("postgres statement_timeout exceeded")]
    Timeout,

    /// A sqlx database error occurred.
    #[error("sqlx error: {0}")]
    Sqlx(sqlx::Error),

    /// A data conversion error occurred.
    #[error("conversion error: {0}")]
    Convert(String),
}

impl From<sqlx::Error> for SqlColdError {
    fn from(e: sqlx::Error) -> Self {
        if let sqlx::Error::Database(ref db) = e
            && db.code().as_deref() == Some(PG_SQLSTATE_QUERY_CANCELED)
        {
            return Self::Timeout;
        }
        Self::Sqlx(e)
    }
}

impl From<SqlColdError> for signet_cold::ColdStorageError {
    fn from(error: SqlColdError) -> Self {
        match error {
            // Duration unknown at this conversion boundary; surface as
            // ZERO so callers can match on `DeadlineExceeded(_)` and
            // metrics see the deadline error class. Threading the
            // configured deadline is a separate refactor.
            SqlColdError::Timeout => Self::DeadlineExceeded(std::time::Duration::ZERO),
            other => Self::Backend(Box::new(other)),
        }
    }
}
