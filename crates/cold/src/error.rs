//! Error types for cold storage operations.

/// Result type alias for cold storage operations.
pub type ColdResult<T, E = ColdStorageError> = Result<T, E>;

/// Error type for cold storage operations.
#[derive(Debug, thiserror::Error)]
pub enum ColdStorageError {
    /// An error occurred in the storage backend.
    #[error("Backend error: {0}")]
    Backend(#[from] Box<dyn core::error::Error + Send + Sync + 'static>),

    /// The requested resource was not found.
    #[error("Not found: {0}")]
    NotFound(String),

    /// The storage task was cancelled.
    #[error("Task cancelled")]
    Cancelled,

    /// Failed to send request to cold storage task.
    ///
    /// This error occurs in two scenarios:
    ///
    /// - **Backpressure**: The channel is full because the cold storage task
    ///   cannot keep up with incoming requests. This is typically transient
    ///   and indicates the caller is producing faster than cold storage can
    ///   consume. Callers may retry after a delay or accept data loss in cold
    ///   storage (hot storage remains authoritative).
    ///
    /// - **Task failure**: The channel is closed because the cold storage task
    ///   has terminated (panic, cancellation, or shutdown). This is persistent
    ///   and all subsequent dispatches will fail until the task is restarted.
    ///
    /// Callers cannot distinguish between these cases from the error alone.
    /// Use [`ColdStorageHandle::get_latest_block`] to probe task health: a
    /// response indicates the task is alive (backpressure), while
    /// [`ColdStorageError::Cancelled`] indicates task failure.
    #[error("failed to send request to cold storage task")]
    SendFailed,
}

impl ColdStorageError {
    /// Create a new backend error from any error type.
    pub fn backend<E>(error: E) -> Self
    where
        E: core::error::Error + Send + Sync + 'static,
    {
        Self::Backend(Box::new(error))
    }
}
