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

    /// The cold storage task cannot keep up with incoming requests.
    ///
    /// The channel is full, indicating transient backpressure. The cold storage
    /// task is still running but processing requests slower than they arrive.
    ///
    /// Callers may:
    /// - Accept the gap (hot storage is authoritative) and repair later
    /// - Retry after a delay with exponential backoff
    /// - Increase channel capacity at construction time
    #[error("cold storage backpressure: channel full")]
    Backpressure,

    /// The query matched more logs than the caller-specified `max_logs` limit.
    ///
    /// The query is rejected entirely rather than returning a partial result.
    /// Callers should narrow the filter (smaller block range, more specific
    /// address/topic filters) or increase the limit.
    #[error("too many logs: query exceeds {limit}")]
    TooManyLogs {
        /// The limit that was exceeded.
        limit: usize,
    },

    /// The cold storage task has terminated.
    ///
    /// The channel is closed because the task has stopped (panic, cancellation,
    /// or shutdown). This is persistent: all subsequent dispatches will fail
    /// until the task is restarted.
    ///
    /// Hot storage remains consistent and authoritative. Cold storage can be
    /// replayed from hot storage after task recovery.
    #[error("cold storage task terminated: channel closed")]
    TaskTerminated,
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
