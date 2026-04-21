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

    /// The streaming operation exceeded its deadline.
    ///
    /// The backend enforces a wall-clock limit on streaming operations
    /// to prevent unbounded resource acquisition. Partial results may
    /// have been delivered before this error.
    #[error("stream deadline exceeded")]
    StreamDeadlineExceeded,

    /// A reorg was detected during a streaming operation.
    ///
    /// The anchor block hash changed between chunks, indicating that
    /// the data being streamed may no longer be consistent. Partial
    /// results may have been delivered before this error.
    #[error("reorg detected during streaming")]
    ReorgDetected,

    /// The cold storage task has terminated.
    ///
    /// A concurrency semaphore was closed, indicating shutdown. Reads and
    /// writes cannot be dispatched until the handle is recreated.
    #[error("cold storage task terminated: semaphore closed")]
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

    /// Short, stable label identifying the error variant. Used as a metric
    /// label.
    pub(crate) const fn kind(&self) -> &'static str {
        match self {
            Self::Backend(_) => "backend",
            Self::NotFound(_) => "not_found",
            Self::TooManyLogs { .. } => "too_many_logs",
            Self::StreamDeadlineExceeded => "stream_deadline",
            Self::ReorgDetected => "reorg",
            Self::TaskTerminated => "task_terminated",
        }
    }
}
