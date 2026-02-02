//! Error types for cold MDBX storage.

use signet_hot::ser::DeserError;

/// Errors that can occur in cold MDBX storage operations.
#[derive(Debug, thiserror::Error)]
pub enum MdbxColdError {
    /// A serialization/deserialization error occurred.
    #[error("serialization error: {0}")]
    Ser(#[from] DeserError),
}
