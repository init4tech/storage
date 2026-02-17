//! Connection traits for cold storage backends.

use crate::ColdStorage;

/// Connector trait for cold storage backends.
///
/// Abstracts the connection/opening process for cold storage, allowing
/// different backends to implement their own initialization logic.
pub trait ColdConnect {
    /// The cold storage type produced by this connector.
    type Cold: ColdStorage;

    /// The error type returned by connection attempts.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Connect to the cold storage backend asynchronously.
    ///
    /// Async to support backends that require async initialization
    /// (like SQL connection pools).
    fn connect(&self) -> impl std::future::Future<Output = Result<Self::Cold, Self::Error>> + Send;
}
