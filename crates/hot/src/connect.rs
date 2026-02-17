//! Connection traits for hot storage backends.

use crate::model::HotKv;

/// Connector trait for hot storage backends.
///
/// Abstracts the connection/opening process for hot storage, allowing
/// different backends to implement their own initialization logic.
pub trait HotConnect {
    /// The hot storage type produced by this connector.
    type Hot: HotKv;

    /// The error type returned by connection attempts.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Connect to the hot storage backend.
    ///
    /// Synchronous since most hot storage backends use sync initialization.
    fn connect(&self) -> Result<Self::Hot, Self::Error>;
}
