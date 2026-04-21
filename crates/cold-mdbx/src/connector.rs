//! MDBX storage connector.
//!
//! Unified connector that can open both hot and cold MDBX databases.

use crate::{
    MdbxColdBackend, MdbxColdError,
    backend::{DEFAULT_READ_TIMEOUT, DEFAULT_WRITE_TIMEOUT},
};
use signet_cold::ColdConnect;
use signet_hot::HotConnect;
use signet_hot_mdbx::{DatabaseArguments, DatabaseEnv, MdbxError};
use std::{path::PathBuf, time::Duration};

/// Errors that can occur when initializing MDBX connectors.
#[derive(Debug, thiserror::Error)]
pub enum MdbxConnectorError {
    /// Missing environment variable.
    #[error("missing environment variable: {0}")]
    MissingEnvVar(&'static str),

    /// Hot storage initialization failed.
    #[error("hot storage initialization failed: {0}")]
    HotInit(#[from] MdbxError),

    /// Cold storage initialization failed.
    #[error("cold storage initialization failed: {0}")]
    ColdInit(#[from] MdbxColdError),
}

/// Connector for MDBX storage (both hot and cold).
///
/// This unified connector can open MDBX databases for both hot and cold storage.
/// It holds the path and database arguments, which can include custom geometry,
/// sync mode, max readers, and other MDBX-specific configuration.
///
/// # Example
///
/// ```ignore
/// use signet_hot_mdbx::{MdbxConnector, DatabaseArguments};
///
/// // Hot storage with custom args
/// let hot = MdbxConnector::new("/tmp/hot")
///     .with_db_args(DatabaseArguments::new().with_max_readers(1000));
///
/// // Cold storage with default args
/// let cold = MdbxConnector::new("/tmp/cold");
/// ```
#[derive(Debug, Clone)]
pub struct MdbxConnector {
    path: PathBuf,
    db_args: DatabaseArguments,
    read_timeout: Duration,
    write_timeout: Duration,
}

impl MdbxConnector {
    /// Create a new MDBX connector with default database arguments.
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            db_args: DatabaseArguments::new(),
            read_timeout: DEFAULT_READ_TIMEOUT,
            write_timeout: DEFAULT_WRITE_TIMEOUT,
        }
    }

    /// Set custom database arguments.
    ///
    /// This allows configuring MDBX-specific settings like geometry, sync mode,
    /// max readers, and exclusive mode.
    #[must_use]
    pub const fn with_db_args(mut self, db_args: DatabaseArguments) -> Self {
        self.db_args = db_args;
        self
    }

    /// Set the read deadline applied to the cold backend produced by
    /// [`ColdConnect::connect`]. See [`MdbxColdBackend::with_read_timeout`].
    #[must_use]
    pub const fn with_read_timeout(mut self, read_timeout: Duration) -> Self {
        self.read_timeout = read_timeout;
        self
    }

    /// Set the advisory write deadline applied to the cold backend produced
    /// by [`ColdConnect::connect`]. See [`MdbxColdBackend::with_write_timeout`].
    #[must_use]
    pub const fn with_write_timeout(mut self, write_timeout: Duration) -> Self {
        self.write_timeout = write_timeout;
        self
    }

    /// Get a reference to the path.
    pub fn path(&self) -> &std::path::Path {
        &self.path
    }

    /// Get a reference to the database arguments.
    pub const fn db_args(&self) -> &DatabaseArguments {
        &self.db_args
    }

    /// Create a connector from environment variables.
    ///
    /// Reads the path from the specified environment variable.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use signet_cold_mdbx::MdbxConnector;
    ///
    /// let hot = MdbxConnector::from_env("SIGNET_HOT_PATH")?;
    /// let cold = MdbxConnector::from_env("SIGNET_COLD_PATH")?;
    /// ```
    pub fn from_env(env_var: &'static str) -> Result<Self, MdbxConnectorError> {
        let path: PathBuf =
            std::env::var(env_var).map_err(|_| MdbxConnectorError::MissingEnvVar(env_var))?.into();
        Ok(Self::new(path))
    }
}

impl HotConnect for MdbxConnector {
    type Hot = DatabaseEnv;
    type Error = MdbxError;

    fn connect(&self) -> Result<Self::Hot, Self::Error> {
        self.db_args.clone().open_rw(&self.path)
    }
}

impl ColdConnect for MdbxConnector {
    type Cold = MdbxColdBackend;
    type Error = MdbxColdError;

    #[allow(clippy::manual_async_fn)]
    fn connect(&self) -> impl std::future::Future<Output = Result<Self::Cold, Self::Error>> + Send {
        // MDBX open is sync, but wrapped in async for trait consistency
        // Opens read-write and creates tables
        let path = self.path.clone();
        let read_timeout = self.read_timeout;
        let write_timeout = self.write_timeout;
        async move {
            MdbxColdBackend::open_rw(&path)
                .map(|b| b.with_read_timeout(read_timeout).with_write_timeout(write_timeout))
        }
    }
}
