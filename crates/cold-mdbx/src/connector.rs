//! MDBX storage connector.
//!
//! Unified connector that can open both hot and cold MDBX databases.

use crate::{MdbxColdBackend, MdbxColdError};
use signet_cold::ColdConnect;
use signet_hot::HotConnect;
use signet_hot_mdbx::{DatabaseArguments, DatabaseEnv, MdbxError};
use std::path::PathBuf;

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
}

impl MdbxConnector {
    /// Create a new MDBX connector with default database arguments.
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into(), db_args: DatabaseArguments::new() }
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
    /// use signet_hot_mdbx::MdbxConnector;
    ///
    /// let hot = MdbxConnector::from_env("SIGNET_HOT_PATH")?;
    /// let cold = MdbxConnector::from_env("SIGNET_COLD_PATH")?;
    /// ```
    pub fn from_env(env_var: &str) -> Result<Self, MdbxError> {
        let path: PathBuf = std::env::var(env_var)
            .map_err(|_| MdbxError::Config(format!("missing environment variable: {env_var}")))?
            .into();
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

    async fn connect(&self) -> Result<Self::Cold, Self::Error> {
        // MDBX open is sync, but wrapped in async for trait consistency
        // Opens read-write and creates tables
        MdbxColdBackend::open_rw(&self.path)
    }
}
