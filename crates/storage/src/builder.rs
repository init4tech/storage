//! Storage builder for programmatic and environment-based configuration.
//!
//! This module provides a builder pattern for instantiating storage with
//! different backend combinations. The builder supports both programmatic
//! configuration and automatic loading from environment variables.
//!
//! # Examples
//!
//! ## From Environment
//!
//! ```ignore
//! use signet_storage::builder::StorageBuilder;
//! use std::env;
//!
//! // Set environment variables
//! env::set_var("STORAGE_MODE", "hot-only");
//! env::set_var("SIGNET_HOT_PATH", "/tmp/hot");
//!
//! // Build from environment
//! let storage = StorageBuilder::from_env()?.build()?;
//! ```
//!
//! ## Programmatic Hot-Only
//!
//! ```ignore
//! use signet_storage::builder::{StorageBuilder, StorageInstance};
//! use signet_storage::config::StorageMode;
//!
//! let storage = StorageBuilder::new()
//!     .mode(StorageMode::HotOnly)
//!     .hot_path("/tmp/hot")
//!     .build()?;
//!
//! match storage {
//!     StorageInstance::HotOnly(db) => {
//!         // Use hot-only database
//!     }
//!     StorageInstance::Unified(_) => unreachable!(),
//! }
//! ```
//!
//! ## Programmatic Hot + Cold MDBX
//!
//! ```ignore
//! use signet_storage::builder::StorageBuilder;
//! use signet_storage::config::StorageMode;
//! use tokio_util::sync::CancellationToken;
//!
//! let cancel = CancellationToken::new();
//! let storage = StorageBuilder::new()
//!     .mode(StorageMode::HotColdMdbx)
//!     .hot_path("/tmp/hot")
//!     .cold_path("/tmp/cold")
//!     .cancel_token(cancel)
//!     .build()?;
//! ```

use crate::{
    StorageError, StorageResult, UnifiedStorage,
    config::{ConfigError, ENV_COLD_PATH, ENV_COLD_SQL_URL, ENV_HOT_PATH, StorageMode},
};
use signet_cold_mdbx::MdbxColdBackend;
use signet_hot_mdbx::{DatabaseArguments, DatabaseEnv};
use std::{
    env,
    path::{Path, PathBuf},
};
use tokio_util::sync::CancellationToken;

/// Storage instance returned by the builder.
///
/// This enum forces callers to handle both hot-only and unified storage cases
/// explicitly, preventing accidental calls to cold storage methods on hot-only
/// instances.
#[derive(Debug)]
pub enum StorageInstance {
    /// Unified storage with both hot and cold backends.
    Unified(UnifiedStorage<DatabaseEnv>),
    /// Hot-only storage without cold backend.
    HotOnly(DatabaseEnv),
}

impl StorageInstance {
    /// Get a reference to the hot database environment.
    ///
    /// This method works for both unified and hot-only instances.
    pub const fn hot(&self) -> &DatabaseEnv {
        match self {
            Self::Unified(unified) => unified.hot(),
            Self::HotOnly(db) => db,
        }
    }

    /// Convert into the hot database environment, consuming self.
    ///
    /// This method works for both unified and hot-only instances.
    pub fn into_hot(self) -> DatabaseEnv {
        match self {
            Self::Unified(unified) => unified.into_hot(),
            Self::HotOnly(db) => db,
        }
    }

    /// Get a reference to unified storage, if available.
    ///
    /// Returns `None` for hot-only instances.
    pub const fn as_unified(&self) -> Option<&UnifiedStorage<DatabaseEnv>> {
        match self {
            Self::Unified(unified) => Some(unified),
            Self::HotOnly(_) => None,
        }
    }

    /// Convert into unified storage, if available.
    ///
    /// Returns `None` for hot-only instances.
    pub fn into_unified(self) -> Option<UnifiedStorage<DatabaseEnv>> {
        match self {
            Self::Unified(unified) => Some(unified),
            Self::HotOnly(_) => None,
        }
    }
}

/// Builder for storage configuration.
///
/// Supports both programmatic configuration and automatic loading from
/// environment variables. Use [`from_env`](Self::from_env) to load from
/// environment or [`new`](Self::new) for programmatic configuration.
#[derive(Debug)]
pub struct StorageBuilder {
    mode: Option<StorageMode>,
    hot_path: Option<PathBuf>,
    cold_path: Option<PathBuf>,
    cold_sql_url: Option<String>,
    db_args: Option<DatabaseArguments>,
    cancel_token: Option<CancellationToken>,
}

impl Default for StorageBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl StorageBuilder {
    /// Create a new storage builder.
    ///
    /// Use the setter methods to configure the builder, then call
    /// [`build`](Self::build) to instantiate storage.
    pub const fn new() -> Self {
        Self {
            mode: None,
            hot_path: None,
            cold_path: None,
            cold_sql_url: None,
            db_args: None,
            cancel_token: None,
        }
    }

    /// Create a builder from environment variables.
    ///
    /// Reads configuration from:
    /// - `STORAGE_MODE`: Storage mode selection
    /// - `SIGNET_HOT_PATH`: Hot storage path
    /// - `SIGNET_COLD_PATH`: Cold MDBX path (if applicable)
    /// - `SIGNET_COLD_SQL_URL`: Cold SQL URL (if applicable)
    ///
    /// # Errors
    ///
    /// Returns an error if required environment variables are missing or invalid.
    pub fn from_env() -> Result<Self, ConfigError> {
        let mode = StorageMode::from_env()?;

        let hot_path: PathBuf =
            env::var(ENV_HOT_PATH).map_err(|_| ConfigError::MissingEnvVar(ENV_HOT_PATH))?.into();

        let mut builder = Self::new().mode(mode).hot_path(hot_path);

        match mode {
            StorageMode::HotOnly => {}
            StorageMode::HotColdMdbx => {
                let cold_path: PathBuf = env::var(ENV_COLD_PATH)
                    .map_err(|_| ConfigError::MissingPath { mode, env_var: ENV_COLD_PATH })?
                    .into();
                builder = builder.cold_path(cold_path);
            }
            StorageMode::HotColdSql => {
                let cold_sql_url = env::var(ENV_COLD_SQL_URL)
                    .map_err(|_| ConfigError::MissingPath { mode, env_var: ENV_COLD_SQL_URL })?;
                builder = builder.cold_sql_url(cold_sql_url);
            }
        }

        Ok(builder)
    }

    /// Set the storage mode.
    #[must_use]
    pub const fn mode(mut self, mode: StorageMode) -> Self {
        self.mode = Some(mode);
        self
    }

    /// Set the hot storage path.
    #[must_use]
    pub fn hot_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.hot_path = Some(path.into());
        self
    }

    /// Set the cold MDBX storage path.
    #[must_use]
    pub fn cold_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.cold_path = Some(path.into());
        self
    }

    /// Set the cold SQL connection URL.
    #[must_use]
    pub fn cold_sql_url(mut self, url: impl Into<String>) -> Self {
        self.cold_sql_url = Some(url.into());
        self
    }

    /// Set custom database arguments for MDBX backends.
    ///
    /// If not set, default arguments are used.
    #[must_use]
    pub const fn database_arguments(mut self, args: DatabaseArguments) -> Self {
        self.db_args = Some(args);
        self
    }

    /// Set the cancellation token for cold storage tasks.
    ///
    /// Required for modes with cold storage. If not set, a new token is created.
    #[must_use]
    pub fn cancel_token(mut self, token: CancellationToken) -> Self {
        self.cancel_token = Some(token);
        self
    }

    /// Build the storage instance.
    ///
    /// Opens the appropriate backends based on the configured mode and spawns
    /// the cold storage task if needed.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Required configuration is missing
    /// - Backend initialization fails
    /// - Paths are invalid or inaccessible
    pub fn build(self) -> StorageResult<StorageInstance> {
        let mode = self
            .mode
            .ok_or_else(|| StorageError::Config("storage mode not configured".to_owned()))?;

        let hot_path = self
            .hot_path
            .ok_or_else(|| StorageError::Config("hot storage path not configured".to_owned()))?;

        let db_args = self.db_args.unwrap_or_default();
        let cancel_token = self.cancel_token.unwrap_or_default();

        match mode {
            StorageMode::HotOnly => Self::build_hot_only_impl(&hot_path, db_args),
            StorageMode::HotColdMdbx => {
                let cold_path = self.cold_path.ok_or_else(|| {
                    StorageError::Config(
                        "cold storage path not configured for hot-cold-mdbx mode".to_owned(),
                    )
                })?;
                Self::build_hot_cold_mdbx_impl(&hot_path, &cold_path, db_args, cancel_token)
            }
            StorageMode::HotColdSql => Err(StorageError::Config(
                "hot-cold-sql mode requires async initialization, use build_async() instead"
                    .to_owned(),
            )),
        }
    }

    /// Build the storage instance asynchronously.
    ///
    /// Required for SQL-based cold storage which needs async initialization.
    /// Works for all storage modes.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Required configuration is missing
    /// - Backend initialization fails
    /// - Paths or URLs are invalid or inaccessible
    #[cfg(feature = "sql")]
    pub async fn build_async(self) -> StorageResult<StorageInstance> {
        let mode = self
            .mode
            .ok_or_else(|| StorageError::Config("storage mode not configured".to_owned()))?;

        let hot_path = self
            .hot_path
            .ok_or_else(|| StorageError::Config("hot storage path not configured".to_owned()))?;

        let db_args = self.db_args.unwrap_or_default();
        let cancel_token = self.cancel_token.unwrap_or_default();

        match mode {
            StorageMode::HotOnly => Self::build_hot_only_impl(&hot_path, db_args),
            StorageMode::HotColdMdbx => {
                let cold_path = self.cold_path.ok_or_else(|| {
                    StorageError::Config(
                        "cold storage path not configured for hot-cold-mdbx mode".to_owned(),
                    )
                })?;
                Self::build_hot_cold_mdbx_impl(&hot_path, &cold_path, db_args, cancel_token)
            }
            StorageMode::HotColdSql => {
                let cold_sql_url = self.cold_sql_url.ok_or_else(|| {
                    StorageError::Config(
                        "cold SQL URL not configured for hot-cold-sql mode".to_owned(),
                    )
                })?;
                Self::build_hot_cold_sql_impl(&hot_path, &cold_sql_url, db_args, cancel_token).await
            }
        }
    }

    fn build_hot_only_impl(
        hot_path: &Path,
        db_args: DatabaseArguments,
    ) -> StorageResult<StorageInstance> {
        let hot_db = db_args.open_rw(hot_path)?;
        Ok(StorageInstance::HotOnly(hot_db))
    }

    fn build_hot_cold_mdbx_impl(
        hot_path: &Path,
        cold_path: &Path,
        db_args: DatabaseArguments,
        cancel_token: CancellationToken,
    ) -> StorageResult<StorageInstance> {
        // Open hot storage
        let hot_db = db_args.open_rw(hot_path)?;

        // Open cold storage
        let cold_backend = MdbxColdBackend::open_rw(cold_path)?;

        // Spawn cold storage task
        let unified = UnifiedStorage::spawn(hot_db, cold_backend, cancel_token);

        Ok(StorageInstance::Unified(unified))
    }

    #[cfg(feature = "sql")]
    async fn build_hot_cold_sql_impl(
        hot_path: &Path,
        cold_sql_url: &str,
        db_args: DatabaseArguments,
        cancel_token: CancellationToken,
    ) -> StorageResult<StorageInstance> {
        // Open hot storage
        let hot_db = db_args.open_rw(hot_path)?;

        // Connect to SQL backend
        let cold_backend = signet_cold_sql::SqlColdBackend::connect(cold_sql_url).await?;

        // Spawn cold storage task
        let unified = UnifiedStorage::spawn(hot_db, cold_backend, cancel_token);

        Ok(StorageInstance::Unified(unified))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ENV_STORAGE_MODE;

    #[test]
    fn builder_requires_mode() {
        let result = StorageBuilder::new().build();
        assert!(result.is_err());
    }

    #[test]
    fn builder_requires_hot_path() {
        let result = StorageBuilder::new().mode(StorageMode::HotOnly).build();
        assert!(result.is_err());
    }

    #[test]
    fn from_env_missing_mode() {
        // SAFETY: Test environment, single-threaded test execution
        unsafe {
            env::remove_var(ENV_STORAGE_MODE);
        }
        assert!(StorageBuilder::from_env().is_err());
    }

    #[test]
    fn from_env_missing_hot_path() {
        // SAFETY: Test environment, single-threaded test execution
        unsafe {
            env::set_var(ENV_STORAGE_MODE, "hot-only");
            env::remove_var(ENV_HOT_PATH);
        }
        let result = StorageBuilder::from_env();
        assert!(result.is_err());
        // SAFETY: Test environment, single-threaded test execution
        unsafe {
            env::remove_var(ENV_STORAGE_MODE);
        }
    }

    #[test]
    fn from_env_hot_only_valid() {
        // SAFETY: Test environment, single-threaded test execution
        unsafe {
            env::set_var(ENV_STORAGE_MODE, "hot-only");
            env::set_var(ENV_HOT_PATH, "/tmp/hot");
        }

        let builder = StorageBuilder::from_env().unwrap();
        assert!(matches!(builder.mode, Some(StorageMode::HotOnly)));
        assert_eq!(builder.hot_path, Some(PathBuf::from("/tmp/hot")));

        // SAFETY: Test environment, single-threaded test execution
        unsafe {
            env::remove_var(ENV_STORAGE_MODE);
            env::remove_var(ENV_HOT_PATH);
        }
    }

    #[test]
    fn from_env_hot_cold_mdbx_missing_cold_path() {
        // SAFETY: Test environment, single-threaded test execution
        unsafe {
            env::set_var(ENV_STORAGE_MODE, "hot-cold-mdbx");
            env::set_var(ENV_HOT_PATH, "/tmp/hot");
            env::remove_var(ENV_COLD_PATH);
        }

        let result = StorageBuilder::from_env();
        assert!(result.is_err());

        // SAFETY: Test environment, single-threaded test execution
        unsafe {
            env::remove_var(ENV_STORAGE_MODE);
            env::remove_var(ENV_HOT_PATH);
        }
    }

    #[test]
    fn from_env_hot_cold_mdbx_valid() {
        // SAFETY: Test environment, single-threaded test execution
        unsafe {
            env::set_var(ENV_STORAGE_MODE, "hot-cold-mdbx");
            env::set_var(ENV_HOT_PATH, "/tmp/hot");
            env::set_var(ENV_COLD_PATH, "/tmp/cold");
        }

        let builder = StorageBuilder::from_env().unwrap();
        assert!(matches!(builder.mode, Some(StorageMode::HotColdMdbx)));
        assert_eq!(builder.hot_path, Some(PathBuf::from("/tmp/hot")));
        assert_eq!(builder.cold_path, Some(PathBuf::from("/tmp/cold")));

        // SAFETY: Test environment, single-threaded test execution
        unsafe {
            env::remove_var(ENV_STORAGE_MODE);
            env::remove_var(ENV_HOT_PATH);
            env::remove_var(ENV_COLD_PATH);
        }
    }
}
