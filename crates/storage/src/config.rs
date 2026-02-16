//! Storage configuration types and environment parsing.
//!
//! This module provides configuration types for instantiating storage with
//! different backend combinations:
//!
//! - **Hot-only**: MDBX hot storage without cold storage
//! - **Hot + Cold MDBX**: Both hot and cold use MDBX backends
//! - **Hot + Cold SQL**: Hot uses MDBX, cold uses PostgreSQL or SQLite
//!
//! # Environment Variables
//!
//! | Variable | Description | Required When |
//! |----------|-------------|---------------|
//! | `STORAGE_MODE` | Storage mode (`hot-only`, `hot-cold-mdbx`, `hot-cold-sql`) | Always |
//! | `SIGNET_HOT_PATH` | Path to hot MDBX database | Always |
//! | `SIGNET_COLD_PATH` | Path to cold MDBX database | `mode=hot-cold-mdbx` |
//! | `SIGNET_COLD_SQL_URL` | SQL connection string | `mode=hot-cold-sql` |
//!
//! # Example
//!
//! ```rust
//! use signet_storage::config::StorageMode;
//! use std::env;
//!
//! // Parse from string
//! let mode: StorageMode = "hot-only".parse().unwrap();
//! assert!(matches!(mode, StorageMode::HotOnly));
//!
//! // Parse from environment
//! unsafe {
//!     env::set_var("STORAGE_MODE", "hot-cold-mdbx");
//! }
//! let mode = StorageMode::from_env().unwrap();
//! assert!(matches!(mode, StorageMode::HotColdMdbx));
//! # unsafe { env::remove_var("STORAGE_MODE"); }
//! ```

use std::{env, fmt, str::FromStr};
use thiserror::Error;

/// Environment variable name for storage mode selection.
pub const ENV_STORAGE_MODE: &str = "STORAGE_MODE";

/// Environment variable name for hot storage path.
pub const ENV_HOT_PATH: &str = "SIGNET_HOT_PATH";

/// Environment variable name for cold MDBX storage path.
pub const ENV_COLD_PATH: &str = "SIGNET_COLD_PATH";

/// Environment variable name for cold SQL connection URL.
pub const ENV_COLD_SQL_URL: &str = "SIGNET_COLD_SQL_URL";

/// Storage mode configuration.
///
/// Defines the backend combination for storage instantiation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StorageMode {
    /// Hot-only mode: MDBX hot storage without cold storage.
    HotOnly,
    /// Hot + Cold MDBX: Both hot and cold use MDBX backends.
    HotColdMdbx,
    /// Hot MDBX + Cold SQL: Hot uses MDBX, cold uses SQL backend.
    HotColdSql,
}

impl StorageMode {
    /// Load storage mode from environment variables.
    ///
    /// Reads the `STORAGE_MODE` environment variable and parses it.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::MissingEnvVar`] if the environment variable is not set,
    /// or [`ConfigError::InvalidMode`] if the value cannot be parsed.
    ///
    /// # Example
    ///
    /// ```rust
    /// use signet_storage::config::StorageMode;
    /// use std::env;
    ///
    /// unsafe {
    ///     env::set_var("STORAGE_MODE", "hot-only");
    /// }
    /// let mode = StorageMode::from_env().unwrap();
    /// assert!(matches!(mode, StorageMode::HotOnly));
    /// # unsafe { env::remove_var("STORAGE_MODE"); }
    /// ```
    pub fn from_env() -> Result<Self, ConfigError> {
        let value =
            env::var(ENV_STORAGE_MODE).map_err(|_| ConfigError::MissingEnvVar(ENV_STORAGE_MODE))?;
        value.parse()
    }
}

impl FromStr for StorageMode {
    type Err = ConfigError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "hot-only" => Ok(Self::HotOnly),
            "hot-cold-mdbx" => Ok(Self::HotColdMdbx),
            "hot-cold-sql" => Ok(Self::HotColdSql),
            _ => Err(ConfigError::InvalidMode(s.to_owned())),
        }
    }
}

impl fmt::Display for StorageMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::HotOnly => write!(f, "hot-only"),
            Self::HotColdMdbx => write!(f, "hot-cold-mdbx"),
            Self::HotColdSql => write!(f, "hot-cold-sql"),
        }
    }
}

/// Configuration errors.
#[derive(Debug, Error)]
pub enum ConfigError {
    /// Required environment variable is missing.
    #[error("missing environment variable: {0}")]
    MissingEnvVar(&'static str),

    /// Invalid storage mode string.
    #[error("invalid storage mode: {0} (expected: hot-only, hot-cold-mdbx, hot-cold-sql)")]
    InvalidMode(String),

    /// Missing required path for the selected mode.
    #[error("missing required path for mode {mode}: environment variable {env_var} not set")]
    MissingPath {
        /// The storage mode that requires the path.
        mode: StorageMode,
        /// The environment variable name.
        env_var: &'static str,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_storage_mode() {
        assert_eq!("hot-only".parse::<StorageMode>().unwrap(), StorageMode::HotOnly);
        assert_eq!("hot-cold-mdbx".parse::<StorageMode>().unwrap(), StorageMode::HotColdMdbx);
        assert_eq!("hot-cold-sql".parse::<StorageMode>().unwrap(), StorageMode::HotColdSql);
    }

    #[test]
    fn parse_invalid_mode() {
        assert!("invalid".parse::<StorageMode>().is_err());
        assert!("hot_only".parse::<StorageMode>().is_err());
        assert!("".parse::<StorageMode>().is_err());
    }

    #[test]
    fn display_storage_mode() {
        assert_eq!(StorageMode::HotOnly.to_string(), "hot-only");
        assert_eq!(StorageMode::HotColdMdbx.to_string(), "hot-cold-mdbx");
        assert_eq!(StorageMode::HotColdSql.to_string(), "hot-cold-sql");
    }

    #[test]
    fn from_env_missing_var() {
        // SAFETY: Test environment, single-threaded test execution
        unsafe {
            env::remove_var(ENV_STORAGE_MODE);
        }
        assert!(StorageMode::from_env().is_err());
    }

    #[test]
    fn from_env_valid() {
        // SAFETY: Test environment, single-threaded test execution
        unsafe {
            env::set_var(ENV_STORAGE_MODE, "hot-only");
        }
        assert_eq!(StorageMode::from_env().unwrap(), StorageMode::HotOnly);
        // SAFETY: Test environment, single-threaded test execution
        unsafe {
            env::remove_var(ENV_STORAGE_MODE);
        }

        // SAFETY: Test environment, single-threaded test execution
        unsafe {
            env::set_var(ENV_STORAGE_MODE, "hot-cold-mdbx");
        }
        assert_eq!(StorageMode::from_env().unwrap(), StorageMode::HotColdMdbx);
        // SAFETY: Test environment, single-threaded test execution
        unsafe {
            env::remove_var(ENV_STORAGE_MODE);
        }
    }

    #[test]
    fn from_env_invalid() {
        // SAFETY: Test environment, single-threaded test execution
        unsafe {
            env::set_var(ENV_STORAGE_MODE, "invalid-mode");
        }
        assert!(StorageMode::from_env().is_err());
        // SAFETY: Test environment, single-threaded test execution
        unsafe {
            env::remove_var(ENV_STORAGE_MODE);
        }
    }
}
