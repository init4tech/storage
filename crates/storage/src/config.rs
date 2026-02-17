//! Storage configuration types and environment parsing.
//!
//! This module provides environment variable constants and error types
//! for storage configuration.
//!
//! # Environment Variables
//!
//! | Variable | Description | Required When |
//! |----------|-------------|---------------|
//! | `SIGNET_HOT_PATH` | Path to hot MDBX database | Always |
//! | `SIGNET_COLD_PATH` | Path to cold MDBX database | Cold backend is MDBX |
//! | `SIGNET_COLD_SQL_URL` | SQL connection string | Cold backend is SQL |
//!
//! Exactly one of `SIGNET_COLD_PATH` or `SIGNET_COLD_SQL_URL` must be set.

use signet_cold_mdbx::MdbxConnectorError;
use thiserror::Error;

#[cfg(any(feature = "postgres", feature = "sqlite"))]
use signet_cold_sql::SqlConnectorError;

/// Environment variable name for hot storage path.
pub const ENV_HOT_PATH: &str = "SIGNET_HOT_PATH";

/// Environment variable name for cold MDBX storage path.
pub const ENV_COLD_PATH: &str = "SIGNET_COLD_PATH";

/// Environment variable name for cold SQL connection URL.
pub const ENV_COLD_SQL_URL: &str = "SIGNET_COLD_SQL_URL";

/// Configuration errors.
#[derive(Debug, Error)]
pub enum ConfigError {
    /// Required environment variable is missing.
    #[error("missing environment variable: {0}")]
    MissingEnvVar(&'static str),

    /// Cold backend not specified.
    #[error("no cold backend specified: set either {ENV_COLD_PATH} or {ENV_COLD_SQL_URL}")]
    MissingColdBackend,

    /// Multiple cold backends specified.
    #[error("ambiguous cold backend: both {ENV_COLD_PATH} and {ENV_COLD_SQL_URL} are set")]
    AmbiguousColdBackend,

    /// Required feature not enabled.
    #[error("feature '{feature}' required for {env_var} but not enabled")]
    FeatureNotEnabled {
        /// The feature name that is required.
        feature: &'static str,
        /// The environment variable that requires the feature.
        env_var: &'static str,
    },

    /// MDBX connector error.
    #[error("MDBX connector error: {0}")]
    MdbxConnector(#[from] MdbxConnectorError),

    /// SQL connector error.
    #[cfg(any(feature = "postgres", feature = "sqlite"))]
    #[error("SQL connector error: {0}")]
    SqlConnector(#[from] SqlConnectorError),
}
