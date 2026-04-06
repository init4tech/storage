//! SQL cold storage connector.

use crate::{PoolOverrides, SqlColdBackend, SqlColdError};
use signet_cold::ColdConnect;
use std::time::Duration;

/// Errors that can occur when initializing SQL connectors.
#[derive(Debug, thiserror::Error)]
pub enum SqlConnectorError {
    /// Missing environment variable.
    #[error("missing environment variable: {0}")]
    MissingEnvVar(&'static str),

    /// Cold storage initialization failed.
    #[error("cold storage initialization failed: {0}")]
    ColdInit(#[from] SqlColdError),
}

/// Connector for SQL cold storage (PostgreSQL or SQLite).
///
/// Automatically detects the database type from the URL:
/// - URLs starting with `postgres://` or `postgresql://` use PostgreSQL
/// - URLs starting with `sqlite:` use SQLite
///
/// Pool settings use backend-specific defaults. Use
/// [`with_max_connections`] and [`with_acquire_timeout`] to override.
///
/// [`with_max_connections`]: Self::with_max_connections
/// [`with_acquire_timeout`]: Self::with_acquire_timeout
///
/// # Example
///
/// ```ignore
/// use signet_cold_sql::SqlConnector;
///
/// // PostgreSQL with custom pool size
/// let pg = SqlConnector::new("postgres://localhost/signet")
///     .with_max_connections(20);
/// let backend = pg.connect().await?;
///
/// // SQLite (defaults)
/// let sqlite = SqlConnector::new("sqlite::memory:");
/// let backend = sqlite.connect().await?;
/// ```
#[cfg(any(feature = "sqlite", feature = "postgres"))]
#[derive(Debug, Clone)]
pub struct SqlConnector {
    url: String,
    overrides: PoolOverrides,
}

#[cfg(any(feature = "sqlite", feature = "postgres"))]
impl SqlConnector {
    /// Create a new SQL connector.
    ///
    /// The database type is detected from the URL prefix. Pool settings
    /// use backend-specific defaults. Use [`with_max_connections`] and
    /// [`with_acquire_timeout`] to override.
    ///
    /// [`with_max_connections`]: Self::with_max_connections
    /// [`with_acquire_timeout`]: Self::with_acquire_timeout
    pub fn new(url: impl Into<String>) -> Self {
        Self { url: url.into(), overrides: PoolOverrides::default() }
    }

    /// Get a reference to the connection URL.
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Override the maximum number of pool connections.
    ///
    /// Default: 1 for SQLite, 10 for PostgreSQL.
    pub const fn with_max_connections(mut self, n: u32) -> Self {
        self.overrides.max_connections = Some(n);
        self
    }

    /// Override the connection acquire timeout.
    ///
    /// Default: 5 seconds for all backends.
    pub const fn with_acquire_timeout(mut self, timeout: Duration) -> Self {
        self.overrides.acquire_timeout = Some(timeout);
        self
    }

    /// Create a connector from environment variables.
    ///
    /// Reads the SQL URL from the specified environment variable.
    /// Uses default pool settings.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use signet_cold_sql::SqlConnector;
    ///
    /// let cold = SqlConnector::from_env("SIGNET_COLD_SQL_URL")?;
    /// ```
    pub fn from_env(env_var: &'static str) -> Result<Self, SqlConnectorError> {
        let url = std::env::var(env_var).map_err(|_| SqlConnectorError::MissingEnvVar(env_var))?;
        Ok(Self::new(url))
    }
}

#[cfg(any(feature = "sqlite", feature = "postgres"))]
impl ColdConnect for SqlConnector {
    type Cold = SqlColdBackend;
    type Error = SqlColdError;

    fn connect(&self) -> impl std::future::Future<Output = Result<Self::Cold, Self::Error>> + Send {
        let url = self.url.clone();
        let overrides = self.overrides;
        async move { SqlColdBackend::connect_with(&url, overrides).await }
    }
}
