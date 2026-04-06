//! SQL cold storage connector.

use crate::{SqlColdBackend, SqlColdError};
use signet_cold::ColdConnect;
use sqlx::pool::PoolOptions;
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
/// Pool behaviour is configured via builder methods that mirror
/// [`sqlx::pool::PoolOptions`], or by passing a complete
/// [`PoolOptions`] via [`with_pool_options`](Self::with_pool_options).
/// For in-memory SQLite URLs, `max_connections` is forced to 1
/// regardless of the provided options.
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
    pool_opts: PoolOptions<sqlx::Any>,
}

#[cfg(any(feature = "sqlite", feature = "postgres"))]
impl SqlConnector {
    /// Create a new SQL connector with default pool options.
    ///
    /// The database type is detected from the URL prefix.
    pub fn new(url: impl Into<String>) -> Self {
        Self { url: url.into(), pool_opts: PoolOptions::new() }
    }

    /// Get a reference to the connection URL.
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Replace the pool options entirely.
    ///
    /// For in-memory SQLite URLs, `max_connections` is forced to 1
    /// regardless of the value set here.
    pub fn with_pool_options(mut self, pool_opts: PoolOptions<sqlx::Any>) -> Self {
        self.pool_opts = pool_opts;
        self
    }

    /// Set the maximum number of pool connections.
    ///
    /// Ignored for in-memory SQLite URLs, which always use 1.
    pub fn with_max_connections(mut self, n: u32) -> Self {
        self.pool_opts = self.pool_opts.max_connections(n);
        self
    }

    /// Set the minimum number of connections to maintain at all times.
    pub fn with_min_connections(mut self, n: u32) -> Self {
        self.pool_opts = self.pool_opts.min_connections(n);
        self
    }

    /// Set the connection acquire timeout.
    pub fn with_acquire_timeout(mut self, timeout: Duration) -> Self {
        self.pool_opts = self.pool_opts.acquire_timeout(timeout);
        self
    }

    /// Set the maximum lifetime of individual connections.
    pub fn with_max_lifetime(mut self, lifetime: Option<Duration>) -> Self {
        self.pool_opts = self.pool_opts.max_lifetime(lifetime);
        self
    }

    /// Set the idle timeout for connections.
    pub fn with_idle_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.pool_opts = self.pool_opts.idle_timeout(timeout);
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
        let pool_opts = self.pool_opts.clone();
        async move { SqlColdBackend::connect_with(&url, pool_opts).await }
    }
}
