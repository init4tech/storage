//! SQL cold storage connector.

use crate::{SqlColdBackend, SqlColdError};
use signet_cold::ColdConnect;
use sqlx::pool::PoolOptions;

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
/// Pool behaviour is configured via [`sqlx::pool::PoolOptions`] passed
/// to [`with_pool_options`](Self::with_pool_options). For in-memory
/// SQLite URLs, `max_connections` is forced to 1 regardless of the
/// provided options.
///
/// # Example
///
/// ```ignore
/// use signet_cold_sql::SqlConnector;
/// use sqlx::pool::PoolOptions;
///
/// // PostgreSQL with custom pool size
/// let pg = SqlConnector::new("postgres://localhost/signet")
///     .with_pool_options(PoolOptions::new().max_connections(20));
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

    /// Set the pool options for this connector.
    ///
    /// For in-memory SQLite URLs, `max_connections` is forced to 1
    /// regardless of the value set here.
    pub fn with_pool_options(mut self, pool_opts: PoolOptions<sqlx::Any>) -> Self {
        self.pool_opts = pool_opts;
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
