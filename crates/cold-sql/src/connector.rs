//! SQL cold storage connector.

use crate::{SqlColdBackend, SqlColdError};
use signet_cold::ColdConnect;

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
/// # Example
///
/// ```ignore
/// use signet_cold_sql::SqlConnector;
///
/// // PostgreSQL
/// let pg = SqlConnector::new("postgres://localhost/signet");
/// let backend = pg.connect().await?;
///
/// // SQLite
/// let sqlite = SqlConnector::new("sqlite::memory:");
/// let backend = sqlite.connect().await?;
/// ```
#[cfg(any(feature = "sqlite", feature = "postgres"))]
#[derive(Debug, Clone)]
pub struct SqlConnector {
    url: String,
}

#[cfg(any(feature = "sqlite", feature = "postgres"))]
impl SqlConnector {
    /// Create a new SQL connector.
    ///
    /// The database type is detected from the URL prefix.
    pub fn new(url: impl Into<String>) -> Self {
        Self { url: url.into() }
    }

    /// Get a reference to the connection URL.
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Create a connector from environment variables.
    ///
    /// Reads the SQL URL from the specified environment variable.
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

    async fn connect(&self) -> Result<Self::Cold, Self::Error> {
        SqlColdBackend::connect(&self.url).await
    }
}
