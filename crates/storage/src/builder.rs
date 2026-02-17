//! Storage builder for programmatic and environment-based configuration.

use crate::{
    StorageError, StorageResult, UnifiedStorage,
    config::{ConfigError, ENV_COLD_PATH, ENV_COLD_SQL_URL, ENV_HOT_PATH},
    either::Either,
};
use signet_cold::ColdConnect;
use signet_cold_mdbx::MdbxConnector;
use signet_hot::HotConnect;
use std::env;
use tokio_util::sync::CancellationToken;

#[cfg(any(feature = "postgres", feature = "sqlite"))]
use signet_cold_sql::SqlConnector;

#[cfg(any(feature = "postgres", feature = "sqlite"))]
type EnvColdConnector = Either<MdbxConnector, SqlConnector>;

#[cfg(not(any(feature = "postgres", feature = "sqlite")))]
type EnvColdConnector = Either<MdbxConnector, ()>;

/// Builder for unified storage configuration.
///
/// Uses a fluent API with `hot()`, `cold()`, and `build()` methods.
///
/// # Example
///
/// ```ignore
/// use signet_storage::StorageBuilder;
/// use signet_hot_mdbx::MdbxConnector;
///
/// let hot = MdbxConnector::new("/tmp/hot");
/// let cold = MdbxConnector::new("/tmp/cold");
///
/// let storage = StorageBuilder::default()
///     .hot(hot)
///     .cold(cold)
///     .build()
///     .await?;
/// ```
#[derive(Default, Debug)]
pub struct StorageBuilder<H = (), C = ()> {
    hot_connector: H,
    cold_connector: C,
    cancel_token: Option<CancellationToken>,
}

impl StorageBuilder<(), ()> {
    /// Create a new empty storage builder.
    pub fn new() -> Self {
        Self::default()
    }
}

impl<H, C> StorageBuilder<H, C> {
    /// Set the hot storage connector.
    pub fn hot<NewH>(self, hot_connector: NewH) -> StorageBuilder<NewH, C> {
        StorageBuilder {
            hot_connector,
            cold_connector: self.cold_connector,
            cancel_token: self.cancel_token,
        }
    }

    /// Set the cold storage connector.
    pub fn cold<NewC>(self, cold_connector: NewC) -> StorageBuilder<H, NewC> {
        StorageBuilder {
            hot_connector: self.hot_connector,
            cold_connector,
            cancel_token: self.cancel_token,
        }
    }

    /// Set the cancellation token for the cold storage task.
    #[must_use]
    pub fn cancel_token(mut self, token: CancellationToken) -> Self {
        self.cancel_token = Some(token);
        self
    }
}

impl<H, C> StorageBuilder<H, C>
where
    H: HotConnect,
    C: ColdConnect,
{
    /// Build the unified storage instance.
    ///
    /// Opens both hot and cold backends and spawns the cold storage task.
    pub async fn build(self) -> StorageResult<UnifiedStorage<H::Hot>> {
        // Connect to hot storage (sync)
        let hot = self
            .hot_connector
            .connect()
            .map_err(|e| StorageError::Config(format!("hot connection failed: {e}")))?;

        // Connect to cold storage (async)
        let cold = self
            .cold_connector
            .connect()
            .await
            .map_err(|e| StorageError::Config(format!("cold connection failed: {e}")))?;

        // Use provided cancel token or create new one
        let cancel_token = self.cancel_token.unwrap_or_default();

        // Spawn unified storage with cold task
        Ok(UnifiedStorage::spawn(hot, cold, cancel_token))
    }
}

impl StorageBuilder<MdbxConnector, EnvColdConnector> {
    /// Create a builder from environment variables.
    ///
    /// Reads configuration from:
    /// - `SIGNET_HOT_PATH`: Hot storage path (required)
    /// - `SIGNET_COLD_PATH`: Cold MDBX path (optional)
    /// - `SIGNET_COLD_SQL_URL`: Cold SQL URL (optional, requires postgres or sqlite feature)
    ///
    /// Checks for `SIGNET_COLD_PATH` first. If present, uses MDBX cold backend.
    /// Otherwise checks for `SIGNET_COLD_SQL_URL` for SQL backend.
    /// Exactly one cold backend must be specified.
    pub fn from_env() -> Result<Self, ConfigError> {
        // Hot connector from environment (always MDBX)
        let hot_connector = MdbxConnector::from_env(ENV_HOT_PATH)?;

        // Determine cold backend from environment
        let has_mdbx = env::var(ENV_COLD_PATH).is_ok();
        let has_sql = env::var(ENV_COLD_SQL_URL).is_ok();

        let cold_connector = match (has_mdbx, has_sql) {
            (true, false) => {
                let mdbx = MdbxConnector::from_env(ENV_COLD_PATH)?;
                Either::left(mdbx)
            }
            (false, true) => {
                #[cfg(any(feature = "postgres", feature = "sqlite"))]
                {
                    let sql = SqlConnector::from_env(ENV_COLD_SQL_URL)?;
                    Either::right(sql)
                }
                #[cfg(not(any(feature = "postgres", feature = "sqlite")))]
                {
                    return Err(ConfigError::FeatureNotEnabled {
                        feature: "postgres or sqlite",
                        env_var: ENV_COLD_SQL_URL,
                    });
                }
            }
            (true, true) => {
                return Err(ConfigError::AmbiguousColdBackend);
            }
            (false, false) => {
                return Err(ConfigError::MissingColdBackend);
            }
        };

        Ok(Self { hot_connector, cold_connector, cancel_token: None })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

    #[test]
    #[serial]
    fn from_env_missing_hot_path() {
        // SAFETY: Test environment
        unsafe {
            env::remove_var(ENV_HOT_PATH);
        }
        assert!(StorageBuilder::from_env().is_err());
    }

    #[test]
    #[serial]
    fn from_env_missing_cold_backend() {
        // SAFETY: Test environment
        unsafe {
            env::set_var(ENV_HOT_PATH, "/tmp/hot");
            env::remove_var(ENV_COLD_PATH);
            env::remove_var(ENV_COLD_SQL_URL);
        }
        let result = StorageBuilder::from_env();
        assert!(matches!(result, Err(ConfigError::MissingColdBackend)));
    }

    #[test]
    #[serial]
    fn from_env_ambiguous_cold_backend() {
        // SAFETY: Test environment
        unsafe {
            env::set_var(ENV_HOT_PATH, "/tmp/hot");
            env::set_var(ENV_COLD_PATH, "/tmp/cold");
            env::set_var(ENV_COLD_SQL_URL, "postgres://localhost/db");
        }
        let result = StorageBuilder::from_env();
        assert!(matches!(result, Err(ConfigError::AmbiguousColdBackend)));
    }

    #[test]
    #[serial]
    fn from_env_mdbx_cold() {
        // SAFETY: Test environment
        unsafe {
            env::set_var(ENV_HOT_PATH, "/tmp/hot");
            env::set_var(ENV_COLD_PATH, "/tmp/cold");
            env::remove_var(ENV_COLD_SQL_URL);
        }
        let builder = StorageBuilder::from_env().unwrap();
        // Verify it's an Either::Left
        assert!(matches!(builder.cold_connector, Either::Left(_)));
    }
}
