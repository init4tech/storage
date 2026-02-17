//! Either type for holding one of two connector types.

use alloy::primitives::BlockNumber;
use signet_cold::{
    BlockData, ColdConnect, ColdReceipt, ColdResult, ColdStorage, Confirmed, Filter,
    HeaderSpecifier, ReceiptSpecifier, SignetEventsSpecifier, StreamParams, TransactionSpecifier,
    ZenithHeaderSpecifier,
};
use signet_cold_mdbx::{MdbxColdBackend, MdbxConnector};
use signet_storage_types::{DbSignetEvent, DbZenithHeader, RecoveredTx, SealedHeader};
use std::future::Future;

#[cfg(any(feature = "postgres", feature = "sqlite"))]
use signet_cold_sql::{SqlColdBackend, SqlConnector};

type RpcLog = alloy::rpc::types::Log;

/// Either type that holds one of two cold connectors.
///
/// Used by `from_env()` to support both MDBX and SQL cold backends.
#[derive(Debug, Clone)]
pub enum Either<L, R> {
    /// Left variant.
    Left(L),
    /// Right variant.
    Right(R),
}

impl<L, R> Either<L, R> {
    /// Create a left variant.
    pub const fn left(value: L) -> Self {
        Self::Left(value)
    }

    /// Create a right variant.
    pub const fn right(value: R) -> Self {
        Self::Right(value)
    }
}

/// Enum to hold either cold backend type.
#[derive(Debug)]
pub enum EitherCold {
    /// MDBX cold backend.
    Mdbx(MdbxColdBackend),
    /// SQL cold backend (PostgreSQL or SQLite).
    #[cfg(any(feature = "postgres", feature = "sqlite"))]
    Sql(SqlColdBackend),
}

// Implement ColdStorage for EitherCold by dispatching to inner type
impl ColdStorage for EitherCold {
    fn get_header(
        &self,
        spec: HeaderSpecifier,
    ) -> impl Future<Output = ColdResult<Option<SealedHeader>>> + Send {
        async move {
            match self {
                Self::Mdbx(backend) => backend.get_header(spec).await,
                #[cfg(any(feature = "postgres", feature = "sqlite"))]
                Self::Sql(backend) => backend.get_header(spec).await,
            }
        }
    }

    fn get_headers(
        &self,
        specs: Vec<HeaderSpecifier>,
    ) -> impl Future<Output = ColdResult<Vec<Option<SealedHeader>>>> + Send {
        async move {
            match self {
                Self::Mdbx(backend) => backend.get_headers(specs).await,
                #[cfg(any(feature = "postgres", feature = "sqlite"))]
                Self::Sql(backend) => backend.get_headers(specs).await,
            }
        }
    }

    fn get_transaction(
        &self,
        spec: TransactionSpecifier,
    ) -> impl Future<Output = ColdResult<Option<Confirmed<RecoveredTx>>>> + Send {
        async move {
            match self {
                Self::Mdbx(backend) => backend.get_transaction(spec).await,
                #[cfg(any(feature = "postgres", feature = "sqlite"))]
                Self::Sql(backend) => backend.get_transaction(spec).await,
            }
        }
    }

    fn get_transactions_in_block(
        &self,
        block: BlockNumber,
    ) -> impl Future<Output = ColdResult<Vec<RecoveredTx>>> + Send {
        async move {
            match self {
                Self::Mdbx(backend) => backend.get_transactions_in_block(block).await,
                #[cfg(any(feature = "postgres", feature = "sqlite"))]
                Self::Sql(backend) => backend.get_transactions_in_block(block).await,
            }
        }
    }

    fn get_transaction_count(
        &self,
        block: BlockNumber,
    ) -> impl Future<Output = ColdResult<u64>> + Send {
        async move {
            match self {
                Self::Mdbx(backend) => backend.get_transaction_count(block).await,
                #[cfg(any(feature = "postgres", feature = "sqlite"))]
                Self::Sql(backend) => backend.get_transaction_count(block).await,
            }
        }
    }

    fn get_receipt(
        &self,
        spec: ReceiptSpecifier,
    ) -> impl Future<Output = ColdResult<Option<ColdReceipt>>> + Send {
        async move {
            match self {
                Self::Mdbx(backend) => backend.get_receipt(spec).await,
                #[cfg(any(feature = "postgres", feature = "sqlite"))]
                Self::Sql(backend) => backend.get_receipt(spec).await,
            }
        }
    }

    fn get_receipts_in_block(
        &self,
        block: BlockNumber,
    ) -> impl Future<Output = ColdResult<Vec<ColdReceipt>>> + Send {
        async move {
            match self {
                Self::Mdbx(backend) => backend.get_receipts_in_block(block).await,
                #[cfg(any(feature = "postgres", feature = "sqlite"))]
                Self::Sql(backend) => backend.get_receipts_in_block(block).await,
            }
        }
    }

    fn get_signet_events(
        &self,
        spec: SignetEventsSpecifier,
    ) -> impl Future<Output = ColdResult<Vec<DbSignetEvent>>> + Send {
        async move {
            match self {
                Self::Mdbx(backend) => backend.get_signet_events(spec).await,
                #[cfg(any(feature = "postgres", feature = "sqlite"))]
                Self::Sql(backend) => backend.get_signet_events(spec).await,
            }
        }
    }

    fn get_zenith_header(
        &self,
        spec: ZenithHeaderSpecifier,
    ) -> impl Future<Output = ColdResult<Option<DbZenithHeader>>> + Send {
        async move {
            match self {
                Self::Mdbx(backend) => backend.get_zenith_header(spec).await,
                #[cfg(any(feature = "postgres", feature = "sqlite"))]
                Self::Sql(backend) => backend.get_zenith_header(spec).await,
            }
        }
    }

    fn get_zenith_headers(
        &self,
        spec: ZenithHeaderSpecifier,
    ) -> impl Future<Output = ColdResult<Vec<DbZenithHeader>>> + Send {
        async move {
            match self {
                Self::Mdbx(backend) => backend.get_zenith_headers(spec).await,
                #[cfg(any(feature = "postgres", feature = "sqlite"))]
                Self::Sql(backend) => backend.get_zenith_headers(spec).await,
            }
        }
    }

    fn get_latest_block(&self) -> impl Future<Output = ColdResult<Option<BlockNumber>>> + Send {
        async move {
            match self {
                Self::Mdbx(backend) => backend.get_latest_block().await,
                #[cfg(any(feature = "postgres", feature = "sqlite"))]
                Self::Sql(backend) => backend.get_latest_block().await,
            }
        }
    }

    fn get_logs(
        &self,
        filter: &Filter,
        max_logs: usize,
    ) -> impl Future<Output = ColdResult<Vec<RpcLog>>> + Send {
        async move {
            match self {
                Self::Mdbx(backend) => backend.get_logs(filter, max_logs).await,
                #[cfg(any(feature = "postgres", feature = "sqlite"))]
                Self::Sql(backend) => backend.get_logs(filter, max_logs).await,
            }
        }
    }

    fn produce_log_stream(
        &self,
        filter: &Filter,
        params: StreamParams,
    ) -> impl Future<Output = ()> + Send {
        async move {
            match self {
                Self::Mdbx(backend) => backend.produce_log_stream(filter, params).await,
                #[cfg(any(feature = "postgres", feature = "sqlite"))]
                Self::Sql(backend) => backend.produce_log_stream(filter, params).await,
            }
        }
    }

    fn append_block(&self, data: BlockData) -> impl Future<Output = ColdResult<()>> + Send {
        async move {
            match self {
                Self::Mdbx(backend) => backend.append_block(data).await,
                #[cfg(any(feature = "postgres", feature = "sqlite"))]
                Self::Sql(backend) => backend.append_block(data).await,
            }
        }
    }

    fn append_blocks(&self, data: Vec<BlockData>) -> impl Future<Output = ColdResult<()>> + Send {
        async move {
            match self {
                Self::Mdbx(backend) => backend.append_blocks(data).await,
                #[cfg(any(feature = "postgres", feature = "sqlite"))]
                Self::Sql(backend) => backend.append_blocks(data).await,
            }
        }
    }

    fn truncate_above(&self, block: BlockNumber) -> impl Future<Output = ColdResult<()>> + Send {
        async move {
            match self {
                Self::Mdbx(backend) => backend.truncate_above(block).await,
                #[cfg(any(feature = "postgres", feature = "sqlite"))]
                Self::Sql(backend) => backend.truncate_above(block).await,
            }
        }
    }
}

// When SQL features are enabled
#[cfg(any(feature = "postgres", feature = "sqlite"))]
impl ColdConnect for Either<MdbxConnector, SqlConnector> {
    type Cold = EitherCold;
    type Error = crate::StorageError;

    async fn connect(&self) -> Result<Self::Cold, Self::Error> {
        match self {
            Either::Left(mdbx) => {
                let backend = mdbx.connect().await.map_err(|e| crate::StorageError::MdbxCold(e))?;
                Ok(EitherCold::Mdbx(backend))
            }
            Either::Right(sql) => {
                let backend = sql.connect().await.map_err(|e| crate::StorageError::SqlCold(e))?;
                Ok(EitherCold::Sql(backend))
            }
        }
    }
}

// Fallback for when no SQL features are enabled
#[cfg(not(any(feature = "postgres", feature = "sqlite")))]
impl ColdConnect for Either<MdbxConnector, ()> {
    type Cold = MdbxColdBackend;
    type Error = crate::StorageError;

    async fn connect(&self) -> Result<Self::Cold, Self::Error> {
        match self {
            Either::Left(mdbx) => {
                mdbx.connect().await.map_err(|e| crate::StorageError::MdbxCold(e))
            }
            Either::Right(()) => unreachable!("SQL not enabled"),
        }
    }
}
