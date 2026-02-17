//! Either type for holding one of two connector types.
//!
//! The `Either` type enables runtime backend selection while maintaining compile-time
//! type safety and zero-cost abstraction. The `dispatch_async!` macro reduces
//! boilerplate for the `EitherCold` implementation by generating the repetitive
//! match-and-forward pattern for all ColdStorage trait methods.

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

/// Dispatches an async method call to the inner cold storage backend.
///
/// This macro reduces boilerplate for EitherCold by generating the match-and-forward
/// pattern. It preserves the method signatures for clarity while eliminating the
/// repetitive async match blocks.
macro_rules! dispatch_async {
    ($self:expr, $method:ident($($param:expr),*)) => {
        async move {
            match $self {
                Self::Mdbx(backend) => backend.$method($($param),*).await,
                #[cfg(any(feature = "postgres", feature = "sqlite"))]
                Self::Sql(backend) => backend.$method($($param),*).await,
            }
        }
    };
}

// Implement ColdStorage for EitherCold by dispatching to inner type
impl ColdStorage for EitherCold {
    fn get_header(
        &self,
        spec: HeaderSpecifier,
    ) -> impl Future<Output = ColdResult<Option<SealedHeader>>> + Send {
        dispatch_async!(self, get_header(spec))
    }

    fn get_headers(
        &self,
        specs: Vec<HeaderSpecifier>,
    ) -> impl Future<Output = ColdResult<Vec<Option<SealedHeader>>>> + Send {
        dispatch_async!(self, get_headers(specs))
    }

    fn get_transaction(
        &self,
        spec: TransactionSpecifier,
    ) -> impl Future<Output = ColdResult<Option<Confirmed<RecoveredTx>>>> + Send {
        dispatch_async!(self, get_transaction(spec))
    }

    fn get_transactions_in_block(
        &self,
        block: BlockNumber,
    ) -> impl Future<Output = ColdResult<Vec<RecoveredTx>>> + Send {
        dispatch_async!(self, get_transactions_in_block(block))
    }

    fn get_transaction_count(
        &self,
        block: BlockNumber,
    ) -> impl Future<Output = ColdResult<u64>> + Send {
        dispatch_async!(self, get_transaction_count(block))
    }

    fn get_receipt(
        &self,
        spec: ReceiptSpecifier,
    ) -> impl Future<Output = ColdResult<Option<ColdReceipt>>> + Send {
        dispatch_async!(self, get_receipt(spec))
    }

    fn get_receipts_in_block(
        &self,
        block: BlockNumber,
    ) -> impl Future<Output = ColdResult<Vec<ColdReceipt>>> + Send {
        dispatch_async!(self, get_receipts_in_block(block))
    }

    fn get_signet_events(
        &self,
        spec: SignetEventsSpecifier,
    ) -> impl Future<Output = ColdResult<Vec<DbSignetEvent>>> + Send {
        dispatch_async!(self, get_signet_events(spec))
    }

    fn get_zenith_header(
        &self,
        spec: ZenithHeaderSpecifier,
    ) -> impl Future<Output = ColdResult<Option<DbZenithHeader>>> + Send {
        dispatch_async!(self, get_zenith_header(spec))
    }

    fn get_zenith_headers(
        &self,
        spec: ZenithHeaderSpecifier,
    ) -> impl Future<Output = ColdResult<Vec<DbZenithHeader>>> + Send {
        dispatch_async!(self, get_zenith_headers(spec))
    }

    fn get_latest_block(&self) -> impl Future<Output = ColdResult<Option<BlockNumber>>> + Send {
        dispatch_async!(self, get_latest_block())
    }

    fn get_logs(
        &self,
        filter: &Filter,
        max_logs: usize,
    ) -> impl Future<Output = ColdResult<Vec<RpcLog>>> + Send {
        dispatch_async!(self, get_logs(filter, max_logs))
    }

    fn produce_log_stream(
        &self,
        filter: &Filter,
        params: StreamParams,
    ) -> impl Future<Output = ()> + Send {
        dispatch_async!(self, produce_log_stream(filter, params))
    }

    fn append_block(&self, data: BlockData) -> impl Future<Output = ColdResult<()>> + Send {
        dispatch_async!(self, append_block(data))
    }

    fn append_blocks(&self, data: Vec<BlockData>) -> impl Future<Output = ColdResult<()>> + Send {
        dispatch_async!(self, append_blocks(data))
    }

    fn truncate_above(&self, block: BlockNumber) -> impl Future<Output = ColdResult<()>> + Send {
        dispatch_async!(self, truncate_above(block))
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
