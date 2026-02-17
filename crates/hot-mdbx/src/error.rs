use crate::lock::StorageLockError;
use signet_hot::{
    DeserError,
    model::{HotKvError, HotKvReadError},
};
use signet_libmdbx::{MdbxError as LibMdbxError, ReadError};

/// Error type for [`signet_libmdbx`] based hot storage.
#[derive(Debug, thiserror::Error)]
pub enum MdbxError {
    /// Inner error
    #[error(transparent)]
    Mdbx(#[from] LibMdbxError),

    /// Error when a raw value does not conform to expected fixed size.
    #[error("Error with dup fixed value size: expected {expected} bytes, found {found} bytes")]
    DupFixedErr {
        /// Expected size
        expected: usize,
        /// Found size
        found: usize,
    },

    /// Tried to invoke a DUPSORT operation on a table that is not flagged
    /// DUPSORT
    #[error("tried to invoke a DUPSORT operation on a table that is not flagged DUPSORT")]
    NotDupSort,

    /// Tried to invoke a DUP_FIXED operation on a table that is not DUP_FIXED
    #[error("tried to invoke a DUP_FIXED operation on a table that is not DUP_FIXED")]
    NotDupFixed,

    /// Key2 size is unknown, cannot split DUPSORT value.
    /// This error occurs when using raw cursor methods on a DUP_FIXED table
    /// without first setting the key2/value sizes via typed methods.
    /// Use typed methods instead of raw methods when working with dual-key tables.
    #[error(
        "fixed size for DUPSORT value is unknown. Hint: use typed methods instead of raw methods when working with dual-key tables"
    )]
    UnknownFixedSize,

    /// `raw_get_dual` is not supported by the MDBX backend.
    /// Use typed `get_dual` method instead, which uses cursor-based lookup.
    #[error("raw_get_dual is not supported by MDBX; use get_dual instead")]
    RawDualUnsupported,

    /// Table not found
    #[error("table not found: {0}")]
    UnknownTable(&'static str),

    /// Storage lock error
    #[error(transparent)]
    Locked(#[from] StorageLockError),

    /// Deser.
    #[error(transparent)]
    Deser(#[from] DeserError),

    /// Configuration error.
    #[error("configuration error: {0}")]
    Config(String),
}

impl trevm::revm::database::DBErrorMarker for MdbxError {}

impl HotKvReadError for MdbxError {
    fn into_hot_kv_error(self) -> HotKvError {
        match self {
            MdbxError::Deser(e) => HotKvError::Deser(e),
            _ => HotKvError::from_err(self),
        }
    }
}

impl From<ReadError> for MdbxError {
    fn from(err: ReadError) -> Self {
        match err {
            ReadError::Mdbx(e) => MdbxError::Mdbx(e),
            ReadError::Decoding(e) => MdbxError::Deser(DeserError::Boxed(e)),
        }
    }
}
