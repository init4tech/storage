//! Request and response types for the cold storage task.
//!
//! These types define the messages sent over channels to the cold storage task.
//! Reads and writes use separate channels with their own request types.

use crate::{
    BlockData, ColdReceipt, ColdStorageError, Confirmed, Filter, HeaderSpecifier, LogStream,
    ReceiptSpecifier, RpcLog, SignetEventsSpecifier, TransactionSpecifier, ZenithHeaderSpecifier,
};
use alloy::primitives::BlockNumber;
use signet_storage_types::{DbSignetEvent, DbZenithHeader, RecoveredTx, SealedHeader};
use std::time::Duration;
use tokio::sync::{OwnedSemaphorePermit, oneshot};

/// Response sender type alias that propagates Result types.
pub type Responder<T, E = ColdStorageError> = oneshot::Sender<Result<T, E>>;

/// Block append request data (wrapper struct).
#[derive(Debug)]
pub struct AppendBlockRequest {
    /// The block data to append.
    pub data: BlockData,
    /// The response channel.
    pub resp: Responder<()>,
}

/// A read request with an attached concurrency permit.
///
/// The permit is acquired on the handle side before sending, bounds
/// concurrent in-flight readers, and doubles as the drain-before-write
/// marker in the task runner. It is released when the spawned handler
/// completes (or panics, or is dropped on deadline expiry).
#[derive(Debug)]
pub struct PermittedReadRequest {
    /// The concurrency permit, released when the handler future is dropped.
    pub permit: OwnedSemaphorePermit,
    /// The read request itself.
    pub req: ColdReadRequest,
}

impl PermittedReadRequest {
    /// Construct a new permitted request.
    pub const fn new(permit: OwnedSemaphorePermit, req: ColdReadRequest) -> Self {
        Self { permit, req }
    }
}

/// Read requests for cold storage.
///
/// These requests are processed concurrently (up to 64 in flight).
#[derive(Debug)]
pub enum ColdReadRequest {
    // --- Headers ---
    /// Get a single header by specifier.
    GetHeader {
        /// The header specifier.
        spec: HeaderSpecifier,
        /// The response channel.
        resp: Responder<Option<SealedHeader>>,
    },
    /// Get multiple headers by specifiers.
    GetHeaders {
        /// The header specifiers.
        specs: Vec<HeaderSpecifier>,
        /// The response channel.
        resp: Responder<Vec<Option<SealedHeader>>>,
    },

    // --- Transactions ---
    /// Get a single transaction by specifier.
    GetTransaction {
        /// The transaction specifier.
        spec: TransactionSpecifier,
        /// The response channel.
        resp: Responder<Option<Confirmed<RecoveredTx>>>,
    },
    /// Get all transactions in a block.
    GetTransactionsInBlock {
        /// The block number.
        block: BlockNumber,
        /// The response channel.
        resp: Responder<Vec<RecoveredTx>>,
    },
    /// Get the transaction count for a block.
    GetTransactionCount {
        /// The block number.
        block: BlockNumber,
        /// The response channel.
        resp: Responder<u64>,
    },

    // --- Receipts ---
    /// Get a single receipt by specifier.
    GetReceipt {
        /// The receipt specifier.
        spec: ReceiptSpecifier,
        /// The response channel.
        resp: Responder<Option<ColdReceipt>>,
    },
    /// Get all receipts in a block.
    GetReceiptsInBlock {
        /// The block number.
        block: BlockNumber,
        /// The response channel.
        resp: Responder<Vec<ColdReceipt>>,
    },

    // --- SignetEvents ---
    /// Get signet events by specifier.
    GetSignetEvents {
        /// The signet events specifier.
        spec: SignetEventsSpecifier,
        /// The response channel.
        resp: Responder<Vec<DbSignetEvent>>,
    },

    // --- ZenithHeaders ---
    /// Get a single zenith header by specifier.
    GetZenithHeader {
        /// The zenith header specifier.
        spec: ZenithHeaderSpecifier,
        /// The response channel.
        resp: Responder<Option<DbZenithHeader>>,
    },
    /// Get multiple zenith headers by specifier.
    GetZenithHeaders {
        /// The zenith header specifier.
        spec: ZenithHeaderSpecifier,
        /// The response channel.
        resp: Responder<Vec<DbZenithHeader>>,
    },

    // --- Logs ---
    /// Filter logs by block range, address, and topics.
    GetLogs {
        /// The log filter.
        filter: Box<Filter>,
        /// Maximum number of logs to return.
        max_logs: usize,
        /// The response channel.
        resp: Responder<Vec<RpcLog>>,
    },
    /// Stream logs matching a filter.
    StreamLogs {
        /// The log filter.
        filter: Box<Filter>,
        /// Maximum number of logs to stream.
        max_logs: usize,
        /// Requested stream deadline (clamped to the task's max).
        deadline: Duration,
        /// Response channel returning the log stream.
        resp: Responder<LogStream>,
    },

    // --- Metadata ---
    /// Get the latest block number.
    GetLatestBlock {
        /// The response channel.
        resp: Responder<Option<BlockNumber>>,
    },
}

impl ColdReadRequest {
    /// Short static name of the request variant, for logging and metrics.
    pub const fn variant_name(&self) -> &'static str {
        match self {
            Self::GetHeader { .. } => "GetHeader",
            Self::GetHeaders { .. } => "GetHeaders",
            Self::GetTransaction { .. } => "GetTransaction",
            Self::GetTransactionsInBlock { .. } => "GetTransactionsInBlock",
            Self::GetTransactionCount { .. } => "GetTransactionCount",
            Self::GetReceipt { .. } => "GetReceipt",
            Self::GetReceiptsInBlock { .. } => "GetReceiptsInBlock",
            Self::GetSignetEvents { .. } => "GetSignetEvents",
            Self::GetZenithHeader { .. } => "GetZenithHeader",
            Self::GetZenithHeaders { .. } => "GetZenithHeaders",
            Self::GetLogs { .. } => "GetLogs",
            Self::StreamLogs { .. } => "StreamLogs",
            Self::GetLatestBlock { .. } => "GetLatestBlock",
        }
    }
}

/// Write requests for cold storage.
///
/// These requests are processed sequentially to maintain ordering.
#[derive(Debug)]
pub enum ColdWriteRequest {
    /// Append a single block.
    AppendBlock(Box<AppendBlockRequest>),
    /// Append multiple blocks.
    AppendBlocks {
        /// The block data to append.
        data: Vec<BlockData>,
        /// The response channel.
        resp: Responder<()>,
    },
    /// Truncate all data above the given block.
    TruncateAbove {
        /// The block number to truncate above.
        block: BlockNumber,
        /// The response channel.
        resp: Responder<()>,
    },
    /// Read receipts and truncate all data above the given block.
    DrainAbove {
        /// The block number to drain above.
        block: BlockNumber,
        /// The response channel.
        resp: Responder<Vec<Vec<ColdReceipt>>>,
    },
}
