//! Unified storage combining hot and cold backends.
//!
//! The [`UnifiedStorage`] struct provides a single interface for writing
//! execution data to both hot storage (for fast state access) and cold storage
//! (for historical archival).

use crate::{StorageError, StorageResult};
use alloy::primitives::BlockNumber;
use signet_cold::{BlockData, ColdStorageError, ColdStorageHandle};
use signet_hot::{
    HistoryError, HistoryRead, HistoryWrite, HotKv,
    model::{HotKvError, HotKvWrite},
};
use signet_storage_types::ExecutedBlock;

/// Helper to convert a `HistoryError<E>` to `HistoryError<HotKvError>`.
fn map_history_error<E: std::error::Error + Send + Sync + 'static>(
    err: HistoryError<E>,
) -> HistoryError<HotKvError> {
    match err {
        HistoryError::NonContiguousBlock { expected, got } => {
            HistoryError::NonContiguousBlock { expected, got }
        }
        HistoryError::ParentHashMismatch { expected, got } => {
            HistoryError::ParentHashMismatch { expected, got }
        }
        HistoryError::DbNotEmpty => HistoryError::DbNotEmpty,
        HistoryError::EmptyRange => HistoryError::EmptyRange,
        HistoryError::Db(e) => HistoryError::Db(HotKvError::from_err(e)),
        HistoryError::IntList(e) => HistoryError::IntList(e),
    }
}

/// Unified storage combining hot and cold backends.
///
/// This struct provides a single interface for writing execution data to both
/// hot storage (for fast state access) and cold storage (for historical archival).
///
/// # Write Semantics
///
/// - Hot storage writes are synchronous and use database transactions
/// - Cold storage writes are dispatched synchronously via the handle (non-blocking)
/// - On `append_blocks`, hot storage is written first, then cold storage is notified
///
/// # Error Handling
///
/// Both hot storage and cold storage errors are returned. Cold storage dispatch
/// fails immediately if the channel is full (non-blocking).
///
/// # Example
///
/// ```ignore
/// use signet_storage::UnifiedStorage;
///
/// let storage = UnifiedStorage::new(hot_db, cold_handle);
///
/// // Append executed blocks
/// storage.append_blocks(&blocks)?;
///
/// // Handle reorgs
/// storage.unwind_above(reorg_block)?;
/// ```
#[derive(Debug)]
pub struct UnifiedStorage<H: HotKv> {
    hot: H,
    cold: ColdStorageHandle,
}

impl<H: HotKv> UnifiedStorage<H> {
    /// Create a new unified storage instance.
    pub const fn new(hot: H, cold: ColdStorageHandle) -> Self {
        Self { hot, cold }
    }

    /// Get a reference to the hot storage backend.
    pub const fn hot(&self) -> &H {
        &self.hot
    }

    /// Get a reference to the cold storage handle.
    pub const fn cold(&self) -> &ColdStorageHandle {
        &self.cold
    }

    /// Create a read-only transaction for hot storage.
    ///
    /// # Errors
    ///
    /// Returns an error if the transaction cannot be created.
    pub fn reader(&self) -> StorageResult<H::RoTx> {
        self.hot.reader().map_err(|e| StorageError::Hot(HistoryError::Db(HotKvError::from_err(e))))
    }

    /// Append executed blocks to both hot and cold storage.
    ///
    /// This method:
    /// 1. Writes to hot storage synchronously (validates chain extension, updates state)
    /// 2. Dispatches writes to cold storage synchronously (non-blocking)
    ///
    /// # Errors
    ///
    /// Returns an error if hot storage write fails or if cold storage channel is full.
    pub fn append_blocks(&self, blocks: &[ExecutedBlock]) -> StorageResult<()> {
        if blocks.is_empty() {
            return Ok(());
        }

        // 1. Write to hot storage synchronously
        self.write_hot(blocks)?;

        // 2. Dispatch to cold storage synchronously (non-blocking)
        self.dispatch_cold(blocks)?;

        Ok(())
    }

    /// Write blocks to hot storage.
    fn write_hot(&self, blocks: &[ExecutedBlock]) -> StorageResult<()> {
        let writer = self
            .hot
            .writer()
            .map_err(|e| StorageError::Hot(HistoryError::Db(HotKvError::from_err(e))))?;

        // Convert to hot storage format
        let hot_data: Vec<_> =
            blocks.iter().map(|b| (b.header.clone(), b.bundle.clone())).collect();

        writer.append_blocks(&hot_data).map_err(|e| StorageError::Hot(map_history_error(e)))?;
        writer
            .raw_commit()
            .map_err(|e| StorageError::Hot(HistoryError::Db(HotKvError::from_err(e))))?;

        Ok(())
    }

    /// Dispatch blocks to cold storage synchronously (non-blocking).
    fn dispatch_cold(&self, blocks: &[ExecutedBlock]) -> StorageResult<()> {
        let cold_data: Vec<_> = blocks
            .iter()
            .map(|b| {
                BlockData::new(
                    b.header.clone().into_inner(),
                    b.transactions.clone(),
                    b.receipts.clone(),
                    b.signet_events.clone(),
                    b.zenith_header,
                )
            })
            .collect();

        self.cold.dispatch_append_blocks(cold_data).map_err(StorageError::Cold)
    }

    /// Unwind storage above the given block number (reorg handling).
    ///
    /// This method:
    /// 1. Unwinds hot storage synchronously (restores previous state)
    /// 2. Truncates cold storage synchronously (non-blocking dispatch)
    ///
    /// # Errors
    ///
    /// Returns an error if hot storage unwind fails or if cold storage channel is full.
    pub fn unwind_above(&self, block: BlockNumber) -> StorageResult<()> {
        // 1. Unwind hot storage synchronously
        let writer = self
            .hot
            .writer()
            .map_err(|e| StorageError::Hot(HistoryError::Db(HotKvError::from_err(e))))?;

        writer.unwind_above(block).map_err(|e| StorageError::Hot(map_history_error(e)))?;
        writer
            .raw_commit()
            .map_err(|e| StorageError::Hot(HistoryError::Db(HotKvError::from_err(e))))?;

        // 2. Truncate cold storage synchronously (non-blocking dispatch)
        self.cold.dispatch_truncate_above(block).map_err(StorageError::Cold)
    }

    /// Check how far behind cold storage is compared to hot storage.
    ///
    /// Returns `Some(first_missing_block)` if cold is behind, `None` if synced.
    ///
    /// # Errors
    ///
    /// Returns an error if either storage cannot be queried.
    pub async fn cold_lag(&self) -> StorageResult<Option<BlockNumber>> {
        let reader = self.reader()?;
        let hot_tip = reader
            .get_chain_tip()
            .map_err(|e| StorageError::Hot(HistoryError::Db(HotKvError::from_err(e))))?;

        let cold_tip = self.cold.get_latest_block().await.map_err(StorageError::Cold)?;

        match (hot_tip, cold_tip) {
            (Some((hot_num, _)), Some(cold_num)) if cold_num < hot_num => Ok(Some(cold_num + 1)),
            (Some((_, _)), None) => Ok(Some(0)),
            _ => Ok(None),
        }
    }

    /// Replay blocks to cold storage from an external source.
    ///
    /// Use this to recover cold storage after failures. The caller is
    /// responsible for fetching the missing block data.
    ///
    /// # Errors
    ///
    /// Returns an error if cold storage write fails.
    pub async fn replay_to_cold(&self, blocks: &[ExecutedBlock]) -> Result<(), ColdStorageError> {
        let cold_data: Vec<_> = blocks
            .iter()
            .map(|b| {
                BlockData::new(
                    b.header.clone().into_inner(),
                    b.transactions.clone(),
                    b.receipts.clone(),
                    b.signet_events.clone(),
                    b.zenith_header,
                )
            })
            .collect();

        self.cold.append_blocks(cold_data).await
    }
}
