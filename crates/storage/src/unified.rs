//! Unified storage combining hot and cold backends.
//!
//! The [`UnifiedStorage`] struct provides a single interface for writing
//! execution data to both hot storage (for fast state access) and cold storage
//! (for historical archival).

use crate::{StorageError, StorageResult};
use alloy::primitives::BlockNumber;
use signet_cold::{BlockData, ColdStorageError, ColdStorageHandle};
use signet_hot::{
    HistoryRead, HistoryWrite, HotKv,
    model::{HotKvRead, HotKvWrite},
};
use signet_storage_types::ExecutedBlock;

/// Unified storage combining hot and cold backends.
///
/// This struct provides a single interface for writing execution data to both
/// hot storage (for fast state access) and cold storage (for historical archival).
///
/// # Write Semantics
///
/// - Hot storage writes are synchronous and use database transactions
/// - Cold storage writes are dispatched asynchronously via the handle
/// - On `append_blocks`, hot storage is written first (sync), then cold storage
///   is notified (async dispatch)
///
/// # Error Handling
///
/// Hot storage errors are returned immediately. Cold storage write errors are
/// logged but do not block the caller - cold storage is fire-and-forget.
///
/// # Example
///
/// ```ignore
/// use signet_storage::UnifiedStorage;
///
/// let storage = UnifiedStorage::new(hot_db, cold_handle);
///
/// // Append executed blocks
/// storage.append_blocks(&blocks).await?;
///
/// // Handle reorgs
/// storage.unwind_above(reorg_block).await?;
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
    pub fn reader(&self) -> Result<H::RoTx, StorageError<<H::RoTx as HotKvRead>::Error>> {
        self.hot.reader().map_err(StorageError::HotTx)
    }

    /// Append executed blocks to both hot and cold storage.
    ///
    /// This method:
    /// 1. Writes to hot storage synchronously (validates chain extension, updates state)
    /// 2. Dispatches writes to cold storage asynchronously (fire-and-forget)
    ///
    /// Hot storage errors are returned immediately. Cold storage writes are
    /// dispatched asynchronously; errors are logged but not propagated.
    ///
    /// # Errors
    ///
    /// Returns an error if hot storage write fails. Cold storage errors are
    /// logged but not returned.
    pub async fn append_blocks(
        &self,
        blocks: &[ExecutedBlock],
    ) -> StorageResult<(), <H::RwTx as HotKvRead>::Error> {
        if blocks.is_empty() {
            return Ok(());
        }

        // 1. Write to hot storage synchronously
        self.write_hot(blocks)?;

        // 2. Dispatch to cold storage asynchronously (fire-and-forget)
        self.dispatch_cold(blocks).await;

        Ok(())
    }

    /// Write blocks to hot storage.
    fn write_hot(
        &self,
        blocks: &[ExecutedBlock],
    ) -> StorageResult<(), <H::RwTx as HotKvRead>::Error> {
        let writer = self.hot.writer().map_err(StorageError::HotTx)?;

        // Convert to hot storage format
        let hot_data: Vec<_> =
            blocks.iter().map(|b| (b.header.clone(), b.bundle.clone())).collect();

        writer.append_blocks(&hot_data)?;
        writer.raw_commit().map_err(|e| StorageError::Hot(signet_hot::HistoryError::Db(e)))?;

        Ok(())
    }

    /// Dispatch blocks to cold storage asynchronously.
    ///
    /// Errors are logged but not propagated (fire-and-forget).
    async fn dispatch_cold(&self, blocks: &[ExecutedBlock]) {
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

        if let Err(e) = self.cold.append_blocks(cold_data).await {
            tracing::error!(
                error = ?e,
                block_count = blocks.len(),
                first_block = blocks.first().map(|b| b.block_number()),
                "failed to append blocks to cold storage"
            );
        }
    }

    /// Unwind storage above the given block number (reorg handling).
    ///
    /// This method:
    /// 1. Unwinds hot storage synchronously (restores previous state)
    /// 2. Truncates cold storage asynchronously (fire-and-forget)
    ///
    /// # Errors
    ///
    /// Returns an error if hot storage unwind fails. Cold storage errors are
    /// logged but not returned.
    pub async fn unwind_above(
        &self,
        block: BlockNumber,
    ) -> StorageResult<(), <H::RwTx as HotKvRead>::Error> {
        // 1. Unwind hot storage synchronously
        let writer = self.hot.writer().map_err(StorageError::HotTx)?;

        writer.unwind_above(block)?;
        writer.raw_commit().map_err(|e| StorageError::Hot(signet_hot::HistoryError::Db(e)))?;

        // 2. Truncate cold storage asynchronously (fire-and-forget)
        if let Err(e) = self.cold.truncate_above(block).await {
            tracing::error!(
                error = ?e,
                block,
                "failed to truncate cold storage"
            );
        }

        Ok(())
    }

    /// Check how far behind cold storage is compared to hot storage.
    ///
    /// Returns `Some(first_missing_block)` if cold is behind, `None` if synced.
    ///
    /// # Errors
    ///
    /// Returns an error if either storage cannot be queried.
    pub async fn cold_lag(
        &self,
    ) -> Result<Option<BlockNumber>, StorageError<<H::RoTx as HotKvRead>::Error>> {
        let reader = self.reader()?;
        let hot_tip = reader
            .get_chain_tip()
            .map_err(|e| StorageError::Hot(signet_hot::HistoryError::Db(e)))?;

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
