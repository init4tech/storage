//! Unified storage combining hot and cold backends.
//!
//! The [`UnifiedStorage`] struct provides a single interface for writing
//! execution data to both hot storage (for fast state access) and cold storage
//! (for historical archival).

use crate::StorageResult;
use alloy::primitives::BlockNumber;
use signet_cold::{BlockData, ColdReceipt, ColdStorage, ColdStorageBackend, ColdStorageError};
use signet_hot::{
    HistoryRead, HistoryWrite, HotKv,
    model::{HotKvReadError, HotKvWrite, RevmRead},
};
use signet_storage_types::{ExecutedBlock, SealedHeader};
use tokio_util::sync::CancellationToken;

/// Block data drained during a reorg unwind.
///
/// Contains the header (always available from hot storage) and receipts
/// (best-effort from cold storage — empty if cold storage lags behind hot).
#[derive(Debug, Clone)]
pub struct DrainedBlock {
    /// The sealed header of the removed block.
    pub header: SealedHeader,
    /// Receipts from cold storage. Empty if cold hasn't indexed this block yet.
    pub receipts: Vec<ColdReceipt>,
}

/// Unified storage combining hot and cold backends.
///
/// This struct provides a single interface for writing execution data to both
/// hot storage (for fast state access) and cold storage (for historical archival).
///
/// # Write Semantics
///
/// - Hot storage writes are synchronous and use database transactions.
/// - Cold storage writes are awaited through a [`ColdStorage`] handle, which
///   serializes writes internally.
///
/// # Error Handling
///
/// Both hot storage and cold storage errors are returned. Callers decide
/// whether cold-storage failures should halt execution or be retried via
/// [`replay_to_cold`](Self::replay_to_cold).
///
/// # Backpressure and Failure Recovery
///
/// Cold storage writes are serialized by the handle's write semaphore. If the
/// backend is slow, `append_blocks` will block the caller until capacity is
/// available. If the handle is shutting down, writes return
/// [`ColdStorageError::TaskTerminated`].
///
/// **Important**: Hot storage is always authoritative. When cold writes fail:
///
/// 1. Hot storage already contains the committed data
/// 2. Cold storage may lag or be unavailable
/// 3. Use [`cold_lag`](Self::cold_lag) to detect gaps between hot and cold
/// 4. Use [`replay_to_cold`](Self::replay_to_cold) to recover cold storage
///
/// [`ColdStorageError::TaskTerminated`]: signet_cold::ColdStorageError::TaskTerminated
///
/// # Example
///
/// ```ignore
/// use signet_storage::UnifiedStorage;
///
/// let storage = UnifiedStorage::new(hot_db, cold_handle);
///
/// // Append executed blocks (takes ownership)
/// storage.append_blocks(blocks).await?;
///
/// // Handle reorgs
/// storage.unwind_above(reorg_block).await?;
/// ```
#[derive(Debug)]
pub struct UnifiedStorage<H: HotKv, B: ColdStorageBackend> {
    hot: H,
    cold: ColdStorage<B>,
}

impl<H: HotKv, B: ColdStorageBackend> UnifiedStorage<H, B> {
    /// Create a new unified storage instance.
    pub const fn new(hot: H, cold: ColdStorage<B>) -> Self {
        Self { hot, cold }
    }

    /// Spawn a unified storage instance from hot and cold backends.
    ///
    /// Constructs a [`ColdStorage`] wrapping `cold_backend` and returns a
    /// fully-assembled [`UnifiedStorage`]. The cold storage handle retains
    /// the `cancel_token` for future shutdown coordination.
    pub fn spawn(hot: H, cold_backend: B, cancel_token: CancellationToken) -> Self {
        let cold = ColdStorage::new(cold_backend, cancel_token);
        Self::new(hot, cold)
    }

    /// Get a reference to the hot storage backend.
    pub const fn hot(&self) -> &H {
        &self.hot
    }

    /// Consume self and return the hot storage backend.
    pub fn into_hot(self) -> H {
        self.hot
    }

    /// Get a reference to the cold storage handle.
    pub const fn cold(&self) -> &ColdStorage<B> {
        &self.cold
    }

    /// Get a clone of the cold storage handle.
    ///
    /// Cloning is cheap (reference-counted). Components that only perform
    /// read operations should use this instead of borrowing.
    pub fn cold_reader(&self) -> ColdStorage<B> {
        self.cold.clone()
    }

    /// Create a read-only transaction for hot storage.
    ///
    /// # Errors
    ///
    /// Returns an error if the transaction cannot be created.
    pub fn reader(&self) -> StorageResult<H::RoTx> {
        self.hot.reader().map_err(Into::into)
    }

    /// Create a revm-compatible read-only database adapter.
    pub fn revm_reader(&self) -> StorageResult<RevmRead<H::RoTx>> {
        self.hot.revm_reader().map_err(Into::into)
    }

    /// Create a revm-compatible read-only database adapter that reads state
    /// at a specific block height.
    pub fn revm_reader_at_height(&self, height: u64) -> StorageResult<RevmRead<H::RoTx>> {
        self.hot.revm_reader_at_height(height).map_err(Into::into)
    }

    /// Append executed blocks to both hot and cold storage.
    ///
    /// This method:
    /// 1. Writes to hot storage synchronously (validates chain extension, updates state)
    /// 2. Awaits cold storage write via the [`ColdStorage`] handle
    ///
    /// # Errors
    ///
    /// - [`Hot`]: Hot storage write failed. No data was written.
    /// - [`Cold`]: Hot storage succeeded but cold write failed. Data is safely
    ///   in hot storage and can be recovered later via
    ///   [`replay_to_cold`](Self::replay_to_cold).
    ///
    /// [`Hot`]: crate::StorageError::Hot
    /// [`Cold`]: crate::StorageError::Cold
    pub async fn append_blocks(&self, blocks: Vec<ExecutedBlock>) -> StorageResult<()> {
        if blocks.is_empty() {
            return Ok(());
        }

        // 1. Write to hot storage (borrows blocks)
        self.write_hot(&blocks)?;

        // 2. Write to cold storage (consumes blocks)
        let cold_data: Vec<_> = blocks.into_iter().map(BlockData::from).collect();
        self.cold.append_blocks(cold_data).await.map_err(Into::into)
    }

    /// Write blocks to hot storage.
    fn write_hot(&self, blocks: &[ExecutedBlock]) -> StorageResult<()> {
        let writer = self.hot.writer()?;

        writer
            .append_blocks(blocks.iter().map(|b| (&b.header, &b.bundle)))
            .map_err(|e| e.map_db(|e| e.into_hot_kv_error()))?;
        writer.raw_commit().map_err(|e| e.into_hot_kv_error())?;

        Ok(())
    }

    /// Read and remove all blocks above the given block number.
    ///
    /// This combines reading the about-to-be-removed data with unwinding,
    /// returning the drained blocks so callers can emit reorg notifications.
    ///
    /// # Implementation
    ///
    /// 1. Reads headers from hot storage (sync)
    /// 2. Unwinds hot storage (sync)
    /// 3. Drains cold storage (async, best-effort)
    ///
    /// # Cold Lag
    ///
    /// If cold storage hasn't processed a block yet, its receipts will be
    /// empty. This is correct: no subscriber has seen those logs, so there
    /// is nothing to "remove" from their perspective.
    ///
    /// # Errors
    ///
    /// - [`Hot`]: Hot storage read or unwind failed.
    /// - [`Cold`]: Hot storage unwound but cold drain failed.
    ///
    /// [`Hot`]: crate::StorageError::Hot
    /// [`Cold`]: crate::StorageError::Cold
    pub async fn drain_above(&self, block: BlockNumber) -> StorageResult<Vec<DrainedBlock>> {
        // 1–2. Read headers and unwind hot storage synchronously.
        let headers = self.unwind_hot_above(block)?;
        if headers.is_empty() {
            return Ok(Vec::new());
        }

        // 3. Atomically drain cold (best-effort — failure = normal cold lag)
        let cold_receipts = self.cold.drain_above(block).await.unwrap_or_default();

        // 4. Assemble drained blocks (zip headers with receipts, default empty).
        let drained = headers
            .into_iter()
            .zip(cold_receipts.into_iter().chain(std::iter::repeat_with(Vec::new)))
            .map(|(header, receipts)| DrainedBlock { header, receipts })
            .collect();

        Ok(drained)
    }

    /// Read headers above `block` and unwind hot storage in a single write
    /// transaction to avoid TOCTOU races. Returns an empty vec if there is
    /// nothing to unwind.
    fn unwind_hot_above(&self, block: BlockNumber) -> StorageResult<Vec<SealedHeader>> {
        let writer = self.hot.writer()?;
        let last = match writer.get_execution_range().map_err(|e| e.into_hot_kv_error())? {
            Some((_, last)) if last > block => last,
            _ => return Ok(Vec::new()),
        };
        let headers =
            writer.get_headers_range(block + 1, last).map_err(|e| e.into_hot_kv_error())?;
        writer.unwind_above(block).map_err(|e| e.map_db(|e| e.into_hot_kv_error()))?;
        writer.raw_commit().map_err(|e| e.into_hot_kv_error())?;
        Ok(headers)
    }

    /// Unwind storage above the given block number (reorg handling).
    ///
    /// This method:
    /// 1. Unwinds hot storage synchronously (restores previous state)
    /// 2. Truncates cold storage via the [`ColdStorage`] handle
    ///
    /// # Errors
    ///
    /// - [`Hot`]: Hot storage unwind failed. State is unchanged.
    /// - [`Cold`]: Hot storage unwound but cold truncate failed.
    ///
    /// [`Hot`]: crate::StorageError::Hot
    /// [`Cold`]: crate::StorageError::Cold
    pub async fn unwind_above(&self, block: BlockNumber) -> StorageResult<()> {
        // 1. Unwind hot storage synchronously (helper scopes the !Send writer)
        self.unwind_hot_sync(block)?;

        // 2. Truncate cold storage
        self.cold.truncate_above(block).await.map_err(Into::into)
    }

    /// Unwind hot storage to `block`. Kept sync so the `!Send` write
    /// transaction does not leak into any async state machine.
    fn unwind_hot_sync(&self, block: BlockNumber) -> StorageResult<()> {
        let writer = self.hot.writer()?;
        writer.unwind_above(block).map_err(|e| e.map_db(|e| e.into_hot_kv_error()))?;
        writer.raw_commit().map_err(|e| e.into_hot_kv_error())?;
        Ok(())
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
        let hot_tip = reader.get_chain_tip().map_err(|e| e.into_hot_kv_error())?;

        let cold_tip = self.cold.get_latest_block().await?;

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
    /// Consumes the blocks to avoid cloning.
    ///
    /// # Errors
    ///
    /// Returns an error if cold storage write fails.
    pub async fn replay_to_cold(&self, blocks: Vec<ExecutedBlock>) -> Result<(), ColdStorageError> {
        let cold_data: Vec<_> = blocks.into_iter().map(BlockData::from).collect();
        self.cold.append_blocks(cold_data).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use signet_cold_mdbx::MdbxColdBackend;
    use signet_hot_mdbx::DatabaseEnv;

    /// Compile-time canaries: all async methods on `UnifiedStorage<DatabaseEnv, MdbxColdBackend>`
    /// must return `Send` futures, even though MDBX write transactions are
    /// `!Send`. If a `!Send` type leaks into the async state machine, these
    /// will fail to compile.
    fn _assert_send<T: Send>(_: T) {}
    fn _drain_above_is_send(s: &UnifiedStorage<DatabaseEnv, MdbxColdBackend>) {
        _assert_send(s.drain_above(0));
    }
    fn _cold_lag_is_send(s: &UnifiedStorage<DatabaseEnv, MdbxColdBackend>) {
        _assert_send(s.cold_lag());
    }
    fn _replay_to_cold_is_send(s: &UnifiedStorage<DatabaseEnv, MdbxColdBackend>) {
        _assert_send(s.replay_to_cold(Vec::new()));
    }
    fn _append_blocks_is_send(s: &UnifiedStorage<DatabaseEnv, MdbxColdBackend>) {
        _assert_send(s.append_blocks(Vec::new()));
    }
    fn _unwind_above_is_send(s: &UnifiedStorage<DatabaseEnv, MdbxColdBackend>) {
        _assert_send(s.unwind_above(0));
    }
}
