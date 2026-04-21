//! Test-only backend that gates read operations on a semaphore.
//!
//! Use for tests that need to saturate the read pool deterministically.

use alloy::primitives::BlockNumber;
use signet_cold::{
    BlockData, ColdReceipt, ColdResult, ColdStorageBackend, ColdStorageRead, ColdStorageWrite,
    Confirmed, Filter, HeaderSpecifier, ReceiptSpecifier, RpcLog, SignetEventsSpecifier,
    StreamParams, TransactionSpecifier, ZenithHeaderSpecifier, mem::MemColdBackend,
};
use signet_storage_types::{DbSignetEvent, DbZenithHeader, RecoveredTx, SealedHeader};
use std::sync::Arc;
use tokio::sync::Semaphore;

/// Backend that parks all reads on a semaphore gate.
///
/// Writes and stream production are ungated so tests can distinguish
/// read-pool saturation from write/drain blocking.
#[derive(Clone)]
pub struct GatedBackend {
    inner: MemColdBackend,
    gate: Arc<Semaphore>,
}

impl GatedBackend {
    /// Build a backend whose read gate is closed (zero permits).
    pub fn closed() -> Self {
        Self { inner: MemColdBackend::new(), gate: Arc::new(Semaphore::new(0)) }
    }

    /// Build a backend whose read gate is effectively open.
    #[allow(dead_code)]
    pub fn open() -> Self {
        let me = Self::closed();
        me.gate.add_permits(usize::MAX >> 4);
        me
    }

    /// Release `n` additional read permits on the gate.
    #[allow(dead_code)]
    pub fn release(&self, n: usize) {
        self.gate.add_permits(n);
    }
}

impl std::fmt::Debug for GatedBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GatedBackend").finish_non_exhaustive()
    }
}

impl ColdStorageRead for GatedBackend {
    async fn get_header(&self, spec: HeaderSpecifier) -> ColdResult<Option<SealedHeader>> {
        let _p = self.gate.clone().acquire_owned().await.ok();
        self.inner.get_header(spec).await
    }

    async fn get_headers(
        &self,
        specs: Vec<HeaderSpecifier>,
    ) -> ColdResult<Vec<Option<SealedHeader>>> {
        let _p = self.gate.clone().acquire_owned().await.ok();
        self.inner.get_headers(specs).await
    }

    async fn get_transaction(
        &self,
        spec: TransactionSpecifier,
    ) -> ColdResult<Option<Confirmed<RecoveredTx>>> {
        let _p = self.gate.clone().acquire_owned().await.ok();
        self.inner.get_transaction(spec).await
    }

    async fn get_transactions_in_block(&self, block: BlockNumber) -> ColdResult<Vec<RecoveredTx>> {
        let _p = self.gate.clone().acquire_owned().await.ok();
        self.inner.get_transactions_in_block(block).await
    }

    async fn get_transaction_count(&self, block: BlockNumber) -> ColdResult<u64> {
        let _p = self.gate.clone().acquire_owned().await.ok();
        self.inner.get_transaction_count(block).await
    }

    async fn get_receipt(&self, spec: ReceiptSpecifier) -> ColdResult<Option<ColdReceipt>> {
        let _p = self.gate.clone().acquire_owned().await.ok();
        self.inner.get_receipt(spec).await
    }

    async fn get_receipts_in_block(&self, block: BlockNumber) -> ColdResult<Vec<ColdReceipt>> {
        let _p = self.gate.clone().acquire_owned().await.ok();
        self.inner.get_receipts_in_block(block).await
    }

    async fn get_signet_events(
        &self,
        spec: SignetEventsSpecifier,
    ) -> ColdResult<Vec<DbSignetEvent>> {
        let _p = self.gate.clone().acquire_owned().await.ok();
        self.inner.get_signet_events(spec).await
    }

    async fn get_zenith_header(
        &self,
        spec: ZenithHeaderSpecifier,
    ) -> ColdResult<Option<DbZenithHeader>> {
        let _p = self.gate.clone().acquire_owned().await.ok();
        self.inner.get_zenith_header(spec).await
    }

    async fn get_zenith_headers(
        &self,
        spec: ZenithHeaderSpecifier,
    ) -> ColdResult<Vec<DbZenithHeader>> {
        let _p = self.gate.clone().acquire_owned().await.ok();
        self.inner.get_zenith_headers(spec).await
    }

    async fn get_latest_block(&self) -> ColdResult<Option<BlockNumber>> {
        let _p = self.gate.clone().acquire_owned().await.ok();
        self.inner.get_latest_block().await
    }

    async fn get_logs(&self, filter: &Filter, max_logs: usize) -> ColdResult<Vec<RpcLog>> {
        let _p = self.gate.clone().acquire_owned().await.ok();
        self.inner.get_logs(filter, max_logs).await
    }

    async fn produce_log_stream(&self, filter: &Filter, params: StreamParams) {
        // Streams ungated.
        self.inner.produce_log_stream(filter, params).await;
    }
}

impl ColdStorageWrite for GatedBackend {
    async fn append_block(&self, data: BlockData) -> ColdResult<()> {
        // Ungated — writers need to distinguish drain-blocking from
        // read-gating in tests.
        self.inner.append_block(data).await
    }

    async fn append_blocks(&self, data: Vec<BlockData>) -> ColdResult<()> {
        self.inner.append_blocks(data).await
    }

    async fn truncate_above(&self, block: BlockNumber) -> ColdResult<()> {
        self.inner.truncate_above(block).await
    }
}

impl ColdStorageBackend for GatedBackend {
    async fn drain_above(&self, block: BlockNumber) -> ColdResult<Vec<Vec<ColdReceipt>>> {
        self.inner.drain_above(block).await
    }
}
