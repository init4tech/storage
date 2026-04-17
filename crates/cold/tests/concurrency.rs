//! Concurrency regression tests for the cold storage task.
//!
//! These tests exercise the read/write concurrency machinery in
//! [`signet_cold::ColdStorageTask`] directly, independent of any particular
//! backend. They use the in-memory backend (optionally wrapped in a
//! [`GatedBackend`]) as a fast, deterministic fixture.

use alloy::{
    consensus::{Header, Sealable},
    primitives::BlockNumber,
};
use signet_cold::{
    BlockData, ColdReceipt, ColdResult, ColdStorage, ColdStorageError, ColdStorageRead,
    ColdStorageTask, ColdStorageWrite, Confirmed, Filter, HeaderSpecifier, ReceiptSpecifier,
    RpcLog, SignetEventsSpecifier, StreamParams, TransactionSpecifier, ZenithHeaderSpecifier,
    mem::MemColdBackend, produce_log_stream_default,
};
use signet_storage_types::{DbSignetEvent, DbZenithHeader, RecoveredTx, SealedHeader};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{sync::Semaphore, time::timeout};
use tokio_util::sync::CancellationToken;

/// Upper bound for the whole test. Far larger than any correct
/// execution; small enough that a deadlock regression trips the
/// timeout quickly.
const DEADLOCK_GUARD: Duration = Duration::from_secs(15);

fn block(n: u64) -> BlockData {
    let header = Header { number: n, ..Default::default() };
    BlockData::new(header.seal_slow(), vec![], vec![], vec![], None)
}

/// Test wrapper that gates every read call on a test-owned semaphore.
///
/// Each read method acquires a permit on [`GatedBackend::gate`] before
/// delegating to the inner backend. Tests control permit availability
/// to hold readers in a specific state, then release them on demand.
///
/// Writes pass through unchanged. `produce_log_stream` is not gated
/// (streaming isn't part of the drain-before-write invariant).
#[derive(Clone)]
struct GatedBackend<B> {
    inner: B,
    gate: Arc<Semaphore>,
    /// Count of read calls that have entered the gate (incremented
    /// before `acquire`). Lets tests observe how many readers are
    /// currently parked waiting for a permit.
    reads_entered: Arc<AtomicUsize>,
}

impl<B> GatedBackend<B> {
    fn new(inner: B, initial_permits: usize) -> Self {
        Self {
            inner,
            gate: Arc::new(Semaphore::new(initial_permits)),
            reads_entered: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn gate(&self) -> Arc<Semaphore> {
        Arc::clone(&self.gate)
    }

    async fn acquire(&self) {
        self.reads_entered.fetch_add(1, Ordering::SeqCst);
        // Permits are returned to the pool when the guard drops at the
        // end of the method — we only need to serialize at the gate,
        // not keep the permit across the call.
        let _ = self.gate.acquire().await.expect("gate never closed in tests");
    }
}

impl<B: ColdStorageRead> ColdStorageRead for GatedBackend<B> {
    async fn get_header(&self, spec: HeaderSpecifier) -> ColdResult<Option<SealedHeader>> {
        self.acquire().await;
        self.inner.get_header(spec).await
    }

    async fn get_headers(
        &self,
        specs: Vec<HeaderSpecifier>,
    ) -> ColdResult<Vec<Option<SealedHeader>>> {
        self.acquire().await;
        self.inner.get_headers(specs).await
    }

    async fn get_transaction(
        &self,
        spec: TransactionSpecifier,
    ) -> ColdResult<Option<Confirmed<RecoveredTx>>> {
        self.acquire().await;
        self.inner.get_transaction(spec).await
    }

    async fn get_transactions_in_block(&self, block: BlockNumber) -> ColdResult<Vec<RecoveredTx>> {
        self.acquire().await;
        self.inner.get_transactions_in_block(block).await
    }

    async fn get_transaction_count(&self, block: BlockNumber) -> ColdResult<u64> {
        self.acquire().await;
        self.inner.get_transaction_count(block).await
    }

    async fn get_receipt(&self, spec: ReceiptSpecifier) -> ColdResult<Option<ColdReceipt>> {
        self.acquire().await;
        self.inner.get_receipt(spec).await
    }

    async fn get_receipts_in_block(&self, block: BlockNumber) -> ColdResult<Vec<ColdReceipt>> {
        self.acquire().await;
        self.inner.get_receipts_in_block(block).await
    }

    async fn get_signet_events(
        &self,
        spec: SignetEventsSpecifier,
    ) -> ColdResult<Vec<DbSignetEvent>> {
        self.acquire().await;
        self.inner.get_signet_events(spec).await
    }

    async fn get_zenith_header(
        &self,
        spec: ZenithHeaderSpecifier,
    ) -> ColdResult<Option<DbZenithHeader>> {
        self.acquire().await;
        self.inner.get_zenith_header(spec).await
    }

    async fn get_zenith_headers(
        &self,
        spec: ZenithHeaderSpecifier,
    ) -> ColdResult<Vec<DbZenithHeader>> {
        self.acquire().await;
        self.inner.get_zenith_headers(spec).await
    }

    async fn get_latest_block(&self) -> ColdResult<Option<BlockNumber>> {
        self.acquire().await;
        self.inner.get_latest_block().await
    }

    async fn get_logs(&self, filter: &Filter, max_logs: usize) -> ColdResult<Vec<RpcLog>> {
        self.acquire().await;
        self.inner.get_logs(filter, max_logs).await
    }

    async fn produce_log_stream(&self, filter: &Filter, params: StreamParams) {
        produce_log_stream_default(self, filter, params).await;
    }
}

impl<B: ColdStorageWrite> ColdStorageWrite for GatedBackend<B> {
    async fn append_block(&mut self, data: BlockData) -> ColdResult<()> {
        self.inner.append_block(data).await
    }

    async fn append_blocks(&mut self, data: Vec<BlockData>) -> ColdResult<()> {
        self.inner.append_blocks(data).await
    }

    async fn truncate_above(&mut self, block: BlockNumber) -> ColdResult<()> {
        self.inner.truncate_above(block).await
    }
}

impl<B: ColdStorage> ColdStorage for GatedBackend<B> {}

/// Regression test for the read-arm backpressure deadlock.
///
/// Prior to the semaphore-based backpressure fix, the run loop used
/// `TaskTracker::wait()` to throttle readers once `MAX_CONCURRENT_READERS`
/// tasks were in flight. `TaskTracker::wait()` only resolves when the
/// tracker is both closed and empty, and the read arm never closes the
/// tracker — so the whole run loop wedged the moment the 65th concurrent
/// read arrived. All subsequent reads piled up in the mpsc channel and
/// writes could not be dispatched either.
///
/// This test issues 256 concurrent reads (4× the in-flight cap) and asserts
/// that every one of them completes. Without the fix the task deadlocks and
/// this test hits [`DEADLOCK_GUARD`].
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reads_above_concurrency_cap_do_not_deadlock() {
    let backend = MemColdBackend::new();
    let cancel = CancellationToken::new();
    let handle = ColdStorageTask::spawn(backend, cancel.clone());

    handle.append_blocks((1..=8).map(block).collect()).await.unwrap();

    let result = timeout(DEADLOCK_GUARD, async {
        let reader = handle.reader();
        let mut set = tokio::task::JoinSet::new();
        for i in 0..256u64 {
            let reader = reader.clone();
            set.spawn(async move { reader.get_header_by_number(1 + (i % 8)).await });
        }
        let mut completed = 0usize;
        while let Some(res) = set.join_next().await {
            res.expect("read task panicked").expect("read returned an error");
            completed += 1;
        }
        completed
    })
    .await
    .expect("cold storage task deadlocked under concurrent reads");

    assert_eq!(result, 256);

    cancel.cancel();
}

/// A write arriving while reads are saturated must still be processed.
///
/// With the old backpressure scheme, once 64 reads were in flight the read
/// arm would wedge on `TaskTracker::wait()`, so the outer `select!` never
/// re-polled and the write arm was starved. This test interleaves a write
/// into a flood of reads and requires it to complete promptly.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn write_after_saturating_reads_makes_progress() {
    let backend = MemColdBackend::new();
    let cancel = CancellationToken::new();
    let handle = ColdStorageTask::spawn(backend, cancel.clone());

    handle.append_blocks((1..=8).map(block).collect()).await.unwrap();

    let result = timeout(DEADLOCK_GUARD, async {
        let reader = handle.reader();
        let mut set = tokio::task::JoinSet::new();
        for i in 0..256u64 {
            let reader = reader.clone();
            set.spawn(async move { reader.get_header_by_number(1 + (i % 8)).await });
        }

        handle.append_blocks(vec![block(9)]).await.unwrap();

        while let Some(res) = set.join_next().await {
            res.expect("read task panicked").expect("read returned an error");
        }
    })
    .await;

    result.expect("write was starved by saturated readers");

    cancel.cancel();
}

/// A write queued behind saturated readers completes before any readers
/// that arrive after it. Relies on tokio's FIFO semaphore fairness.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fairness_write_serves_before_later_readers() {
    let mem = MemColdBackend::new();
    let backend = GatedBackend::new(mem, 0);
    let gate = backend.gate();
    let entered = Arc::clone(&backend.reads_entered);

    let cancel = CancellationToken::new();
    let (task, handle) = ColdStorageTask::new(backend, cancel.clone());
    let task_handle = tokio::spawn(task.run());

    // Seed via a bypass so we don't have to wait on the gate for setup.
    // append_blocks is a write — not gated.
    handle.append_blocks((1..=8).map(block).collect()).await.unwrap();

    timeout(DEADLOCK_GUARD, async {
        // First wave: 64 readers that will saturate the in-flight cap and
        // park on the gate.
        let reader = handle.reader();
        let mut first_wave = tokio::task::JoinSet::new();
        for i in 0..64u64 {
            let reader = reader.clone();
            first_wave.spawn(async move { reader.get_header_by_number(1 + (i % 8)).await });
        }

        // Wait until all 64 have reached the gate.
        while entered.load(Ordering::SeqCst) < 64 {
            tokio::task::yield_now().await;
        }

        // Writer queues behind the 64 saturated readers (it needs all
        // 64 permits). Its completion is ordered before the second wave
        // by FIFO fairness on the semaphore.
        let write_done = Arc::new(tokio::sync::Notify::new());
        let write_done_tx = Arc::clone(&write_done);
        let handle_for_write = handle.clone();
        let write_task = tokio::spawn(async move {
            handle_for_write.append_blocks(vec![block(9)]).await.unwrap();
            write_done_tx.notify_one();
        });

        // Give the writer a moment to enter its drain.
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Second wave: 64 readers queued *after* the writer.
        let mut second_wave = tokio::task::JoinSet::new();
        for i in 0..64u64 {
            let reader = reader.clone();
            second_wave.spawn(async move { reader.get_header_by_number(1 + (i % 8)).await });
        }

        // Release all gate permits. Let every queued reader proceed.
        // First-wave readers finish (releasing their cold semaphore
        // permits); writer accumulates; writer completes; second-wave
        // readers then acquire permits and run.
        gate.add_permits(256);

        // Writer must complete before any second-wave reader finishes.
        let write_first = tokio::select! {
            biased;
            _ = write_done.notified() => true,
            Some(_) = second_wave.join_next() => false,
        };
        assert!(write_first, "second-wave reader completed before queued write");

        // Drain everything.
        write_task.await.expect("write task panicked");
        while first_wave.join_next().await.is_some() {}
        while second_wave.join_next().await.is_some() {}
    })
    .await
    .expect("fairness test timed out");

    cancel.cancel();
    let _ = timeout(DEADLOCK_GUARD, task_handle).await;
}

/// Cancellation must cut through reader backpressure: a reader parked
/// waiting for a permit cannot keep the task alive.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancel_during_reader_backpressure_shuts_down() {
    let mem = MemColdBackend::new();
    let backend = GatedBackend::new(mem, 0);
    let entered = Arc::clone(&backend.reads_entered);

    let cancel = CancellationToken::new();
    let (task, handle) = ColdStorageTask::new(backend, cancel.clone());
    let task_handle = tokio::spawn(task.run());

    handle.append_blocks((1..=4).map(block).collect()).await.unwrap();

    // Saturate the in-flight cap: all 64 readers park on the gate.
    let reader = handle.reader();
    let mut set = tokio::task::JoinSet::new();
    for i in 0..64u64 {
        let reader = reader.clone();
        set.spawn(async move { reader.get_header_by_number(1 + (i % 4)).await });
    }

    while entered.load(Ordering::SeqCst) < 64 {
        tokio::task::yield_now().await;
    }

    // A 65th read queues on the cold semaphore (not yet in the backend).
    let reader_65 = reader.clone();
    let waiter = tokio::spawn(async move { reader_65.get_header_by_number(1).await });

    // Now cancel.
    cancel.cancel();

    // Task must shut down within the deadlock guard. The stuck readers
    // are released by the 5s read deadline.
    timeout(DEADLOCK_GUARD, task_handle).await.expect("task did not shut down").unwrap();

    // The 65th read either sees Cancelled or TaskTerminated — both are
    // acceptable outcomes of shutdown.
    let res = waiter.await.expect("waiter panicked");
    assert!(
        matches!(res, Err(ColdStorageError::Cancelled | ColdStorageError::TaskTerminated)),
        "unexpected waiter result: {res:?}"
    );

    drop(set);
}

/// Cancellation must cut through the write drain: a stuck reader
/// cannot block shutdown by holding a permit the writer is waiting on.
///
/// Without the cancel-select around the writer's `acquire_many_owned`,
/// this test hits [`DEADLOCK_GUARD`].
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancel_during_write_drain_shuts_down() {
    let mem = MemColdBackend::new();
    let backend = GatedBackend::new(mem, 0);
    let entered = Arc::clone(&backend.reads_entered);

    let cancel = CancellationToken::new();
    let (task, handle) = ColdStorageTask::new(backend, cancel.clone());
    let task_handle = tokio::spawn(task.run());

    handle.append_blocks((1..=4).map(block).collect()).await.unwrap();

    // Saturate the in-flight cap.
    let reader = handle.reader();
    let mut set = tokio::task::JoinSet::new();
    for i in 0..64u64 {
        let reader = reader.clone();
        set.spawn(async move { reader.get_header_by_number(1 + (i % 4)).await });
    }

    while entered.load(Ordering::SeqCst) < 64 {
        tokio::task::yield_now().await;
    }

    // Queue a write. It'll enter the drain and block on acquire_many.
    let handle_for_write = handle.clone();
    let write_waiter =
        tokio::spawn(async move { handle_for_write.append_blocks(vec![block(5)]).await });

    // Give the writer a moment to start draining.
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Cancel while the writer is blocked inside acquire_many.
    cancel.cancel();

    // Task must shut down within the guard. Without the cancel-select
    // around acquire_many_owned, the writer never sees the cancel.
    timeout(DEADLOCK_GUARD, task_handle).await.expect("task did not shut down").unwrap();

    // The aborted write receives Cancelled (its oneshot sender was
    // dropped when the writer task exited).
    let write_res = write_waiter.await.expect("write waiter panicked");
    assert!(
        matches!(write_res, Err(ColdStorageError::Cancelled | ColdStorageError::TaskTerminated)),
        "unexpected write result: {write_res:?}"
    );

    drop(set);
}

/// A read that exceeds the operation deadline returns
/// `ColdStorageError::Timeout` and releases its permit so subsequent
/// reads can proceed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn operation_deadline_releases_permit() {
    let mem = MemColdBackend::new();
    let backend = GatedBackend::new(mem, 0);
    let gate = backend.gate();
    let entered = Arc::clone(&backend.reads_entered);

    let cancel = CancellationToken::new();
    let (task, handle) = ColdStorageTask::new(backend, cancel.clone())
        .map_first(|t| t.with_read_deadline(Duration::from_millis(200)));
    let task_handle = tokio::spawn(task.run());

    handle.append_blocks((1..=4).map(block).collect()).await.unwrap();

    let reader = handle.reader();

    // First read: hits the gate, times out after 200ms.
    let first = tokio::spawn({
        let reader = reader.clone();
        async move { reader.get_header_by_number(1).await }
    });

    while entered.load(Ordering::SeqCst) < 1 {
        tokio::task::yield_now().await;
    }

    // Wait long enough for the deadline to fire.
    let res = timeout(Duration::from_secs(2), first).await.expect("first read stuck");
    let res = res.expect("first read panicked");
    assert!(matches!(res, Err(ColdStorageError::Timeout)), "expected Timeout, got {res:?}");

    // Second read: the permit from the timed-out first read must be
    // back in the pool. Open the gate so this one succeeds.
    gate.add_permits(1);
    let second = timeout(Duration::from_secs(2), reader.get_header_by_number(1))
        .await
        .expect("second read stuck");
    second.expect("second read failed");

    cancel.cancel();
    let _ = timeout(DEADLOCK_GUARD, task_handle).await;
}

/// Helper to transform `(Task, Handle)` returned by `ColdStorageTask::new`
/// by applying a function to the task without touching the handle.
trait MapFirst<A, B> {
    fn map_first<F, A2>(self, f: F) -> (A2, B)
    where
        F: FnOnce(A) -> A2;
}

impl<A, B> MapFirst<A, B> for (A, B) {
    fn map_first<F, A2>(self, f: F) -> (A2, B)
    where
        F: FnOnce(A) -> A2,
    {
        (f(self.0), self.1)
    }
}
