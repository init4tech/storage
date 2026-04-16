//! Concurrency regression tests for the cold storage task.
//!
//! These tests exercise the read/write concurrency machinery in
//! [`signet_cold::ColdStorageTask`] directly, independent of any particular
//! backend. They use the in-memory backend as a fast, deterministic fixture.

use alloy::consensus::{Header, Sealable};
use signet_cold::{BlockData, ColdStorageTask, mem::MemColdBackend};
use std::time::Duration;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

/// Upper bound for the whole test. Far larger than any correct execution;
/// small enough that a deadlock regression trips the timeout quickly.
const DEADLOCK_GUARD: Duration = Duration::from_secs(15);

fn block(n: u64) -> BlockData {
    let header = Header { number: n, ..Default::default() };
    BlockData::new(header.seal_slow(), vec![], vec![], vec![], None)
}

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

    // Seed a handful of blocks so the reads have something to find.
    handle.append_blocks((1..=8).map(block).collect()).await.unwrap();

    let result = timeout(DEADLOCK_GUARD, async {
        let reader = handle.reader();
        let mut set = tokio::task::JoinSet::new();
        // 4× the 64-reader concurrency cap — enough to reach the previously
        // deadlocking path many times over.
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

        // Issue a write while the readers are still queuing. With the fix
        // it completes as soon as in-flight readers drain; without it, it
        // never dispatches because the run loop is wedged.
        handle.append_blocks(vec![block(9)]).await.unwrap();

        while let Some(res) = set.join_next().await {
            res.expect("read task panicked").expect("read returned an error");
        }
    })
    .await;

    result.expect("write was starved by saturated readers");

    cancel.cancel();
}
