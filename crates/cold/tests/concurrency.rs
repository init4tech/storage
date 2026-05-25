//! Concurrency scenarios for the unified `ColdStorage` handle.
//!
//! Per-task tests cover narrow cases (`drain_barrier.rs`,
//! `stream_isolation.rs`, `shutdown.rs`, `handle_shape.rs`); this file
//! collects the broader load scenarios from the architecture spec.

mod common;

use alloy::rpc::types::Filter;
use common::gated::{BackendOp, GatedBackend};
use signet_cold::{
    ColdStorage, ColdStorageError, HeaderSpecifier, conformance::make_test_block,
    mem::MemColdBackend,
};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

/// 1. 256 concurrent reads against an ungated backend must all complete
///    despite only 64 `read_sem` permits.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reads_above_concurrency_cap_do_not_deadlock() {
    let cs = ColdStorage::new(MemColdBackend::new(), CancellationToken::new());

    let mut handles = Vec::with_capacity(256);
    for _ in 0..256 {
        let cs2 = cs.clone();
        handles.push(tokio::spawn(async move { cs2.get_latest_block().await }));
    }

    for h in handles {
        let r = tokio::time::timeout(Duration::from_secs(15), h)
            .await
            .expect("read deadlocked")
            .expect("task panicked");
        r.expect("read failed");
    }
}

/// 2. A write interleaved with saturating reads still completes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn write_after_saturating_reads_makes_progress() {
    let cs = ColdStorage::new(MemColdBackend::new(), CancellationToken::new());

    let mut readers = Vec::with_capacity(128);
    for _ in 0..128 {
        let cs2 = cs.clone();
        readers.push(tokio::spawn(async move { cs2.get_latest_block().await }));
    }

    let cs_w = cs.clone();
    let writer = tokio::spawn(async move { cs_w.truncate_above(0).await });

    let mut more_readers = Vec::with_capacity(128);
    for _ in 0..128 {
        let cs2 = cs.clone();
        more_readers.push(tokio::spawn(async move { cs2.get_latest_block().await }));
    }

    tokio::time::timeout(Duration::from_secs(15), writer)
        .await
        .expect("writer deadlocked")
        .expect("writer panicked")
        .expect("writer failed");

    for h in readers.into_iter().chain(more_readers) {
        tokio::time::timeout(Duration::from_secs(15), h)
            .await
            .expect("reader deadlocked")
            .expect("reader panicked")
            .expect("reader failed");
    }
}

/// 3. Fairness: a writer acquired after saturating readers must complete
///    before readers queued *after* the writer.
///
/// The invariant is observed inside the backend, not via caller-side
/// completion signals. `GetLatestBlock` is recorded after passing the
/// read gate; `TruncateAbove` is recorded after the inner write returns,
/// while the writer still holds the drain barrier. So when `TruncateAbove`
/// appears in the log, the write has completed and no later reader can
/// yet be running. The recording is therefore strictly ordered by the
/// semaphore, without racing the post-drain wake-up window where reader
/// and writer wrappers would otherwise compete to signal downstream.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fairness_write_serves_before_later_readers() {
    let backend = GatedBackend::closed();
    let cs = ColdStorage::new(backend.clone(), CancellationToken::new());

    // Saturate all 64 read permits behind the backend gate.
    let mut saturating = Vec::with_capacity(64);
    for _ in 0..64 {
        let cs2 = cs.clone();
        saturating.push(tokio::spawn(async move { cs2.get_latest_block().await }));
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Queue a writer. It holds `write_sem`, then blocks on the drain
    // barrier waiting for the 64 in-flight readers.
    let cs_w = cs.clone();
    let writer = tokio::spawn(async move { cs_w.truncate_above(0).await });
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Queue 64 "later" readers. They park on `read_sem::acquire_owned`
    // because the writer's drain has claimed every permit.
    let mut later = Vec::with_capacity(64);
    for _ in 0..64 {
        let cs2 = cs.clone();
        later.push(tokio::spawn(async move { cs2.get_latest_block().await }));
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Release the backend gate so the 64 saturating readers complete,
    // which lets the drain barrier acquire and the writer run.
    backend.release(usize::MAX >> 4);

    // Drive everything to completion before inspecting the log.
    for h in saturating {
        tokio::time::timeout(Duration::from_secs(5), h)
            .await
            .expect("saturating reader hung")
            .expect("saturating reader panicked")
            .expect("saturating reader failed");
    }
    tokio::time::timeout(Duration::from_secs(5), writer)
        .await
        .expect("writer hung")
        .expect("writer panicked")
        .expect("writer failed");
    for h in later {
        tokio::time::timeout(Duration::from_secs(5), h)
            .await
            .expect("later reader hung")
            .expect("later reader panicked")
            .expect("later reader failed");
    }

    // Expect 64 saturating reads, then the write, then 64 later reads.
    let events = backend.events();
    assert_eq!(
        events.len(),
        129,
        "expected 64 saturating reads + 1 write + 64 later reads, got {events:?}",
    );
    assert!(
        events[..64].iter().all(|op| *op == BackendOp::GetLatestBlock),
        "first 64 events must be saturating reads: {events:?}",
    );
    assert_eq!(
        events[64],
        BackendOp::TruncateAbove,
        "write must follow the saturating reads: {events:?}",
    );
    assert!(
        events[65..].iter().all(|op| *op == BackendOp::GetLatestBlock),
        "last 64 events must be later reads: {events:?}",
    );
}

/// 4. Cancel during reader backpressure: queued acquisitions fail fast.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancel_during_reader_backpressure_shuts_down() {
    let backend = GatedBackend::closed();
    let cancel = CancellationToken::new();
    let cs = ColdStorage::new(backend.clone(), cancel.clone());

    // Saturate all 64 read permits.
    let mut readers = Vec::with_capacity(65);
    for _ in 0..64 {
        let cs2 = cs.clone();
        readers.push(tokio::spawn(async move { cs2.get_latest_block().await }));
    }
    // One queued reader — parked on `read_sem::acquire_owned`.
    let cs_q = cs.clone();
    readers.push(tokio::spawn(async move { cs_q.get_latest_block().await }));
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Cancel: coordinator closes the semaphores.
    cancel.cancel();
    tokio::time::sleep(Duration::from_millis(50)).await;

    // New acquisitions fail fast.
    let err = cs.get_latest_block().await.unwrap_err();
    assert!(matches!(err, ColdStorageError::TaskTerminated));

    // Release the backend gate so any in-flight readers finish.
    backend.release(usize::MAX >> 4);

    // All spawned readers resolve within the bound. Those that had
    // acquired permits complete with `Ok(None)`; the queued one resolves
    // with `Err(TaskTerminated)` because semaphore close propagates.
    for h in readers {
        let _ = tokio::time::timeout(Duration::from_secs(1), h)
            .await
            .expect("reader hung after cancel")
            .expect("reader task panicked");
    }
}

/// 5. Cancel during write drain: writer parked on `acquire_many_owned`
///    exits promptly.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancel_during_write_drain_shuts_down() {
    let backend = GatedBackend::closed();
    let cancel = CancellationToken::new();
    let cs = ColdStorage::new(backend.clone(), cancel.clone());

    for _ in 0..64 {
        let cs2 = cs.clone();
        tokio::spawn(async move {
            let _ = cs2.get_latest_block().await;
        });
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Queue a writer. Parks on the drain barrier.
    let cs_w = cs.clone();
    let writer = tokio::spawn(async move { cs_w.truncate_above(0).await });
    tokio::time::sleep(Duration::from_millis(100)).await;

    cancel.cancel();
    tokio::time::sleep(Duration::from_millis(50)).await;
    backend.release(usize::MAX >> 4);

    let result = tokio::time::timeout(Duration::from_secs(1), writer)
        .await
        .expect("writer hung after cancel")
        .expect("writer task panicked");
    assert!(matches!(result, Err(ColdStorageError::TaskTerminated)));
}

/// 6. Stream caller cancellation releases its `stream_sem` permit so a
///    later stream can acquire it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stream_setup_caller_cancel_releases_stream_permit() {
    let backend = GatedBackend::closed().with_gated_streams();
    let cs = ColdStorage::new(backend.clone(), CancellationToken::new());

    // Fully open read permits for this test (we only care about stream_sem).
    backend.release(usize::MAX >> 4);

    // Seed a block so `stream_logs` can resolve `to` without parking.
    cs.append_block(make_test_block(0)).await.unwrap();

    // Saturate all 8 stream_sem slots. Each spawned task acquires the
    // stream permit inside `stream_logs`, then the produced `LogStream`
    // is dropped — but the BACKEND's `produce_log_stream` task is still
    // holding the permit via `_p = permit` and parked on the stream gate.
    let filter = Filter::new().from_block(0).to_block(0);
    for _ in 0..8 {
        let cs2 = cs.clone();
        let f = filter.clone();
        tokio::spawn(async move {
            let _ = cs2.stream_logs(f, 1000, Duration::from_secs(5)).await;
        });
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    // 9th stream setup must park on `stream_sem`. Cancel it via timeout.
    let cs9 = cs.clone();
    let f9 = filter.clone();
    let attempt = tokio::time::timeout(
        Duration::from_millis(200),
        cs9.stream_logs(f9, 1000, Duration::from_secs(5)),
    )
    .await;
    assert!(attempt.is_err(), "stream_logs should park on saturated stream_sem");

    // Release one stream so a permit becomes available.
    backend.release_streams(1);
    tokio::time::sleep(Duration::from_millis(50)).await;

    // A fresh stream_logs call should acquire within the bound.
    let cs10 = cs.clone();
    tokio::time::timeout(
        Duration::from_secs(2),
        cs10.stream_logs(filter, 1000, Duration::from_secs(5)),
    )
    .await
    .expect("stream_logs should acquire after dropped attempt released permit")
    .expect("stream_logs returned error");
}

/// 7. Cache invalidation on destructive writes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cache_consistent_through_truncate() {
    let cs = ColdStorage::new(MemColdBackend::new(), CancellationToken::new());

    for n in 0..=20 {
        cs.append_block(make_test_block(n)).await.unwrap();
    }

    // Seed the cache with block 10.
    let h = cs.get_header(HeaderSpecifier::Number(10)).await.unwrap();
    assert!(h.is_some(), "block 10 should be present after append");

    // Destructive write above block 5.
    cs.truncate_above(5).await.unwrap();

    // Block 10 must now be absent: cache invalidated AND backend dropped it.
    let h = cs.get_header(HeaderSpecifier::Number(10)).await.unwrap();
    assert!(h.is_none(), "block 10 must be absent after truncate_above(5)");
}
