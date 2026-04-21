//! Concurrency scenarios for the unified `ColdStorage` handle.
//!
//! Per-task tests cover narrow cases (`drain_barrier.rs`,
//! `stream_isolation.rs`, `shutdown.rs`, `handle_shape.rs`); this file
//! collects the broader load scenarios from the architecture spec.

mod common;

use alloy::rpc::types::Filter;
use common::gated::GatedBackend;
use signet_cold::{
    ColdStorage, ColdStorageError, HeaderSpecifier, conformance::make_test_block,
    mem::MemColdBackend,
};
use std::{sync::Arc, time::Duration};
use tokio::sync::Notify;
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
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fairness_write_serves_before_later_readers() {
    let backend = GatedBackend::closed();
    let cs = ColdStorage::new(backend.clone(), CancellationToken::new());

    // Saturate all 64 read permits behind the backend gate.
    for _ in 0..64 {
        let cs2 = cs.clone();
        tokio::spawn(async move {
            let _ = cs2.get_latest_block().await;
        });
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Queue a writer. It holds `write_sem`, then blocks on the drain
    // barrier waiting for the 64 in-flight readers.
    let writer_done = Arc::new(Notify::new());
    let wd = writer_done.clone();
    let cs_w = cs.clone();
    tokio::spawn(async move {
        let _ = cs_w.truncate_above(0).await;
        wd.notify_one();
    });
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Queue 64 "later" readers. They park on `read_sem::acquire_owned`
    // because the writer's drain has claimed every permit.
    let later_done = Arc::new(Notify::new());
    let mut later_count = 0usize;
    for _ in 0..64 {
        let cs2 = cs.clone();
        let ld = later_done.clone();
        tokio::spawn(async move {
            let _ = cs2.get_latest_block().await;
            ld.notify_one();
        });
        later_count += 1;
    }
    assert_eq!(later_count, 64);
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Release the backend gate so the 64 saturating readers complete,
    // which lets the drain barrier acquire and the writer run.
    backend.release(usize::MAX >> 4);

    // The writer MUST resolve before any later reader.
    tokio::select! {
        biased;
        () = writer_done.notified() => {}
        () = later_done.notified() => panic!("later reader resolved before writer"),
    }

    // And the later readers still complete shortly after.
    tokio::time::timeout(Duration::from_secs(5), later_done.notified())
        .await
        .expect("later readers should complete after writer");
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
