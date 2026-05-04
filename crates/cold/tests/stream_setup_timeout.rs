//! `stream_logs` setup must fail fast when the backend's
//! `get_latest_block` call hangs.
//!
//! Pre-fix: `stream_logs` resolved `to = latest` via an un-bounded
//! backend call before acquiring `stream_sem`. A stuck point lookup
//! (cold MDBX page, saturated PG pool) could stall N concurrent setup
//! callers indefinitely with no permit cap. The fix wraps the setup
//! read in `tokio::time::timeout(backend.read_timeout(), …)`. This
//! test pins that behaviour against a regression.

mod common;

use alloy::rpc::types::Filter;
use common::gated::GatedBackend;
use signet_cold::{ColdStorage, ColdStorageError};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stream_setup_fails_fast_when_get_latest_block_hangs() {
    // Backend whose read gate is closed: every read parks forever,
    // including `get_latest_block`. Advertise a 50 ms `read_timeout`
    // so the handle's setup wrap-around expires quickly.
    let backend = GatedBackend::closed().with_read_timeout(Duration::from_millis(50));
    let cs = ColdStorage::new(backend, CancellationToken::new());

    // No `to_block` on the filter forces the handle to call
    // `get_latest_block` during setup — exactly the path the timeout
    // protects.
    let filter = Filter::new().from_block(0);

    let started = std::time::Instant::now();
    let outcome = tokio::time::timeout(
        Duration::from_secs(1),
        cs.stream_logs(filter, 1000, Duration::from_secs(5)),
    )
    .await
    .expect("stream_logs setup must not hang past the wall-clock timeout");
    let elapsed = started.elapsed();

    let err = outcome.expect_err("setup must fail with DeadlineExceeded, not succeed");
    assert!(
        matches!(err, ColdStorageError::DeadlineExceeded(_)),
        "expected DeadlineExceeded, got: {err}"
    );
    // Generous upper bound — the configured deadline is 50 ms and
    // tokio's timer wheel adds slop. We mostly care that we are
    // bounded by the configured value, not by a 1 s outer harness.
    assert!(elapsed < Duration::from_millis(500), "setup took too long: {elapsed:?}");
}
