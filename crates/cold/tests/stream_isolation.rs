//! Streams must not consume a read permit.
//!
//! If a saturated read pool could block stream setup, the existing
//! PR #57 bug would reappear. This test asserts the fix.

mod common;

use alloy::rpc::types::Filter;
use common::gated::GatedBackend;
use signet_cold::ColdStorage;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stream_setup_not_blocked_by_saturated_reads() {
    let backend = GatedBackend::closed();
    let cs = ColdStorage::new(backend.clone(), CancellationToken::new());

    // Saturate all 64 read permits. Each spawned task acquires a read
    // permit inside the handle, then parks in the backend behind the
    // gate. With the gate closed, these tasks hold their permits for
    // the duration of the test.
    for _ in 0..64 {
        let cs2 = cs.clone();
        tokio::spawn(async move {
            let _ = cs2.get_latest_block().await;
        });
    }
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Stream setup must complete even though every read permit is held.
    // Use a bounded `to_block` so setup does not need to resolve
    // "latest" via the backend (which our gate also blocks). What we
    // are asserting is that setup does not acquire a `read_sem` permit;
    // the stream body itself runs after setup returns.
    let stream = tokio::time::timeout(
        Duration::from_secs(1),
        cs.stream_logs(Filter::new().from_block(0).to_block(10), 1000, Duration::from_secs(5)),
    )
    .await
    .expect("stream setup must not block on saturated read pool")
    .expect("stream setup must succeed");

    drop(stream);
}
