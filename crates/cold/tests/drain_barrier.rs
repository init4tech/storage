//! Writes must wait for in-flight reads to drain.

mod common;

use common::gated::GatedBackend;
use signet_cold::ColdStorage;
use std::{sync::Arc, time::Duration};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn write_waits_for_in_flight_reads_to_drain() {
    let backend = GatedBackend::closed();
    let cs = ColdStorage::new(backend.clone(), CancellationToken::new());

    // 16 gated readers — hold 16 of the 64 read permits.
    for _ in 0..16 {
        let cs2 = cs.clone();
        tokio::spawn(async move {
            let _ = cs2.get_latest_block().await;
        });
    }
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Writer should block on drain: only 48 of 64 permits are free.
    let cs2 = cs.clone();
    let done = Arc::new(Notify::new());
    let wd = done.clone();
    tokio::spawn(async move {
        let _ = cs2.truncate_above(0).await;
        wd.notify_one();
    });

    // Writer must NOT complete before readers are released.
    let early = tokio::time::timeout(Duration::from_millis(150), done.notified()).await;
    assert!(early.is_err(), "writer completed before read drain");

    // Release all readers; writer should now progress.
    backend.release(usize::MAX >> 4);
    tokio::time::timeout(Duration::from_secs(2), done.notified())
        .await
        .expect("writer should complete once readers release");
}
