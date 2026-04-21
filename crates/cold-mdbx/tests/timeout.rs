//! Read timeouts must trip when an iterator exceeds the configured budget.

use signet_cold::{ColdStorageRead, SignetEventsSpecifier};
use signet_cold_mdbx::MdbxColdBackend;
use std::time::Duration;
use tempfile::TempDir;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn iterator_read_timeout_trips_on_huge_range() {
    let dir = TempDir::new().unwrap();
    let backend =
        MdbxColdBackend::open_rw(dir.path()).unwrap().with_read_timeout(Duration::from_nanos(1));

    let res = backend
        .get_signet_events(SignetEventsSpecifier::BlockRange { start: 0, end: 1_000_000 })
        .await;

    let err = res.expect_err("should time out");
    let msg = err.to_string();
    assert!(
        msg.contains("timed out") || msg.contains("Timeout"),
        "expected timeout in error: {msg}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn point_lookup_does_not_check_deadline() {
    let dir = TempDir::new().unwrap();
    let backend =
        MdbxColdBackend::open_rw(dir.path()).unwrap().with_read_timeout(Duration::from_nanos(1));

    let res = backend.get_latest_block().await.unwrap();
    assert!(res.is_none());
}
