//! Shutdown coordinator closes semaphores on cancel; handle methods
//! fail fast afterwards.

use signet_cold::{ColdStorage, ColdStorageError, mem::MemColdBackend};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

#[tokio::test]
async fn shutdown_acquire_fails_fast() {
    let cancel = CancellationToken::new();
    let cs = ColdStorage::new(MemColdBackend::new(), cancel.clone());

    cancel.cancel();
    // Give the coordinator a tick to run.
    tokio::time::sleep(Duration::from_millis(50)).await;

    let err = cs.get_latest_block().await.unwrap_err();
    assert!(matches!(err, ColdStorageError::TaskTerminated));
}

#[tokio::test]
async fn shutdown_write_fails_fast() {
    let cancel = CancellationToken::new();
    let cs = ColdStorage::new(MemColdBackend::new(), cancel.clone());

    cancel.cancel();
    tokio::time::sleep(Duration::from_millis(50)).await;

    let err = cs.truncate_above(0).await.unwrap_err();
    assert!(matches!(err, ColdStorageError::TaskTerminated));
}
