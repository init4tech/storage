use signet_cold::{ColdStorage, mem::MemColdBackend};
use tokio_util::sync::CancellationToken;

#[tokio::test]
async fn handle_is_clone_cheap() {
    let cs: ColdStorage<MemColdBackend> =
        ColdStorage::new(MemColdBackend::new(), CancellationToken::new());
    let cs2 = cs.clone();
    let _ = cs.truncate_above(0).await;
    let latest = cs2.get_latest_block().await.unwrap();
    assert!(latest.is_none());
}
