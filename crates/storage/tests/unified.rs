//! Integration tests for [`UnifiedStorage`].

use alloy::consensus::{Header, Sealable};
use signet_cold::{ColdStorageTask, HeaderSpecifier, mem::MemColdBackend};
use signet_hot::{HistoryRead, HotKv, mem::MemKv};
use signet_storage::UnifiedStorage;
use signet_storage_types::{ExecutedBlock, ExecutedBlockBuilder, SealedHeader};
use tokio_util::sync::CancellationToken;
use trevm::revm::database::BundleState;

/// Build a chain of blocks with valid parent hash linkage.
fn make_chain(count: u64) -> Vec<ExecutedBlock> {
    let mut blocks = Vec::with_capacity(count as usize);
    let mut parent_hash = alloy::primitives::B256::ZERO;

    for number in 0..count {
        let header = Header { number, parent_hash, ..Default::default() };
        let sealed: SealedHeader = header.seal_slow();
        parent_hash = sealed.hash();
        let block = ExecutedBlockBuilder::new()
            .header(sealed)
            .bundle(BundleState::default())
            .build()
            .unwrap();
        blocks.push(block);
    }
    blocks
}

#[tokio::test]
async fn append_and_read_back() {
    let hot = MemKv::new();
    let cancel = CancellationToken::new();
    let cold_handle = ColdStorageTask::spawn(MemColdBackend::new(), cancel.clone());

    let storage = UnifiedStorage::new(hot.clone(), cold_handle.clone());

    // Append a block
    let blocks = make_chain(1);
    let expected_hash = blocks[0].header.hash();
    storage.append_blocks(blocks).unwrap();

    // Verify hot storage has the block
    let reader = hot.reader().unwrap();
    let tip = reader.get_chain_tip().unwrap();
    assert_eq!(tip, Some((0, expected_hash)));

    // Verify cold storage has the header
    let header = cold_handle.get_header(HeaderSpecifier::Number(0)).await.unwrap();
    assert!(header.is_some());

    cancel.cancel();
}

#[tokio::test]
async fn append_multiple_and_unwind() {
    let hot = MemKv::new();
    let cancel = CancellationToken::new();
    let cold_handle = ColdStorageTask::spawn(MemColdBackend::new(), cancel.clone());

    let storage = UnifiedStorage::new(hot.clone(), cold_handle.clone());

    // Append blocks 0, 1, 2
    storage.append_blocks(make_chain(3)).unwrap();

    // Verify tip is at block 2
    assert_eq!(hot.reader().unwrap().get_chain_tip().unwrap().unwrap().0, 2);

    // Unwind above block 0
    storage.unwind_above(0).unwrap();

    // Hot tip should be block 0
    assert_eq!(hot.reader().unwrap().get_chain_tip().unwrap().unwrap().0, 0);

    cancel.cancel();
}
