//! Integration tests for [`UnifiedStorage`].

use alloy::{
    consensus::{Header, Sealable, Signed, TxLegacy, transaction::Recovered},
    primitives::{Address, B256, Signature, TxKind, U256},
};
use signet_cold::{ColdStorage, HeaderSpecifier, mem::MemColdBackend};
use signet_hot::{HistoryRead, HistoryWrite, HotKv, mem::MemKv, model::HotKvWrite};
use signet_storage::UnifiedStorage;
use signet_storage_types::{
    ExecutedBlock, ExecutedBlockBuilder, Receipt, RecoveredTx, SealedHeader, TransactionSigned,
};
use tokio_util::sync::CancellationToken;
use trevm::revm::database::BundleState;

/// Build a chain of blocks with valid parent hash linkage.
fn make_chain(count: u64) -> Vec<ExecutedBlock> {
    let mut blocks = Vec::with_capacity(count as usize);
    let mut parent_hash = B256::ZERO;

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

/// Create a test transaction with a unique nonce.
fn make_test_tx(nonce: u64) -> RecoveredTx {
    let tx = TxLegacy { nonce, to: TxKind::Call(Default::default()), ..Default::default() };
    let sig = Signature::new(U256::from(nonce + 1), U256::from(nonce + 2), false);
    let signed: TransactionSigned =
        alloy::consensus::EthereumTxEnvelope::Legacy(Signed::new_unhashed(tx, sig));
    let sender = Address::with_last_byte(nonce as u8);
    Recovered::new_unchecked(signed, sender)
}

/// Create a test receipt.
fn make_test_receipt() -> Receipt {
    use alloy::consensus::Receipt as AlloyReceipt;
    Receipt {
        inner: AlloyReceipt { status: true.into(), ..Default::default() },
        ..Default::default()
    }
}

/// Build a chain of blocks with transactions and receipts.
fn make_chain_with_txs(count: u64, tx_count: usize) -> Vec<ExecutedBlock> {
    let mut blocks = Vec::with_capacity(count as usize);
    let mut parent_hash = B256::ZERO;

    for number in 0..count {
        let header = Header { number, parent_hash, ..Default::default() };
        let sealed: SealedHeader = header.seal_slow();
        parent_hash = sealed.hash();
        let transactions: Vec<RecoveredTx> =
            (0..tx_count).map(|i| make_test_tx(number * 100 + i as u64)).collect();
        let receipts: Vec<Receipt> = (0..tx_count).map(|_| make_test_receipt()).collect();
        let block = ExecutedBlockBuilder::new()
            .header(sealed)
            .bundle(BundleState::default())
            .transactions(transactions)
            .receipts(receipts)
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
    let cold_handle = ColdStorage::new(MemColdBackend::new(), cancel.clone());

    let storage = UnifiedStorage::new(hot.clone(), cold_handle.clone());

    // Append a block
    let blocks = make_chain(1);
    let expected_hash = blocks[0].header.hash();
    storage.append_blocks(blocks).await.unwrap();

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
    let cold_handle = ColdStorage::new(MemColdBackend::new(), cancel.clone());

    let storage = UnifiedStorage::new(hot.clone(), cold_handle.clone());

    // Append blocks 0, 1, 2
    storage.append_blocks(make_chain(3)).await.unwrap();

    // Verify tip is at block 2
    assert_eq!(hot.reader().unwrap().get_chain_tip().unwrap().unwrap().0, 2);

    // Unwind above block 0
    storage.unwind_above(0).await.unwrap();

    // Hot tip should be block 0
    assert_eq!(hot.reader().unwrap().get_chain_tip().unwrap().unwrap().0, 0);

    cancel.cancel();
}

#[tokio::test]
async fn drain_above_returns_headers_and_receipts() {
    let hot = MemKv::new();
    let cancel = CancellationToken::new();
    let cold_handle = ColdStorage::new(MemColdBackend::new(), cancel.clone());
    let storage = UnifiedStorage::new(hot.clone(), cold_handle);

    // Append 3 blocks (0, 1, 2) with 2 txs each
    let blocks = make_chain_with_txs(3, 2);
    storage.append_blocks(blocks).await.unwrap();

    // drain_above(0) — returns blocks 1 and 2
    let drained = storage.drain_above(0).await.unwrap();
    assert_eq!(drained.len(), 2);
    assert_eq!(drained[0].header.number, 1);
    assert_eq!(drained[1].header.number, 2);
    assert_eq!(drained[0].receipts.len(), 2);
    assert_eq!(drained[1].receipts.len(), 2);

    // Verify hot tip is now block 0
    assert_eq!(hot.reader().unwrap().get_chain_tip().unwrap().unwrap().0, 0);
    assert_eq!(storage.cold().get_latest_block().await.unwrap().unwrap(), 0);

    cancel.cancel();
}

#[tokio::test]
async fn drain_above_empty_when_at_tip() {
    let hot = MemKv::new();
    let cancel = CancellationToken::new();
    let cold_handle = ColdStorage::new(MemColdBackend::new(), cancel.clone());
    let storage = UnifiedStorage::new(hot.clone(), cold_handle);

    // Append 2 blocks (0, 1)
    storage.append_blocks(make_chain_with_txs(2, 1)).await.unwrap();

    // drain_above(1) — nothing above tip
    let drained = storage.drain_above(1).await.unwrap();
    assert!(drained.is_empty());

    // Verify hot tip still 1
    assert_eq!(hot.reader().unwrap().get_chain_tip().unwrap().unwrap().0, 1);
    assert_eq!(storage.cold().get_latest_block().await.unwrap().unwrap(), 1);

    cancel.cancel();
}

#[tokio::test]
async fn drain_above_cold_lag() {
    let hot = MemKv::new();
    let cancel = CancellationToken::new();
    let cold_handle = ColdStorage::new(MemColdBackend::new(), cancel.clone());
    let storage = UnifiedStorage::new(hot.clone(), cold_handle);

    // Write 3 blocks with txs directly to hot — skips cold entirely
    let blocks = make_chain_with_txs(3, 2);
    let writer = hot.writer().unwrap();
    writer.append_blocks(blocks.iter().map(|b| (&b.header, &b.bundle))).unwrap();
    writer.raw_commit().unwrap();

    // drain_above(0) — returns blocks 1 and 2
    let drained = storage.drain_above(0).await.unwrap();
    assert_eq!(drained.len(), 2);
    assert_eq!(drained[0].header.number, 1);
    assert_eq!(drained[1].header.number, 2);
    // Receipts should be empty since cold storage was skipped
    assert!(drained[0].receipts.is_empty());
    assert!(drained[1].receipts.is_empty());

    cancel.cancel();
}
