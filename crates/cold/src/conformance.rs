//! Conformance tests for ColdStorage backends.
//!
//! These tests verify that any backend implementation behaves correctly
//! according to the ColdStorage trait contract. To use these tests with
//! a custom backend, call the test functions with your backend instance.

use crate::{
    BlockData, ColdResult, ColdStorage, HeaderSpecifier, ReceiptSpecifier, TransactionSpecifier,
};
use alloy::{
    consensus::{Header, Receipt as AlloyReceipt, Signed, TxLegacy},
    primitives::{B256, BlockNumber, Signature, TxKind, U256},
};
use signet_storage_types::{Receipt, TransactionSigned};

/// Run all conformance tests against a backend.
///
/// This is the main entry point for testing a custom backend implementation.
pub async fn conformance<B: ColdStorage>(backend: &B) -> ColdResult<()> {
    test_empty_storage(backend).await?;
    test_append_and_read_header(backend).await?;
    test_header_hash_lookup(backend).await?;
    test_transaction_lookups(backend).await?;
    test_receipt_lookups(backend).await?;
    test_confirmation_metadata(backend).await?;
    test_truncation(backend).await?;
    test_batch_append(backend).await?;
    test_latest_block_tracking(backend).await?;
    test_get_receipt_with_context(backend).await?;
    Ok(())
}

/// Create test block data for conformance tests.
///
/// Creates a minimal valid block with the given block number.
pub fn make_test_block(block_number: BlockNumber) -> BlockData {
    let header = Header { number: block_number, ..Default::default() };

    BlockData::new(header, vec![], vec![], vec![], None)
}

/// Create a test transaction with a unique nonce.
fn make_test_tx(nonce: u64) -> TransactionSigned {
    let tx = TxLegacy { nonce, to: TxKind::Call(Default::default()), ..Default::default() };
    let sig = Signature::new(U256::from(nonce + 1), U256::from(nonce + 2), false);
    alloy::consensus::EthereumTxEnvelope::Legacy(Signed::new_unhashed(tx, sig))
}

/// Create a test receipt.
fn make_test_receipt() -> Receipt {
    Receipt {
        inner: AlloyReceipt { status: true.into(), ..Default::default() },
        ..Default::default()
    }
}

/// Create test block data with transactions and receipts.
fn make_test_block_with_txs(block_number: BlockNumber, tx_count: usize) -> BlockData {
    let header = Header { number: block_number, ..Default::default() };
    let transactions: Vec<_> =
        (0..tx_count).map(|i| make_test_tx(block_number * 100 + i as u64)).collect();
    let receipts: Vec<_> = (0..tx_count).map(|_| make_test_receipt()).collect();
    BlockData::new(header, transactions, receipts, vec![], None)
}

/// Test that empty storage returns None/empty for all lookups.
pub async fn test_empty_storage<B: ColdStorage>(backend: &B) -> ColdResult<()> {
    assert!(backend.get_header(HeaderSpecifier::Number(0)).await?.is_none());
    assert!(backend.get_header(HeaderSpecifier::Hash(B256::ZERO)).await?.is_none());
    assert!(backend.get_latest_block().await?.is_none());
    assert!(backend.get_transactions_in_block(0).await?.is_empty());
    assert!(backend.get_receipts_in_block(0).await?.is_empty());
    assert_eq!(backend.get_transaction_count(0).await?, 0);
    Ok(())
}

/// Test basic append and read for headers.
pub async fn test_append_and_read_header<B: ColdStorage>(backend: &B) -> ColdResult<()> {
    let block_data = make_test_block(100);
    let expected_header = block_data.header.clone();

    backend.append_block(block_data).await?;

    let retrieved = backend.get_header(HeaderSpecifier::Number(100)).await?;
    assert!(retrieved.is_some());
    assert_eq!(retrieved.unwrap(), expected_header);

    Ok(())
}

/// Test header lookup by hash.
pub async fn test_header_hash_lookup<B: ColdStorage>(backend: &B) -> ColdResult<()> {
    let block_data = make_test_block(101);
    let header_hash = block_data.header.hash_slow();

    backend.append_block(block_data).await?;

    let retrieved = backend.get_header(HeaderSpecifier::Hash(header_hash)).await?;
    assert!(retrieved.is_some());

    // Non-existent hash should return None
    let missing = backend.get_header(HeaderSpecifier::Hash(B256::ZERO)).await?;
    assert!(missing.is_none());

    Ok(())
}

/// Test transaction lookups by hash and by block+index.
pub async fn test_transaction_lookups<B: ColdStorage>(backend: &B) -> ColdResult<()> {
    let block_data = make_test_block(200);

    backend.append_block(block_data).await?;

    let txs = backend.get_transactions_in_block(200).await?;
    let count = backend.get_transaction_count(200).await?;
    assert_eq!(txs.len() as u64, count);

    Ok(())
}

/// Test receipt lookups.
pub async fn test_receipt_lookups<B: ColdStorage>(backend: &B) -> ColdResult<()> {
    let block_data = make_test_block(201);

    backend.append_block(block_data).await?;

    let receipts = backend.get_receipts_in_block(201).await?;
    assert!(receipts.is_empty());

    Ok(())
}

/// Test that transaction and receipt lookups return correct confirmation
/// metadata (block number, block hash, transaction index).
pub async fn test_confirmation_metadata<B: ColdStorage>(backend: &B) -> ColdResult<()> {
    let block = make_test_block_with_txs(600, 3);
    let expected_hash = block.header.hash_slow();
    let tx_hashes: Vec<_> = block.transactions.iter().map(|tx| *tx.tx_hash()).collect();

    backend.append_block(block).await?;

    // Verify transaction metadata via hash lookup
    for (idx, tx_hash) in tx_hashes.iter().enumerate() {
        let confirmed =
            backend.get_transaction(TransactionSpecifier::Hash(*tx_hash)).await?.unwrap();
        assert_eq!(confirmed.meta().block_number(), 600);
        assert_eq!(confirmed.meta().block_hash(), expected_hash);
        assert_eq!(confirmed.meta().transaction_index(), idx as u64);
    }

    // Verify transaction metadata via block+index lookup
    let confirmed = backend
        .get_transaction(TransactionSpecifier::BlockAndIndex { block: 600, index: 1 })
        .await?
        .unwrap();
    assert_eq!(confirmed.meta().block_number(), 600);
    assert_eq!(confirmed.meta().block_hash(), expected_hash);
    assert_eq!(confirmed.meta().transaction_index(), 1);

    // Verify transaction metadata via block_hash+index lookup
    let confirmed = backend
        .get_transaction(TransactionSpecifier::BlockHashAndIndex {
            block_hash: expected_hash,
            index: 2,
        })
        .await?
        .unwrap();
    assert_eq!(confirmed.meta().block_number(), 600);
    assert_eq!(confirmed.meta().transaction_index(), 2);

    // Verify receipt metadata via tx hash lookup
    let confirmed =
        backend.get_receipt(crate::ReceiptSpecifier::TxHash(tx_hashes[0])).await?.unwrap();
    assert_eq!(confirmed.meta().block_number(), 600);
    assert_eq!(confirmed.meta().block_hash(), expected_hash);
    assert_eq!(confirmed.meta().transaction_index(), 0);

    // Verify receipt metadata via block+index lookup
    let confirmed = backend
        .get_receipt(crate::ReceiptSpecifier::BlockAndIndex { block: 600, index: 2 })
        .await?
        .unwrap();
    assert_eq!(confirmed.meta().block_number(), 600);
    assert_eq!(confirmed.meta().transaction_index(), 2);

    // Non-existent lookups return None
    assert!(backend.get_transaction(TransactionSpecifier::Hash(B256::ZERO)).await?.is_none());
    assert!(backend.get_receipt(crate::ReceiptSpecifier::TxHash(B256::ZERO)).await?.is_none());

    Ok(())
}

/// Test truncation removes data correctly.
pub async fn test_truncation<B: ColdStorage>(backend: &B) -> ColdResult<()> {
    // Append blocks 300, 301, 302
    backend.append_block(make_test_block(300)).await?;
    backend.append_block(make_test_block(301)).await?;
    backend.append_block(make_test_block(302)).await?;

    // Truncate above 300 (removes 301, 302)
    backend.truncate_above(300).await?;

    // Block 300 should still exist
    assert!(backend.get_header(HeaderSpecifier::Number(300)).await?.is_some());

    // Blocks 301, 302 should be gone
    assert!(backend.get_header(HeaderSpecifier::Number(301)).await?.is_none());
    assert!(backend.get_header(HeaderSpecifier::Number(302)).await?.is_none());

    // Latest should now be 300
    assert_eq!(backend.get_latest_block().await?, Some(300));

    Ok(())
}

/// Test batch append.
pub async fn test_batch_append<B: ColdStorage>(backend: &B) -> ColdResult<()> {
    let blocks = vec![make_test_block(400), make_test_block(401), make_test_block(402)];

    backend.append_blocks(blocks).await?;

    assert!(backend.get_header(HeaderSpecifier::Number(400)).await?.is_some());
    assert!(backend.get_header(HeaderSpecifier::Number(401)).await?.is_some());
    assert!(backend.get_header(HeaderSpecifier::Number(402)).await?.is_some());

    Ok(())
}

/// Test latest block tracking.
pub async fn test_latest_block_tracking<B: ColdStorage>(backend: &B) -> ColdResult<()> {
    // Append out of order
    backend.append_block(make_test_block(502)).await?;
    assert_eq!(backend.get_latest_block().await?, Some(502));

    backend.append_block(make_test_block(500)).await?;
    // Latest should still be 502
    assert_eq!(backend.get_latest_block().await?, Some(502));

    backend.append_block(make_test_block(505)).await?;
    assert_eq!(backend.get_latest_block().await?, Some(505));

    Ok(())
}

/// Test get_receipt_with_context returns complete receipt context.
pub async fn test_get_receipt_with_context<B: ColdStorage>(backend: &B) -> ColdResult<()> {
    let block = make_test_block_with_txs(700, 3);
    let expected_header = block.header.clone();
    let tx_hash = *block.transactions[1].tx_hash();

    backend.append_block(block).await?;

    // Lookup by block+index
    let ctx = backend
        .get_receipt_with_context(ReceiptSpecifier::BlockAndIndex { block: 700, index: 1 })
        .await?
        .unwrap();
    assert_eq!(ctx.header, expected_header);
    assert_eq!(ctx.receipt.meta().block_number(), 700);
    assert_eq!(ctx.receipt.meta().transaction_index(), 1);

    // prior_cumulative_gas should equal receipt[0].cumulative_gas_used
    let first = backend
        .get_receipt_with_context(ReceiptSpecifier::BlockAndIndex { block: 700, index: 0 })
        .await?
        .unwrap();
    assert_eq!(first.prior_cumulative_gas, 0);
    assert_eq!(ctx.prior_cumulative_gas, first.receipt.inner().inner.cumulative_gas_used);

    // Lookup by tx hash
    let by_hash =
        backend.get_receipt_with_context(ReceiptSpecifier::TxHash(tx_hash)).await?.unwrap();
    assert_eq!(by_hash.receipt.meta().transaction_index(), 1);

    // Non-existent returns None
    assert!(
        backend
            .get_receipt_with_context(ReceiptSpecifier::BlockAndIndex { block: 999, index: 0 })
            .await?
            .is_none()
    );

    Ok(())
}
