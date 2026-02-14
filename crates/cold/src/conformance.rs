//! Conformance tests for ColdStorage backends.
//!
//! These tests verify that any backend implementation behaves correctly
//! according to the ColdStorage trait contract. To use these tests with
//! a custom backend, call the test functions with your backend instance.

use crate::{
    BlockData, ColdResult, ColdStorage, Filter, HeaderSpecifier, ReceiptSpecifier,
    TransactionSpecifier,
};
use alloy::{
    consensus::transaction::Recovered,
    consensus::{Header, Receipt as AlloyReceipt, Sealable, Signed, TxLegacy},
    primitives::{
        Address, B256, BlockNumber, Bytes, Log, LogData, Signature, TxKind, U256, address,
    },
};
use signet_storage_types::{Receipt, RecoveredTx, TransactionSigned};

/// Run all conformance tests against a backend.
///
/// This is the main entry point for testing a custom backend implementation.
pub async fn conformance<B: ColdStorage>(backend: &B) -> ColdResult<()> {
    test_empty_storage(backend).await?;
    test_append_and_read_header(backend).await?;
    test_header_hash_lookup(backend).await?;
    test_transaction_lookups(backend).await?;
    test_receipt_lookups(backend).await?;
    test_truncation(backend).await?;
    test_batch_append(backend).await?;
    test_confirmation_metadata(backend).await?;
    test_cold_receipt_metadata(backend).await?;
    test_get_logs(backend).await?;
    Ok(())
}

/// Create test block data for conformance tests.
///
/// Creates a minimal valid block with the given block number.
pub fn make_test_block(block_number: BlockNumber) -> BlockData {
    let header = Header { number: block_number, ..Default::default() };

    BlockData::new(header.seal_slow(), vec![], vec![], vec![], None)
}

/// Create a test transaction with a unique nonce and deterministic sender.
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
    Receipt {
        inner: AlloyReceipt { status: true.into(), ..Default::default() },
        ..Default::default()
    }
}

/// Create a test receipt with the given number of logs.
fn make_test_receipt_with_logs(log_count: usize, cumulative_gas: u64) -> Receipt {
    let logs = (0..log_count)
        .map(|_| {
            Log::new_unchecked(
                address!("0x0000000000000000000000000000000000000001"),
                vec![],
                Bytes::new(),
            )
        })
        .collect();
    Receipt {
        inner: AlloyReceipt { status: true.into(), cumulative_gas_used: cumulative_gas, logs },
        ..Default::default()
    }
}

/// Create test block data with transactions and receipts.
fn make_test_block_with_txs(block_number: BlockNumber, tx_count: usize) -> BlockData {
    let header = Header { number: block_number, ..Default::default() };
    let transactions: Vec<RecoveredTx> =
        (0..tx_count).map(|i| make_test_tx(block_number * 100 + i as u64)).collect();
    let receipts: Vec<_> = (0..tx_count).map(|_| make_test_receipt()).collect();
    BlockData::new(header.seal_slow(), transactions, receipts, vec![], None)
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
    assert_eq!(retrieved.unwrap().hash(), expected_header.hash());

    Ok(())
}

/// Test header lookup by hash.
pub async fn test_header_hash_lookup<B: ColdStorage>(backend: &B) -> ColdResult<()> {
    let block_data = make_test_block(101);
    let header_hash = block_data.header.hash();

    backend.append_block(block_data).await?;

    let retrieved = backend.get_header(HeaderSpecifier::Hash(header_hash)).await?;
    assert!(retrieved.is_some());
    assert_eq!(retrieved.unwrap().hash(), header_hash);

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

/// Test that transaction and receipt lookups return correct metadata.
pub async fn test_confirmation_metadata<B: ColdStorage>(backend: &B) -> ColdResult<()> {
    let block = make_test_block_with_txs(600, 3);
    let expected_hash = block.header.hash();
    let tx_hashes: Vec<_> = block.transactions.iter().map(|tx| *tx.tx_hash()).collect();
    let expected_senders: Vec<_> = block.transactions.iter().map(|tx| *tx.signer()).collect();

    backend.append_block(block).await?;

    // Verify transaction metadata via hash lookup
    for (idx, tx_hash) in tx_hashes.iter().enumerate() {
        let confirmed =
            backend.get_transaction(TransactionSpecifier::Hash(*tx_hash)).await?.unwrap();
        assert_eq!(confirmed.meta().block_number(), 600);
        assert_eq!(confirmed.meta().block_hash(), expected_hash);
        assert_eq!(confirmed.meta().transaction_index(), idx as u64);
        assert_eq!(*confirmed.inner().signer(), expected_senders[idx]);
    }

    // Verify transaction metadata via block+index lookup
    let confirmed = backend
        .get_transaction(TransactionSpecifier::BlockAndIndex { block: 600, index: 1 })
        .await?
        .unwrap();
    assert_eq!(confirmed.meta().block_number(), 600);
    assert_eq!(confirmed.meta().block_hash(), expected_hash);
    assert_eq!(confirmed.meta().transaction_index(), 1);
    assert_eq!(*confirmed.inner().signer(), expected_senders[1]);

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
    assert_eq!(*confirmed.inner().signer(), expected_senders[2]);

    // Verify receipt metadata via tx hash lookup
    let cold_receipt = backend.get_receipt(ReceiptSpecifier::TxHash(tx_hashes[0])).await?.unwrap();
    assert_eq!(cold_receipt.block_number, 600);
    assert_eq!(cold_receipt.block_hash, expected_hash);
    assert_eq!(cold_receipt.transaction_index, 0);

    // Verify receipt metadata via block+index lookup
    let cold_receipt = backend
        .get_receipt(ReceiptSpecifier::BlockAndIndex { block: 600, index: 2 })
        .await?
        .unwrap();
    assert_eq!(cold_receipt.block_number, 600);
    assert_eq!(cold_receipt.transaction_index, 2);

    // Non-existent lookups return None
    assert!(backend.get_transaction(TransactionSpecifier::Hash(B256::ZERO)).await?.is_none());
    assert!(backend.get_receipt(ReceiptSpecifier::TxHash(B256::ZERO)).await?.is_none());

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

/// Test ColdReceipt metadata: gas_used, first_log_index, tx_hash,
/// block_hash, block_number, transaction_index, from.
pub async fn test_cold_receipt_metadata<B: ColdStorage>(backend: &B) -> ColdResult<()> {
    // Block with 3 receipts having 2, 3, and 1 logs respectively.
    let header = Header { number: 700, ..Default::default() };
    let sealed = header.seal_slow();
    let block_hash = sealed.hash();
    let transactions: Vec<RecoveredTx> = (0..3).map(|i| make_test_tx(700 * 100 + i)).collect();
    let tx_hashes: Vec<_> = transactions.iter().map(|t| *t.tx_hash()).collect();
    let expected_senders: Vec<_> = transactions.iter().map(|t| t.signer()).collect();
    let receipts = vec![
        make_test_receipt_with_logs(2, 21000),
        make_test_receipt_with_logs(3, 42000),
        make_test_receipt_with_logs(1, 63000),
    ];
    let block = BlockData::new(sealed, transactions, receipts, vec![], None);

    backend.append_block(block).await?;

    // First receipt: gas_used=21000, first log index=0
    let first = backend
        .get_receipt(ReceiptSpecifier::BlockAndIndex { block: 700, index: 0 })
        .await?
        .unwrap();
    assert_eq!(first.block_number, 700);
    assert_eq!(first.block_hash, block_hash);
    assert_eq!(first.transaction_index, 0);
    assert_eq!(first.tx_hash, tx_hashes[0]);
    assert_eq!(first.from, expected_senders[0]);
    assert_eq!(first.gas_used, 21000);
    assert_eq!(first.receipt.logs[0].log_index, Some(0));
    assert_eq!(first.receipt.logs[1].log_index, Some(1));

    // Second receipt: gas_used=21000, first log index=2
    let second = backend
        .get_receipt(ReceiptSpecifier::BlockAndIndex { block: 700, index: 1 })
        .await?
        .unwrap();
    assert_eq!(second.transaction_index, 1);
    assert_eq!(second.gas_used, 21000);
    assert_eq!(second.receipt.logs[0].log_index, Some(2));
    assert_eq!(second.receipt.logs.len(), 3);

    // Third receipt: gas_used=21000, first log index=5 (2+3)
    let third = backend
        .get_receipt(ReceiptSpecifier::BlockAndIndex { block: 700, index: 2 })
        .await?
        .unwrap();
    assert_eq!(third.transaction_index, 2);
    assert_eq!(third.gas_used, 21000);
    assert_eq!(third.receipt.logs[0].log_index, Some(5));

    // Lookup by tx hash
    let by_hash = backend.get_receipt(ReceiptSpecifier::TxHash(tx_hashes[1])).await?.unwrap();
    assert_eq!(by_hash.transaction_index, 1);
    assert_eq!(by_hash.tx_hash, tx_hashes[1]);
    assert_eq!(by_hash.from, expected_senders[1]);
    assert_eq!(by_hash.receipt.logs[0].log_index, Some(2));

    // Verify gas_used on all receipts via get_receipts_in_block
    let all = backend.get_receipts_in_block(700).await?;
    assert_eq!(all.len(), 3);
    assert_eq!(all[0].gas_used, 21000);
    assert_eq!(all[1].gas_used, 21000);
    assert_eq!(all[2].gas_used, 21000);

    // Non-existent returns None
    assert!(
        backend
            .get_receipt(ReceiptSpecifier::BlockAndIndex { block: 999, index: 0 })
            .await?
            .is_none()
    );

    Ok(())
}

/// Create a test log with the given address and topics.
fn make_test_log(address: Address, topics: Vec<B256>, data: Vec<u8>) -> Log {
    Log { address, data: LogData::new_unchecked(topics, Bytes::from(data)) }
}

/// Create a test receipt from explicit logs.
fn make_receipt_from_logs(logs: Vec<Log>) -> Receipt {
    Receipt {
        inner: AlloyReceipt { status: true.into(), cumulative_gas_used: 21000, logs },
        ..Default::default()
    }
}

/// Create test block data with custom receipts (and matching dummy transactions).
fn make_test_block_with_receipts(block_number: BlockNumber, receipts: Vec<Receipt>) -> BlockData {
    let header =
        Header { number: block_number, timestamp: block_number * 12, ..Default::default() };
    let transactions: Vec<RecoveredTx> =
        (0..receipts.len()).map(|i| make_test_tx(block_number * 100 + i as u64)).collect();
    BlockData::new(header.seal_slow(), transactions, receipts, vec![], None)
}

/// Test get_logs with various filter combinations.
pub async fn test_get_logs<B: ColdStorage>(backend: &B) -> ColdResult<()> {
    let addr_a = Address::with_last_byte(0xAA);
    let addr_b = Address::with_last_byte(0xBB);
    let topic0_transfer = B256::with_last_byte(0x01);
    let topic0_approval = B256::with_last_byte(0x02);
    let topic1_sender = B256::with_last_byte(0x10);
    let topic1_other = B256::with_last_byte(0x11);

    // Block 800: 2 txs, tx0 has 2 logs, tx1 has 1 log
    let receipts_800 = vec![
        make_receipt_from_logs(vec![
            make_test_log(addr_a, vec![topic0_transfer, topic1_sender], vec![1]),
            make_test_log(addr_b, vec![topic0_approval], vec![2]),
        ]),
        make_receipt_from_logs(vec![make_test_log(
            addr_a,
            vec![topic0_transfer, topic1_other],
            vec![3],
        )]),
    ];
    let block_800 = make_test_block_with_receipts(800, receipts_800);
    let block_800_hash = block_800.header.hash();
    let tx0_hash_800 = *block_800.transactions[0].tx_hash();
    let tx1_hash_800 = *block_800.transactions[1].tx_hash();

    // Block 801: 1 tx, 1 log
    let receipts_801 =
        vec![make_receipt_from_logs(vec![make_test_log(addr_b, vec![topic0_transfer], vec![4])])];
    let block_801 = make_test_block_with_receipts(801, receipts_801);

    backend.append_block(block_800).await?;
    backend.append_block(block_801).await?;

    // --- Empty range returns empty ---
    let empty = backend.get_logs(Filter::new().from_block(900).to_block(999), usize::MAX).await?;
    assert!(empty.is_empty());

    // --- All logs in range 800..=801 (no address/topic filter) ---
    let all = backend.get_logs(Filter::new().from_block(800).to_block(801), usize::MAX).await?;
    assert_eq!(all.len(), 4);
    // Verify ordering by (block_number, transaction_index)
    assert_eq!((all[0].block_number, all[0].transaction_index), (Some(800), Some(0)));
    assert_eq!((all[1].block_number, all[1].transaction_index), (Some(800), Some(0)));
    assert_eq!((all[2].block_number, all[2].transaction_index), (Some(800), Some(1)));
    assert_eq!((all[3].block_number, all[3].transaction_index), (Some(801), Some(0)));

    // Verify log_index (absolute position within block)
    // Block 800: tx0 has 2 logs (indices 0,1), tx1 has 1 log (index 2)
    assert_eq!(all[0].log_index, Some(0));
    assert_eq!(all[1].log_index, Some(1));
    assert_eq!(all[2].log_index, Some(2));
    // Block 801: tx0 has 1 log (index 0)
    assert_eq!(all[3].log_index, Some(0));

    // --- Metadata correctness ---
    assert_eq!(all[0].block_hash, Some(block_800_hash));
    assert_eq!(all[0].block_timestamp, Some(800 * 12));
    assert_eq!(all[3].block_timestamp, Some(801 * 12));
    assert_eq!(all[0].transaction_hash, Some(tx0_hash_800));
    assert_eq!(all[2].transaction_hash, Some(tx1_hash_800));

    // --- Block range filtering ---
    let only_800 =
        backend.get_logs(Filter::new().from_block(800).to_block(800), usize::MAX).await?;
    assert_eq!(only_800.len(), 3);

    // --- Single address filter ---
    let addr_a_logs = backend
        .get_logs(Filter::new().from_block(800).to_block(801).address(addr_a), usize::MAX)
        .await?;
    assert_eq!(addr_a_logs.len(), 2);
    assert!(addr_a_logs.iter().all(|l| l.inner.address == addr_a));

    // --- Multi-address filter ---
    let both_addr = backend
        .get_logs(
            Filter::new().from_block(800).to_block(801).address(vec![addr_a, addr_b]),
            usize::MAX,
        )
        .await?;
    assert_eq!(both_addr.len(), 4);

    // --- Topic0 filter ---
    let transfers = backend
        .get_logs(
            Filter::new().from_block(800).to_block(801).event_signature(topic0_transfer),
            usize::MAX,
        )
        .await?;
    assert_eq!(transfers.len(), 3);

    // --- Topic0 multi-value (OR within position) ---
    let transfer_or_approval = backend
        .get_logs(
            Filter::new()
                .from_block(800)
                .to_block(801)
                .event_signature(vec![topic0_transfer, topic0_approval]),
            usize::MAX,
        )
        .await?;
    assert_eq!(transfer_or_approval.len(), 4);

    // --- Multi-topic: topic0 AND topic1 ---
    let specific = backend
        .get_logs(
            Filter::new()
                .from_block(800)
                .to_block(801)
                .event_signature(topic0_transfer)
                .topic1(topic1_sender),
            usize::MAX,
        )
        .await?;
    assert_eq!(specific.len(), 1);
    assert_eq!(specific[0].inner.address, addr_a);

    // --- Topic1 filter with topic0 wildcard ---
    let by_sender = backend
        .get_logs(Filter::new().from_block(800).to_block(801).topic1(topic1_sender), usize::MAX)
        .await?;
    assert_eq!(by_sender.len(), 1);

    // --- Combined address + topic filter ---
    let addr_a_transfers = backend
        .get_logs(
            Filter::new()
                .from_block(800)
                .to_block(801)
                .address(addr_a)
                .event_signature(topic0_transfer),
            usize::MAX,
        )
        .await?;
    assert_eq!(addr_a_transfers.len(), 2);

    // --- max_logs limits results ---
    let limited = backend.get_logs(Filter::new().from_block(800).to_block(801), 2).await?;
    assert_eq!(limited.len(), 2);
    // Verify the 2 returned are the first 2 in order
    assert_eq!((limited[0].block_number, limited[0].log_index), (Some(800), Some(0)));
    assert_eq!((limited[1].block_number, limited[1].log_index), (Some(800), Some(1)));

    Ok(())
}
