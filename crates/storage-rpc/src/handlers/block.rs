//! Block-related RPC handlers.
//!
//! This module contains handlers for block query endpoints that read from
//! cold storage.

use crate::error::{RpcResult, internal_err, rpc_ok};
use crate::router::RpcContext;
use crate::types::{BlockTransactions, RpcBlock, RpcLog, RpcReceipt, format_hex_u64};
use alloy::{
    eips::BlockNumberOrTag,
    primitives::{B256, U64},
};
use signet_cold::{BlockTag, HeaderSpecifier};
use signet_hot::HotKv;

/// Convert a BlockNumberOrTag to a HeaderSpecifier.
pub const fn block_id_to_specifier(block_id: BlockNumberOrTag) -> HeaderSpecifier {
    match block_id {
        BlockNumberOrTag::Number(num) => HeaderSpecifier::Number(num),
        BlockNumberOrTag::Latest => HeaderSpecifier::Tag(BlockTag::Latest),
        BlockNumberOrTag::Finalized => HeaderSpecifier::Tag(BlockTag::Finalized),
        BlockNumberOrTag::Safe => HeaderSpecifier::Tag(BlockTag::Safe),
        BlockNumberOrTag::Earliest => HeaderSpecifier::Tag(BlockTag::Earliest),
        BlockNumberOrTag::Pending => HeaderSpecifier::Tag(BlockTag::Latest),
    }
}

/// Handler for `eth_blockNumber`.
///
/// Returns the latest block number.
pub(crate) async fn eth_block_number<H: HotKv>(state: RpcContext<H>) -> RpcResult<U64> {
    let cold = state.storage.cold_reader();
    let latest = cold
        .get_latest_block()
        .await
        .map_err(|e| internal_err::<U64>(format!("Failed to get latest block: {e}")));

    match latest {
        Ok(Some(num)) => rpc_ok(U64::from(num)),
        Ok(None) => rpc_ok(U64::ZERO),
        Err(rp) => rp,
    }
}

/// Handler for `eth_getBlockByHash`.
///
/// Returns block information by hash.
pub(crate) async fn eth_get_block_by_hash<H: HotKv>(
    (hash, _full_txs): (B256, bool),
    state: RpcContext<H>,
) -> RpcResult<Option<RpcBlock>> {
    let cold = state.storage.cold_reader();

    let header = match cold.get_header(HeaderSpecifier::Hash(hash)).await {
        Ok(h) => h,
        Err(e) => return internal_err(format!("Failed to get header: {e}")),
    };

    let Some(header) = header else {
        return rpc_ok(None);
    };

    let block_number = header.number;

    let txs = match cold.get_transactions_in_block(block_number).await {
        Ok(t) => t,
        Err(e) => return internal_err(format!("Failed to get transactions: {e}")),
    };

    // Note: We currently return hashes only. When full_txs is true,
    // we would populate with full RpcTransaction objects.
    let tx_hashes: Vec<B256> = txs.iter().map(|tx| *tx.tx_hash()).collect();

    rpc_ok(Some(RpcBlock {
        hash,
        number: format_hex_u64(header.number),
        parent_hash: header.parent_hash,
        timestamp: format_hex_u64(header.timestamp),
        gas_limit: format_hex_u64(header.gas_limit),
        gas_used: format_hex_u64(header.gas_used),
        base_fee_per_gas: header.base_fee_per_gas.map(format_hex_u64),
        transactions: BlockTransactions::Hashes(tx_hashes),
        uncles: vec![],
    }))
}

/// Handler for `eth_getBlockByNumber`.
///
/// Returns block information by number or tag.
pub(crate) async fn eth_get_block_by_number<H: HotKv>(
    (block_id, full_txs): (BlockNumberOrTag, bool),
    state: RpcContext<H>,
) -> RpcResult<Option<RpcBlock>> {
    let cold = state.storage.cold_reader();
    let spec = block_id_to_specifier(block_id);

    let header = match cold.get_header(spec).await {
        Ok(h) => h,
        Err(e) => return internal_err(format!("Failed to get header: {e}")),
    };

    let Some(header) = header else {
        return rpc_ok(None);
    };

    let hash = header.hash_slow();
    eth_get_block_by_hash((hash, full_txs), state).await
}

/// Handler for `eth_getBlockTransactionCountByHash`.
///
/// Returns the number of transactions in a block by hash.
pub(crate) async fn eth_get_block_transaction_count_by_hash<H: HotKv>(
    hash: B256,
    state: RpcContext<H>,
) -> RpcResult<Option<U64>> {
    let cold = state.storage.cold_reader();

    let header = match cold.get_header(HeaderSpecifier::Hash(hash)).await {
        Ok(h) => h,
        Err(e) => return internal_err(format!("Failed to get header: {e}")),
    };

    let Some(header) = header else {
        return rpc_ok(None);
    };

    let count = match cold.get_transaction_count(header.number).await {
        Ok(c) => c,
        Err(e) => return internal_err(format!("Failed to get transaction count: {e}")),
    };

    rpc_ok(Some(U64::from(count)))
}

/// Handler for `eth_getBlockTransactionCountByNumber`.
///
/// Returns the number of transactions in a block by number.
pub(crate) async fn eth_get_block_transaction_count_by_number<H: HotKv>(
    block_id: BlockNumberOrTag,
    state: RpcContext<H>,
) -> RpcResult<Option<U64>> {
    let cold = state.storage.cold_reader();
    let spec = block_id_to_specifier(block_id);

    let header = match cold.get_header(spec).await {
        Ok(h) => h,
        Err(e) => return internal_err(format!("Failed to get header: {e}")),
    };

    let Some(header) = header else {
        return rpc_ok(None);
    };

    let count = match cold.get_transaction_count(header.number).await {
        Ok(c) => c,
        Err(e) => return internal_err(format!("Failed to get transaction count: {e}")),
    };

    rpc_ok(Some(U64::from(count)))
}

/// Handler for `eth_getBlockReceipts`.
///
/// Returns all receipts for a block.
pub(crate) async fn eth_get_block_receipts<H: HotKv>(
    block_id: BlockNumberOrTag,
    state: RpcContext<H>,
) -> RpcResult<Option<Vec<RpcReceipt>>> {
    let cold = state.storage.cold_reader();
    let spec = block_id_to_specifier(block_id);

    let header = match cold.get_header(spec).await {
        Ok(h) => h,
        Err(e) => return internal_err(format!("Failed to get header: {e}")),
    };

    let Some(header) = header else {
        return rpc_ok(None);
    };

    let block_hash = header.hash_slow();
    let block_number = header.number;

    let receipts = match cold.get_receipts_in_block(block_number).await {
        Ok(r) => r,
        Err(e) => return internal_err(format!("Failed to get receipts: {e}")),
    };

    let txs = match cold.get_transactions_in_block(block_number).await {
        Ok(t) => t,
        Err(e) => return internal_err(format!("Failed to get transactions: {e}")),
    };

    let rpc_receipts = receipts
        .iter()
        .zip(txs.iter())
        .enumerate()
        .map(|(idx, (receipt, tx))| {
            let gas_used = if idx > 0 {
                receipt
                    .inner
                    .cumulative_gas_used
                    .saturating_sub(receipts[idx - 1].inner.cumulative_gas_used)
            } else {
                receipt.inner.cumulative_gas_used
            };

            let logs: Vec<RpcLog> = receipt
                .inner
                .logs
                .iter()
                .enumerate()
                .map(|(log_idx, log)| RpcLog {
                    log_index: format_hex_u64(log_idx as u64),
                    transaction_index: Some(format_hex_u64(idx as u64)),
                    transaction_hash: *tx.tx_hash(),
                    block_hash: Some(block_hash),
                    block_number: Some(format_hex_u64(block_number)),
                    address: log.address,
                    data: log.data.data.clone(),
                    topics: log.data.topics().to_vec(),
                })
                .collect();

            RpcReceipt {
                transaction_hash: *tx.tx_hash(),
                transaction_index: Some(format_hex_u64(idx as u64)),
                block_hash: Some(block_hash),
                block_number: Some(format_hex_u64(block_number)),
                cumulative_gas_used: format_hex_u64(receipt.inner.cumulative_gas_used),
                gas_used: format_hex_u64(gas_used),
                status: if receipt.inner.status.coerce_status() {
                    "0x1".to_string()
                } else {
                    "0x0".to_string()
                },
                to: None,
                logs,
            }
        })
        .collect();

    rpc_ok(Some(rpc_receipts))
}
