//! Block-related RPC handlers.
//!
//! This module contains handlers for block query endpoints that read from
//! cold storage.

use crate::error::{RpcError, RpcResult};
use crate::types::{BlockTransactions, RpcBlock, RpcLog, RpcReceipt, format_hex_u64};
use alloy::{
    eips::BlockNumberOrTag,
    primitives::{B256, U64},
};
use signet_cold::{BlockTag, ColdStorageReadHandle, HeaderSpecifier};

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
pub async fn eth_block_number(cold: &ColdStorageReadHandle) -> RpcResult<U64> {
    let latest = cold.get_latest_block().await.map_err(|e| RpcError::internal(e.to_string()))?;

    match latest {
        Some(num) => Ok(U64::from(num)),
        None => Ok(U64::ZERO),
    }
}

/// Handler for `eth_getBlockByHash`.
///
/// Returns block information by hash.
pub async fn eth_get_block_by_hash(
    cold: &ColdStorageReadHandle,
    hash: B256,
    _full_txs: bool,
) -> RpcResult<Option<RpcBlock>> {
    let header = cold
        .get_header(HeaderSpecifier::Hash(hash))
        .await
        .map_err(|e| RpcError::internal(e.to_string()))?;

    let Some(header) = header else {
        return Ok(None);
    };

    let block_number = header.number;

    // Get transaction hashes
    let txs = cold
        .get_transactions_in_block(block_number)
        .await
        .map_err(|e| RpcError::internal(e.to_string()))?;

    // Note: We currently return hashes only. When full_txs is true,
    // we would populate with full RpcTransaction objects.
    let tx_hashes: Vec<B256> = txs.iter().map(|tx| *tx.tx_hash()).collect();

    let block = RpcBlock {
        hash,
        number: format_hex_u64(header.number),
        parent_hash: header.parent_hash,
        timestamp: format_hex_u64(header.timestamp),
        gas_limit: format_hex_u64(header.gas_limit),
        gas_used: format_hex_u64(header.gas_used),
        base_fee_per_gas: header.base_fee_per_gas.map(format_hex_u64),
        transactions: BlockTransactions::Hashes(tx_hashes),
        uncles: vec![],
    };

    Ok(Some(block))
}

/// Handler for `eth_getBlockByNumber`.
///
/// Returns block information by number or tag.
pub async fn eth_get_block_by_number(
    cold: &ColdStorageReadHandle,
    block_id: BlockNumberOrTag,
    full_txs: bool,
) -> RpcResult<Option<RpcBlock>> {
    let spec = block_id_to_specifier(block_id);

    let header = cold.get_header(spec).await.map_err(|e| RpcError::internal(e.to_string()))?;

    let Some(header) = header else {
        return Ok(None);
    };

    let hash = header.hash_slow();
    eth_get_block_by_hash(cold, hash, full_txs).await
}

/// Handler for `eth_getBlockTransactionCountByHash`.
///
/// Returns the number of transactions in a block by hash.
pub async fn eth_get_block_transaction_count_by_hash(
    cold: &ColdStorageReadHandle,
    hash: B256,
) -> RpcResult<Option<U64>> {
    let header = cold
        .get_header(HeaderSpecifier::Hash(hash))
        .await
        .map_err(|e| RpcError::internal(e.to_string()))?;

    let Some(header) = header else {
        return Ok(None);
    };

    let count = cold
        .get_transaction_count(header.number)
        .await
        .map_err(|e| RpcError::internal(e.to_string()))?;

    Ok(Some(U64::from(count)))
}

/// Handler for `eth_getBlockTransactionCountByNumber`.
///
/// Returns the number of transactions in a block by number.
pub async fn eth_get_block_transaction_count_by_number(
    cold: &ColdStorageReadHandle,
    block_id: BlockNumberOrTag,
) -> RpcResult<Option<U64>> {
    let spec = block_id_to_specifier(block_id);

    let header = cold.get_header(spec).await.map_err(|e| RpcError::internal(e.to_string()))?;

    let Some(header) = header else {
        return Ok(None);
    };

    let count = cold
        .get_transaction_count(header.number)
        .await
        .map_err(|e| RpcError::internal(e.to_string()))?;

    Ok(Some(U64::from(count)))
}

/// Handler for `eth_getBlockReceipts`.
///
/// Returns all receipts for a block.
pub async fn eth_get_block_receipts(
    cold: &ColdStorageReadHandle,
    block_id: BlockNumberOrTag,
) -> RpcResult<Option<Vec<RpcReceipt>>> {
    let spec = block_id_to_specifier(block_id);

    let header = cold.get_header(spec).await.map_err(|e| RpcError::internal(e.to_string()))?;

    let Some(header) = header else {
        return Ok(None);
    };

    let block_hash = header.hash_slow();
    let block_number = header.number;

    let receipts = cold
        .get_receipts_in_block(block_number)
        .await
        .map_err(|e| RpcError::internal(e.to_string()))?;

    let txs = cold
        .get_transactions_in_block(block_number)
        .await
        .map_err(|e| RpcError::internal(e.to_string()))?;

    // Build receipts
    let mut rpc_receipts = Vec::new();

    for (idx, (receipt, tx)) in receipts.iter().zip(txs.iter()).enumerate() {
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

        let rpc_receipt = RpcReceipt {
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
            to: None, // Would need to get from transaction
            logs,
        };

        rpc_receipts.push(rpc_receipt);
    }

    Ok(Some(rpc_receipts))
}
