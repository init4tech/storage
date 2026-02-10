//! Block-related RPC handlers.
//!
//! This module contains handlers for block query endpoints that read from
//! cold storage.

use crate::error::{RpcError, RpcResult};
use alloy::{
    eips::BlockNumberOrTag,
    primitives::{B256, U64},
};
use signet_cold::{BlockTag, ColdStorageReadHandle, HeaderSpecifier};
use serde_json::Value;

// Transaction trait is used implicitly by tx.tx_hash(), etc.
use alloy::consensus::Transaction as _;

/// Convert a BlockNumberOrTag to a HeaderSpecifier.
pub fn block_id_to_specifier(block_id: BlockNumberOrTag) -> HeaderSpecifier {
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
    full_txs: bool,
) -> RpcResult<Option<Value>> {
    let header = cold
        .get_header(HeaderSpecifier::Hash(hash))
        .await
        .map_err(|e| RpcError::internal(e.to_string()))?;

    let Some(header) = header else {
        return Ok(None);
    };

    let block_number = header.number;

    // Build a JSON representation of the block
    let transactions = if full_txs {
        let txs = cold
            .get_transactions_in_block(block_number)
            .await
            .map_err(|e| RpcError::internal(e.to_string()))?;

        // Return transaction hashes as simplified JSON
        Value::Array(txs.iter().map(|tx| {
            Value::String(format!("{:?}", tx.tx_hash()))
        }).collect())
    } else {
        let txs = cold
            .get_transactions_in_block(block_number)
            .await
            .map_err(|e| RpcError::internal(e.to_string()))?;

        Value::Array(txs.iter().map(|tx| Value::String(format!("{:?}", tx.tx_hash()))).collect())
    };

    // Build block JSON
    let block = serde_json::json!({
        "hash": format!("{:?}", hash),
        "number": format!("{:#x}", header.number),
        "parentHash": format!("{:?}", header.parent_hash),
        "timestamp": format!("{:#x}", header.timestamp),
        "gasLimit": format!("{:#x}", header.gas_limit),
        "gasUsed": format!("{:#x}", header.gas_used),
        "baseFeePerGas": header.base_fee_per_gas.map(|x| format!("{:#x}", x)),
        "transactions": transactions,
        "uncles": [],
    });

    Ok(Some(block))
}

/// Handler for `eth_getBlockByNumber`.
///
/// Returns block information by number or tag.
pub async fn eth_get_block_by_number(
    cold: &ColdStorageReadHandle,
    block_id: BlockNumberOrTag,
    full_txs: bool,
) -> RpcResult<Option<Value>> {
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

    let count =
        cold.get_transaction_count(header.number).await.map_err(|e| RpcError::internal(e.to_string()))?;

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

    let count =
        cold.get_transaction_count(header.number).await.map_err(|e| RpcError::internal(e.to_string()))?;

    Ok(Some(U64::from(count)))
}

/// Handler for `eth_getBlockReceipts`.
///
/// Returns all receipts for a block.
pub async fn eth_get_block_receipts(
    cold: &ColdStorageReadHandle,
    block_id: BlockNumberOrTag,
) -> RpcResult<Option<Value>> {
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

    // Build receipts as JSON
    let mut rpc_receipts = Vec::new();

    for (idx, (receipt, tx)) in receipts.iter().zip(txs.iter()).enumerate() {

        let gas_used = if idx > 0 {
            receipt.inner.cumulative_gas_used.saturating_sub(receipts[idx - 1].inner.cumulative_gas_used)
        } else {
            receipt.inner.cumulative_gas_used
        };

        let receipt_json = serde_json::json!({
            "transactionHash": format!("{:?}", tx.tx_hash()),
            "transactionIndex": format!("{:#x}", idx),
            "blockHash": format!("{:?}", block_hash),
            "blockNumber": format!("{:#x}", block_number),
            "cumulativeGasUsed": format!("{:#x}", receipt.inner.cumulative_gas_used),
            "gasUsed": format!("{:#x}", gas_used),
            "status": if receipt.inner.status.coerce_status() { "0x1" } else { "0x0" },
            "logs": receipt.inner.logs.iter().enumerate().map(|(log_idx, log)| {
                serde_json::json!({
                    "logIndex": format!("{:#x}", log_idx),
                    "transactionIndex": format!("{:#x}", idx),
                    "transactionHash": format!("{:?}", tx.tx_hash()),
                    "blockHash": format!("{:?}", block_hash),
                    "blockNumber": format!("{:#x}", block_number),
                    "address": format!("{:?}", log.address),
                    "data": format!("{}", log.data.data),
                    "topics": log.data.topics().iter().map(|t| format!("{:?}", t)).collect::<Vec<_>>(),
                })
            }).collect::<Vec<_>>(),
        });

        rpc_receipts.push(receipt_json);
    }

    Ok(Some(Value::Array(rpc_receipts)))
}
