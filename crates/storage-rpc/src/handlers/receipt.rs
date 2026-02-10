//! Receipt-related RPC handlers.
//!
//! This module contains handlers for transaction receipt query endpoints that
//! read from cold storage.

use crate::error::{RpcError, RpcResult};
use alloy::{
    consensus::Transaction,
    primitives::B256,
};
use signet_cold::{ColdStorageReadHandle, ReceiptSpecifier, TransactionSpecifier};
use serde_json::Value;

/// Handler for `eth_getTransactionReceipt`.
///
/// Returns the receipt of a transaction by transaction hash.
pub async fn eth_get_transaction_receipt(
    cold: &ColdStorageReadHandle,
    tx_hash: B256,
) -> RpcResult<Option<Value>> {
    // Get the receipt
    let receipt = cold
        .get_receipt(ReceiptSpecifier::TxHash(tx_hash))
        .await
        .map_err(|e| RpcError::internal(e.to_string()))?;

    let Some(receipt) = receipt else {
        return Ok(None);
    };

    // Get the transaction to get additional info (from, to, etc.)
    let tx = cold
        .get_transaction(TransactionSpecifier::Hash(tx_hash))
        .await
        .map_err(|e| RpcError::internal(e.to_string()))?;

    let Some(tx) = tx else {
        return Err(RpcError::internal("Receipt found but transaction missing"));
    };

    // Build receipt JSON
    let receipt_json = serde_json::json!({
        "transactionHash": format!("{:?}", tx_hash),
        "cumulativeGasUsed": format!("{:#x}", receipt.inner.cumulative_gas_used),
        "gasUsed": format!("{:#x}", receipt.inner.cumulative_gas_used),
        "status": if receipt.inner.status.coerce_status() { "0x1" } else { "0x0" },
        "to": tx.to().map(|a| format!("{:?}", a)),
        "logs": receipt.inner.logs.iter().enumerate().map(|(log_idx, log)| {
            serde_json::json!({
                "logIndex": format!("{:#x}", log_idx),
                "transactionHash": format!("{:?}", tx_hash),
                "address": format!("{:?}", log.address),
                "data": format!("{}", log.data.data),
                "topics": log.data.topics().iter().map(|t| format!("{:?}", t)).collect::<Vec<_>>(),
            })
        }).collect::<Vec<_>>(),
    });

    Ok(Some(receipt_json))
}
