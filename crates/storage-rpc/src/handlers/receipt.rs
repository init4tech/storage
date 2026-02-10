//! Receipt-related RPC handlers.
//!
//! This module contains handlers for transaction receipt query endpoints that
//! read from cold storage.

use crate::error::{RpcError, RpcResult};
use crate::types::{RpcLog, RpcReceipt, format_hex_u64};
use alloy::{consensus::Transaction, primitives::B256};
use signet_cold::{ColdStorageReadHandle, ReceiptSpecifier, TransactionSpecifier};

/// Handler for `eth_getTransactionReceipt`.
///
/// Returns the receipt of a transaction by transaction hash.
pub async fn eth_get_transaction_receipt(
    cold: &ColdStorageReadHandle,
    tx_hash: B256,
) -> RpcResult<Option<RpcReceipt>> {
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

    // Build logs
    let logs: Vec<RpcLog> = receipt
        .inner
        .logs
        .iter()
        .enumerate()
        .map(|(log_idx, log)| RpcLog {
            log_index: format_hex_u64(log_idx as u64),
            transaction_index: None,
            transaction_hash: tx_hash,
            block_hash: None,
            block_number: None,
            address: log.address,
            data: log.data.data.clone(),
            topics: log.data.topics().to_vec(),
        })
        .collect();

    // Build receipt
    let rpc_receipt = RpcReceipt {
        transaction_hash: tx_hash,
        transaction_index: None,
        block_hash: None,
        block_number: None,
        cumulative_gas_used: format_hex_u64(receipt.inner.cumulative_gas_used),
        gas_used: format_hex_u64(receipt.inner.cumulative_gas_used), // Approximate
        status: if receipt.inner.status.coerce_status() {
            "0x1".to_string()
        } else {
            "0x0".to_string()
        },
        to: tx.to(),
        logs,
    };

    Ok(Some(rpc_receipt))
}
