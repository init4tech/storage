//! Receipt-related RPC handlers.
//!
//! This module contains handlers for transaction receipt query endpoints that
//! read from cold storage.

use crate::error::{RpcResult, internal_err, rpc_ok};
use crate::router::RpcContext;
use crate::types::{RpcLog, RpcReceipt};
use alloy::{
    consensus::Transaction,
    primitives::{B256, U64},
};
use signet_cold::{ReceiptSpecifier, TransactionSpecifier};
use signet_hot::HotKv;

/// Handler for `eth_getTransactionReceipt`.
///
/// Returns the receipt of a transaction by transaction hash.
pub(crate) async fn eth_get_transaction_receipt<H: HotKv>(
    tx_hash: B256,
    state: RpcContext<H>,
) -> RpcResult<Option<RpcReceipt>> {
    let cold = state.storage.cold_reader();

    let receipt = match cold.get_receipt(ReceiptSpecifier::TxHash(tx_hash)).await {
        Ok(r) => r,
        Err(e) => return internal_err(format!("Failed to get receipt: {e}")),
    };

    let Some(receipt) = receipt else {
        return rpc_ok(None);
    };

    let tx = match cold.get_transaction(TransactionSpecifier::Hash(tx_hash)).await {
        Ok(t) => t,
        Err(e) => return internal_err(format!("Failed to get transaction: {e}")),
    };

    let Some(tx) = tx else {
        return internal_err("Receipt found but transaction missing");
    };

    let logs: Vec<RpcLog> = receipt
        .inner
        .logs
        .iter()
        .enumerate()
        .map(|(log_idx, log)| RpcLog {
            log_index: U64::from(log_idx as u64),
            transaction_index: None,
            transaction_hash: tx_hash,
            block_hash: None,
            block_number: None,
            address: log.address,
            data: log.data.data.clone(),
            topics: log.data.topics().to_vec(),
        })
        .collect();

    rpc_ok(Some(RpcReceipt {
        transaction_hash: tx_hash,
        transaction_index: None,
        block_hash: None,
        block_number: None,
        cumulative_gas_used: U64::from(receipt.inner.cumulative_gas_used),
        gas_used: U64::from(receipt.inner.cumulative_gas_used), // Approximate
        status: U64::from(u64::from(receipt.inner.status.coerce_status())),
        to: tx.to(),
        logs,
    }))
}
