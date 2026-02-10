//! Transaction-related RPC handlers.
//!
//! This module contains handlers for transaction query endpoints that read from
//! cold storage.

use crate::error::{RpcError, RpcResult};
use alloy::{
    consensus::Transaction,
    primitives::{Bytes, B256},
    rlp::Encodable,
};
use signet_cold::{ColdStorageReadHandle, TransactionSpecifier};
use signet_storage_types::TransactionSigned;
use serde_json::Value;

/// RLP encode a transaction to raw bytes.
fn encode_raw_tx(tx: &TransactionSigned) -> Bytes {
    let mut buf = Vec::new();
    tx.encode(&mut buf);
    Bytes::from(buf)
}

/// Handler for `eth_getTransactionByHash`.
///
/// Returns transaction information by hash.
pub async fn eth_get_transaction_by_hash(
    cold: &ColdStorageReadHandle,
    hash: B256,
) -> RpcResult<Option<Value>> {
    let tx = cold
        .get_transaction(TransactionSpecifier::Hash(hash))
        .await
        .map_err(|e| RpcError::internal(e.to_string()))?;

    let Some(tx) = tx else {
        return Ok(None);
    };

    // Build transaction JSON
    let tx_json = serde_json::json!({
        "hash": format!("{:?}", tx.tx_hash()),
        "nonce": format!("{:#x}", tx.nonce()),
        "to": tx.to().map(|a| format!("{:?}", a)),
        "value": format!("{:#x}", tx.value()),
        "gas": format!("{:#x}", tx.gas_limit()),
        "input": format!("{}", tx.input()),
    });

    Ok(Some(tx_json))
}

/// Handler for `eth_getRawTransactionByHash`.
///
/// Returns raw RLP-encoded transaction by hash.
pub async fn eth_get_raw_transaction_by_hash(
    cold: &ColdStorageReadHandle,
    hash: B256,
) -> RpcResult<Option<Bytes>> {
    let tx = cold
        .get_transaction(TransactionSpecifier::Hash(hash))
        .await
        .map_err(|e| RpcError::internal(e.to_string()))?;

    Ok(tx.map(|tx| encode_raw_tx(&tx)))
}
