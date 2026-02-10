//! Transaction-related RPC handlers.
//!
//! This module contains handlers for transaction query endpoints that read from
//! cold storage.

use crate::error::{RpcError, RpcResult};
use crate::types::{RpcTransaction, format_hex_u64, format_hex_u256};
use alloy::{
    consensus::Transaction,
    primitives::{B256, Bytes},
    rlp::Encodable,
};
use signet_cold::{ColdStorageReadHandle, TransactionSpecifier};
use signet_storage_types::TransactionSigned;

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
) -> RpcResult<Option<RpcTransaction>> {
    let tx = cold
        .get_transaction(TransactionSpecifier::Hash(hash))
        .await
        .map_err(|e| RpcError::internal(e.to_string()))?;

    let Some(tx) = tx else {
        return Ok(None);
    };

    // Build transaction response
    let rpc_tx = RpcTransaction {
        hash: *tx.tx_hash(),
        nonce: format_hex_u64(tx.nonce()),
        to: tx.to(),
        value: format_hex_u256(tx.value()),
        gas: format_hex_u64(tx.gas_limit()),
        input: tx.input().clone(),
    };

    Ok(Some(rpc_tx))
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
