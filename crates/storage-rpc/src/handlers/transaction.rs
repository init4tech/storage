//! Transaction-related RPC handlers.
//!
//! This module contains handlers for transaction query endpoints that read from
//! cold storage.

use crate::error::{RpcResult, internal_err, rpc_ok};
use crate::router::RpcContext;
use crate::types::RpcTransaction;
use alloy::{
    consensus::Transaction,
    primitives::{B256, Bytes, U64},
    rlp::Encodable,
};
use signet_cold::TransactionSpecifier;
use signet_hot::HotKv;
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
pub(crate) async fn eth_get_transaction_by_hash<H: HotKv>(
    hash: B256,
    state: RpcContext<H>,
) -> RpcResult<Option<RpcTransaction>> {
    let cold = state.storage.cold_reader();

    let tx = match cold.get_transaction(TransactionSpecifier::Hash(hash)).await {
        Ok(t) => t,
        Err(e) => return internal_err(format!("Failed to get transaction: {e}")),
    };

    let Some(tx) = tx else {
        return rpc_ok(None);
    };

    rpc_ok(Some(RpcTransaction {
        hash: *tx.tx_hash(),
        nonce: U64::from(tx.nonce()),
        to: tx.to(),
        value: tx.value(),
        gas: U64::from(tx.gas_limit()),
        input: tx.input().clone(),
    }))
}

/// Handler for `eth_getRawTransactionByHash`.
///
/// Returns raw RLP-encoded transaction by hash.
pub(crate) async fn eth_get_raw_transaction_by_hash<H: HotKv>(
    hash: B256,
    state: RpcContext<H>,
) -> RpcResult<Option<Bytes>> {
    let cold = state.storage.cold_reader();

    let tx = match cold.get_transaction(TransactionSpecifier::Hash(hash)).await {
        Ok(t) => t,
        Err(e) => return internal_err(format!("Failed to get transaction: {e}")),
    };

    rpc_ok(tx.map(|tx| encode_raw_tx(&tx)))
}
