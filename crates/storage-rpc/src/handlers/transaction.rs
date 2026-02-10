//! Transaction-related RPC handlers.
//!
//! This module contains handlers for transaction query endpoints that read from
//! cold storage.

use crate::error::RpcResult;
use crate::router::RpcContext;
use crate::types::RpcTransaction;
use ajj::ResponsePayload;
use alloy::{
    consensus::Transaction,
    eips::eip2718::Encodable2718,
    primitives::{B256, Bytes, U64},
};
use signet_cold::TransactionSpecifier;
use signet_hot::HotKv;
use std::borrow::Cow;

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
        Err(e) => {
            return ResponsePayload::internal_error_message(Cow::Owned(format!(
                "Failed to get transaction: {e}"
            )));
        }
    };

    let Some(tx) = tx else {
        return ResponsePayload(Ok(None));
    };

    ResponsePayload(Ok(Some(RpcTransaction {
        hash: *tx.tx_hash(),
        nonce: U64::from(tx.nonce()),
        to: tx.to(),
        value: tx.value(),
        gas: U64::from(tx.gas_limit()),
        input: tx.input().clone(),
    })))
}

/// Handler for `eth_getRawTransactionByHash`.
///
/// Returns raw EIP-2718 encoded transaction by hash.
pub(crate) async fn eth_get_raw_transaction_by_hash<H: HotKv>(
    hash: B256,
    state: RpcContext<H>,
) -> RpcResult<Option<Bytes>> {
    let cold = state.storage.cold_reader();

    let tx = match cold.get_transaction(TransactionSpecifier::Hash(hash)).await {
        Ok(t) => t,
        Err(e) => {
            return ResponsePayload::internal_error_message(Cow::Owned(format!(
                "Failed to get transaction: {e}"
            )));
        }
    };

    ResponsePayload(Ok(tx.map(|tx| tx.encoded_2718().into())))
}
