//! Block-related RPC handlers.
//!
//! This module contains handlers for block query endpoints that read from
//! cold storage.

use crate::error::RpcResult;
use crate::router::RpcContext;
use crate::types::{BlockTransactions, RpcBlock, RpcLog, RpcReceipt};
use ajj::ResponsePayload;
use alloy::{
    eips::BlockNumberOrTag,
    primitives::{B256, U64},
};
use signet_cold::{BlockTag, HeaderSpecifier};
use signet_hot::HotKv;
use std::borrow::Cow;

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

    match cold.get_latest_block().await {
        Ok(Some(num)) => ResponsePayload(Ok(U64::from(num))),
        Ok(None) => ResponsePayload::internal_error_message(Cow::Borrowed("no blocks in storage")),
        Err(e) => ResponsePayload::internal_error_message(Cow::Owned(format!(
            "Failed to get latest block: {e}"
        ))),
    }
}

/// Shared implementation for getting a block by specifier.
async fn get_block<H: HotKv>(
    spec: HeaderSpecifier,
    state: &RpcContext<H>,
) -> RpcResult<Option<RpcBlock>> {
    let cold = state.storage.cold_reader();

    let header = match cold.get_header(spec).await {
        Ok(h) => h,
        Err(e) => {
            return ResponsePayload::internal_error_message(Cow::Owned(format!(
                "Failed to get header: {e}"
            )));
        }
    };

    let Some(header) = header else {
        return ResponsePayload(Ok(None));
    };

    let hash = header.hash_slow();
    let block_number = header.number;

    let txs = match cold.get_transactions_in_block(block_number).await {
        Ok(t) => t,
        Err(e) => {
            return ResponsePayload::internal_error_message(Cow::Owned(format!(
                "Failed to get transactions: {e}"
            )));
        }
    };

    // Note: We currently return hashes only. When full_txs is true,
    // we would populate with full RpcTransaction objects.
    let tx_hashes: Vec<B256> = txs.iter().map(|tx| *tx.tx_hash()).collect();

    ResponsePayload(Ok(Some(RpcBlock {
        hash,
        number: U64::from(header.number),
        parent_hash: header.parent_hash,
        timestamp: U64::from(header.timestamp),
        gas_limit: U64::from(header.gas_limit),
        gas_used: U64::from(header.gas_used),
        base_fee_per_gas: header.base_fee_per_gas.map(U64::from),
        transactions: BlockTransactions::Hashes(tx_hashes),
        uncles: vec![],
    })))
}

/// Handler for `eth_getBlockByHash`.
///
/// Returns block information by hash.
pub(crate) async fn eth_get_block_by_hash<H: HotKv>(
    (hash, _full_txs): (B256, bool),
    state: RpcContext<H>,
) -> RpcResult<Option<RpcBlock>> {
    get_block(HeaderSpecifier::Hash(hash), &state).await
}

/// Handler for `eth_getBlockByNumber`.
///
/// Returns block information by number or tag.
pub(crate) async fn eth_get_block_by_number<H: HotKv>(
    (block_id, _full_txs): (BlockNumberOrTag, bool),
    state: RpcContext<H>,
) -> RpcResult<Option<RpcBlock>> {
    get_block(block_id_to_specifier(block_id), &state).await
}

/// Shared implementation for getting transaction count by specifier.
async fn get_block_tx_count<H: HotKv>(
    spec: HeaderSpecifier,
    state: &RpcContext<H>,
) -> RpcResult<Option<U64>> {
    let cold = state.storage.cold_reader();

    let header = match cold.get_header(spec).await {
        Ok(h) => h,
        Err(e) => {
            return ResponsePayload::internal_error_message(Cow::Owned(format!(
                "Failed to get header: {e}"
            )));
        }
    };

    let Some(header) = header else {
        return ResponsePayload(Ok(None));
    };

    let count = match cold.get_transaction_count(header.number).await {
        Ok(c) => c,
        Err(e) => {
            return ResponsePayload::internal_error_message(Cow::Owned(format!(
                "Failed to get transaction count: {e}"
            )));
        }
    };

    ResponsePayload(Ok(Some(U64::from(count))))
}

/// Handler for `eth_getBlockTransactionCountByHash`.
///
/// Returns the number of transactions in a block by hash.
pub(crate) async fn eth_get_block_transaction_count_by_hash<H: HotKv>(
    hash: B256,
    state: RpcContext<H>,
) -> RpcResult<Option<U64>> {
    get_block_tx_count(HeaderSpecifier::Hash(hash), &state).await
}

/// Handler for `eth_getBlockTransactionCountByNumber`.
///
/// Returns the number of transactions in a block by number.
pub(crate) async fn eth_get_block_transaction_count_by_number<H: HotKv>(
    block_id: BlockNumberOrTag,
    state: RpcContext<H>,
) -> RpcResult<Option<U64>> {
    get_block_tx_count(block_id_to_specifier(block_id), &state).await
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
        Err(e) => {
            return ResponsePayload::internal_error_message(Cow::Owned(format!(
                "Failed to get header: {e}"
            )));
        }
    };

    let Some(header) = header else {
        return ResponsePayload(Ok(None));
    };

    let block_hash = header.hash_slow();
    let block_number = header.number;

    let receipts = match cold.get_receipts_in_block(block_number).await {
        Ok(r) => r,
        Err(e) => {
            return ResponsePayload::internal_error_message(Cow::Owned(format!(
                "Failed to get receipts: {e}"
            )));
        }
    };

    let txs = match cold.get_transactions_in_block(block_number).await {
        Ok(t) => t,
        Err(e) => {
            return ResponsePayload::internal_error_message(Cow::Owned(format!(
                "Failed to get transactions: {e}"
            )));
        }
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
                    log_index: U64::from(log_idx as u64),
                    transaction_index: Some(U64::from(idx as u64)),
                    transaction_hash: *tx.tx_hash(),
                    block_hash: Some(block_hash),
                    block_number: Some(U64::from(block_number)),
                    address: log.address,
                    data: log.data.data.clone(),
                    topics: log.data.topics().to_vec(),
                })
                .collect();

            RpcReceipt {
                transaction_hash: *tx.tx_hash(),
                transaction_index: Some(U64::from(idx as u64)),
                block_hash: Some(block_hash),
                block_number: Some(U64::from(block_number)),
                cumulative_gas_used: U64::from(receipt.inner.cumulative_gas_used),
                gas_used: U64::from(gas_used),
                status: U64::from(u64::from(receipt.inner.status.coerce_status())),
                to: None,
                logs,
            }
        })
        .collect();

    ResponsePayload(Ok(Some(rpc_receipts)))
}
