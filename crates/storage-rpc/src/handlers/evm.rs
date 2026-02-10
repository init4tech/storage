//! EVM execution handlers.
//!
//! These handlers execute EVM operations using the trevm typestate API:
//! - `eth_call` - Execute a read-only EVM call
//! - `eth_estimateGas` - Estimate gas for a transaction
//! - `eth_sendRawTransaction` - Submit a raw transaction (stub)

use crate::error::{METHOD_NOT_SUPPORTED, RpcResult};
use crate::router::RpcContext;
use ajj::{ErrorPayload, ResponsePayload};
use alloy::{
    consensus::TxEnvelope,
    primitives::{Bytes, U64},
    rlp::Decodable,
    rpc::types::TransactionRequest,
};
use signet_hot::{HotKv, db::HistoryRead, model::HotKvRead};
use std::borrow::Cow;
use trevm::{
    EstimationResult, NoopBlock, NoopCfg, TrevmBuilder,
    revm::{
        context::result::ExecutionResult, database::DBErrorMarker, primitives::hardfork::SpecId,
    },
};

/// Handler for `eth_call`.
///
/// Executes a read-only message call against the current state.
/// This does not create a transaction or modify state.
///
/// # Parameters
/// - `call`: Transaction request with call parameters
/// - `_block`: Block identifier (ignored, always uses latest state)
pub(crate) async fn eth_call<H: HotKv>(
    (call, _block): (TransactionRequest, Option<String>),
    state: RpcContext<H>,
) -> RpcResult<Bytes>
where
    <<H as HotKv>::RoTx as HotKvRead>::Error: DBErrorMarker,
{
    let db = match state.storage.revm_reader() {
        Ok(r) => r,
        Err(e) => {
            return ResponsePayload::internal_error_message(Cow::Owned(format!(
                "Failed to create revm reader: {e}"
            )));
        }
    };

    let reader = match state.storage.reader() {
        Ok(r) => r,
        Err(e) => {
            return ResponsePayload::internal_error_message(Cow::Owned(format!(
                "Failed to create reader: {e}"
            )));
        }
    };
    let header = match reader.last_header() {
        Ok(h) => h,
        Err(e) => {
            return ResponsePayload::internal_error_message(Cow::Owned(format!(
                "Failed to read header: {e}"
            )));
        }
    };

    let trevm = TrevmBuilder::new()
        .with_spec_id(SpecId::CANCUN)
        .with_db(db)
        .build_trevm()
        .fill_cfg(&NoopCfg);

    let trevm = match header {
        Some(ref h) => trevm.fill_block(h),
        None => trevm.fill_block(&NoopBlock),
    };

    let (result, _) = match trevm.fill_tx(&call).call() {
        Ok(r) => r,
        Err(e) => {
            return ResponsePayload::internal_error_message(Cow::Owned(format!(
                "EVM call failed: {:?}",
                e.into_error()
            )));
        }
    };

    match result {
        ExecutionResult::Success { output, .. } => ResponsePayload(Ok(output.data().clone())),
        ExecutionResult::Revert { output, .. } => {
            ResponsePayload::internal_error_with_message_and_obj(
                Cow::Borrowed("execution reverted"),
                format!("0x{}", alloy::hex::encode(&output)),
            )
        }
        ExecutionResult::Halt { reason, .. } => ResponsePayload::internal_error_message(
            Cow::Owned(format!("execution halted: {reason:?}")),
        ),
    }
}

/// Handler for `eth_estimateGas`.
///
/// Estimates the gas required to execute a transaction.
/// Uses trevm's built-in binary search to find the minimum gas.
///
/// # Parameters
/// - `call`: Transaction request with call parameters
/// - `_block`: Block identifier (ignored, always uses latest state)
pub(crate) async fn eth_estimate_gas<H: HotKv>(
    (call, _block): (TransactionRequest, Option<String>),
    state: RpcContext<H>,
) -> RpcResult<U64>
where
    <<H as HotKv>::RoTx as HotKvRead>::Error: DBErrorMarker,
{
    let db = match state.storage.revm_reader() {
        Ok(r) => r,
        Err(e) => {
            return ResponsePayload::internal_error_message(Cow::Owned(format!(
                "Failed to create revm reader: {e}"
            )));
        }
    };

    let reader = match state.storage.reader() {
        Ok(r) => r,
        Err(e) => {
            return ResponsePayload::internal_error_message(Cow::Owned(format!(
                "Failed to create reader: {e}"
            )));
        }
    };
    let header = match reader.last_header() {
        Ok(h) => h,
        Err(e) => {
            return ResponsePayload::internal_error_message(Cow::Owned(format!(
                "Failed to read header: {e}"
            )));
        }
    };

    let trevm = TrevmBuilder::new()
        .with_spec_id(SpecId::CANCUN)
        .with_db(db)
        .build_trevm()
        .fill_cfg(&NoopCfg);

    let trevm = match header {
        Some(ref h) => trevm.fill_block(h),
        None => trevm.fill_block(&NoopBlock),
    };

    let (estimation, _) = match trevm.fill_tx(&call).estimate_gas() {
        Ok(r) => r,
        Err(e) => {
            return ResponsePayload::internal_error_message(Cow::Owned(format!(
                "Gas estimation failed: {:?}",
                e.into_error()
            )));
        }
    };

    match estimation {
        EstimationResult::Success { limit, .. } => ResponsePayload(Ok(U64::from(limit))),
        EstimationResult::Revert { reason, .. } => {
            ResponsePayload::internal_error_with_message_and_obj(
                Cow::Borrowed("execution reverted"),
                format!("0x{}", alloy::hex::encode(&reason)),
            )
        }
        EstimationResult::Halt { reason, .. } => ResponsePayload::internal_error_message(
            Cow::Owned(format!("execution halted: {reason:?}")),
        ),
    }
}

/// Handler for `eth_sendRawTransaction`.
///
/// Submits a signed raw transaction for inclusion in a block.
///
/// **Note**: This is currently a stub implementation. In production, this would:
/// 1. Decode and validate the transaction
/// 2. Forward to a transaction pool or builder
///
/// # Parameters
/// - `data`: RLP-encoded signed transaction
pub(crate) async fn eth_send_raw_transaction<H: HotKv>(
    data: Bytes,
    _state: RpcContext<H>,
) -> RpcResult<alloy::primitives::B256> {
    let tx = match TxEnvelope::decode(&mut data.as_ref()) {
        Ok(t) => t,
        Err(e) => {
            return ResponsePayload(Err(ErrorPayload {
                code: -32602,
                message: Cow::Owned(format!("invalid transaction: {e}")),
                data: None,
            }));
        }
    };

    let tx_hash = *tx.tx_hash();

    // In production, this would forward to a tx-cache or mempool
    // For now, we reject with a clear message
    ResponsePayload(Err(ErrorPayload {
        code: METHOD_NOT_SUPPORTED,
        message: Cow::Owned(format!(
            "transaction submission not yet implemented (tx_hash: {tx_hash})"
        )),
        data: None,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::{Address, TxKind, U256};
    use signet_cold::{ColdStorageTask, mem::MemColdBackend};
    use signet_hot::{mem::MemKv, model::HotKvWrite, tables::PlainAccountState};
    use signet_storage::UnifiedStorage;
    use signet_storage_types::Account;
    use std::sync::Arc;
    use tokio_util::sync::CancellationToken;

    fn create_test_storage() -> Arc<UnifiedStorage<MemKv>> {
        let mem_kv = MemKv::default();
        let cold_handle = ColdStorageTask::spawn(MemColdBackend::new(), CancellationToken::new());
        Arc::new(UnifiedStorage::new(mem_kv, cold_handle))
    }

    fn test_ctx(storage: &Arc<UnifiedStorage<MemKv>>) -> RpcContext<MemKv> {
        RpcContext { storage: storage.clone(), chain_id: 31337 }
    }

    #[tokio::test]
    async fn test_eth_call_simple() {
        let storage = create_test_storage();
        let address = Address::from_slice(&[0x1; 20]);

        {
            let writer = storage.hot().writer().unwrap();
            let account = Account {
                nonce: 0,
                balance: U256::from(1_000_000_000_000_000_000u128), // 1 ETH
                bytecode_hash: None,
            };
            writer.queue_put::<PlainAccountState>(&address, &account).unwrap();
            writer.raw_commit().unwrap();
        }

        let call = TransactionRequest {
            from: Some(address),
            to: Some(TxKind::Call(address)),
            value: Some(U256::ZERO),
            ..Default::default()
        };

        let result = eth_call((call, None), test_ctx(&storage)).await;
        assert!(result.is_success());
    }

    #[tokio::test]
    async fn test_eth_estimate_gas_simple() {
        let storage = create_test_storage();
        let from = Address::from_slice(&[0x1; 20]);
        let to = Address::from_slice(&[0x2; 20]);

        {
            let writer = storage.hot().writer().unwrap();
            let account = Account {
                nonce: 0,
                balance: U256::from(1_000_000_000_000_000_000u128), // 1 ETH
                bytecode_hash: None,
            };
            writer.queue_put::<PlainAccountState>(&from, &account).unwrap();
            writer.raw_commit().unwrap();
        }

        let call = TransactionRequest {
            from: Some(from),
            to: Some(TxKind::Call(to)),
            value: Some(U256::from(1000u64)),
            ..Default::default()
        };

        let result = eth_estimate_gas((call, None), test_ctx(&storage)).await;
        assert!(result.is_success());

        let gas = *result.as_success().unwrap();
        assert!(gas >= U64::from(21000));
    }

    #[tokio::test]
    async fn test_eth_send_raw_transaction_stub() {
        let storage = create_test_storage();

        let invalid_data = Bytes::from(vec![0x01, 0x02, 0x03]);

        let result = eth_send_raw_transaction(invalid_data, test_ctx(&storage)).await;
        assert!(result.is_error());
    }
}
