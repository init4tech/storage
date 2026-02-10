//! EVM execution handlers.
//!
//! These handlers execute EVM operations using the revm database adapter:
//! - `eth_call` - Execute a read-only EVM call
//! - `eth_estimateGas` - Estimate gas for a transaction
//! - `eth_sendRawTransaction` - Submit a raw transaction (stub)

use crate::error::{
    RpcResult, internal_err, internal_err_with_data, invalid_params, method_not_supported, rpc_ok,
};
use crate::router::RpcContext;
use alloy::{
    consensus::TxEnvelope,
    primitives::{Bytes, TxKind, U64, U256},
    rlp::Decodable,
    rpc::types::TransactionRequest,
};
use signet_hot::{HotKv, db::HistoryRead, model::HotKvRead};
use trevm::revm::{
    Context, ExecuteEvm, MainBuilder, MainContext,
    context_interface::result::{ExecutionResult, HaltReason, ResultAndState},
    database::{DBErrorMarker, Database},
    primitives::hardfork::SpecId,
};

/// Default gas limit for calls (30M gas).
const DEFAULT_GAS_LIMIT: u64 = 30_000_000;

/// Minimum gas for any transaction (21000 for plain transfer).
const MIN_GAS: u64 = 21_000;

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
        Err(e) => return internal_err(format!("Failed to create revm reader: {e}")),
    };

    let reader = match state.storage.reader() {
        Ok(r) => r,
        Err(e) => return internal_err(format!("Failed to create reader: {e}")),
    };
    let header = match reader.last_header() {
        Ok(h) => h,
        Err(e) => return internal_err(format!("Failed to read header: {e}")),
    };

    let result = match execute_call(db, &call, header.as_ref()) {
        Ok(r) => r,
        Err(msg) => return internal_err(msg),
    };

    match result.result {
        ExecutionResult::Success { output, .. } => rpc_ok(Bytes::copy_from_slice(output.data())),
        ExecutionResult::Revert { output, .. } => internal_err_with_data(
            "execution reverted",
            format!("0x{}", alloy::hex::encode(&output)),
        ),
        ExecutionResult::Halt { reason, .. } => {
            internal_err(format!("execution halted: {:?}", reason))
        }
    }
}

/// Handler for `eth_estimateGas`.
///
/// Estimates the gas required to execute a transaction.
/// Uses binary search to find the minimum gas that allows execution to succeed.
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
    let reader = match state.storage.reader() {
        Ok(r) => r,
        Err(e) => return internal_err(format!("Failed to create reader: {e}")),
    };
    let header = match reader.last_header() {
        Ok(h) => h,
        Err(e) => return internal_err(format!("Failed to read header: {e}")),
    };

    let user_gas_limit = call.gas.unwrap_or(DEFAULT_GAS_LIMIT);
    let mut low = MIN_GAS;
    let mut high = user_gas_limit;

    // First, try with maximum gas to ensure the transaction can succeed
    let best_estimate = {
        let db = match state.storage.revm_reader() {
            Ok(r) => r,
            Err(e) => return internal_err(format!("Failed to create revm reader: {e}")),
        };

        let mut test_call = call.clone();
        test_call.gas = Some(high);

        let result = match execute_call(db, &test_call, header.as_ref()) {
            Ok(r) => r,
            Err(msg) => return internal_err(msg),
        };

        match result.result {
            ExecutionResult::Success { gas_used, .. } => gas_used,
            ExecutionResult::Revert { output, .. } => {
                return internal_err_with_data(
                    "execution reverted",
                    format!("0x{}", alloy::hex::encode(&output)),
                );
            }
            ExecutionResult::Halt { reason, .. } => {
                return internal_err(format!("execution halted: {:?}", reason));
            }
        }
    };

    // Binary search for minimum gas
    high = best_estimate;
    low = low.min(high);

    while low < high {
        let mid = (low + high) / 2;

        let db = match state.storage.revm_reader() {
            Ok(r) => r,
            Err(e) => return internal_err(format!("Failed to create revm reader: {e}")),
        };

        let mut test_call = call.clone();
        test_call.gas = Some(mid);

        let result = match execute_call(db, &test_call, header.as_ref()) {
            Ok(r) => r,
            Err(msg) => return internal_err(msg),
        };

        match result.result {
            ExecutionResult::Success { .. } => {
                high = mid;
            }
            ExecutionResult::Revert { .. } | ExecutionResult::Halt { .. } => {
                low = mid + 1;
            }
        }
    }

    // Add a small buffer (10%) to account for execution variance
    let estimate = high.saturating_add(high / 10);
    rpc_ok(U64::from(estimate.min(user_gas_limit)))
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
        Err(e) => return invalid_params(format!("invalid transaction: {e}")),
    };

    let tx_hash = *tx.tx_hash();

    // In production, this would forward to a tx-cache or mempool
    // For now, we reject with a clear message
    method_not_supported(&format!(
        "transaction submission not yet implemented (tx_hash: {tx_hash})"
    ))
}

/// Execute an EVM call with the given database and call parameters.
fn execute_call<D: Database>(
    db: D,
    call: &TransactionRequest,
    header: Option<&alloy::consensus::Header>,
) -> Result<ResultAndState<HaltReason>, String>
where
    D::Error: core::fmt::Debug,
{
    let from = call.from.unwrap_or_default();
    let to = call.to.unwrap_or(TxKind::Create);
    let value = call.value.unwrap_or_default();
    let input = call.input.input().cloned().unwrap_or_default();
    let gas_limit = call.gas.unwrap_or(DEFAULT_GAS_LIMIT);

    let (block_number, timestamp, base_fee) = match header {
        Some(h) => (h.number, h.timestamp, h.base_fee_per_gas.unwrap_or(0)),
        None => (0, 0, 0),
    };

    let ctx = Context::mainnet()
        .with_db(db)
        .modify_cfg_chained(|c| {
            c.spec = SpecId::CANCUN;
            c.disable_balance_check = true;
            c.disable_base_fee = true;
        })
        .modify_block_chained(|b| {
            b.number = U256::from(block_number);
            b.timestamp = U256::from(timestamp);
            b.basefee = base_fee;
        })
        .modify_tx_chained(|tx| {
            tx.caller = from;
            tx.kind = to;
            tx.value = value;
            tx.data = input;
            tx.gas_limit = gas_limit;
            tx.gas_price = base_fee as u128;
        });

    let mut evm = ctx.build_mainnet();

    evm.replay().map_err(|e| format!("EVM execution failed: {:?}", e))
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::{Address, U256};
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
        assert!(gas <= U64::from(30000));
    }

    #[tokio::test]
    async fn test_eth_send_raw_transaction_stub() {
        let storage = create_test_storage();

        let invalid_data = Bytes::from(vec![0x01, 0x02, 0x03]);

        let result = eth_send_raw_transaction(invalid_data, test_ctx(&storage)).await;
        assert!(result.is_error());
    }
}
