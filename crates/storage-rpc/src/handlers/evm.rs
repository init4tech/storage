//! EVM execution handlers.
//!
//! These handlers execute EVM operations using the revm database adapter:
//! - `eth_call` - Execute a read-only EVM call
//! - `eth_estimateGas` - Estimate gas for a transaction
//! - `eth_sendRawTransaction` - Submit a raw transaction (stub)

use crate::error::{RpcError, RpcResult};
use alloy::{
    consensus::TxEnvelope,
    primitives::{Bytes, TxKind, U64, U256},
    rlp::Decodable,
    rpc::types::TransactionRequest,
};
use signet_hot::{HotKv, db::HistoryRead, model::HotKvRead};
use signet_storage::UnifiedStorage;
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
pub async fn eth_call<H: HotKv>(
    storage: &UnifiedStorage<H>,
    call: TransactionRequest,
    _block: Option<String>,
) -> RpcResult<Bytes>
where
    <<H as HotKv>::RoTx as HotKvRead>::Error: DBErrorMarker,
{
    // Get revm database adapter
    let db = storage
        .revm_reader()
        .map_err(|e| RpcError::internal(format!("Failed to create revm reader: {e}")))?;

    // Get latest block info for EVM context
    let reader = storage
        .reader()
        .map_err(|e| RpcError::internal(format!("Failed to create reader: {e}")))?;
    let header = reader
        .last_header()
        .map_err(|e| RpcError::internal(format!("Failed to read header: {e}")))?;

    // Build and execute EVM
    let result = execute_call(db, &call, header.as_ref())?;

    match result.result {
        ExecutionResult::Success { output, .. } => Ok(Bytes::copy_from_slice(output.data())),
        ExecutionResult::Revert { output, .. } => {
            // Return revert data as error
            Err(RpcError::with_data(
                crate::error::ErrorCode::InternalError,
                "execution reverted",
                format!("0x{}", alloy::hex::encode(&output)),
            ))
        }
        ExecutionResult::Halt { reason, .. } => {
            Err(RpcError::internal(format!("execution halted: {:?}", reason)))
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
pub async fn eth_estimate_gas<H: HotKv>(
    storage: &UnifiedStorage<H>,
    call: TransactionRequest,
    _block: Option<String>,
) -> RpcResult<U64>
where
    <<H as HotKv>::RoTx as HotKvRead>::Error: DBErrorMarker,
{
    // Get latest block info
    let reader = storage
        .reader()
        .map_err(|e| RpcError::internal(format!("Failed to create reader: {e}")))?;
    let header = reader
        .last_header()
        .map_err(|e| RpcError::internal(format!("Failed to read header: {e}")))?;

    // Determine gas limits for binary search
    let user_gas_limit = call.gas.unwrap_or(DEFAULT_GAS_LIMIT);
    let mut low = MIN_GAS;
    let mut high = user_gas_limit;
    // First, try with maximum gas to ensure the transaction can succeed
    let best_estimate = {
        let db = storage
            .revm_reader()
            .map_err(|e| RpcError::internal(format!("Failed to create revm reader: {e}")))?;

        let mut test_call = call.clone();
        test_call.gas = Some(high);

        let result = execute_call(db, &test_call, header.as_ref())?;

        match result.result {
            ExecutionResult::Success { gas_used, .. } => gas_used,
            ExecutionResult::Revert { output, .. } => {
                return Err(RpcError::with_data(
                    crate::error::ErrorCode::InternalError,
                    "execution reverted",
                    format!("0x{}", alloy::hex::encode(&output)),
                ));
            }
            ExecutionResult::Halt { reason, .. } => {
                return Err(RpcError::internal(format!("execution halted: {:?}", reason)));
            }
        }
    };

    // Binary search for minimum gas
    // Start with the gas actually used as the high bound
    high = best_estimate;
    low = low.min(high);

    while low < high {
        let mid = (low + high) / 2;

        let db = storage
            .revm_reader()
            .map_err(|e| RpcError::internal(format!("Failed to create revm reader: {e}")))?;

        let mut test_call = call.clone();
        test_call.gas = Some(mid);

        let result = execute_call(db, &test_call, header.as_ref())?;

        match result.result {
            ExecutionResult::Success { .. } => {
                // Can succeed with this gas, try lower
                high = mid;
            }
            ExecutionResult::Revert { .. } | ExecutionResult::Halt { .. } => {
                // Need more gas
                low = mid + 1;
            }
        }
    }

    // Add a small buffer (10%) to account for execution variance
    let estimate = high.saturating_add(high / 10);
    Ok(U64::from(estimate.min(user_gas_limit)))
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
pub async fn eth_send_raw_transaction<H: HotKv>(
    _storage: &UnifiedStorage<H>,
    data: Bytes,
) -> RpcResult<alloy::primitives::B256> {
    // Decode the transaction to validate it and extract the hash
    let tx = TxEnvelope::decode(&mut data.as_ref())
        .map_err(|e| RpcError::invalid_params(format!("invalid transaction: {e}")))?;

    let tx_hash = *tx.tx_hash();

    // In production, this would forward to a tx-cache or mempool
    // For now, we reject with a clear message
    Err(RpcError::with_data(
        crate::error::ErrorCode::MethodNotSupported,
        "transaction submission not yet implemented",
        format!("tx_hash: {tx_hash}"),
    ))
}

/// Execute an EVM call with the given database and call parameters.
fn execute_call<D: Database>(
    db: D,
    call: &TransactionRequest,
    header: Option<&alloy::consensus::Header>,
) -> RpcResult<ResultAndState<HaltReason>>
where
    D::Error: core::fmt::Debug,
{
    // Extract call parameters
    let from = call.from.unwrap_or_default();
    let to = call.to.unwrap_or(TxKind::Create);
    let value = call.value.unwrap_or_default();
    let input = call.input.input().cloned().unwrap_or_default();
    let gas_limit = call.gas.unwrap_or(DEFAULT_GAS_LIMIT);

    // Get block context from header
    let (block_number, timestamp, base_fee) = match header {
        Some(h) => (h.number, h.timestamp, h.base_fee_per_gas.unwrap_or(0)),
        None => (0, 0, 0),
    };

    // Build EVM with revm 22 API using Context builder pattern
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
            // Set gas price equal to base fee to pass validation
            tx.gas_price = base_fee as u128;
        });

    let mut evm = ctx.build_mainnet();

    evm.replay().map_err(|e| RpcError::internal(format!("EVM execution failed: {:?}", e)))
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

    #[tokio::test]
    async fn test_eth_call_simple() {
        let storage = create_test_storage();
        let address = Address::from_slice(&[0x1; 20]);

        // Create an account with some balance
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

        // Simple call with no data (balance check essentially)
        let call = TransactionRequest {
            from: Some(address),
            to: Some(TxKind::Call(address)),
            value: Some(U256::ZERO),
            ..Default::default()
        };

        let result = eth_call(&storage, call, None).await;
        // Should succeed with empty output (no contract code)
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_eth_estimate_gas_simple() {
        let storage = create_test_storage();
        let from = Address::from_slice(&[0x1; 20]);
        let to = Address::from_slice(&[0x2; 20]);

        // Create sender account with balance
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

        // Estimate gas for simple transfer
        let call = TransactionRequest {
            from: Some(from),
            to: Some(TxKind::Call(to)),
            value: Some(U256::from(1000u64)),
            ..Default::default()
        };

        let result = eth_estimate_gas(&storage, call, None).await;
        assert!(result.is_ok());

        let gas = result.unwrap();
        // Should be around 21000 for simple transfer (+ buffer)
        assert!(gas >= U64::from(21000));
        assert!(gas <= U64::from(30000)); // With 10% buffer
    }

    #[tokio::test]
    async fn test_eth_send_raw_transaction_stub() {
        let storage = create_test_storage();

        // Create a minimal valid transaction bytes (this will fail decoding)
        let invalid_data = Bytes::from(vec![0x01, 0x02, 0x03]);

        let result = eth_send_raw_transaction(&storage, invalid_data).await;
        // Should fail because data isn't valid RLP
        assert!(result.is_err());
    }
}
