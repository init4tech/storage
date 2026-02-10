//! Gas price handlers.
//!
//! These handlers provide gas price information:
//! - `eth_gasPrice` - Current gas price suggestion
//! - `eth_maxPriorityFeePerGas` - Current priority fee suggestion

use crate::error::RpcResult;
use crate::router::RpcContext;
use ajj::ResponsePayload;
use alloy::primitives::U256;
use signet_hot::{HotKv, db::HistoryRead};
use std::borrow::Cow;

/// Default minimum gas price in wei (1 gwei).
const DEFAULT_MIN_GAS_PRICE: u64 = 1_000_000_000;

/// Default priority fee suggestion in wei (1 gwei).
const DEFAULT_PRIORITY_FEE: u64 = 1_000_000_000;

/// Handler for `eth_gasPrice`.
///
/// Returns the current gas price based on the latest block's base fee.
/// This is a simplified implementation that uses base_fee + priority_fee.
///
/// For EIP-1559 transactions, users should use `eth_maxPriorityFeePerGas`
/// and the base fee from the latest block header.
pub(crate) async fn eth_gas_price<H: HotKv>(state: RpcContext<H>) -> RpcResult<U256> {
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

    let gas_price = match header {
        Some(header) => {
            let base_fee = header.base_fee_per_gas.unwrap_or(DEFAULT_MIN_GAS_PRICE);
            let priority_fee = DEFAULT_PRIORITY_FEE;
            U256::from(base_fee.saturating_add(priority_fee))
        }
        None => U256::from(DEFAULT_MIN_GAS_PRICE),
    };

    ResponsePayload(Ok(gas_price))
}

/// Handler for `eth_maxPriorityFeePerGas`.
///
/// Returns the suggested priority fee (tip) for EIP-1559 transactions.
/// This is a simplified implementation that returns a fixed default value.
///
/// A more sophisticated implementation would analyze recent blocks to
/// calculate an optimal priority fee based on network congestion.
pub(crate) async fn eth_max_priority_fee_per_gas<H: HotKv>(
    _state: RpcContext<H>,
) -> RpcResult<U256> {
    ResponsePayload(Ok(U256::from(DEFAULT_PRIORITY_FEE)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::consensus::Header;
    use signet_cold::{ColdStorageTask, mem::MemColdBackend};
    use signet_hot::{mem::MemKv, model::HotKvWrite, tables::Headers};
    use signet_storage::UnifiedStorage;
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
    async fn test_gas_price_no_blocks() {
        let storage = create_test_storage();
        let result = eth_gas_price(test_ctx(&storage)).await;
        assert_eq!(result.0.unwrap(), U256::from(DEFAULT_MIN_GAS_PRICE));
    }

    #[tokio::test]
    async fn test_gas_price_with_block() {
        let storage = create_test_storage();
        let base_fee = 2_000_000_000u64; // 2 gwei

        {
            let writer = storage.hot().writer().unwrap();
            let header = Header { base_fee_per_gas: Some(base_fee), ..Default::default() };
            writer.queue_put::<Headers>(&0u64, &header).unwrap();
            writer.raw_commit().unwrap();
        }

        let result = eth_gas_price(test_ctx(&storage)).await;
        assert_eq!(result.0.unwrap(), U256::from(base_fee.saturating_add(DEFAULT_PRIORITY_FEE)));
    }

    #[tokio::test]
    async fn test_max_priority_fee_per_gas() {
        let storage = create_test_storage();
        let result = eth_max_priority_fee_per_gas(test_ctx(&storage)).await;
        assert_eq!(result.0.unwrap(), U256::from(DEFAULT_PRIORITY_FEE));
    }
}
