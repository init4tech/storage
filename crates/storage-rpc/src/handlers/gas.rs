//! Gas price handlers.
//!
//! These handlers provide gas price information:
//! - `eth_gasPrice` - Current gas price suggestion
//! - `eth_maxPriorityFeePerGas` - Current priority fee suggestion

use crate::error::{RpcError, RpcResult};
use alloy::primitives::U256;
use signet_hot::{HotKv, db::HistoryRead};
use signet_storage::UnifiedStorage;

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
pub async fn eth_gas_price<H: HotKv>(storage: &UnifiedStorage<H>) -> RpcResult<U256> {
    let reader = storage
        .reader()
        .map_err(|e| RpcError::internal(format!("Failed to create reader: {e}")))?;

    // Get the latest block header to read base fee
    let header = reader
        .last_header()
        .map_err(|e| RpcError::internal(format!("Failed to read header: {e}")))?;

    let gas_price = match header {
        Some(header) => {
            // Use base_fee_per_gas + suggested priority fee
            let base_fee = header.base_fee_per_gas.unwrap_or(DEFAULT_MIN_GAS_PRICE);
            let priority_fee = DEFAULT_PRIORITY_FEE;
            U256::from(base_fee.saturating_add(priority_fee))
        }
        None => {
            // No blocks yet, return default minimum
            U256::from(DEFAULT_MIN_GAS_PRICE)
        }
    };

    Ok(gas_price)
}

/// Handler for `eth_maxPriorityFeePerGas`.
///
/// Returns the suggested priority fee (tip) for EIP-1559 transactions.
/// This is a simplified implementation that returns a fixed default value.
///
/// A more sophisticated implementation would analyze recent blocks to
/// calculate an optimal priority fee based on network congestion.
pub async fn eth_max_priority_fee_per_gas<H: HotKv>(
    _storage: &UnifiedStorage<H>,
) -> RpcResult<U256> {
    // Return a fixed default priority fee
    // In production, this would analyze recent block priority fees
    Ok(U256::from(DEFAULT_PRIORITY_FEE))
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::consensus::Header;
    use signet_cold::ColdStorageHandle;
    use signet_hot::{
        mem::MemKv,
        model::{HotKv as _, HotKvWrite},
        tables::Headers,
    };
    use signet_storage::UnifiedStorage;
    use std::sync::Arc;

    fn create_test_storage() -> Arc<UnifiedStorage<MemKv>> {
        let mem_kv = MemKv::default();
        let (cold_handle, _cold_task) = ColdStorageHandle::new_null();
        Arc::new(UnifiedStorage::new(mem_kv, cold_handle))
    }

    #[tokio::test]
    async fn test_gas_price_no_blocks() {
        let storage = create_test_storage();

        let result = eth_gas_price(&storage).await;
        assert!(result.is_ok());
        // Should return default minimum gas price
        assert_eq!(result.unwrap(), U256::from(DEFAULT_MIN_GAS_PRICE));
    }

    #[tokio::test]
    async fn test_gas_price_with_block() {
        let storage = create_test_storage();
        let base_fee = 2_000_000_000u128; // 2 gwei

        // Write a header with base fee
        {
            let writer = storage.hot().writer().unwrap();
            let mut header = Header::default();
            header.base_fee_per_gas = Some(base_fee);
            writer.queue_put::<Headers>(&0u64, &header).unwrap();
            writer.raw_commit().unwrap();
        }

        let result = eth_gas_price(&storage).await;
        assert!(result.is_ok());
        // Should return base_fee + priority_fee
        assert_eq!(
            result.unwrap(),
            U256::from(base_fee + DEFAULT_PRIORITY_FEE)
        );
    }

    #[tokio::test]
    async fn test_max_priority_fee_per_gas() {
        let storage = create_test_storage();

        let result = eth_max_priority_fee_per_gas(&storage).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), U256::from(DEFAULT_PRIORITY_FEE));
    }
}
