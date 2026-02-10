//! State query handlers.
//!
//! These handlers query account state from hot storage:
//! - `eth_getBalance` - Account balance
//! - `eth_getTransactionCount` - Account nonce
//! - `eth_getCode` - Contract bytecode
//! - `eth_getStorageAt` - Contract storage slot

use crate::error::{RpcError, RpcResult};
use alloy::primitives::{Address, Bytes, B256, U256, U64};
use signet_hot::{HotKv, db::HotDbRead};
use signet_storage::UnifiedStorage;

/// Handler for `eth_getBalance`.
///
/// Returns the balance of the account at the given address.
/// Returns 0 for non-existent accounts.
///
/// # Parameters
/// - `address`: The address to query
/// - `_block`: Block identifier (ignored, always uses latest state)
pub async fn eth_get_balance<H: HotKv>(
    storage: &UnifiedStorage<H>,
    address: Address,
    _block: Option<String>,
) -> RpcResult<U256> {
    let reader = storage
        .reader()
        .map_err(|e| RpcError::internal(format!("Failed to create reader: {e}")))?;

    let account = reader
        .get_account(&address)
        .map_err(|e| RpcError::internal(format!("Failed to read account: {e}")))?;

    Ok(account.map(|a| a.balance).unwrap_or_default())
}

/// Handler for `eth_getTransactionCount`.
///
/// Returns the number of transactions sent from the given address (nonce).
/// Returns 0 for non-existent accounts.
///
/// # Parameters
/// - `address`: The address to query
/// - `_block`: Block identifier (ignored, always uses latest state)
pub async fn eth_get_transaction_count<H: HotKv>(
    storage: &UnifiedStorage<H>,
    address: Address,
    _block: Option<String>,
) -> RpcResult<U64> {
    let reader = storage
        .reader()
        .map_err(|e| RpcError::internal(format!("Failed to create reader: {e}")))?;

    let account = reader
        .get_account(&address)
        .map_err(|e| RpcError::internal(format!("Failed to read account: {e}")))?;

    Ok(U64::from(account.map(|a| a.nonce).unwrap_or_default()))
}

/// Handler for `eth_getCode`.
///
/// Returns the bytecode at the given address.
/// Returns empty bytes for non-existent accounts or EOAs.
///
/// # Parameters
/// - `address`: The address to query
/// - `_block`: Block identifier (ignored, always uses latest state)
pub async fn eth_get_code<H: HotKv>(
    storage: &UnifiedStorage<H>,
    address: Address,
    _block: Option<String>,
) -> RpcResult<Bytes> {
    let reader = storage
        .reader()
        .map_err(|e| RpcError::internal(format!("Failed to create reader: {e}")))?;

    // First get the account to find its bytecode hash
    let account = reader
        .get_account(&address)
        .map_err(|e| RpcError::internal(format!("Failed to read account: {e}")))?;

    let Some(account) = account else {
        return Ok(Bytes::default());
    };

    let Some(code_hash) = account.bytecode_hash else {
        return Ok(Bytes::default());
    };

    // Fetch the actual bytecode
    let bytecode = reader
        .get_bytecode(&code_hash)
        .map_err(|e| RpcError::internal(format!("Failed to read bytecode: {e}")))?;

    Ok(bytecode
        .map(|bc| Bytes::copy_from_slice(bc.original_byte_slice()))
        .unwrap_or_default())
}

/// Handler for `eth_getStorageAt`.
///
/// Returns the value from a storage position at a given address.
/// Returns zero for non-existent accounts or unset storage slots.
///
/// # Parameters
/// - `address`: The address to query
/// - `slot`: The storage slot position
/// - `_block`: Block identifier (ignored, always uses latest state)
pub async fn eth_get_storage_at<H: HotKv>(
    storage: &UnifiedStorage<H>,
    address: Address,
    slot: B256,
    _block: Option<String>,
) -> RpcResult<B256> {
    let reader = storage
        .reader()
        .map_err(|e| RpcError::internal(format!("Failed to create reader: {e}")))?;

    // Convert B256 slot to U256 for the query
    let slot_u256 = U256::from_be_bytes(slot.0);

    let value = reader
        .get_storage(&address, &slot_u256)
        .map_err(|e| RpcError::internal(format!("Failed to read storage: {e}")))?;

    // Convert back to B256 for the response
    Ok(B256::from(value.unwrap_or_default().to_be_bytes()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::{Address, B256, U256};
    use signet_cold::ColdStorageHandle;
    use signet_hot::{
        mem::MemKv,
        model::{HotKv as _, HotKvWrite},
        tables::{Bytecodes, PlainAccountState, PlainStorageState},
    };
    use signet_storage::UnifiedStorage;
    use signet_storage_types::Account;
    use std::sync::Arc;
    use trevm::revm::bytecode::Bytecode;

    fn create_test_storage() -> (Arc<UnifiedStorage<MemKv>>, ColdStorageHandle) {
        let mem_kv = MemKv::default();
        let (cold_handle, _cold_task) = ColdStorageHandle::new_null();
        let storage = Arc::new(UnifiedStorage::new(mem_kv, cold_handle.clone()));
        (storage, cold_handle)
    }

    #[tokio::test]
    async fn test_get_balance_missing_account() {
        let (storage, _) = create_test_storage();
        let address = Address::from_slice(&[0x1; 20]);

        let result = eth_get_balance(&storage, address, None).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), U256::ZERO);
    }

    #[tokio::test]
    async fn test_get_balance_existing_account() {
        let (storage, _) = create_test_storage();
        let address = Address::from_slice(&[0x1; 20]);
        let expected_balance = U256::from(1000u64);

        // Write account data
        {
            let writer = storage.hot().writer().unwrap();
            let account = Account {
                nonce: 0,
                balance: expected_balance,
                bytecode_hash: None,
            };
            writer.queue_put::<PlainAccountState>(&address, &account).unwrap();
            writer.raw_commit().unwrap();
        }

        let result = eth_get_balance(&storage, address, None).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), expected_balance);
    }

    #[tokio::test]
    async fn test_get_transaction_count_missing_account() {
        let (storage, _) = create_test_storage();
        let address = Address::from_slice(&[0x1; 20]);

        let result = eth_get_transaction_count(&storage, address, None).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), U64::ZERO);
    }

    #[tokio::test]
    async fn test_get_transaction_count_existing_account() {
        let (storage, _) = create_test_storage();
        let address = Address::from_slice(&[0x1; 20]);
        let expected_nonce = 42u64;

        // Write account data
        {
            let writer = storage.hot().writer().unwrap();
            let account = Account {
                nonce: expected_nonce,
                balance: U256::ZERO,
                bytecode_hash: None,
            };
            writer.queue_put::<PlainAccountState>(&address, &account).unwrap();
            writer.raw_commit().unwrap();
        }

        let result = eth_get_transaction_count(&storage, address, None).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), U64::from(expected_nonce));
    }

    #[tokio::test]
    async fn test_get_code_missing_account() {
        let (storage, _) = create_test_storage();
        let address = Address::from_slice(&[0x1; 20]);

        let result = eth_get_code(&storage, address, None).await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_get_code_eoa() {
        let (storage, _) = create_test_storage();
        let address = Address::from_slice(&[0x1; 20]);

        // Write EOA (no bytecode)
        {
            let writer = storage.hot().writer().unwrap();
            let account = Account {
                nonce: 1,
                balance: U256::from(1000u64),
                bytecode_hash: None,
            };
            writer.queue_put::<PlainAccountState>(&address, &account).unwrap();
            writer.raw_commit().unwrap();
        }

        let result = eth_get_code(&storage, address, None).await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_get_code_contract() {
        let (storage, _) = create_test_storage();
        let address = Address::from_slice(&[0x1; 20]);
        let code_bytes = vec![0x60, 0x80, 0x60, 0x40, 0x52]; // PUSH1 0x80 PUSH1 0x40 MSTORE
        let code_hash = B256::from_slice(&alloy::primitives::keccak256(&code_bytes).0);

        // Write contract account and bytecode
        {
            let writer = storage.hot().writer().unwrap();
            let account = Account {
                nonce: 1,
                balance: U256::ZERO,
                bytecode_hash: Some(code_hash),
            };
            writer.queue_put::<PlainAccountState>(&address, &account).unwrap();

            let bytecode = Bytecode::new_raw(code_bytes.clone().into());
            writer.queue_put::<Bytecodes>(&code_hash, &bytecode).unwrap();
            writer.raw_commit().unwrap();
        }

        let result = eth_get_code(&storage, address, None).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap().as_ref(), &code_bytes);
    }

    #[tokio::test]
    async fn test_get_storage_at_missing_account() {
        let (storage, _) = create_test_storage();
        let address = Address::from_slice(&[0x1; 20]);
        let slot = B256::from_slice(&[0x2; 32]);

        let result = eth_get_storage_at(&storage, address, slot, None).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), B256::ZERO);
    }

    #[tokio::test]
    async fn test_get_storage_at_existing_slot() {
        let (storage, _) = create_test_storage();
        let address = Address::from_slice(&[0x1; 20]);
        let slot = B256::from_slice(&[0x0; 32]);
        let expected_value = U256::from(12345u64);

        // Write storage data
        {
            let writer = storage.hot().writer().unwrap();
            let slot_u256 = U256::from_be_bytes(slot.0);
            writer
                .queue_put_dual::<PlainStorageState>(&address, &slot_u256, &expected_value)
                .unwrap();
            writer.raw_commit().unwrap();
        }

        let result = eth_get_storage_at(&storage, address, slot, None).await;
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap(),
            B256::from(expected_value.to_be_bytes())
        );
    }
}
