//! State query handlers.
//!
//! These handlers query account state from hot storage:
//! - `eth_getBalance` - Account balance
//! - `eth_getTransactionCount` - Account nonce
//! - `eth_getCode` - Contract bytecode
//! - `eth_getStorageAt` - Contract storage slot

use crate::error::RpcResult;
use crate::router::RpcContext;
use ajj::ResponsePayload;
use alloy::primitives::{Address, B256, Bytes, U64, U256};
use signet_hot::{HotKv, db::HotDbRead};
use std::borrow::Cow;

/// Handler for `eth_getBalance`.
///
/// Returns the balance of the account at the given address.
/// Returns 0 for non-existent accounts.
///
/// # Parameters
/// - `address`: The address to query
/// - `_block`: Block identifier (ignored, always uses latest state)
pub(crate) async fn eth_get_balance<H: HotKv>(
    (address, _block): (Address, String),
    state: RpcContext<H>,
) -> RpcResult<U256> {
    let reader = match state.storage.reader() {
        Ok(r) => r,
        Err(e) => {
            return ResponsePayload::internal_error_message(Cow::Owned(format!(
                "Failed to create reader: {e}"
            )));
        }
    };

    let account = match reader.get_account(&address) {
        Ok(a) => a,
        Err(e) => {
            return ResponsePayload::internal_error_message(Cow::Owned(format!(
                "Failed to read account: {e}"
            )));
        }
    };

    ResponsePayload(Ok(account.map(|a| a.balance).unwrap_or_default()))
}

/// Handler for `eth_getTransactionCount`.
///
/// Returns the number of transactions sent from the given address (nonce).
/// Returns 0 for non-existent accounts.
///
/// # Parameters
/// - `address`: The address to query
/// - `_block`: Block identifier (ignored, always uses latest state)
pub(crate) async fn eth_get_transaction_count<H: HotKv>(
    (address, _block): (Address, String),
    state: RpcContext<H>,
) -> RpcResult<U64> {
    let reader = match state.storage.reader() {
        Ok(r) => r,
        Err(e) => {
            return ResponsePayload::internal_error_message(Cow::Owned(format!(
                "Failed to create reader: {e}"
            )));
        }
    };

    let account = match reader.get_account(&address) {
        Ok(a) => a,
        Err(e) => {
            return ResponsePayload::internal_error_message(Cow::Owned(format!(
                "Failed to read account: {e}"
            )));
        }
    };

    ResponsePayload(Ok(U64::from(account.map(|a| a.nonce).unwrap_or_default())))
}

/// Handler for `eth_getCode`.
///
/// Returns the bytecode at the given address.
/// Returns empty bytes for non-existent accounts or EOAs.
///
/// # Parameters
/// - `address`: The address to query
/// - `_block`: Block identifier (ignored, always uses latest state)
pub(crate) async fn eth_get_code<H: HotKv>(
    (address, _block): (Address, String),
    state: RpcContext<H>,
) -> RpcResult<Bytes> {
    let reader = match state.storage.reader() {
        Ok(r) => r,
        Err(e) => {
            return ResponsePayload::internal_error_message(Cow::Owned(format!(
                "Failed to create reader: {e}"
            )));
        }
    };

    let account = match reader.get_account(&address) {
        Ok(a) => a,
        Err(e) => {
            return ResponsePayload::internal_error_message(Cow::Owned(format!(
                "Failed to read account: {e}"
            )));
        }
    };

    let Some(account) = account else {
        return ResponsePayload(Ok(Bytes::default()));
    };

    let Some(code_hash) = account.bytecode_hash else {
        return ResponsePayload(Ok(Bytes::default()));
    };

    let bytecode = match reader.get_bytecode(&code_hash) {
        Ok(b) => b,
        Err(e) => {
            return ResponsePayload::internal_error_message(Cow::Owned(format!(
                "Failed to read bytecode: {e}"
            )));
        }
    };

    ResponsePayload(Ok(bytecode
        .map(|bc| Bytes::copy_from_slice(bc.original_byte_slice()))
        .unwrap_or_default()))
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
pub(crate) async fn eth_get_storage_at<H: HotKv>(
    (address, slot, _block): (Address, B256, String),
    state: RpcContext<H>,
) -> RpcResult<B256> {
    let reader = match state.storage.reader() {
        Ok(r) => r,
        Err(e) => {
            return ResponsePayload::internal_error_message(Cow::Owned(format!(
                "Failed to create reader: {e}"
            )));
        }
    };

    let slot_u256 = U256::from_be_bytes(slot.0);

    let value = match reader.get_storage(&address, &slot_u256) {
        Ok(v) => v,
        Err(e) => {
            return ResponsePayload::internal_error_message(Cow::Owned(format!(
                "Failed to read storage: {e}"
            )));
        }
    };

    ResponsePayload(Ok(B256::from(value.unwrap_or_default().to_be_bytes())))
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use alloy::primitives::{Address, B256, U256};
    use signet_cold::{ColdStorageTask, mem::MemColdBackend};
    use signet_hot::{
        mem::MemKv,
        model::HotKvWrite,
        tables::{Bytecodes, PlainAccountState, PlainStorageState},
    };
    use signet_storage::UnifiedStorage;
    use signet_storage_types::Account;
    use std::sync::Arc;
    use tokio_util::sync::CancellationToken;
    use trevm::revm::bytecode::Bytecode;

    pub(crate) fn create_test_storage() -> Arc<UnifiedStorage<MemKv>> {
        let mem_kv = MemKv::default();
        let cold_handle = ColdStorageTask::spawn(MemColdBackend::new(), CancellationToken::new());
        Arc::new(UnifiedStorage::new(mem_kv, cold_handle))
    }

    fn test_ctx(storage: &Arc<UnifiedStorage<MemKv>>) -> RpcContext<MemKv> {
        RpcContext { storage: storage.clone(), chain_id: 31337 }
    }

    #[tokio::test]
    async fn test_get_balance_missing_account() {
        let storage = create_test_storage();
        let address = Address::from_slice(&[0x1; 20]);

        let result = eth_get_balance((address, "latest".into()), test_ctx(&storage)).await;
        assert_eq!(result.0.unwrap(), U256::ZERO);
    }

    #[tokio::test]
    async fn test_get_balance_existing_account() {
        let storage = create_test_storage();
        let address = Address::from_slice(&[0x1; 20]);
        let expected_balance = U256::from(1000u64);

        {
            let writer = storage.hot().writer().unwrap();
            let account = Account { nonce: 0, balance: expected_balance, bytecode_hash: None };
            writer.queue_put::<PlainAccountState>(&address, &account).unwrap();
            writer.raw_commit().unwrap();
        }

        let result = eth_get_balance((address, "latest".into()), test_ctx(&storage)).await;
        assert_eq!(result.0.unwrap(), expected_balance);
    }

    #[tokio::test]
    async fn test_get_transaction_count_missing_account() {
        let storage = create_test_storage();
        let address = Address::from_slice(&[0x1; 20]);

        let result =
            eth_get_transaction_count((address, "latest".into()), test_ctx(&storage)).await;
        assert_eq!(result.0.unwrap(), U64::ZERO);
    }

    #[tokio::test]
    async fn test_get_transaction_count_existing_account() {
        let storage = create_test_storage();
        let address = Address::from_slice(&[0x1; 20]);
        let expected_nonce = 42u64;

        {
            let writer = storage.hot().writer().unwrap();
            let account =
                Account { nonce: expected_nonce, balance: U256::ZERO, bytecode_hash: None };
            writer.queue_put::<PlainAccountState>(&address, &account).unwrap();
            writer.raw_commit().unwrap();
        }

        let result =
            eth_get_transaction_count((address, "latest".into()), test_ctx(&storage)).await;
        assert_eq!(result.0.unwrap(), U64::from(expected_nonce));
    }

    #[tokio::test]
    async fn test_get_code_missing_account() {
        let storage = create_test_storage();
        let address = Address::from_slice(&[0x1; 20]);

        let result = eth_get_code((address, "latest".into()), test_ctx(&storage)).await;
        assert!(result.0.unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_get_code_eoa() {
        let storage = create_test_storage();
        let address = Address::from_slice(&[0x1; 20]);

        {
            let writer = storage.hot().writer().unwrap();
            let account = Account { nonce: 1, balance: U256::from(1000u64), bytecode_hash: None };
            writer.queue_put::<PlainAccountState>(&address, &account).unwrap();
            writer.raw_commit().unwrap();
        }

        let result = eth_get_code((address, "latest".into()), test_ctx(&storage)).await;
        assert!(result.0.unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_get_code_contract() {
        let storage = create_test_storage();
        let address = Address::from_slice(&[0x1; 20]);
        let code_bytes = vec![0x60, 0x80, 0x60, 0x40, 0x52]; // PUSH1 0x80 PUSH1 0x40 MSTORE
        let code_hash = B256::from_slice(&alloy::primitives::keccak256(&code_bytes).0);

        {
            let writer = storage.hot().writer().unwrap();
            let account = Account { nonce: 1, balance: U256::ZERO, bytecode_hash: Some(code_hash) };
            writer.queue_put::<PlainAccountState>(&address, &account).unwrap();

            let bytecode = Bytecode::new_raw(code_bytes.clone().into());
            writer.queue_put::<Bytecodes>(&code_hash, &bytecode).unwrap();
            writer.raw_commit().unwrap();
        }

        let result = eth_get_code((address, "latest".into()), test_ctx(&storage)).await;
        assert_eq!(result.0.unwrap().as_ref(), &code_bytes);
    }

    #[tokio::test]
    async fn test_get_storage_at_missing_account() {
        let storage = create_test_storage();
        let address = Address::from_slice(&[0x1; 20]);
        let slot = B256::from_slice(&[0x2; 32]);

        let result = eth_get_storage_at((address, slot, "latest".into()), test_ctx(&storage)).await;
        assert_eq!(result.0.unwrap(), B256::ZERO);
    }

    #[tokio::test]
    async fn test_get_storage_at_existing_slot() {
        let storage = create_test_storage();
        let address = Address::from_slice(&[0x1; 20]);
        let slot = B256::from_slice(&[0x0; 32]);
        let expected_value = U256::from(12345u64);

        {
            let writer = storage.hot().writer().unwrap();
            let slot_u256 = U256::from_be_bytes(slot.0);
            writer
                .queue_put_dual::<PlainStorageState>(&address, &slot_u256, &expected_value)
                .unwrap();
            writer.raw_commit().unwrap();
        }

        let result = eth_get_storage_at((address, slot, "latest".into()), test_ctx(&storage)).await;
        assert_eq!(result.0.unwrap(), B256::from(expected_value.to_be_bytes()));
    }
}
