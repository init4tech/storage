//! Column name constants for SQL row extraction.
//!
//! These constants are used with `sqlx::Row::get()` to extract values from
//! query results. Centralising them here prevents typos in string literals
//! that would otherwise only surface as runtime panics.

// ── shared / index columns ──────────────────────────────────────────────────
pub(crate) const COL_BLOCK_NUMBER: &str = "block_number";
pub(crate) const COL_BLOCK_HASH: &str = "block_hash";
pub(crate) const COL_TX_INDEX: &str = "tx_index";
pub(crate) const COL_TX_HASH: &str = "tx_hash";

// ── header columns ──────────────────────────────────────────────────────────
pub(crate) const COL_PARENT_HASH: &str = "parent_hash";
pub(crate) const COL_OMMERS_HASH: &str = "ommers_hash";
pub(crate) const COL_BENEFICIARY: &str = "beneficiary";
pub(crate) const COL_STATE_ROOT: &str = "state_root";
pub(crate) const COL_TRANSACTIONS_ROOT: &str = "transactions_root";
pub(crate) const COL_RECEIPTS_ROOT: &str = "receipts_root";
pub(crate) const COL_LOGS_BLOOM: &str = "logs_bloom";
pub(crate) const COL_DIFFICULTY: &str = "difficulty";
pub(crate) const COL_GAS_LIMIT: &str = "gas_limit";
pub(crate) const COL_GAS_USED: &str = "gas_used";
pub(crate) const COL_TIMESTAMP: &str = "timestamp";
pub(crate) const COL_EXTRA_DATA: &str = "extra_data";
pub(crate) const COL_MIX_HASH: &str = "mix_hash";
pub(crate) const COL_NONCE: &str = "nonce";
pub(crate) const COL_BASE_FEE_PER_GAS: &str = "base_fee_per_gas";
pub(crate) const COL_WITHDRAWALS_ROOT: &str = "withdrawals_root";
pub(crate) const COL_BLOB_GAS_USED: &str = "blob_gas_used";
pub(crate) const COL_EXCESS_BLOB_GAS: &str = "excess_blob_gas";
pub(crate) const COL_PARENT_BEACON_BLOCK_ROOT: &str = "parent_beacon_block_root";
pub(crate) const COL_REQUESTS_HASH: &str = "requests_hash";

// ── transaction columns ─────────────────────────────────────────────────────
pub(crate) const COL_TX_TYPE: &str = "tx_type";
pub(crate) const COL_SIG_R: &str = "sig_r";
pub(crate) const COL_SIG_S: &str = "sig_s";
pub(crate) const COL_SIG_Y_PARITY: &str = "sig_y_parity";
pub(crate) const COL_CHAIN_ID: &str = "chain_id";
pub(crate) const COL_TO_ADDRESS: &str = "to_address";
pub(crate) const COL_VALUE: &str = "value";
pub(crate) const COL_INPUT: &str = "input";
pub(crate) const COL_GAS_PRICE: &str = "gas_price";
pub(crate) const COL_MAX_FEE_PER_GAS: &str = "max_fee_per_gas";
pub(crate) const COL_MAX_PRIORITY_FEE_PER_GAS: &str = "max_priority_fee_per_gas";
pub(crate) const COL_MAX_FEE_PER_BLOB_GAS: &str = "max_fee_per_blob_gas";
pub(crate) const COL_BLOB_VERSIONED_HASHES: &str = "blob_versioned_hashes";
pub(crate) const COL_ACCESS_LIST: &str = "access_list";
pub(crate) const COL_AUTHORIZATION_LIST: &str = "authorization_list";
pub(crate) const COL_FROM_ADDRESS: &str = "from_address";

// ── receipt columns ─────────────────────────────────────────────────────────
pub(crate) const COL_SUCCESS: &str = "success";
pub(crate) const COL_CUMULATIVE_GAS_USED: &str = "cumulative_gas_used";
pub(crate) const COL_FIRST_LOG_INDEX: &str = "first_log_index";

// ── log columns ─────────────────────────────────────────────────────────────
pub(crate) const COL_ADDRESS: &str = "address";
pub(crate) const COL_DATA: &str = "data";
pub(crate) const COL_TOPIC0: &str = "topic0";
pub(crate) const COL_TOPIC1: &str = "topic1";
pub(crate) const COL_TOPIC2: &str = "topic2";
pub(crate) const COL_TOPIC3: &str = "topic3";

// ── signet event columns ────────────────────────────────────────────────────
pub(crate) const COL_EVENT_TYPE: &str = "event_type";
pub(crate) const COL_ORDER_INDEX: &str = "order_index";
pub(crate) const COL_ROLLUP_CHAIN_ID: &str = "rollup_chain_id";
pub(crate) const COL_SENDER: &str = "sender";
pub(crate) const COL_GAS: &str = "gas";
pub(crate) const COL_ROLLUP_RECIPIENT: &str = "rollup_recipient";
pub(crate) const COL_AMOUNT: &str = "amount";
pub(crate) const COL_TOKEN: &str = "token";

// ── zenith header columns ───────────────────────────────────────────────────
pub(crate) const COL_HOST_BLOCK_NUMBER: &str = "host_block_number";
pub(crate) const COL_REWARD_ADDRESS: &str = "reward_address";
pub(crate) const COL_BLOCK_DATA_HASH: &str = "block_data_hash";

// ── query-specific aliases ──────────────────────────────────────────────────
pub(crate) const COL_CNT: &str = "cnt";
pub(crate) const COL_PRIOR_GAS: &str = "prior_gas";
pub(crate) const COL_BLOCK_LOG_INDEX: &str = "block_log_index";
pub(crate) const COL_BLOCK_TIMESTAMP: &str = "block_timestamp";
pub(crate) const COL_MAX_BN: &str = "max_bn";
