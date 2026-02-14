//! Unified SQL backend for cold storage.
//!
//! Supports both PostgreSQL and SQLite via [`sqlx::Any`]. The backend
//! auto-detects the database type at construction time and runs the
//! appropriate migration.

use crate::SqlColdError;
use crate::convert::{
    EVENT_ENTER, EVENT_ENTER_TOKEN, EVENT_TRANSACT, build_receipt, decode_access_list_or_empty,
    decode_authorization_list, decode_b256_vec, decode_u128_required, decode_u256,
    encode_access_list, encode_authorization_list, encode_b256_vec, encode_u128, encode_u256,
    from_address, from_i64, to_address, to_i64,
};
use alloy::{
    consensus::{
        Header, Signed, TxEip1559, TxEip2930, TxEip4844, TxEip7702, TxLegacy, TxType,
        transaction::Recovered,
    },
    primitives::{Address, B256, BlockNumber, Bloom, Bytes, Log, LogData, Sealable, Signature},
};
use signet_cold::{
    BlockData, ColdReceipt, ColdResult, ColdStorage, ColdStorageError, Confirmed, Filter,
    HeaderSpecifier, ReceiptSpecifier, RpcLog, SignetEventsSpecifier, TransactionSpecifier,
    ZenithHeaderSpecifier,
};
use signet_storage_types::{
    ConfirmationMeta, DbSignetEvent, DbZenithHeader, IndexedReceipt, RecoveredTx, SealedHeader,
    TransactionSigned,
};
use signet_zenith::{
    Passage::{Enter, EnterToken},
    Transactor::Transact,
    Zenith,
};
use sqlx::{AnyPool, Row};

/// SQL-based cold storage backend.
///
/// Uses [`sqlx::Any`] for database-agnostic access, supporting both
/// PostgreSQL and SQLite through a single implementation. The backend
/// is determined by the connection URL at construction time.
///
/// # Example
///
/// ```no_run
/// # async fn example() {
/// use signet_cold_sql::SqlColdBackend;
///
/// // SQLite (in-memory)
/// let backend = SqlColdBackend::connect("sqlite::memory:").await.unwrap();
///
/// // PostgreSQL
/// let backend = SqlColdBackend::connect("postgres://localhost/signet").await.unwrap();
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct SqlColdBackend {
    pool: AnyPool,
}

impl SqlColdBackend {
    /// Create a new SQL cold storage backend from an existing [`AnyPool`].
    ///
    /// Auto-detects the database backend and creates all tables if they
    /// do not already exist. Callers must ensure
    /// [`sqlx::any::install_default_drivers`] has been called before
    /// constructing the pool.
    pub async fn new(pool: AnyPool) -> Result<Self, SqlColdError> {
        // Detect backend from a pooled connection.
        let conn = pool.acquire().await?;
        let backend = conn.backend_name().to_owned();
        drop(conn);

        let migration = match backend.as_str() {
            "PostgreSQL" => include_str!("../migrations/001_initial_pg.sql"),
            "SQLite" => include_str!("../migrations/001_initial.sql"),
            other => {
                return Err(SqlColdError::Convert(format!(
                    "unsupported database backend: {other}"
                )));
            }
        };
        // Execute via pool to ensure the migration uses the same
        // connection that subsequent queries will use.
        sqlx::raw_sql(migration).execute(&pool).await?;
        Ok(Self { pool })
    }

    /// Connect to a database URL and create the backend.
    ///
    /// Installs the default sqlx drivers on the first call. The database
    /// type is inferred from the URL scheme (`sqlite:` or `postgres:`).
    ///
    /// For SQLite in-memory databases (`sqlite::memory:`), the pool is
    /// limited to one connection to ensure all operations share the same
    /// database.
    pub async fn connect(url: &str) -> Result<Self, SqlColdError> {
        sqlx::any::install_default_drivers();
        let pool: AnyPool = sqlx::pool::PoolOptions::new().max_connections(1).connect(url).await?;
        Self::new(pool).await
    }

    // ========================================================================
    // Specifier resolution
    // ========================================================================

    async fn resolve_header_spec(
        &self,
        spec: HeaderSpecifier,
    ) -> Result<Option<BlockNumber>, SqlColdError> {
        match spec {
            HeaderSpecifier::Number(n) => Ok(Some(n)),
            HeaderSpecifier::Hash(hash) => {
                let hash_bytes = hash.as_slice();
                let row = sqlx::query("SELECT block_number FROM headers WHERE block_hash = $1")
                    .bind(hash_bytes)
                    .fetch_optional(&self.pool)
                    .await?;
                Ok(row.map(|r| from_i64(r.get::<i64, _>("block_number"))))
            }
        }
    }

    // ========================================================================
    // Read helpers
    // ========================================================================

    async fn fetch_header_by_number(
        &self,
        block_num: BlockNumber,
    ) -> Result<Option<SealedHeader>, SqlColdError> {
        let bn = to_i64(block_num);
        let row = sqlx::query("SELECT * FROM headers WHERE block_number = $1")
            .bind(bn)
            .fetch_optional(&self.pool)
            .await?;

        row.map(|r| header_from_row(&r).map(|h| h.seal_slow())).transpose()
    }

    // ========================================================================
    // Write helpers
    // ========================================================================

    async fn insert_block(&self, data: BlockData) -> Result<(), SqlColdError> {
        let mut tx = self.pool.begin().await?;
        write_block_to_tx(&mut tx, data).await?;
        tx.commit().await?;
        Ok(())
    }
}

// ============================================================================
// Row → domain type conversion (read path)
// ============================================================================

/// Build a [`Header`] from an [`sqlx::any::AnyRow`].
///
/// Accepts optional column-name overrides for `gas_limit` and `nonce`,
/// which conflict in multi-table JOINs.
fn header_from_row_with(
    r: &sqlx::any::AnyRow,
    gas_limit_col: &str,
    nonce_col: &str,
) -> Result<Header, SqlColdError> {
    let blob = |col: &str| -> Vec<u8> { r.get(col) };
    let opt_blob = |col: &str| -> Option<Vec<u8>> { r.get(col) };

    Ok(Header {
        parent_hash: B256::from_slice(&blob("parent_hash")),
        ommers_hash: B256::from_slice(&blob("ommers_hash")),
        beneficiary: Address::from_slice(&blob("beneficiary")),
        state_root: B256::from_slice(&blob("state_root")),
        transactions_root: B256::from_slice(&blob("transactions_root")),
        receipts_root: B256::from_slice(&blob("receipts_root")),
        logs_bloom: Bloom::from_slice(&blob("logs_bloom")),
        difficulty: decode_u256(&blob("difficulty"))?,
        number: from_i64(r.get("block_number")),
        gas_limit: from_i64(r.get(gas_limit_col)),
        gas_used: from_i64(r.get("gas_used")),
        timestamp: from_i64(r.get("timestamp")),
        extra_data: Bytes::from(blob("extra_data")),
        mix_hash: B256::from_slice(&blob("mix_hash")),
        nonce: alloy::primitives::B64::from_slice(&blob(nonce_col)),
        base_fee_per_gas: r.get::<Option<i64>, _>("base_fee_per_gas").map(from_i64),
        withdrawals_root: opt_blob("withdrawals_root").map(|b| B256::from_slice(&b)),
        blob_gas_used: r.get::<Option<i64>, _>("blob_gas_used").map(from_i64),
        excess_blob_gas: r.get::<Option<i64>, _>("excess_blob_gas").map(from_i64),
        parent_beacon_block_root: opt_blob("parent_beacon_block_root")
            .map(|b| B256::from_slice(&b)),
        requests_hash: opt_blob("requests_hash").map(|b| B256::from_slice(&b)),
    })
}

/// Build a [`Header`] from a standard `SELECT * FROM headers` row.
fn header_from_row(r: &sqlx::any::AnyRow) -> Result<Header, SqlColdError> {
    header_from_row_with(r, "gas_limit", "nonce")
}

/// Build a [`TransactionSigned`] from an [`sqlx::any::AnyRow`].
///
/// Accepts optional column-name overrides for `tx_type`, `nonce`, and
/// `gas_limit`, which conflict in multi-table JOINs.
fn tx_from_row_with(
    r: &sqlx::any::AnyRow,
    tx_type_col: &str,
    nonce_col: &str,
    gas_limit_col: &str,
) -> Result<TransactionSigned, SqlColdError> {
    use alloy::consensus::EthereumTxEnvelope;

    let opt_blob = |col: &str| -> Option<Vec<u8>> { r.get(col) };

    let sig = Signature::new(
        decode_u256(&r.get::<Vec<u8>, _>("sig_r"))?,
        decode_u256(&r.get::<Vec<u8>, _>("sig_s"))?,
        r.get::<i32, _>("sig_y_parity") != 0,
    );

    let tx_type_raw = r.get::<i32, _>(tx_type_col) as u8;
    let tx_type = TxType::try_from(tx_type_raw)
        .map_err(|_| SqlColdError::Convert(format!("invalid tx_type: {tx_type_raw}")))?;

    let chain_id: Option<i64> = r.get("chain_id");
    let nonce = from_i64(r.get(nonce_col));
    let gas_limit = from_i64(r.get(gas_limit_col));
    let to_addr = opt_blob("to_address");
    let value = decode_u256(&r.get::<Vec<u8>, _>("value"))?;
    let input = Bytes::from(r.get::<Vec<u8>, _>("input"));

    match tx_type {
        TxType::Legacy => {
            let tx = TxLegacy {
                chain_id: chain_id.map(from_i64),
                nonce,
                gas_price: decode_u128_required(&opt_blob("gas_price"), "gas_price")?,
                gas_limit,
                to: from_address(to_addr.as_deref()),
                value,
                input,
            };
            Ok(EthereumTxEnvelope::Legacy(Signed::new_unhashed(tx, sig)))
        }
        TxType::Eip2930 => {
            let tx = TxEip2930 {
                chain_id: from_i64(chain_id.unwrap_or(0)),
                nonce,
                gas_price: decode_u128_required(&opt_blob("gas_price"), "gas_price")?,
                gas_limit,
                to: from_address(to_addr.as_deref()),
                value,
                input,
                access_list: decode_access_list_or_empty(&opt_blob("access_list"))?,
            };
            Ok(EthereumTxEnvelope::Eip2930(Signed::new_unhashed(tx, sig)))
        }
        TxType::Eip1559 => {
            let tx = TxEip1559 {
                chain_id: from_i64(chain_id.unwrap_or(0)),
                nonce,
                gas_limit,
                max_fee_per_gas: decode_u128_required(
                    &opt_blob("max_fee_per_gas"),
                    "max_fee_per_gas",
                )?,
                max_priority_fee_per_gas: decode_u128_required(
                    &opt_blob("max_priority_fee_per_gas"),
                    "max_priority_fee_per_gas",
                )?,
                to: from_address(to_addr.as_deref()),
                value,
                input,
                access_list: decode_access_list_or_empty(&opt_blob("access_list"))?,
            };
            Ok(EthereumTxEnvelope::Eip1559(Signed::new_unhashed(tx, sig)))
        }
        TxType::Eip4844 => {
            let tx =
                TxEip4844 {
                    chain_id: from_i64(chain_id.unwrap_or(0)),
                    nonce,
                    gas_limit,
                    max_fee_per_gas: decode_u128_required(
                        &opt_blob("max_fee_per_gas"),
                        "max_fee_per_gas",
                    )?,
                    max_priority_fee_per_gas: decode_u128_required(
                        &opt_blob("max_priority_fee_per_gas"),
                        "max_priority_fee_per_gas",
                    )?,
                    to: Address::from_slice(to_addr.as_deref().ok_or_else(|| {
                        SqlColdError::Convert("EIP4844 requires to_address".into())
                    })?),
                    value,
                    input,
                    access_list: decode_access_list_or_empty(&opt_blob("access_list"))?,
                    blob_versioned_hashes: decode_b256_vec(
                        opt_blob("blob_versioned_hashes").as_deref().unwrap_or_default(),
                    ),
                    max_fee_per_blob_gas: decode_u128_required(
                        &opt_blob("max_fee_per_blob_gas"),
                        "max_fee_per_blob_gas",
                    )?,
                };
            Ok(EthereumTxEnvelope::Eip4844(Signed::new_unhashed(tx, sig)))
        }
        TxType::Eip7702 => {
            let tx =
                TxEip7702 {
                    chain_id: from_i64(chain_id.unwrap_or(0)),
                    nonce,
                    gas_limit,
                    max_fee_per_gas: decode_u128_required(
                        &opt_blob("max_fee_per_gas"),
                        "max_fee_per_gas",
                    )?,
                    max_priority_fee_per_gas: decode_u128_required(
                        &opt_blob("max_priority_fee_per_gas"),
                        "max_priority_fee_per_gas",
                    )?,
                    to: Address::from_slice(to_addr.as_deref().ok_or_else(|| {
                        SqlColdError::Convert("EIP7702 requires to_address".into())
                    })?),
                    value,
                    input,
                    access_list: decode_access_list_or_empty(&opt_blob("access_list"))?,
                    authorization_list: decode_authorization_list(
                        opt_blob("authorization_list").as_deref().unwrap_or_default(),
                    )?,
                };
            Ok(EthereumTxEnvelope::Eip7702(Signed::new_unhashed(tx, sig)))
        }
    }
}

/// Build a [`TransactionSigned`] from a standard `SELECT * FROM transactions` row.
fn tx_from_row(r: &sqlx::any::AnyRow) -> Result<TransactionSigned, SqlColdError> {
    tx_from_row_with(r, "tx_type", "nonce", "gas_limit")
}

/// Build a [`RecoveredTx`] from a row that includes `from_address`.
fn recovered_tx_from_row(r: &sqlx::any::AnyRow) -> Result<RecoveredTx, SqlColdError> {
    let sender = Address::from_slice(&r.get::<Vec<u8>, _>("from_address"));
    let tx = tx_from_row(r)?;
    // SAFETY: the sender was recovered at append time and stored in from_address.
    Ok(Recovered::new_unchecked(tx, sender))
}

/// Build a [`Log`] from an [`sqlx::any::AnyRow`].
fn log_from_row(r: &sqlx::any::AnyRow) -> Log {
    let topics = ["topic0", "topic1", "topic2", "topic3"]
        .into_iter()
        .filter_map(|col| r.get::<Option<Vec<u8>>, _>(col))
        .map(|t| B256::from_slice(&t))
        .collect();
    Log {
        address: Address::from_slice(&r.get::<Vec<u8>, _>("address")),
        data: LogData::new_unchecked(topics, Bytes::from(r.get::<Vec<u8>, _>("data"))),
    }
}

/// Build a [`DbSignetEvent`] from an [`sqlx::any::AnyRow`].
fn signet_event_from_row(r: &sqlx::any::AnyRow) -> Result<DbSignetEvent, SqlColdError> {
    let opt_blob = |col: &str| -> Option<Vec<u8>> { r.get(col) };

    let event_type = r.get::<i32, _>("event_type") as i16;
    let order = from_i64(r.get("order_index"));
    let rollup_chain_id = decode_u256(&r.get::<Vec<u8>, _>("rollup_chain_id"))?;

    match event_type {
        EVENT_TRANSACT => {
            let sender = Address::from_slice(
                opt_blob("sender")
                    .as_deref()
                    .ok_or_else(|| SqlColdError::Convert("Transact requires sender".into()))?,
            );
            let to = Address::from_slice(
                opt_blob("to_address")
                    .as_deref()
                    .ok_or_else(|| SqlColdError::Convert("Transact requires to".into()))?,
            );
            let value = decode_u256(
                opt_blob("value")
                    .as_deref()
                    .ok_or_else(|| SqlColdError::Convert("Transact requires value".into()))?,
            )?;
            let gas = decode_u256(
                opt_blob("gas")
                    .as_deref()
                    .ok_or_else(|| SqlColdError::Convert("Transact requires gas".into()))?,
            )?;
            let max_fee =
                decode_u256(opt_blob("max_fee_per_gas").as_deref().ok_or_else(|| {
                    SqlColdError::Convert("Transact requires max_fee_per_gas".into())
                })?)?;
            let data = Bytes::from(opt_blob("data").unwrap_or_default());

            Ok(DbSignetEvent::Transact(
                order,
                Transact {
                    rollupChainId: rollup_chain_id,
                    sender,
                    to,
                    value,
                    gas,
                    maxFeePerGas: max_fee,
                    data,
                },
            ))
        }
        EVENT_ENTER => {
            let recipient =
                Address::from_slice(opt_blob("rollup_recipient").as_deref().ok_or_else(|| {
                    SqlColdError::Convert("Enter requires rollup_recipient".into())
                })?);
            let amount = decode_u256(
                opt_blob("amount")
                    .as_deref()
                    .ok_or_else(|| SqlColdError::Convert("Enter requires amount".into()))?,
            )?;

            Ok(DbSignetEvent::Enter(
                order,
                Enter { rollupChainId: rollup_chain_id, rollupRecipient: recipient, amount },
            ))
        }
        EVENT_ENTER_TOKEN => {
            let token = Address::from_slice(
                opt_blob("token")
                    .as_deref()
                    .ok_or_else(|| SqlColdError::Convert("EnterToken requires token".into()))?,
            );
            let recipient =
                Address::from_slice(opt_blob("rollup_recipient").as_deref().ok_or_else(|| {
                    SqlColdError::Convert("EnterToken requires rollup_recipient".into())
                })?);
            let amount = decode_u256(
                opt_blob("amount")
                    .as_deref()
                    .ok_or_else(|| SqlColdError::Convert("EnterToken requires amount".into()))?,
            )?;

            Ok(DbSignetEvent::EnterToken(
                order,
                EnterToken {
                    rollupChainId: rollup_chain_id,
                    token,
                    rollupRecipient: recipient,
                    amount,
                },
            ))
        }
        _ => Err(SqlColdError::Convert(format!("invalid event_type: {event_type}"))),
    }
}

/// Build a [`DbZenithHeader`] from an [`sqlx::any::AnyRow`].
fn zenith_header_from_row(r: &sqlx::any::AnyRow) -> Result<DbZenithHeader, SqlColdError> {
    let blob = |col: &str| -> Vec<u8> { r.get(col) };

    Ok(DbZenithHeader(Zenith::BlockHeader {
        hostBlockNumber: decode_u256(&blob("host_block_number"))?,
        rollupChainId: decode_u256(&blob("rollup_chain_id"))?,
        gasLimit: decode_u256(&blob("gas_limit"))?,
        rewardAddress: Address::from_slice(&blob("reward_address")),
        blockDataHash: alloy::primitives::FixedBytes::<32>::from_slice(&blob("block_data_hash")),
    }))
}

// ============================================================================
// Domain type → SQL INSERT (write path)
// ============================================================================

/// Write a single block's data into an open SQL transaction.
async fn write_block_to_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Any>,
    data: BlockData,
) -> Result<(), SqlColdError> {
    let bn = to_i64(data.block_number());

    // Insert header
    let block_hash = data.header.hash_slow();
    let difficulty = encode_u256(&data.header.difficulty);
    sqlx::query(
        "INSERT INTO headers (
            block_number, block_hash, parent_hash, ommers_hash, beneficiary,
            state_root, transactions_root, receipts_root, logs_bloom, difficulty,
            gas_limit, gas_used, timestamp, extra_data, mix_hash, nonce,
            base_fee_per_gas, withdrawals_root, blob_gas_used, excess_blob_gas,
            parent_beacon_block_root, requests_hash
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
            $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22
        )",
    )
    .bind(bn)
    .bind(block_hash.as_slice())
    .bind(data.header.parent_hash.as_slice())
    .bind(data.header.ommers_hash.as_slice())
    .bind(data.header.beneficiary.as_slice())
    .bind(data.header.state_root.as_slice())
    .bind(data.header.transactions_root.as_slice())
    .bind(data.header.receipts_root.as_slice())
    .bind(data.header.logs_bloom.as_slice())
    .bind(difficulty.as_slice())
    .bind(to_i64(data.header.gas_limit))
    .bind(to_i64(data.header.gas_used))
    .bind(to_i64(data.header.timestamp))
    .bind(data.header.extra_data.as_ref())
    .bind(data.header.mix_hash.as_slice())
    .bind(data.header.nonce.as_slice())
    .bind(data.header.base_fee_per_gas.map(to_i64))
    .bind(data.header.withdrawals_root.as_ref().map(|r| r.as_slice()))
    .bind(data.header.blob_gas_used.map(to_i64))
    .bind(data.header.excess_blob_gas.map(to_i64))
    .bind(data.header.parent_beacon_block_root.as_ref().map(|r| r.as_slice()))
    .bind(data.header.requests_hash.as_ref().map(|r| r.as_slice()))
    .execute(&mut **tx)
    .await?;

    // Insert transactions
    for (idx, recovered_tx) in data.transactions.iter().enumerate() {
        insert_transaction(tx, bn, to_i64(idx as u64), recovered_tx).await?;
    }

    // Insert receipts and logs
    for (idx, receipt) in data.receipts.iter().enumerate() {
        let tx_idx = to_i64(idx as u64);
        sqlx::query(
            "INSERT INTO receipts (block_number, tx_index, tx_type, success, cumulative_gas_used)
             VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(bn)
        .bind(tx_idx)
        .bind(receipt.tx_type as i32)
        .bind(receipt.inner.status.coerce_status() as i32)
        .bind(to_i64(receipt.inner.cumulative_gas_used))
        .execute(&mut **tx)
        .await?;

        for (log_idx, log) in receipt.inner.logs.iter().enumerate() {
            let topics = log.topics();
            sqlx::query(
                "INSERT INTO logs (block_number, tx_index, log_index, address, topic0, topic1, topic2, topic3, data)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
            )
            .bind(bn)
            .bind(tx_idx)
            .bind(to_i64(log_idx as u64))
            .bind(log.address.as_slice())
            .bind(topics.first().map(|t| t.as_slice()))
            .bind(topics.get(1).map(|t| t.as_slice()))
            .bind(topics.get(2).map(|t| t.as_slice()))
            .bind(topics.get(3).map(|t| t.as_slice()))
            .bind(log.data.data.as_ref())
            .execute(&mut **tx)
            .await?;
        }
    }

    // Insert signet events
    for (idx, event) in data.signet_events.iter().enumerate() {
        insert_signet_event(tx, bn, to_i64(idx as u64), event).await?;
    }

    // Insert zenith header
    if let Some(zh) = &data.zenith_header {
        let h = &zh.0;
        let host_bn = encode_u256(&h.hostBlockNumber);
        let chain_id = encode_u256(&h.rollupChainId);
        let gas_limit = encode_u256(&h.gasLimit);
        sqlx::query(
            "INSERT INTO zenith_headers (
                block_number, host_block_number, rollup_chain_id,
                gas_limit, reward_address, block_data_hash
            ) VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind(bn)
        .bind(host_bn.as_slice())
        .bind(chain_id.as_slice())
        .bind(gas_limit.as_slice())
        .bind(h.rewardAddress.as_slice())
        .bind(h.blockDataHash.as_slice())
        .execute(&mut **tx)
        .await?;
    }

    Ok(())
}

/// Insert a transaction, binding directly from the source type.
async fn insert_transaction(
    conn: &mut sqlx::AnyConnection,
    bn: i64,
    tx_index: i64,
    recovered: &RecoveredTx,
) -> Result<(), SqlColdError> {
    use alloy::consensus::EthereumTxEnvelope;

    let sender = recovered.signer();
    let tx: &TransactionSigned = recovered;
    let tx_hash = tx.tx_hash();
    let tx_type = tx.tx_type() as i32;

    macro_rules! sig {
        ($s:expr) => {{
            let sig = $s.signature();
            (sig.v() as i32, encode_u256(&sig.r()), encode_u256(&sig.s()))
        }};
    }
    let (sig_y, sig_r, sig_s) = match tx {
        EthereumTxEnvelope::Legacy(s) => sig!(s),
        EthereumTxEnvelope::Eip2930(s) => sig!(s),
        EthereumTxEnvelope::Eip1559(s) => sig!(s),
        EthereumTxEnvelope::Eip4844(s) => sig!(s),
        EthereumTxEnvelope::Eip7702(s) => sig!(s),
    };

    let (chain_id, nonce, gas_limit) = match tx {
        EthereumTxEnvelope::Legacy(s) => {
            (s.tx().chain_id.map(to_i64), to_i64(s.tx().nonce), to_i64(s.tx().gas_limit))
        }
        EthereumTxEnvelope::Eip2930(s) => {
            (Some(to_i64(s.tx().chain_id)), to_i64(s.tx().nonce), to_i64(s.tx().gas_limit))
        }
        EthereumTxEnvelope::Eip1559(s) => {
            (Some(to_i64(s.tx().chain_id)), to_i64(s.tx().nonce), to_i64(s.tx().gas_limit))
        }
        EthereumTxEnvelope::Eip4844(s) => {
            (Some(to_i64(s.tx().chain_id)), to_i64(s.tx().nonce), to_i64(s.tx().gas_limit))
        }
        EthereumTxEnvelope::Eip7702(s) => {
            (Some(to_i64(s.tx().chain_id)), to_i64(s.tx().nonce), to_i64(s.tx().gas_limit))
        }
    };

    let (value, to_addr) = match tx {
        EthereumTxEnvelope::Legacy(s) => (encode_u256(&s.tx().value), to_address(&s.tx().to)),
        EthereumTxEnvelope::Eip2930(s) => (encode_u256(&s.tx().value), to_address(&s.tx().to)),
        EthereumTxEnvelope::Eip1559(s) => (encode_u256(&s.tx().value), to_address(&s.tx().to)),
        EthereumTxEnvelope::Eip4844(s) => {
            (encode_u256(&s.tx().value), Some(s.tx().to.as_slice().to_vec()))
        }
        EthereumTxEnvelope::Eip7702(s) => {
            (encode_u256(&s.tx().value), Some(s.tx().to.as_slice().to_vec()))
        }
    };

    let input: &[u8] = match tx {
        EthereumTxEnvelope::Legacy(s) => s.tx().input.as_ref(),
        EthereumTxEnvelope::Eip2930(s) => s.tx().input.as_ref(),
        EthereumTxEnvelope::Eip1559(s) => s.tx().input.as_ref(),
        EthereumTxEnvelope::Eip4844(s) => s.tx().input.as_ref(),
        EthereumTxEnvelope::Eip7702(s) => s.tx().input.as_ref(),
    };

    let (gas_price, max_fee, max_priority_fee, max_blob_fee) = match tx {
        EthereumTxEnvelope::Legacy(s) => (Some(encode_u128(s.tx().gas_price)), None, None, None),
        EthereumTxEnvelope::Eip2930(s) => (Some(encode_u128(s.tx().gas_price)), None, None, None),
        EthereumTxEnvelope::Eip1559(s) => (
            None,
            Some(encode_u128(s.tx().max_fee_per_gas)),
            Some(encode_u128(s.tx().max_priority_fee_per_gas)),
            None,
        ),
        EthereumTxEnvelope::Eip4844(s) => (
            None,
            Some(encode_u128(s.tx().max_fee_per_gas)),
            Some(encode_u128(s.tx().max_priority_fee_per_gas)),
            Some(encode_u128(s.tx().max_fee_per_blob_gas)),
        ),
        EthereumTxEnvelope::Eip7702(s) => (
            None,
            Some(encode_u128(s.tx().max_fee_per_gas)),
            Some(encode_u128(s.tx().max_priority_fee_per_gas)),
            None,
        ),
    };

    let (access_list, blob_hashes, auth_list) = match tx {
        EthereumTxEnvelope::Legacy(_) => (None, None, None),
        EthereumTxEnvelope::Eip2930(s) => {
            (Some(encode_access_list(&s.tx().access_list)), None, None)
        }
        EthereumTxEnvelope::Eip1559(s) => {
            (Some(encode_access_list(&s.tx().access_list)), None, None)
        }
        EthereumTxEnvelope::Eip4844(s) => (
            Some(encode_access_list(&s.tx().access_list)),
            Some(encode_b256_vec(&s.tx().blob_versioned_hashes)),
            None,
        ),
        EthereumTxEnvelope::Eip7702(s) => (
            Some(encode_access_list(&s.tx().access_list)),
            None,
            Some(encode_authorization_list(&s.tx().authorization_list)),
        ),
    };

    sqlx::query(
        "INSERT INTO transactions (
            block_number, tx_index, tx_hash, tx_type,
            sig_y_parity, sig_r, sig_s,
            chain_id, nonce, gas_limit, to_address, value, input,
            gas_price, max_fee_per_gas, max_priority_fee_per_gas,
            max_fee_per_blob_gas, blob_versioned_hashes,
            access_list, authorization_list, from_address
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
            $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21
        )",
    )
    .bind(bn)
    .bind(tx_index)
    .bind(tx_hash.as_slice())
    .bind(tx_type)
    .bind(sig_y)
    .bind(sig_r.as_slice())
    .bind(sig_s.as_slice())
    .bind(chain_id)
    .bind(nonce)
    .bind(gas_limit)
    .bind(to_addr.as_deref())
    .bind(value.as_slice())
    .bind(input)
    .bind(gas_price.as_ref().map(|v| v.as_slice()))
    .bind(max_fee.as_ref().map(|v| v.as_slice()))
    .bind(max_priority_fee.as_ref().map(|v| v.as_slice()))
    .bind(max_blob_fee.as_ref().map(|v| v.as_slice()))
    .bind(blob_hashes.as_deref())
    .bind(access_list.as_deref())
    .bind(auth_list.as_deref())
    .bind(sender.as_slice())
    .execute(&mut *conn)
    .await?;

    Ok(())
}

/// Insert a signet event, binding directly from the source type.
async fn insert_signet_event(
    conn: &mut sqlx::AnyConnection,
    block_number: i64,
    event_index: i64,
    event: &DbSignetEvent,
) -> Result<(), SqlColdError> {
    let (event_type, order, chain_id) = match event {
        DbSignetEvent::Transact(o, t) => (0i32, to_i64(*o), encode_u256(&t.rollupChainId)),
        DbSignetEvent::Enter(o, e) => (1i32, to_i64(*o), encode_u256(&e.rollupChainId)),
        DbSignetEvent::EnterToken(o, e) => (2i32, to_i64(*o), encode_u256(&e.rollupChainId)),
    };

    let (value, gas, max_fee, amount) = match event {
        DbSignetEvent::Transact(_, t) => (
            Some(encode_u256(&t.value)),
            Some(encode_u256(&t.gas)),
            Some(encode_u256(&t.maxFeePerGas)),
            None,
        ),
        DbSignetEvent::Enter(_, e) => (None, None, None, Some(encode_u256(&e.amount))),
        DbSignetEvent::EnterToken(_, e) => (None, None, None, Some(encode_u256(&e.amount))),
    };

    sqlx::query(
        "INSERT INTO signet_events (
            block_number, event_index, event_type, order_index,
            rollup_chain_id, sender, to_address, value, gas,
            max_fee_per_gas, data, rollup_recipient, amount, token
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)",
    )
    .bind(block_number)
    .bind(event_index)
    .bind(event_type)
    .bind(order)
    .bind(chain_id.as_slice())
    .bind(match event {
        DbSignetEvent::Transact(_, t) => Some(t.sender.as_slice()),
        _ => None,
    })
    .bind(match event {
        DbSignetEvent::Transact(_, t) => Some(t.to.as_slice()),
        _ => None,
    })
    .bind(value.as_ref().map(|v| v.as_slice()))
    .bind(gas.as_ref().map(|v| v.as_slice()))
    .bind(max_fee.as_ref().map(|v| v.as_slice()))
    .bind(match event {
        DbSignetEvent::Transact(_, t) => Some(t.data.as_ref()),
        _ => None,
    })
    .bind(match event {
        DbSignetEvent::Enter(_, e) => Some(e.rollupRecipient.as_slice()),
        DbSignetEvent::EnterToken(_, e) => Some(e.rollupRecipient.as_slice()),
        _ => None,
    })
    .bind(amount.as_ref().map(|v| v.as_slice()))
    .bind(match event {
        DbSignetEvent::EnterToken(_, e) => Some(e.token.as_slice()),
        _ => None,
    })
    .execute(&mut *conn)
    .await?;

    Ok(())
}

// ============================================================================
// ColdStorage implementation
// ============================================================================

impl ColdStorage for SqlColdBackend {
    async fn get_header(&self, spec: HeaderSpecifier) -> ColdResult<Option<SealedHeader>> {
        let Some(block_num) = self.resolve_header_spec(spec).await? else {
            return Ok(None);
        };
        self.fetch_header_by_number(block_num).await.map_err(ColdStorageError::from)
    }

    async fn get_headers(
        &self,
        specs: Vec<HeaderSpecifier>,
    ) -> ColdResult<Vec<Option<SealedHeader>>> {
        let mut results = Vec::with_capacity(specs.len());
        for spec in specs {
            let header = self.get_header(spec).await?;
            results.push(header);
        }
        Ok(results)
    }

    async fn get_transaction(
        &self,
        spec: TransactionSpecifier,
    ) -> ColdResult<Option<Confirmed<RecoveredTx>>> {
        let row = match spec {
            TransactionSpecifier::Hash(hash) => sqlx::query(
                "SELECT t.*, h.block_hash
                     FROM transactions t
                     JOIN headers h ON t.block_number = h.block_number
                     WHERE t.tx_hash = $1",
            )
            .bind(hash.as_slice())
            .fetch_optional(&self.pool)
            .await
            .map_err(SqlColdError::from)?,
            TransactionSpecifier::BlockAndIndex { block, index } => sqlx::query(
                "SELECT t.*, h.block_hash
                     FROM transactions t
                     JOIN headers h ON t.block_number = h.block_number
                     WHERE t.block_number = $1 AND t.tx_index = $2",
            )
            .bind(to_i64(block))
            .bind(to_i64(index))
            .fetch_optional(&self.pool)
            .await
            .map_err(SqlColdError::from)?,
            TransactionSpecifier::BlockHashAndIndex { block_hash, index } => sqlx::query(
                "SELECT t.*, h.block_hash
                     FROM transactions t
                     JOIN headers h ON t.block_number = h.block_number
                     WHERE h.block_hash = $1 AND t.tx_index = $2",
            )
            .bind(block_hash.as_slice())
            .bind(to_i64(index))
            .fetch_optional(&self.pool)
            .await
            .map_err(SqlColdError::from)?,
        };

        let Some(r) = row else {
            return Ok(None);
        };

        let block = from_i64(r.get::<i64, _>("block_number"));
        let index = from_i64(r.get::<i64, _>("tx_index"));
        let hash_bytes: Vec<u8> = r.get("block_hash");
        let block_hash = B256::from_slice(&hash_bytes);
        let recovered = recovered_tx_from_row(&r).map_err(ColdStorageError::from)?;
        let meta = ConfirmationMeta::new(block, block_hash, index);
        Ok(Some(Confirmed::new(recovered, meta)))
    }

    async fn get_transactions_in_block(&self, block: BlockNumber) -> ColdResult<Vec<RecoveredTx>> {
        let bn = to_i64(block);
        let rows =
            sqlx::query("SELECT * FROM transactions WHERE block_number = $1 ORDER BY tx_index")
                .bind(bn)
                .fetch_all(&self.pool)
                .await
                .map_err(SqlColdError::from)?;

        rows.iter().map(|r| recovered_tx_from_row(r).map_err(ColdStorageError::from)).collect()
    }

    async fn get_transaction_count(&self, block: BlockNumber) -> ColdResult<u64> {
        let bn = to_i64(block);
        let row = sqlx::query("SELECT COUNT(*) as cnt FROM transactions WHERE block_number = $1")
            .bind(bn)
            .fetch_one(&self.pool)
            .await
            .map_err(SqlColdError::from)?;

        Ok(from_i64(row.get::<i64, _>("cnt")))
    }

    async fn get_receipt(&self, spec: ReceiptSpecifier) -> ColdResult<Option<ColdReceipt>> {
        // Resolve to (block, index)
        let (block, index) = match spec {
            ReceiptSpecifier::TxHash(hash) => {
                let row = sqlx::query(
                    "SELECT block_number, tx_index FROM transactions WHERE tx_hash = $1",
                )
                .bind(hash.as_slice())
                .fetch_optional(&self.pool)
                .await
                .map_err(SqlColdError::from)?;
                let Some(r) = row else { return Ok(None) };
                (from_i64(r.get::<i64, _>("block_number")), from_i64(r.get::<i64, _>("tx_index")))
            }
            ReceiptSpecifier::BlockAndIndex { block, index } => (block, index),
        };

        let Some(header) = self.fetch_header_by_number(block).await? else {
            return Ok(None);
        };

        // Fetch receipt + tx_hash + from_address
        let receipt_row = sqlx::query(
            "SELECT r.*, t.tx_hash, t.from_address
             FROM receipts r
             JOIN transactions t ON r.block_number = t.block_number AND r.tx_index = t.tx_index
             WHERE r.block_number = $1 AND r.tx_index = $2",
        )
        .bind(to_i64(block))
        .bind(to_i64(index))
        .fetch_optional(&self.pool)
        .await
        .map_err(SqlColdError::from)?;

        let Some(rr) = receipt_row else {
            return Ok(None);
        };

        let bn: i64 = rr.get("block_number");
        let tx_idx: i64 = rr.get("tx_index");
        let tx_hash = B256::from_slice(&rr.get::<Vec<u8>, _>("tx_hash"));
        let sender = Address::from_slice(&rr.get::<Vec<u8>, _>("from_address"));
        let tx_type = rr.get::<i32, _>("tx_type") as i16;
        let success = rr.get::<i32, _>("success") != 0;
        let cumulative_gas_used: i64 = rr.get("cumulative_gas_used");

        let log_rows = sqlx::query(
            "SELECT * FROM logs WHERE block_number = $1 AND tx_index = $2 ORDER BY log_index",
        )
        .bind(bn)
        .bind(tx_idx)
        .fetch_all(&self.pool)
        .await
        .map_err(SqlColdError::from)?;

        let logs = log_rows.iter().map(log_from_row).collect();
        let built = build_receipt(tx_type, success, cumulative_gas_used, logs)
            .map_err(ColdStorageError::from)?;

        // Compute gas_used and first_log_index by querying prior receipts
        let prior = sqlx::query(
            "SELECT CAST(SUM(
                (SELECT COUNT(*) FROM logs l WHERE l.block_number = $1 AND l.tx_index = r.tx_index)
             ) AS bigint) as log_count,
             CAST(MAX(r.cumulative_gas_used) AS bigint) as prior_gas
             FROM receipts r WHERE r.block_number = $1 AND r.tx_index < $2",
        )
        .bind(to_i64(block))
        .bind(to_i64(index))
        .fetch_one(&self.pool)
        .await
        .map_err(SqlColdError::from)?;

        let first_log_index: u64 = prior.get::<Option<i64>, _>("log_count").unwrap_or(0) as u64;
        let prior_cumulative_gas: u64 =
            prior.get::<Option<i64>, _>("prior_gas").unwrap_or(0) as u64;
        let gas_used = built.inner.cumulative_gas_used - prior_cumulative_gas;

        let ir = IndexedReceipt { receipt: built, tx_hash, first_log_index, gas_used, sender };
        Ok(Some(ColdReceipt::new(ir, &header, index)))
    }

    async fn get_receipts_in_block(&self, block: BlockNumber) -> ColdResult<Vec<ColdReceipt>> {
        let Some(header) =
            self.fetch_header_by_number(block).await.map_err(ColdStorageError::from)?
        else {
            return Ok(Vec::new());
        };

        let bn = to_i64(block);

        // Fetch receipts joined with tx_hash and from_address
        let receipt_rows = sqlx::query(
            "SELECT r.*, t.tx_hash, t.from_address
             FROM receipts r
             JOIN transactions t ON r.block_number = t.block_number AND r.tx_index = t.tx_index
             WHERE r.block_number = $1
             ORDER BY r.tx_index",
        )
        .bind(bn)
        .fetch_all(&self.pool)
        .await
        .map_err(SqlColdError::from)?;

        let all_log_rows =
            sqlx::query("SELECT * FROM logs WHERE block_number = $1 ORDER BY tx_index, log_index")
                .bind(bn)
                .fetch_all(&self.pool)
                .await
                .map_err(SqlColdError::from)?;

        // Group logs by tx_index
        let mut logs_by_tx: std::collections::BTreeMap<i64, Vec<Log>> =
            std::collections::BTreeMap::new();
        for r in &all_log_rows {
            let tx_idx: i64 = r.get("tx_index");
            logs_by_tx.entry(tx_idx).or_default().push(log_from_row(r));
        }

        let mut first_log_index = 0u64;
        let mut prior_cumulative_gas = 0u64;
        receipt_rows
            .into_iter()
            .enumerate()
            .map(|(idx, rr)| {
                let tx_idx: i64 = rr.get("tx_index");
                let tx_hash = B256::from_slice(&rr.get::<Vec<u8>, _>("tx_hash"));
                let sender = Address::from_slice(&rr.get::<Vec<u8>, _>("from_address"));
                let tx_type = rr.get::<i32, _>("tx_type") as i16;
                let success = rr.get::<i32, _>("success") != 0;
                let cumulative_gas_used: i64 = rr.get("cumulative_gas_used");
                let logs = logs_by_tx.remove(&tx_idx).unwrap_or_default();
                let receipt = build_receipt(tx_type, success, cumulative_gas_used, logs)
                    .map_err(ColdStorageError::from)?;
                let gas_used = receipt.inner.cumulative_gas_used - prior_cumulative_gas;
                prior_cumulative_gas = receipt.inner.cumulative_gas_used;
                let ir = IndexedReceipt { receipt, tx_hash, first_log_index, gas_used, sender };
                first_log_index += ir.receipt.inner.logs.len() as u64;
                Ok(ColdReceipt::new(ir, &header, idx as u64))
            })
            .collect()
    }

    async fn get_signet_events(
        &self,
        spec: SignetEventsSpecifier,
    ) -> ColdResult<Vec<DbSignetEvent>> {
        let rows = match spec {
            SignetEventsSpecifier::Block(block) => {
                let bn = to_i64(block);
                sqlx::query(
                    "SELECT * FROM signet_events WHERE block_number = $1 ORDER BY event_index",
                )
                .bind(bn)
                .fetch_all(&self.pool)
                .await
                .map_err(SqlColdError::from)?
            }
            SignetEventsSpecifier::BlockRange { start, end } => {
                let s = to_i64(start);
                let e = to_i64(end);
                sqlx::query(
                    "SELECT * FROM signet_events WHERE block_number >= $1 AND block_number <= $2
                     ORDER BY block_number, event_index",
                )
                .bind(s)
                .bind(e)
                .fetch_all(&self.pool)
                .await
                .map_err(SqlColdError::from)?
            }
        };

        rows.iter().map(|r| signet_event_from_row(r).map_err(ColdStorageError::from)).collect()
    }

    async fn get_zenith_header(
        &self,
        spec: ZenithHeaderSpecifier,
    ) -> ColdResult<Option<DbZenithHeader>> {
        let block = match spec {
            ZenithHeaderSpecifier::Number(n) => n,
            ZenithHeaderSpecifier::Range { start, .. } => start,
        };
        let bn = to_i64(block);
        let row = sqlx::query("SELECT * FROM zenith_headers WHERE block_number = $1")
            .bind(bn)
            .fetch_optional(&self.pool)
            .await
            .map_err(SqlColdError::from)?;

        row.map(|r| zenith_header_from_row(&r)).transpose().map_err(ColdStorageError::from)
    }

    async fn get_zenith_headers(
        &self,
        spec: ZenithHeaderSpecifier,
    ) -> ColdResult<Vec<DbZenithHeader>> {
        let rows = match spec {
            ZenithHeaderSpecifier::Number(n) => {
                let bn = to_i64(n);
                sqlx::query("SELECT * FROM zenith_headers WHERE block_number = $1")
                    .bind(bn)
                    .fetch_all(&self.pool)
                    .await
                    .map_err(SqlColdError::from)?
            }
            ZenithHeaderSpecifier::Range { start, end } => {
                let s = to_i64(start);
                let e = to_i64(end);
                sqlx::query(
                    "SELECT * FROM zenith_headers WHERE block_number >= $1 AND block_number <= $2
                     ORDER BY block_number",
                )
                .bind(s)
                .bind(e)
                .fetch_all(&self.pool)
                .await
                .map_err(SqlColdError::from)?
            }
        };

        rows.iter().map(|r| zenith_header_from_row(r).map_err(ColdStorageError::from)).collect()
    }

    async fn get_logs(&self, filter: Filter) -> ColdResult<Vec<RpcLog>> {
        let from = filter.get_from_block().unwrap_or(0);
        let to = filter.get_to_block().unwrap_or(u64::MAX);

        // Build dynamic SQL with positional $N placeholders.
        // The correlated subquery computes block_log_index: the absolute
        // position of each log among all logs in its block, leveraging the
        // PK index on (block_number, tx_index, log_index).
        let mut sql = String::from(
            "SELECT l.*, h.block_hash, h.timestamp AS block_timestamp, t.tx_hash, \
               (SELECT COUNT(*) FROM logs l2 \
                WHERE l2.block_number = l.block_number \
                  AND (l2.tx_index < l.tx_index \
                       OR (l2.tx_index = l.tx_index AND l2.log_index < l.log_index)) \
               ) AS block_log_index \
             FROM logs l \
             JOIN headers h ON l.block_number = h.block_number \
             JOIN transactions t ON l.block_number = t.block_number \
               AND l.tx_index = t.tx_index \
             WHERE l.block_number >= $1 AND l.block_number <= $2",
        );
        let mut params: Vec<Vec<u8>> = Vec::new();
        let mut idx = 3u32;

        // Address filter
        if !filter.address.is_empty() {
            let addrs: Vec<_> = filter.address.iter().collect();
            if addrs.len() == 1 {
                sql.push_str(&format!(" AND l.address = ${idx}"));
                params.push(addrs[0].as_slice().to_vec());
                idx += 1;
            } else {
                let placeholders: String = addrs
                    .iter()
                    .enumerate()
                    .map(|(i, _)| format!("${}", idx + i as u32))
                    .collect::<Vec<_>>()
                    .join(", ");
                sql.push_str(&format!(" AND l.address IN ({placeholders})"));
                for addr in &addrs {
                    params.push(addr.as_slice().to_vec());
                }
                idx += addrs.len() as u32;
            }
        }

        // Topic filters
        let topic_cols = ["l.topic0", "l.topic1", "l.topic2", "l.topic3"];
        for (i, topic_filter) in filter.topics.iter().enumerate() {
            if topic_filter.is_empty() {
                continue;
            }
            let values: Vec<_> = topic_filter.iter().collect();
            if values.len() == 1 {
                sql.push_str(&format!(" AND {} = ${idx}", topic_cols[i]));
                params.push(values[0].as_slice().to_vec());
                idx += 1;
            } else {
                let placeholders: String = values
                    .iter()
                    .enumerate()
                    .map(|(j, _)| format!("${}", idx + j as u32))
                    .collect::<Vec<_>>()
                    .join(", ");
                sql.push_str(&format!(" AND {} IN ({placeholders})", topic_cols[i]));
                for v in &values {
                    params.push(v.as_slice().to_vec());
                }
                idx += values.len() as u32;
            }
        }

        sql.push_str(" ORDER BY l.block_number, l.tx_index, l.log_index");

        // Bind parameters and execute.
        let mut query = sqlx::query(&sql).bind(to_i64(from)).bind(to_i64(to));
        for param in &params {
            query = query.bind(param.as_slice());
        }

        let rows = query.fetch_all(&self.pool).await.map_err(SqlColdError::from)?;

        rows.into_iter()
            .map(|r| {
                let log = log_from_row(&r);
                let block_number = from_i64(r.get::<i64, _>("block_number"));
                let block_hash_bytes: Vec<u8> = r.get("block_hash");
                let tx_hash_bytes: Vec<u8> = r.get("tx_hash");
                Ok(RpcLog {
                    inner: log,
                    block_hash: Some(B256::from_slice(&block_hash_bytes)),
                    block_number: Some(block_number),
                    block_timestamp: Some(from_i64(r.get::<i64, _>("block_timestamp"))),
                    transaction_hash: Some(B256::from_slice(&tx_hash_bytes)),
                    transaction_index: Some(from_i64(r.get::<i64, _>("tx_index"))),
                    log_index: Some(from_i64(r.get::<i64, _>("block_log_index"))),
                    removed: false,
                })
            })
            .collect::<ColdResult<Vec<_>>>()
    }

    async fn get_latest_block(&self) -> ColdResult<Option<BlockNumber>> {
        let row = sqlx::query("SELECT MAX(block_number) as max_bn FROM headers")
            .fetch_one(&self.pool)
            .await
            .map_err(SqlColdError::from)?;
        Ok(row.get::<Option<i64>, _>("max_bn").map(from_i64))
    }

    async fn append_block(&self, data: BlockData) -> ColdResult<()> {
        self.insert_block(data).await.map_err(ColdStorageError::from)
    }

    async fn append_blocks(&self, data: Vec<BlockData>) -> ColdResult<()> {
        let mut tx = self.pool.begin().await.map_err(SqlColdError::from)?;
        for block_data in data {
            write_block_to_tx(&mut tx, block_data).await.map_err(ColdStorageError::from)?;
        }
        tx.commit().await.map_err(SqlColdError::from)?;
        Ok(())
    }

    async fn truncate_above(&self, block: BlockNumber) -> ColdResult<()> {
        let bn = to_i64(block);
        let mut tx = self.pool.begin().await.map_err(SqlColdError::from)?;

        sqlx::query("DELETE FROM logs WHERE block_number > $1")
            .bind(bn)
            .execute(&mut *tx)
            .await
            .map_err(SqlColdError::from)?;
        sqlx::query("DELETE FROM transactions WHERE block_number > $1")
            .bind(bn)
            .execute(&mut *tx)
            .await
            .map_err(SqlColdError::from)?;
        sqlx::query("DELETE FROM receipts WHERE block_number > $1")
            .bind(bn)
            .execute(&mut *tx)
            .await
            .map_err(SqlColdError::from)?;
        sqlx::query("DELETE FROM signet_events WHERE block_number > $1")
            .bind(bn)
            .execute(&mut *tx)
            .await
            .map_err(SqlColdError::from)?;
        sqlx::query("DELETE FROM zenith_headers WHERE block_number > $1")
            .bind(bn)
            .execute(&mut *tx)
            .await
            .map_err(SqlColdError::from)?;
        sqlx::query("DELETE FROM headers WHERE block_number > $1")
            .bind(bn)
            .execute(&mut *tx)
            .await
            .map_err(SqlColdError::from)?;

        tx.commit().await.map_err(SqlColdError::from)?;
        Ok(())
    }
}

#[cfg(all(test, feature = "test-utils"))]
mod tests {
    use super::*;
    use signet_cold::conformance::conformance;

    #[tokio::test]
    async fn sqlite_conformance() {
        let backend = SqlColdBackend::connect("sqlite::memory:").await.unwrap();
        conformance(&backend).await.unwrap();
    }

    #[tokio::test]
    async fn pg_conformance() {
        let Ok(url) = std::env::var("DATABASE_URL") else {
            eprintln!("skipping pg conformance: DATABASE_URL not set");
            return;
        };
        let backend = SqlColdBackend::connect(&url).await.unwrap();
        conformance(&backend).await.unwrap();
    }
}
