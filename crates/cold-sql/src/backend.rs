//! Unified SQL backend for cold storage.
//!
//! Supports both PostgreSQL and SQLite via [`sqlx::Any`]. The backend
//! auto-detects the database type at construction time and runs the
//! appropriate migration.

use crate::SqlColdError;
use crate::convert::{
    HeaderRow, LogRow, ReceiptRow, SignetEventRow, TxRow, ZenithHeaderRow, encode_u256, from_i64,
    receipt_from_rows, to_i64,
};
use alloy::{consensus::Header, primitives::BlockNumber};
use signet_cold::{
    BlockData, ColdResult, ColdStorage, ColdStorageError, Confirmed, HeaderSpecifier,
    ReceiptContext, ReceiptSpecifier, SignetEventsSpecifier, TransactionSpecifier,
    ZenithHeaderSpecifier,
};
use signet_storage_types::{
    ConfirmationMeta, DbSignetEvent, DbZenithHeader, Receipt, TransactionSigned,
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
    ) -> Result<Option<Header>, SqlColdError> {
        let bn = to_i64(block_num);
        let row = sqlx::query("SELECT * FROM headers WHERE block_number = $1")
            .bind(bn)
            .fetch_optional(&self.pool)
            .await?;

        row.map(|r| Header::try_from(HeaderRow::from(&r))).transpose()
    }

    // ========================================================================
    // Write helpers
    // ========================================================================

    /// Insert a single block's data using the given connection.
    ///
    /// The caller is responsible for transaction management.
    async fn insert_block_with(
        conn: &mut sqlx::AnyConnection,
        data: &BlockData,
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
        .execute(&mut *conn)
        .await?;

        // Insert transactions
        for (idx, tx_signed) in data.transactions.iter().enumerate() {
            let tr = TxRow::from_tx(tx_signed, bn, to_i64(idx as u64))?;
            sqlx::query(
                "INSERT INTO transactions (
                    block_number, tx_index, tx_hash, tx_type,
                    sig_y_parity, sig_r, sig_s,
                    chain_id, nonce, gas_limit, to_address, value, input,
                    gas_price, max_fee_per_gas, max_priority_fee_per_gas,
                    max_fee_per_blob_gas, blob_versioned_hashes,
                    access_list, authorization_list
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                    $11, $12, $13, $14, $15, $16, $17, $18, $19, $20
                )",
            )
            .bind(tr.block_number)
            .bind(tr.tx_index)
            .bind(&tr.tx_hash)
            .bind(tr.tx_type as i32)
            .bind(tr.sig_y_parity as i32)
            .bind(&tr.sig_r)
            .bind(&tr.sig_s)
            .bind(tr.chain_id)
            .bind(tr.nonce)
            .bind(tr.gas_limit)
            .bind(&tr.to_address)
            .bind(&tr.value)
            .bind(&tr.input)
            .bind(&tr.gas_price)
            .bind(&tr.max_fee_per_gas)
            .bind(&tr.max_priority_fee_per_gas)
            .bind(&tr.max_fee_per_blob_gas)
            .bind(&tr.blob_versioned_hashes)
            .bind(&tr.access_list)
            .bind(&tr.authorization_list)
            .execute(&mut *conn)
            .await?;
        }

        // Insert receipts and logs
        for (idx, receipt) in data.receipts.iter().enumerate() {
            let rr = ReceiptRow::from_receipt(receipt, bn, to_i64(idx as u64));
            sqlx::query(
                "INSERT INTO receipts (block_number, tx_index, tx_type, success, cumulative_gas_used)
                 VALUES ($1, $2, $3, $4, $5)",
            )
            .bind(rr.block_number)
            .bind(rr.tx_index)
            .bind(rr.tx_type as i32)
            .bind(rr.success as i32)
            .bind(rr.cumulative_gas_used)
            .execute(&mut *conn)
            .await?;

            for (log_idx, log) in receipt.inner.logs.iter().enumerate() {
                let topics = log.topics();
                sqlx::query(
                    "INSERT INTO logs (block_number, tx_index, log_index, address, topic0, topic1, topic2, topic3, data)
                     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
                )
                .bind(bn)
                .bind(to_i64(idx as u64))
                .bind(to_i64(log_idx as u64))
                .bind(log.address.as_slice())
                .bind(topics.first().map(|t| t.as_slice()))
                .bind(topics.get(1).map(|t| t.as_slice()))
                .bind(topics.get(2).map(|t| t.as_slice()))
                .bind(topics.get(3).map(|t| t.as_slice()))
                .bind(log.data.data.as_ref())
                .execute(&mut *conn)
                .await?;
            }
        }

        // Insert signet events
        for (idx, event) in data.signet_events.iter().enumerate() {
            insert_signet_event(&mut *conn, bn, to_i64(idx as u64), event).await?;
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
            .execute(&mut *conn)
            .await?;
        }

        Ok(())
    }

    /// Insert a single block wrapped in its own transaction.
    async fn insert_block(&self, data: BlockData) -> Result<(), SqlColdError> {
        let mut tx = self.pool.begin().await?;
        Self::insert_block_with(&mut tx, &data).await?;
        tx.commit().await?;
        Ok(())
    }
}

/// Insert a signet event, binding directly from source types.
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

    // Pre-encode U256 fields on the stack.
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
// From<&AnyRow> impls for row types
// ============================================================================

impl From<&sqlx::any::AnyRow> for HeaderRow {
    fn from(r: &sqlx::any::AnyRow) -> Self {
        Self {
            block_number: r.get("block_number"),
            parent_hash: r.get("parent_hash"),
            ommers_hash: r.get("ommers_hash"),
            beneficiary: r.get("beneficiary"),
            state_root: r.get("state_root"),
            transactions_root: r.get("transactions_root"),
            receipts_root: r.get("receipts_root"),
            logs_bloom: r.get("logs_bloom"),
            difficulty: r.get("difficulty"),
            gas_limit: r.get("gas_limit"),
            gas_used: r.get("gas_used"),
            timestamp: r.get("timestamp"),
            extra_data: r.get("extra_data"),
            mix_hash: r.get("mix_hash"),
            nonce: r.get("nonce"),
            base_fee_per_gas: r.get("base_fee_per_gas"),
            withdrawals_root: r.get("withdrawals_root"),
            blob_gas_used: r.get("blob_gas_used"),
            excess_blob_gas: r.get("excess_blob_gas"),
            parent_beacon_block_root: r.get("parent_beacon_block_root"),
            requests_hash: r.get("requests_hash"),
        }
    }
}

impl From<&sqlx::any::AnyRow> for TxRow {
    fn from(r: &sqlx::any::AnyRow) -> Self {
        Self {
            block_number: r.get("block_number"),
            tx_index: r.get("tx_index"),
            tx_hash: r.get("tx_hash"),
            tx_type: r.get::<i32, _>("tx_type") as i16,
            sig_y_parity: r.get::<i32, _>("sig_y_parity") != 0,
            sig_r: r.get("sig_r"),
            sig_s: r.get("sig_s"),
            chain_id: r.get("chain_id"),
            nonce: r.get("nonce"),
            gas_limit: r.get("gas_limit"),
            to_address: r.get("to_address"),
            value: r.get("value"),
            input: r.get("input"),
            gas_price: r.get("gas_price"),
            max_fee_per_gas: r.get("max_fee_per_gas"),
            max_priority_fee_per_gas: r.get("max_priority_fee_per_gas"),
            max_fee_per_blob_gas: r.get("max_fee_per_blob_gas"),
            blob_versioned_hashes: r.get("blob_versioned_hashes"),
            access_list: r.get("access_list"),
            authorization_list: r.get("authorization_list"),
        }
    }
}

impl From<&sqlx::any::AnyRow> for LogRow {
    fn from(r: &sqlx::any::AnyRow) -> Self {
        Self {
            address: r.get("address"),
            topic0: r.get("topic0"),
            topic1: r.get("topic1"),
            topic2: r.get("topic2"),
            topic3: r.get("topic3"),
            data: r.get("data"),
        }
    }
}

impl From<&sqlx::any::AnyRow> for ReceiptRow {
    fn from(r: &sqlx::any::AnyRow) -> Self {
        Self {
            block_number: r.get("block_number"),
            tx_index: r.get("tx_index"),
            tx_type: r.get::<i32, _>("tx_type") as i16,
            success: r.get::<i32, _>("success") != 0,
            cumulative_gas_used: r.get("cumulative_gas_used"),
        }
    }
}

impl From<&sqlx::any::AnyRow> for SignetEventRow {
    fn from(r: &sqlx::any::AnyRow) -> Self {
        Self {
            event_type: r.get::<i32, _>("event_type") as i16,
            order_index: r.get("order_index"),
            rollup_chain_id: r.get("rollup_chain_id"),
            sender: r.get("sender"),
            to_address: r.get("to_address"),
            value: r.get("value"),
            gas: r.get("gas"),
            max_fee_per_gas: r.get("max_fee_per_gas"),
            data: r.get("data"),
            rollup_recipient: r.get("rollup_recipient"),
            amount: r.get("amount"),
            token: r.get("token"),
        }
    }
}

impl From<&sqlx::any::AnyRow> for ZenithHeaderRow {
    fn from(r: &sqlx::any::AnyRow) -> Self {
        Self {
            host_block_number: r.get("host_block_number"),
            rollup_chain_id: r.get("rollup_chain_id"),
            gas_limit: r.get("gas_limit"),
            reward_address: r.get("reward_address"),
            block_data_hash: r.get("block_data_hash"),
        }
    }
}

// ============================================================================
// ColdStorage implementation
// ============================================================================

impl ColdStorage for SqlColdBackend {
    async fn get_header(&self, spec: HeaderSpecifier) -> ColdResult<Option<Header>> {
        let Some(block_num) = self.resolve_header_spec(spec).await? else {
            return Ok(None);
        };
        self.fetch_header_by_number(block_num).await.map_err(ColdStorageError::from)
    }

    async fn get_headers(&self, specs: Vec<HeaderSpecifier>) -> ColdResult<Vec<Option<Header>>> {
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
    ) -> ColdResult<Option<Confirmed<TransactionSigned>>> {
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
        let block_hash = alloy::primitives::B256::from_slice(&hash_bytes);
        let tx = TransactionSigned::try_from(TxRow::from(&r)).map_err(ColdStorageError::from)?;
        let meta = ConfirmationMeta::new(block, block_hash, index);
        Ok(Some(Confirmed::new(tx, meta)))
    }

    async fn get_transactions_in_block(
        &self,
        block: BlockNumber,
    ) -> ColdResult<Vec<TransactionSigned>> {
        let bn = to_i64(block);
        let rows =
            sqlx::query("SELECT * FROM transactions WHERE block_number = $1 ORDER BY tx_index")
                .bind(bn)
                .fetch_all(&self.pool)
                .await
                .map_err(SqlColdError::from)?;

        rows.into_iter()
            .map(|r| TransactionSigned::try_from(TxRow::from(&r)).map_err(ColdStorageError::from))
            .collect()
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

    async fn get_receipt(&self, spec: ReceiptSpecifier) -> ColdResult<Option<Confirmed<Receipt>>> {
        // Fetch receipt + block_hash in one query via JOIN
        let receipt_row = match spec {
            ReceiptSpecifier::TxHash(hash) => sqlx::query(
                "SELECT r.*, h.block_hash
                     FROM transactions t
                     JOIN receipts r ON t.block_number = r.block_number AND t.tx_index = r.tx_index
                     JOIN headers h ON t.block_number = h.block_number
                     WHERE t.tx_hash = $1",
            )
            .bind(hash.as_slice())
            .fetch_optional(&self.pool)
            .await
            .map_err(SqlColdError::from)?,
            ReceiptSpecifier::BlockAndIndex { block, index } => sqlx::query(
                "SELECT r.*, h.block_hash
                     FROM receipts r
                     JOIN headers h ON r.block_number = h.block_number
                     WHERE r.block_number = $1 AND r.tx_index = $2",
            )
            .bind(to_i64(block))
            .bind(to_i64(index))
            .fetch_optional(&self.pool)
            .await
            .map_err(SqlColdError::from)?,
        };

        let Some(rr) = receipt_row else {
            return Ok(None);
        };

        let bn: i64 = rr.get("block_number");
        let tx_idx: i64 = rr.get("tx_index");
        let hash_bytes: Vec<u8> = rr.get("block_hash");
        let block_hash = alloy::primitives::B256::from_slice(&hash_bytes);
        let receipt = ReceiptRow::from(&rr);

        let log_rows = sqlx::query(
            "SELECT * FROM logs WHERE block_number = $1 AND tx_index = $2 ORDER BY log_index",
        )
        .bind(bn)
        .bind(tx_idx)
        .fetch_all(&self.pool)
        .await
        .map_err(SqlColdError::from)?;

        let logs = log_rows.iter().map(LogRow::from).collect();
        let built = receipt_from_rows(receipt, logs).map_err(ColdStorageError::from)?;
        let meta = ConfirmationMeta::new(from_i64(bn), block_hash, from_i64(tx_idx));
        Ok(Some(Confirmed::new(built, meta)))
    }

    async fn get_receipts_in_block(&self, block: BlockNumber) -> ColdResult<Vec<Receipt>> {
        let bn = to_i64(block);

        let receipt_rows =
            sqlx::query("SELECT * FROM receipts WHERE block_number = $1 ORDER BY tx_index")
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
        let mut logs_by_tx: std::collections::BTreeMap<i64, Vec<LogRow>> =
            std::collections::BTreeMap::new();
        for r in &all_log_rows {
            let tx_idx: i64 = r.get("tx_index");
            logs_by_tx.entry(tx_idx).or_default().push(LogRow::from(r));
        }

        receipt_rows
            .iter()
            .map(|rr| {
                let receipt = ReceiptRow::from(rr);
                let logs = logs_by_tx.remove(&receipt.tx_index).unwrap_or_default();
                receipt_from_rows(receipt, logs).map_err(ColdStorageError::from)
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

        rows.iter()
            .map(|r| {
                DbSignetEvent::try_from(SignetEventRow::from(r)).map_err(ColdStorageError::from)
            })
            .collect()
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

        row.map(|r| DbZenithHeader::try_from(ZenithHeaderRow::from(&r)))
            .transpose()
            .map_err(ColdStorageError::from)
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

        rows.iter()
            .map(|r| {
                DbZenithHeader::try_from(ZenithHeaderRow::from(r)).map_err(ColdStorageError::from)
            })
            .collect()
    }

    async fn get_latest_block(&self) -> ColdResult<Option<BlockNumber>> {
        let row = sqlx::query("SELECT MAX(block_number) as max_bn FROM headers")
            .fetch_one(&self.pool)
            .await
            .map_err(SqlColdError::from)?;
        Ok(row.get::<Option<i64>, _>("max_bn").map(from_i64))
    }

    async fn get_receipt_with_context(
        &self,
        spec: ReceiptSpecifier,
    ) -> ColdResult<Option<ReceiptContext>> {
        // Step 1: Resolve specifier to (block_number, tx_index).
        let (bn, tx_idx) = match spec {
            ReceiptSpecifier::BlockAndIndex { block, index } => (to_i64(block), to_i64(index)),
            ReceiptSpecifier::TxHash(hash) => {
                let row = sqlx::query(
                    "SELECT block_number, tx_index FROM transactions WHERE tx_hash = $1",
                )
                .bind(hash.as_slice())
                .fetch_optional(&self.pool)
                .await
                .map_err(SqlColdError::from)?;

                let Some(r) = row else {
                    return Ok(None);
                };
                (r.get::<i64, _>("block_number"), r.get::<i64, _>("tx_index"))
            }
        };

        // Step 2: Fetch header + receipt + transaction + prior gas in one
        // multi-join query. Column aliases resolve name conflicts between
        // the three tables.
        let row = sqlx::query(
            "SELECT
                h.block_number, h.block_hash, h.parent_hash, h.ommers_hash, h.beneficiary,
                h.state_root, h.transactions_root, h.receipts_root, h.logs_bloom,
                h.difficulty, h.gas_limit as h_gas_limit, h.gas_used, h.timestamp,
                h.extra_data, h.mix_hash, h.nonce as h_nonce, h.base_fee_per_gas,
                h.withdrawals_root, h.blob_gas_used, h.excess_blob_gas,
                h.parent_beacon_block_root, h.requests_hash,
                t.tx_index, t.tx_hash, t.tx_type as t_tx_type,
                t.sig_y_parity, t.sig_r, t.sig_s, t.chain_id, t.nonce as t_nonce,
                t.gas_limit as t_gas_limit, t.to_address, t.value, t.input,
                t.gas_price, t.max_fee_per_gas, t.max_priority_fee_per_gas,
                t.max_fee_per_blob_gas, t.blob_versioned_hashes,
                t.access_list, t.authorization_list,
                r.tx_type as r_tx_type, r.success, r.cumulative_gas_used,
                COALESCE(
                    (SELECT cumulative_gas_used FROM receipts
                     WHERE block_number = $1 AND tx_index = $2 - 1), 0
                ) as prior_cumulative_gas
            FROM receipts r
            JOIN headers h ON h.block_number = r.block_number
            JOIN transactions t ON t.block_number = r.block_number
                AND t.tx_index = r.tx_index
            WHERE r.block_number = $1 AND r.tx_index = $2",
        )
        .bind(bn)
        .bind(tx_idx)
        .fetch_optional(&self.pool)
        .await
        .map_err(SqlColdError::from)?;

        let Some(r) = row else {
            return Ok(None);
        };

        // Construct rows manually — column aliases differ from standard names.
        let header = Header::try_from(HeaderRow {
            block_number: r.get("block_number"),
            parent_hash: r.get("parent_hash"),
            ommers_hash: r.get("ommers_hash"),
            beneficiary: r.get("beneficiary"),
            state_root: r.get("state_root"),
            transactions_root: r.get("transactions_root"),
            receipts_root: r.get("receipts_root"),
            logs_bloom: r.get("logs_bloom"),
            difficulty: r.get("difficulty"),
            gas_limit: r.get("h_gas_limit"),
            gas_used: r.get("gas_used"),
            timestamp: r.get("timestamp"),
            extra_data: r.get("extra_data"),
            mix_hash: r.get("mix_hash"),
            nonce: r.get("h_nonce"),
            base_fee_per_gas: r.get("base_fee_per_gas"),
            withdrawals_root: r.get("withdrawals_root"),
            blob_gas_used: r.get("blob_gas_used"),
            excess_blob_gas: r.get("excess_blob_gas"),
            parent_beacon_block_root: r.get("parent_beacon_block_root"),
            requests_hash: r.get("requests_hash"),
        })
        .map_err(ColdStorageError::from)?;

        let tx = TransactionSigned::try_from(TxRow {
            block_number: bn,
            tx_index: tx_idx,
            tx_hash: r.get("tx_hash"),
            tx_type: r.get::<i32, _>("t_tx_type") as i16,
            sig_y_parity: r.get::<i32, _>("sig_y_parity") != 0,
            sig_r: r.get("sig_r"),
            sig_s: r.get("sig_s"),
            chain_id: r.get("chain_id"),
            nonce: r.get("t_nonce"),
            gas_limit: r.get("t_gas_limit"),
            to_address: r.get("to_address"),
            value: r.get("value"),
            input: r.get("input"),
            gas_price: r.get("gas_price"),
            max_fee_per_gas: r.get("max_fee_per_gas"),
            max_priority_fee_per_gas: r.get("max_priority_fee_per_gas"),
            max_fee_per_blob_gas: r.get("max_fee_per_blob_gas"),
            blob_versioned_hashes: r.get("blob_versioned_hashes"),
            access_list: r.get("access_list"),
            authorization_list: r.get("authorization_list"),
        })
        .map_err(ColdStorageError::from)?;

        let receipt_row = ReceiptRow {
            block_number: bn,
            tx_index: tx_idx,
            tx_type: r.get::<i32, _>("r_tx_type") as i16,
            success: r.get::<i32, _>("success") != 0,
            cumulative_gas_used: r.get("cumulative_gas_used"),
        };

        let block_hash_bytes: Vec<u8> = r.get("block_hash");
        let block_hash = alloy::primitives::B256::from_slice(&block_hash_bytes);
        let prior_cumulative_gas = from_i64(r.get::<i64, _>("prior_cumulative_gas"));

        // Step 3: Fetch logs.
        let log_rows = sqlx::query(
            "SELECT * FROM logs WHERE block_number = $1 AND tx_index = $2 ORDER BY log_index",
        )
        .bind(bn)
        .bind(tx_idx)
        .fetch_all(&self.pool)
        .await
        .map_err(SqlColdError::from)?;

        let logs = log_rows.iter().map(LogRow::from).collect();
        let receipt = receipt_from_rows(receipt_row, logs).map_err(ColdStorageError::from)?;
        let meta = ConfirmationMeta::new(from_i64(bn), block_hash, from_i64(tx_idx));
        let confirmed = Confirmed::new(receipt, meta);

        Ok(Some(ReceiptContext::new(header, tx, confirmed, prior_cumulative_gas)))
    }

    async fn append_block(&self, data: BlockData) -> ColdResult<()> {
        self.insert_block(data).await.map_err(ColdStorageError::from)
    }

    async fn append_blocks(&self, data: Vec<BlockData>) -> ColdResult<()> {
        let mut tx = self.pool.begin().await.map_err(SqlColdError::from)?;
        for block_data in &data {
            Self::insert_block_with(&mut tx, block_data).await.map_err(ColdStorageError::from)?;
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
