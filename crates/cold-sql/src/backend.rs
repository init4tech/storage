//! Unified SQL backend for cold storage.
//!
//! Supports both PostgreSQL and SQLite via [`sqlx::Any`]. The backend
//! auto-detects the database type at construction time and runs the
//! appropriate migration.

use crate::SqlColdError;
use crate::convert::{
    HeaderRow, LogRow, ReceiptRow, SignetEventRow, TxRow, ZenithHeaderRow, from_i64,
    receipt_from_rows, to_i64,
};
use alloy::{consensus::Header, primitives::BlockNumber};
use signet_cold::{
    BlockData, ColdResult, ColdStorage, ColdStorageError, Confirmed, HeaderSpecifier, LogFilter,
    ReceiptSpecifier, RichLog, SignetEventsSpecifier, TransactionSpecifier, ZenithHeaderSpecifier,
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

        row.map(|r| {
            HeaderRow {
                block_number: r.get("block_number"),
                block_hash: r.get("block_hash"),
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
            .into_header()
        })
        .transpose()
    }

    // ========================================================================
    // Write helpers
    // ========================================================================

    async fn insert_block(&self, data: BlockData) -> Result<(), SqlColdError> {
        let mut tx = self.pool.begin().await?;
        let block = data.block_number();
        let bn = to_i64(block);

        // Insert header
        let hr = HeaderRow::from_header(&data.header);
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
        .bind(hr.block_number)
        .bind(&hr.block_hash)
        .bind(&hr.parent_hash)
        .bind(&hr.ommers_hash)
        .bind(&hr.beneficiary)
        .bind(&hr.state_root)
        .bind(&hr.transactions_root)
        .bind(&hr.receipts_root)
        .bind(&hr.logs_bloom)
        .bind(&hr.difficulty)
        .bind(hr.gas_limit)
        .bind(hr.gas_used)
        .bind(hr.timestamp)
        .bind(&hr.extra_data)
        .bind(&hr.mix_hash)
        .bind(&hr.nonce)
        .bind(hr.base_fee_per_gas)
        .bind(&hr.withdrawals_root)
        .bind(hr.blob_gas_used)
        .bind(hr.excess_blob_gas)
        .bind(&hr.parent_beacon_block_root)
        .bind(&hr.requests_hash)
        .execute(&mut *tx)
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
            .execute(&mut *tx)
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
            .execute(&mut *tx)
            .await?;

            for (log_idx, log) in receipt.inner.logs.iter().enumerate() {
                let lr = LogRow::from_log(log, bn, to_i64(idx as u64), to_i64(log_idx as u64));
                sqlx::query(
                    "INSERT INTO logs (block_number, tx_index, log_index, address, topic0, topic1, topic2, topic3, data)
                     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
                )
                .bind(lr.block_number)
                .bind(lr.tx_index)
                .bind(lr.log_index)
                .bind(&lr.address)
                .bind(&lr.topic0)
                .bind(&lr.topic1)
                .bind(&lr.topic2)
                .bind(&lr.topic3)
                .bind(&lr.data)
                .execute(&mut *tx)
                .await?;
            }
        }

        // Insert signet events
        for (idx, event) in data.signet_events.iter().enumerate() {
            let er = SignetEventRow::from_event(event, bn, to_i64(idx as u64));
            sqlx::query(
                "INSERT INTO signet_events (
                    block_number, event_index, event_type, order_index,
                    rollup_chain_id, sender, to_address, value, gas,
                    max_fee_per_gas, data, rollup_recipient, amount, token
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)",
            )
            .bind(er.block_number)
            .bind(er.event_index)
            .bind(er.event_type as i32)
            .bind(er.order_index)
            .bind(&er.rollup_chain_id)
            .bind(&er.sender)
            .bind(&er.to_address)
            .bind(&er.value)
            .bind(&er.gas)
            .bind(&er.max_fee_per_gas)
            .bind(&er.data)
            .bind(&er.rollup_recipient)
            .bind(&er.amount)
            .bind(&er.token)
            .execute(&mut *tx)
            .await?;
        }

        // Insert zenith header
        if let Some(zh) = &data.zenith_header {
            let zr = ZenithHeaderRow::from_zenith(zh, bn);
            sqlx::query(
                "INSERT INTO zenith_headers (
                    block_number, host_block_number, rollup_chain_id,
                    gas_limit, reward_address, block_data_hash
                ) VALUES ($1, $2, $3, $4, $5, $6)",
            )
            .bind(zr.block_number)
            .bind(&zr.host_block_number)
            .bind(&zr.rollup_chain_id)
            .bind(&zr.gas_limit)
            .bind(&zr.reward_address)
            .bind(&zr.block_data_hash)
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;
        Ok(())
    }
}

/// Convert a sqlx row to a TxRow.
fn row_to_tx_row(r: &sqlx::any::AnyRow) -> TxRow {
    TxRow {
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

fn row_to_signet_event_row(r: &sqlx::any::AnyRow) -> SignetEventRow {
    SignetEventRow {
        block_number: r.get("block_number"),
        event_index: r.get("event_index"),
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

fn row_to_log_row(r: &sqlx::any::AnyRow) -> LogRow {
    LogRow {
        block_number: r.get("block_number"),
        tx_index: r.get("tx_index"),
        log_index: r.get("log_index"),
        address: r.get("address"),
        topic0: r.get("topic0"),
        topic1: r.get("topic1"),
        topic2: r.get("topic2"),
        topic3: r.get("topic3"),
        data: r.get("data"),
    }
}

fn row_to_zenith_header_row(r: &sqlx::any::AnyRow) -> ZenithHeaderRow {
    ZenithHeaderRow {
        block_number: r.get("block_number"),
        host_block_number: r.get("host_block_number"),
        rollup_chain_id: r.get("rollup_chain_id"),
        gas_limit: r.get("gas_limit"),
        reward_address: r.get("reward_address"),
        block_data_hash: r.get("block_data_hash"),
    }
}

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

        let tx = row_to_tx_row(&r).into_tx().map_err(ColdStorageError::from)?;
        let block = from_i64(r.get::<i64, _>("block_number"));
        let index = from_i64(r.get::<i64, _>("tx_index"));
        let hash_bytes: Vec<u8> = r.get("block_hash");
        let block_hash = alloy::primitives::B256::from_slice(&hash_bytes);
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
            .map(|r| row_to_tx_row(&r).into_tx().map_err(ColdStorageError::from))
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

        let receipt = ReceiptRow {
            block_number: bn,
            tx_index: tx_idx,
            tx_type: rr.get::<i32, _>("tx_type") as i16,
            success: rr.get::<i32, _>("success") != 0,
            cumulative_gas_used: rr.get("cumulative_gas_used"),
        };

        let log_rows = sqlx::query(
            "SELECT * FROM logs WHERE block_number = $1 AND tx_index = $2 ORDER BY log_index",
        )
        .bind(bn)
        .bind(tx_idx)
        .fetch_all(&self.pool)
        .await
        .map_err(SqlColdError::from)?;

        let logs = log_rows
            .into_iter()
            .map(|r| LogRow {
                block_number: r.get("block_number"),
                tx_index: r.get("tx_index"),
                log_index: r.get("log_index"),
                address: r.get("address"),
                topic0: r.get("topic0"),
                topic1: r.get("topic1"),
                topic2: r.get("topic2"),
                topic3: r.get("topic3"),
                data: r.get("data"),
            })
            .collect();

        let built = receipt_from_rows(receipt, logs).map_err(ColdStorageError::from)?;
        let block = from_i64(bn);
        let index = from_i64(tx_idx);
        let meta = ConfirmationMeta::new(block, block_hash, index);
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
        for r in all_log_rows {
            logs_by_tx.entry(r.get::<i64, _>("tx_index")).or_default().push(row_to_log_row(&r));
        }

        receipt_rows
            .into_iter()
            .map(|rr| {
                let tx_idx: i64 = rr.get("tx_index");
                let receipt = ReceiptRow {
                    block_number: rr.get("block_number"),
                    tx_index: tx_idx,
                    tx_type: rr.get::<i32, _>("tx_type") as i16,
                    success: rr.get::<i32, _>("success") != 0,
                    cumulative_gas_used: rr.get("cumulative_gas_used"),
                };
                let logs = logs_by_tx.remove(&tx_idx).unwrap_or_default();
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

        rows.into_iter()
            .map(|r| row_to_signet_event_row(&r).into_event().map_err(ColdStorageError::from))
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

        row.map(|r| row_to_zenith_header_row(&r).into_zenith().map_err(ColdStorageError::from))
            .transpose()
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

        rows.into_iter()
            .map(|r| row_to_zenith_header_row(&r).into_zenith().map_err(ColdStorageError::from))
            .collect()
    }

    async fn get_logs(&self, filter: LogFilter) -> ColdResult<Vec<RichLog>> {
        // Build dynamic SQL with positional $N placeholders.
        // The correlated subquery computes block_log_index: the absolute
        // position of each log among all logs in its block, leveraging the
        // PK index on (block_number, tx_index, log_index).
        let mut sql = String::from(
            "SELECT l.*, h.block_hash, t.tx_hash, \
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
        if let Some(ref addrs) = filter.address {
            if addrs.len() == 1 {
                sql.push_str(&format!(" AND l.address = ${idx}"));
                params.push(addrs[0].as_slice().to_vec());
                idx += 1;
            } else if !addrs.is_empty() {
                let placeholders: String = addrs
                    .iter()
                    .enumerate()
                    .map(|(i, _)| format!("${}", idx + i as u32))
                    .collect::<Vec<_>>()
                    .join(", ");
                sql.push_str(&format!(" AND l.address IN ({placeholders})"));
                for addr in addrs {
                    params.push(addr.as_slice().to_vec());
                }
                idx += addrs.len() as u32;
            }
        }

        // Topic filters
        let topic_cols = ["l.topic0", "l.topic1", "l.topic2", "l.topic3"];
        for (i, topic_filter) in filter.topics.iter().enumerate() {
            let Some(values) = topic_filter else { continue };
            if values.is_empty() {
                continue;
            }
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
                for v in values {
                    params.push(v.as_slice().to_vec());
                }
                idx += values.len() as u32;
            }
        }

        sql.push_str(" ORDER BY l.block_number, l.tx_index, l.log_index");

        // Bind parameters and execute.
        let mut query =
            sqlx::query(&sql).bind(to_i64(filter.from_block)).bind(to_i64(filter.to_block));
        for param in &params {
            query = query.bind(param.as_slice());
        }

        let rows = query.fetch_all(&self.pool).await.map_err(SqlColdError::from)?;

        rows.into_iter()
            .map(|r| {
                let log = row_to_log_row(&r).into_log();
                let block_number = from_i64(r.get::<i64, _>("block_number"));
                let block_hash_bytes: Vec<u8> = r.get("block_hash");
                let tx_hash_bytes: Vec<u8> = r.get("tx_hash");
                Ok(RichLog {
                    log,
                    block_number,
                    block_hash: alloy::primitives::B256::from_slice(&block_hash_bytes),
                    tx_hash: alloy::primitives::B256::from_slice(&tx_hash_bytes),
                    tx_index: from_i64(r.get::<i64, _>("tx_index")),
                    block_log_index: from_i64(r.get::<i64, _>("block_log_index")),
                    tx_log_index: from_i64(r.get::<i64, _>("log_index")),
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
        for block_data in data {
            self.insert_block(block_data).await?;
        }
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
