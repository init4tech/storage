# Cold SQL Pool & Query Fixes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix single-connection pool starvation and query inefficiencies in the SQL cold storage backend.

**Architecture:** All changes are in `signet-cold-sql`. Pool configuration is split between `backend.rs` (defaults) and `connector.rs` (builder). Query optimizations touch `backend.rs` only. Integer safety touches `convert.rs` and `backend.rs`. Each task is independently testable via the existing conformance suite.

**Tech Stack:** Rust, sqlx (AnyPool), PostgreSQL, SQLite

**Spec:** `docs/superpowers/specs/2026-04-03-cold-sql-pool-and-query-fixes-design.md`

---

### Task 1: Integer Conversion Safety

**Files:**
- Modify: `crates/cold-sql/src/convert.rs:22-30`
- Modify: `crates/cold-sql/src/backend.rs:337-339`

- [ ] **Step 1: Fix `to_i64` and `from_i64` in convert.rs**

Replace the bare `as` casts with debug-asserted conversions:

```rust
/// Convert u64 to i64 for SQL storage.
///
/// # Panics
///
/// Debug-asserts that `v` fits in an `i64`. All block numbers, gas
/// values, and indices in Ethereum are well below `i64::MAX`; a value
/// that overflows indicates data corruption.
pub(crate) fn to_i64(v: u64) -> i64 {
    debug_assert!(v <= i64::MAX as u64, "u64 value {v} overflows i64");
    v as i64
}

/// Convert i64 from SQL back to u64.
///
/// # Panics
///
/// Debug-asserts that `v` is non-negative. Negative values from the
/// database indicate data corruption.
pub(crate) fn from_i64(v: i64) -> u64 {
    debug_assert!(v >= 0, "negative i64 value {v} cannot represent u64");
    v as u64
}
```

Drop `const` — these are never used in const contexts and `debug_assert!` isn't available in const fns.

- [ ] **Step 2: Fix `tx_type` truncation in backend.rs**

In `tx_from_row` (~line 337), replace:

```rust
    let tx_type_raw = r.get::<i32, _>(COL_TX_TYPE) as u8;
    let tx_type = TxType::try_from(tx_type_raw)
        .map_err(|_| SqlColdError::Convert(format!("invalid tx_type: {tx_type_raw}")))?;
```

With:

```rust
    let tx_type_raw: i32 = r.get(COL_TX_TYPE);
    let tx_type_u8: u8 = tx_type_raw
        .try_into()
        .map_err(|_| SqlColdError::Convert(format!("tx_type out of u8 range: {tx_type_raw}")))?;
    let tx_type = TxType::try_from(tx_type_u8)
        .map_err(|_| SqlColdError::Convert(format!("invalid tx_type: {tx_type_u8}")))?;
```

- [ ] **Step 3: Run conformance tests**

Run: `cargo t -p signet-cold-sql --all-features`
Expected: All tests pass (the assertions only fire in debug builds, and valid data won't trigger them).

- [ ] **Step 4: Run clippy**

Run: `cargo clippy -p signet-cold-sql --all-features --all-targets`
Run: `cargo clippy -p signet-cold-sql --no-default-features --all-targets`
Expected: No new warnings.

- [ ] **Step 5: Commit**

```bash
git add crates/cold-sql/src/convert.rs crates/cold-sql/src/backend.rs
git commit -m "fix(cold-sql): add safety checks to integer conversions"
```

---

### Task 2: Pool Configuration

**Files:**
- Modify: `crates/cold-sql/src/backend.rs:107-119`
- Modify: `crates/cold-sql/src/connector.rs`

- [ ] **Step 1: Add `connect_with` to SqlColdBackend**

In `backend.rs`, add a `use std::time::Duration;` import at the top alongside the existing imports.

Then replace the existing `connect` method and add `connect_with`:

```rust
    /// Pool configuration overrides for [`connect_with`](Self::connect_with).
    ///
    /// `None` values use backend-specific defaults:
    /// - SQLite: `max_connections = 1`, `acquire_timeout = 5s`
    /// - PostgreSQL: `max_connections = 10`, `acquire_timeout = 5s`
    #[derive(Debug, Clone, Default)]
    pub struct PoolOverrides {
        /// Override the maximum number of connections in the pool.
        pub max_connections: Option<u32>,
        /// Override the connection acquire timeout.
        pub acquire_timeout: Option<Duration>,
    }
```

Note: `PoolOverrides` should be defined as a top-level struct in `backend.rs` (outside the `impl` block), and re-exported from `lib.rs`.

```rust
    /// Connect to a database URL with explicit pool overrides.
    ///
    /// Installs the default sqlx drivers on the first call. The database
    /// type is inferred from the URL scheme (`sqlite:` or `postgres:`).
    ///
    /// # Pool Defaults
    ///
    /// - **SQLite**: `max_connections = 1` (required for in-memory databases
    ///   to share state), `acquire_timeout = 5s`.
    /// - **PostgreSQL**: `max_connections = 10`, `acquire_timeout = 5s`.
    ///
    /// Override any default by setting the corresponding field in
    /// `overrides`.
    pub async fn connect_with(
        url: &str,
        overrides: PoolOverrides,
    ) -> Result<Self, SqlColdError> {
        sqlx::any::install_default_drivers();
        let is_sqlite = url.starts_with("sqlite:");
        let default_max = if is_sqlite { 1 } else { 10 };
        let default_timeout = Duration::from_secs(5);
        let pool: AnyPool = sqlx::pool::PoolOptions::new()
            .max_connections(overrides.max_connections.unwrap_or(default_max))
            .acquire_timeout(overrides.acquire_timeout.unwrap_or(default_timeout))
            .connect(url)
            .await?;
        Self::new(pool).await
    }

    /// Connect to a database URL with default pool settings.
    ///
    /// Convenience wrapper around [`connect_with`](Self::connect_with).
    /// See that method for default pool sizes per backend.
    pub async fn connect(url: &str) -> Result<Self, SqlColdError> {
        Self::connect_with(url, PoolOverrides::default()).await
    }
```

- [ ] **Step 2: Re-export PoolOverrides from lib.rs**

In `lib.rs`, add the re-export alongside `SqlColdBackend`:

```rust
#[cfg(any(feature = "sqlite", feature = "postgres"))]
pub use backend::{PoolOverrides, SqlColdBackend};
```

- [ ] **Step 3: Add builder methods to SqlConnector**

In `connector.rs`, update `SqlConnector` to carry pool overrides:

```rust
use crate::{PoolOverrides, SqlColdBackend, SqlColdError};
use signet_cold::ColdConnect;
use std::time::Duration;

// ... SqlConnectorError stays unchanged ...

#[cfg(any(feature = "sqlite", feature = "postgres"))]
#[derive(Debug, Clone)]
pub struct SqlConnector {
    url: String,
    overrides: PoolOverrides,
}

#[cfg(any(feature = "sqlite", feature = "postgres"))]
impl SqlConnector {
    /// Create a new SQL connector.
    ///
    /// The database type is detected from the URL prefix. Pool settings
    /// use backend-specific defaults. Use [`with_max_connections`] and
    /// [`with_acquire_timeout`] to override.
    ///
    /// [`with_max_connections`]: Self::with_max_connections
    /// [`with_acquire_timeout`]: Self::with_acquire_timeout
    pub fn new(url: impl Into<String>) -> Self {
        Self { url: url.into(), overrides: PoolOverrides::default() }
    }

    /// Get a reference to the connection URL.
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Override the maximum number of pool connections.
    ///
    /// Default: 1 for SQLite, 10 for PostgreSQL.
    pub fn with_max_connections(mut self, n: u32) -> Self {
        self.overrides.max_connections = Some(n);
        self
    }

    /// Override the connection acquire timeout.
    ///
    /// Default: 5 seconds for all backends.
    pub fn with_acquire_timeout(mut self, timeout: Duration) -> Self {
        self.overrides.acquire_timeout = Some(timeout);
        self
    }

    /// Create a connector from environment variables.
    ///
    /// Reads the SQL URL from the specified environment variable.
    /// Uses default pool settings.
    pub fn from_env(env_var: &'static str) -> Result<Self, SqlConnectorError> {
        let url =
            std::env::var(env_var).map_err(|_| SqlConnectorError::MissingEnvVar(env_var))?;
        Ok(Self::new(url))
    }
}

#[cfg(any(feature = "sqlite", feature = "postgres"))]
impl ColdConnect for SqlConnector {
    type Cold = SqlColdBackend;
    type Error = SqlColdError;

    fn connect(
        &self,
    ) -> impl std::future::Future<Output = Result<Self::Cold, Self::Error>> + Send {
        let url = self.url.clone();
        let overrides = self.overrides.clone();
        async move { SqlColdBackend::connect_with(&url, overrides).await }
    }
}
```

- [ ] **Step 4: Run conformance tests**

Run: `cargo t -p signet-cold-sql --all-features`
Expected: All tests pass. SQLite still uses 1 connection.

- [ ] **Step 5: Run clippy and format**

Run: `cargo clippy -p signet-cold-sql --all-features --all-targets`
Run: `cargo clippy -p signet-cold-sql --no-default-features --all-targets`
Run: `cargo +nightly fmt`
Expected: Clean.

- [ ] **Step 6: Commit**

```bash
git add crates/cold-sql/src/backend.rs crates/cold-sql/src/connector.rs crates/cold-sql/src/lib.rs
git commit -m "fix(cold-sql): use per-backend pool defaults and add SqlConnector builder"
```

---

### Task 3: `get_logs` LIMIT Optimization

**Files:**
- Modify: `crates/cold-sql/src/backend.rs` (fn `get_logs`, ~line 1293)

- [ ] **Step 1: Replace COUNT + SELECT with LIMIT**

Replace the entire `get_logs` implementation:

```rust
    async fn get_logs(&self, filter: &Filter, max_logs: usize) -> ColdResult<Vec<RpcLog>> {
        let from = filter.get_from_block().unwrap_or(0);
        let to = filter.get_to_block().unwrap_or(u64::MAX);

        // Build WHERE clause: block range ($1, $2) + address/topic filters.
        let (filter_clause, params) = build_log_filter_clause(filter, 3);
        let where_clause = format!("l.block_number >= $1 AND l.block_number <= $2{filter_clause}");

        // Use LIMIT to cap results. Fetch one extra row to detect overflow
        // without a separate COUNT query. PostgreSQL stops scanning after
        // finding enough rows, making this faster than COUNT in both the
        // under-limit and over-limit cases.
        let limit_idx = 3 + params.len() as u32;
        let data_sql = format!(
            "SELECT l.*, h.block_hash, h.timestamp AS block_timestamp, t.tx_hash, \
               (r.first_log_index + l.log_index) AS block_log_index \
             FROM logs l \
             JOIN headers h ON l.block_number = h.block_number \
             JOIN transactions t ON l.block_number = t.block_number \
               AND l.tx_index = t.tx_index \
             JOIN receipts r ON l.block_number = r.block_number \
               AND l.tx_index = r.tx_index \
             WHERE {where_clause} \
             ORDER BY l.block_number, l.tx_index, l.log_index \
             LIMIT ${limit_idx}"
        );
        let mut query = sqlx::query(&data_sql).bind(to_i64(from)).bind(to_i64(to));
        for param in &params {
            query = query.bind(*param);
        }
        let limit = max_logs + 1;
        query = query.bind(to_i64(limit as u64));

        let rows = query.fetch_all(&self.pool).await.map_err(SqlColdError::from)?;

        if rows.len() > max_logs {
            return Err(ColdStorageError::TooManyLogs { limit: max_logs });
        }

        rows.into_iter()
            .map(|r| {
                let log = log_from_row(&r);
                Ok(RpcLog {
                    inner: log,
                    block_hash: Some(r.get(COL_BLOCK_HASH)),
                    block_number: Some(from_i64(r.get::<i64, _>(COL_BLOCK_NUMBER))),
                    block_timestamp: Some(from_i64(r.get::<i64, _>(COL_BLOCK_TIMESTAMP))),
                    transaction_hash: Some(r.get(COL_TX_HASH)),
                    transaction_index: Some(from_i64(r.get::<i64, _>(COL_TX_INDEX))),
                    log_index: Some(from_i64(r.get::<i64, _>(COL_BLOCK_LOG_INDEX))),
                    removed: false,
                })
            })
            .collect::<ColdResult<Vec<_>>>()
    }
```

- [ ] **Step 2: Run conformance tests**

Run: `cargo t -p signet-cold-sql --all-features`
Expected: All tests pass. The conformance suite exercises `get_logs` with limits.

- [ ] **Step 3: Clippy**

Run: `cargo clippy -p signet-cold-sql --all-features --all-targets`
Expected: Clean.

- [ ] **Step 4: Commit**

```bash
git add crates/cold-sql/src/backend.rs
git commit -m "perf(cold-sql): replace get_logs COUNT+SELECT with single LIMIT query"
```

---

### Task 4: `get_receipt` Query Consolidation

**Files:**
- Modify: `crates/cold-sql/src/columns.rs`
- Modify: `crates/cold-sql/src/backend.rs` (fn `get_receipt`, ~line 1068)

- [ ] **Step 1: Add aliased column constants**

In `columns.rs`, add at the bottom (in the "query-specific aliases" section):

```rust
// ── get_receipt combined query aliases ──────────────────────────────────────
pub(crate) const COL_R_TX_TYPE: &str = "r_tx_type";
pub(crate) const COL_R_SUCCESS: &str = "r_success";
pub(crate) const COL_R_CUMULATIVE_GAS_USED: &str = "r_cumulative_gas_used";
pub(crate) const COL_R_FIRST_LOG_INDEX: &str = "r_first_log_index";
pub(crate) const COL_R_TX_HASH: &str = "r_tx_hash";
pub(crate) const COL_R_FROM_ADDRESS: &str = "r_from_address";
pub(crate) const COL_R_TX_INDEX: &str = "r_tx_index";
```

Add the corresponding imports in `backend.rs` where the other column imports are (~line 8-21).

- [ ] **Step 2: Replace `get_receipt` implementation**

Replace the `get_receipt` body with a consolidated version. The combined query selects `h.*` (header columns keep standard names for reuse with `header_from_row`), receipt/tx columns with `r_` prefix aliases, and a correlated subquery for `prior_gas`:

```rust
    async fn get_receipt(&self, spec: ReceiptSpecifier) -> ColdResult<Option<ColdReceipt>> {
        // Resolve to (block, index)
        let (block, index) = match spec {
            ReceiptSpecifier::TxHash(hash) => {
                let row = sqlx::query(
                    "SELECT block_number, tx_index FROM transactions WHERE tx_hash = $1",
                )
                .bind(hash)
                .fetch_optional(&self.pool)
                .await
                .map_err(SqlColdError::from)?;
                let Some(r) = row else { return Ok(None) };
                (
                    from_i64(r.get::<i64, _>(COL_BLOCK_NUMBER)),
                    from_i64(r.get::<i64, _>(COL_TX_INDEX)),
                )
            }
            ReceiptSpecifier::BlockAndIndex { block, index } => (block, index),
        };

        // Combined query: receipt + tx metadata + full header + prior gas.
        // Header columns use standard names (h.*) so header_from_row works.
        // Receipt/tx columns use r_ prefix to avoid collisions.
        let combined = sqlx::query(
            "SELECT h.*, \
               r.tx_type AS r_tx_type, r.success AS r_success, \
               r.cumulative_gas_used AS r_cumulative_gas_used, \
               r.first_log_index AS r_first_log_index, \
               r.tx_index AS r_tx_index, \
               t.tx_hash AS r_tx_hash, t.from_address AS r_from_address, \
               COALESCE( \
                 (SELECT CAST(MAX(r2.cumulative_gas_used) AS bigint) \
                  FROM receipts r2 \
                  WHERE r2.block_number = r.block_number \
                    AND r2.tx_index < r.tx_index), \
                 0 \
               ) AS prior_gas \
             FROM receipts r \
             JOIN transactions t ON r.block_number = t.block_number \
               AND r.tx_index = t.tx_index \
             JOIN headers h ON r.block_number = h.block_number \
             WHERE r.block_number = $1 AND r.tx_index = $2",
        )
        .bind(to_i64(block))
        .bind(to_i64(index))
        .fetch_optional(&self.pool)
        .await
        .map_err(SqlColdError::from)?;

        let Some(rr) = combined else {
            return Ok(None);
        };

        // Extract header using existing helper (h.* columns are unaliased).
        let header = header_from_row(&rr).map_err(ColdStorageError::from)?.seal_slow();

        // Extract receipt fields from r_ prefixed aliases.
        let tx_hash = rr.get(COL_R_TX_HASH);
        let sender = rr.get(COL_R_FROM_ADDRESS);
        let tx_type = rr.get::<i32, _>(COL_R_TX_TYPE) as i16;
        let success = rr.get::<i32, _>(COL_R_SUCCESS) != 0;
        let cumulative_gas_used: i64 = rr.get(COL_R_CUMULATIVE_GAS_USED);
        let first_log_index: u64 = from_i64(rr.get::<i64, _>(COL_R_FIRST_LOG_INDEX));
        let prior_cumulative_gas: u64 =
            rr.get::<Option<i64>, _>(COL_PRIOR_GAS).unwrap_or(0) as u64;

        // Logs still require a separate query (variable row count).
        let log_rows = sqlx::query(
            "SELECT * FROM logs WHERE block_number = $1 AND tx_index = $2 ORDER BY log_index",
        )
        .bind(to_i64(block))
        .bind(to_i64(index))
        .fetch_all(&self.pool)
        .await
        .map_err(SqlColdError::from)?;

        let logs = log_rows.iter().map(log_from_row).collect();
        let built = build_receipt(tx_type, success, cumulative_gas_used, logs)
            .map_err(ColdStorageError::from)?;
        let gas_used = built.inner.cumulative_gas_used - prior_cumulative_gas;

        let ir = IndexedReceipt { receipt: built, tx_hash, first_log_index, gas_used, sender };
        Ok(Some(ColdReceipt::new(ir, &header, index)))
    }
```

- [ ] **Step 3: Run conformance tests**

Run: `cargo t -p signet-cold-sql --all-features`
Expected: All tests pass.

- [ ] **Step 4: Clippy and format**

Run: `cargo clippy -p signet-cold-sql --all-features --all-targets`
Run: `cargo +nightly fmt`
Expected: Clean.

- [ ] **Step 5: Commit**

```bash
git add crates/cold-sql/src/backend.rs crates/cold-sql/src/columns.rs
git commit -m "perf(cold-sql): consolidate get_receipt from 5 queries to 2-3"
```

---

### Task 5: `drain_above` Batch Override

**Files:**
- Modify: `crates/cold-sql/src/backend.rs` (replace `impl ColdStorage for SqlColdBackend {}`, ~line 1403)

- [ ] **Step 1: Replace empty ColdStorage impl with drain_above override**

Replace `impl ColdStorage for SqlColdBackend {}` with:

```rust
impl ColdStorage for SqlColdBackend {
    async fn drain_above(
        &mut self,
        block: BlockNumber,
    ) -> ColdResult<Vec<Vec<ColdReceipt>>> {
        let bn = to_i64(block);
        let mut tx = self.pool.begin().await.map_err(SqlColdError::from)?;

        // 1. Fetch all headers above block.
        let header_rows =
            sqlx::query("SELECT * FROM headers WHERE block_number > $1 ORDER BY block_number")
                .bind(bn)
                .fetch_all(&mut *tx)
                .await
                .map_err(SqlColdError::from)?;

        if header_rows.is_empty() {
            tx.commit().await.map_err(SqlColdError::from)?;
            return Ok(Vec::new());
        }

        let mut headers: std::collections::BTreeMap<i64, SealedHeader> =
            std::collections::BTreeMap::new();
        for r in &header_rows {
            let num: i64 = r.get(COL_BLOCK_NUMBER);
            let h = header_from_row(r).map_err(ColdStorageError::from)?.seal_slow();
            headers.insert(num, h);
        }

        // 2. Fetch all receipt + tx metadata above block.
        let receipt_rows = sqlx::query(
            "SELECT r.*, t.tx_hash, t.from_address \
             FROM receipts r \
             JOIN transactions t ON r.block_number = t.block_number \
               AND r.tx_index = t.tx_index \
             WHERE r.block_number > $1 \
             ORDER BY r.block_number, r.tx_index",
        )
        .bind(bn)
        .fetch_all(&mut *tx)
        .await
        .map_err(SqlColdError::from)?;

        // 3. Fetch all logs above block.
        let log_rows = sqlx::query(
            "SELECT * FROM logs WHERE block_number > $1 \
             ORDER BY block_number, tx_index, log_index",
        )
        .bind(bn)
        .fetch_all(&mut *tx)
        .await
        .map_err(SqlColdError::from)?;

        // Group logs by (block_number, tx_index).
        let mut logs_by_block_tx: std::collections::BTreeMap<(i64, i64), Vec<Log>> =
            std::collections::BTreeMap::new();
        for r in &log_rows {
            let block_num: i64 = r.get(COL_BLOCK_NUMBER);
            let tx_idx: i64 = r.get(COL_TX_INDEX);
            logs_by_block_tx.entry((block_num, tx_idx)).or_default().push(log_from_row(r));
        }

        // Group receipt rows by block_number.
        let mut receipts_by_block: std::collections::BTreeMap<i64, Vec<&sqlx::any::AnyRow>> =
            std::collections::BTreeMap::new();
        for r in &receipt_rows {
            let block_num: i64 = r.get(COL_BLOCK_NUMBER);
            receipts_by_block.entry(block_num).or_default().push(r);
        }

        // 4. Assemble ColdReceipts per block.
        let mut all_receipts = Vec::with_capacity(headers.len());
        for (&block_num, header) in &headers {
            let block_receipt_rows =
                receipts_by_block.remove(&block_num).unwrap_or_default();
            let mut first_log_index = 0u64;
            let mut prior_cumulative_gas = 0u64;
            let block_receipts: ColdResult<Vec<ColdReceipt>> = block_receipt_rows
                .into_iter()
                .enumerate()
                .map(|(idx, rr)| {
                    let tx_idx: i64 = rr.get(COL_TX_INDEX);
                    let tx_hash = rr.get(COL_TX_HASH);
                    let sender = rr.get(COL_FROM_ADDRESS);
                    let tx_type = rr.get::<i32, _>(COL_TX_TYPE) as i16;
                    let success = rr.get::<i32, _>(COL_SUCCESS) != 0;
                    let cumulative_gas_used: i64 = rr.get(COL_CUMULATIVE_GAS_USED);
                    let logs = logs_by_block_tx
                        .remove(&(block_num, tx_idx))
                        .unwrap_or_default();
                    let receipt = build_receipt(tx_type, success, cumulative_gas_used, logs)
                        .map_err(ColdStorageError::from)?;
                    let gas_used =
                        receipt.inner.cumulative_gas_used - prior_cumulative_gas;
                    prior_cumulative_gas = receipt.inner.cumulative_gas_used;
                    let ir = IndexedReceipt {
                        receipt,
                        tx_hash,
                        first_log_index,
                        gas_used,
                        sender,
                    };
                    first_log_index += ir.receipt.inner.logs.len() as u64;
                    Ok(ColdReceipt::new(ir, header, idx as u64))
                })
                .collect();
            all_receipts.push(block_receipts?);
        }

        // 5. Delete from all tables (same order as truncate_above).
        for table in
            ["logs", "transactions", "receipts", "signet_events", "zenith_headers", "headers"]
        {
            sqlx::query(&format!("DELETE FROM {table} WHERE block_number > $1"))
                .bind(bn)
                .execute(&mut *tx)
                .await
                .map_err(SqlColdError::from)?;
        }

        tx.commit().await.map_err(SqlColdError::from)?;
        Ok(all_receipts)
    }
}
```

- [ ] **Step 2: Run conformance tests**

Run: `cargo t -p signet-cold-sql --all-features`
Expected: All tests pass. The conformance suite tests `drain_above` (via `ColdStorage` trait).

- [ ] **Step 3: Clippy and format**

Run: `cargo clippy -p signet-cold-sql --all-features --all-targets`
Run: `cargo +nightly fmt`
Expected: Clean.

- [ ] **Step 4: Commit**

```bash
git add crates/cold-sql/src/backend.rs
git commit -m "perf(cold-sql): override drain_above to batch queries in single transaction"
```

---

### Task 6: Final Verification

- [ ] **Step 1: Full clippy pass (both feature configurations)**

Run: `cargo clippy -p signet-cold-sql --all-features --all-targets`
Run: `cargo clippy -p signet-cold-sql --no-default-features --all-targets`
Expected: Clean.

- [ ] **Step 2: Format**

Run: `cargo +nightly fmt`
Expected: No changes.

- [ ] **Step 3: Full test suite**

Run: `cargo t -p signet-cold-sql --all-features`
Expected: All tests pass.

- [ ] **Step 4: PostgreSQL conformance (if available)**

Run: `./scripts/test-postgres.sh`
Expected: All tests pass.

- [ ] **Step 5: Workspace clippy check**

Run: `cargo clippy --workspace --all-features --all-targets`
Expected: No new warnings from our changes.
