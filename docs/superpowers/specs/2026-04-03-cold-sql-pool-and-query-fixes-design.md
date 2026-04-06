# Cold Storage SQL Backend: Pool Starvation & Query Fixes

## Problem

The cold storage task layer supports 64 concurrent readers and 8 stream
producers, but `SqlColdBackend::connect()` creates a **single-connection
pool** for all backends — including PostgreSQL. This serializes every
operation at the sqlx layer and creates several failure modes:

- **Stream starvation**: A REPEATABLE READ streaming transaction holds the
  sole connection for up to 60 seconds, blocking all writes and reads.
- **Write amplification**: The task drains all in-flight reads before each
  write. With 64 readers serialized on one connection, drain time scales
  linearly with queue depth.
- **Cascading backpressure**: Write latency climbs, the 256-slot write
  channel fills, `dispatch_append_blocks` returns `Backpressure`, and the
  node pipeline stalls.

Several query patterns compound the problem with unnecessary round-trips.

## Changes

### 1. Pool Configuration

**Files:** `crates/cold-sql/src/backend.rs`, `crates/cold-sql/src/connector.rs`

#### `SqlColdBackend::connect()`

Detect backend from URL prefix and apply appropriate defaults:

- **SQLite** (`url.starts_with("sqlite:")`): `max_connections(1)` — required
  for in-memory databases to share state across queries.
- **PostgreSQL** (everything else): `max_connections(10)`,
  `acquire_timeout(5s)`.

The `acquire_timeout` prevents indefinite hangs when PostgreSQL is
unresponsive. Without it, `pool.begin().await` blocks forever and the cold
storage task silently stalls.

A new internal method `connect_with(url, overrides)` handles the actual pool
construction. `connect(url)` becomes a convenience wrapper passing no
overrides.

#### `SqlConnector` Builder

Add optional override fields:

```rust
pub struct SqlConnector {
    url: String,
    max_connections: Option<u32>,
    acquire_timeout: Option<Duration>,
}
```

Builder methods:

- `with_max_connections(n: u32) -> Self`
- `with_acquire_timeout(d: Duration) -> Self`

The `ColdConnect::connect()` implementation passes these overrides through to
`SqlColdBackend::connect_with()`. Production callers (e.g., `StorageConfig`
in node-config) can tune pool size without constructing their own `AnyPool`.

`from_env()` and `new(url)` use defaults (no overrides), preserving backward
compatibility.

### 2. `get_logs`: LIMIT Instead of COUNT + SELECT

**File:** `crates/cold-sql/src/backend.rs` (fn `get_logs`, ~line 1293)

Replace the two-query pattern with a single query:

```sql
SELECT l.*, h.block_hash, h.timestamp AS block_timestamp, t.tx_hash,
       (r.first_log_index + l.log_index) AS block_log_index
FROM logs l
JOIN headers h ON l.block_number = h.block_number
JOIN transactions t ON l.block_number = t.block_number AND l.tx_index = t.tx_index
JOIN receipts r ON l.block_number = r.block_number AND l.tx_index = r.tx_index
WHERE <filters>
ORDER BY l.block_number, l.tx_index, l.log_index
LIMIT <max_logs + 1>
```

If the result has more than `max_logs` rows, return `TooManyLogs`.

This is strictly better:

- **Under limit**: 1 query instead of 2.
- **Over limit**: PostgreSQL stops after `max_logs + 1` rows. The COUNT
  approach scans the entire result set.
- **No information lost**: `TooManyLogs { limit }` only reports the limit,
  not the actual count.

The LIMIT parameter is appended as the last bind parameter after the
address/topic filter params. `build_log_filter_clause` already returns the
next available parameter index.

### 3. `get_receipt`: Consolidate to 2-3 Queries

**File:** `crates/cold-sql/src/backend.rs` (fn `get_receipt`, ~line 1068)

Merge the header fetch, receipt data, and prior cumulative gas into one query:

```sql
SELECT r.*, t.tx_hash, t.from_address,
       h.block_hash AS h_block_hash, h.parent_hash AS h_parent_hash, ...,
       COALESCE(
         (SELECT MAX(r2.cumulative_gas_used)
          FROM receipts r2
          WHERE r2.block_number = r.block_number
            AND r2.tx_index < r.tx_index),
         0
       ) AS prior_gas
FROM receipts r
JOIN transactions t ON r.block_number = t.block_number AND r.tx_index = t.tx_index
JOIN headers h ON r.block_number = h.block_number
WHERE r.block_number = $1 AND r.tx_index = $2
```

Then fetch logs in a second query. Total: 2 queries for block+index
specifier, 3 for TxHash specifier (needs initial hash resolve).

Header columns are aliased with an `h_` prefix to avoid collisions with
receipt columns. A new `receipt_with_header_from_row` helper extracts both
types from the combined row.

### 4. `drain_above`: Batch Override

**File:** `crates/cold-sql/src/backend.rs` (impl `ColdStorage`)

Replace the empty `impl ColdStorage for SqlColdBackend {}` with an explicit
`drain_above` override. Single transaction:

1. Fetch all headers where `block_number > $1` (1 query)
2. Fetch all receipts + tx_hash + from_address where `block_number > $1`
   (1 query with JOINs)
3. Fetch all logs where `block_number > $1` (1 query)
4. Group in memory by block number, construct `Vec<Vec<ColdReceipt>>`
5. DELETE from 6 tables where `block_number > $1` (6 queries)
6. COMMIT

Total: 9 queries in one transaction regardless of block count. The default
implementation does `3N + 7` queries for N blocks.

Receipt and log grouping reuses the same in-memory grouping logic from
`get_receipts_in_block`. Extract the shared grouping code into a helper
function.

### 5. Integer Conversion Safety

**File:** `crates/cold-sql/src/convert.rs`

Drop `const` from `to_i64` and `from_i64` and add debug assertions:

- `to_i64(v: u64)`: `debug_assert!(v <= i64::MAX as u64)`
- `from_i64(v: i64)`: `debug_assert!(v >= 0)`

These catch data corruption in debug/test builds with zero runtime cost in
release.

**File:** `crates/cold-sql/src/backend.rs` (~line 337)

Replace `r.get::<i32, _>(COL_TX_TYPE) as u8` with checked conversion:

```rust
let raw: i32 = r.get(COL_TX_TYPE);
let tx_type_u8: u8 = raw.try_into()
    .map_err(|_| SqlColdError::Convert(format!("tx_type out of range: {raw}")))?;
```

## Files Modified

| File | Changes |
|------|---------|
| `crates/cold-sql/src/backend.rs` | Pool defaults, get_logs LIMIT, get_receipt consolidation, drain_above override, tx_type safety |
| `crates/cold-sql/src/connector.rs` | Builder fields + methods for pool configuration |
| `crates/cold-sql/src/convert.rs` | Debug assertions on to_i64 / from_i64 |

## Verification

1. `cargo t -p signet-cold-sql --all-features` — SQLite conformance
2. `./scripts/test-postgres.sh` — PostgreSQL conformance
3. `cargo clippy -p signet-cold-sql --all-features --all-targets`
4. `cargo clippy -p signet-cold-sql --no-default-features --all-targets`
5. `cargo +nightly fmt`
