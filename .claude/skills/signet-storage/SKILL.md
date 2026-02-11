---
name: signet-storage
description: Reference for the signet storage library architecture, traits, types, and usage patterns. Use when working with hot storage, cold storage, unified storage, revm integration, history reads, or any signet-storage crate.
user-invocable: true
---

# Signet Storage Library Reference

## Workspace Crates

| Crate | Path | Purpose |
|-------|------|---------|
| `signet-storage-types` | `crates/types/` | Shared primitive types (Account, ExecutedBlock, SealedHeader, etc.) |
| `signet-cold` | `crates/cold/` | Async cold storage engine with task-based handles and LRU cache |
| `signet-cold-mdbx` | `crates/cold-mdbx/` | MDBX backend implementing `ColdStorage` |
| `signet-hot` | `crates/hot/` | Trait-based hot storage abstractions (HotKv, HistoryRead, RevmRead) |
| `signet-hot-mdbx` | `crates/hot-mdbx/` | MDBX implementation of hot storage traits |
| `signet-storage` | `crates/storage/` | Unified storage combining hot + cold |

## Architecture Overview

**Hot storage** provides fast transactional read/write of current blockchain
state. It uses MDBX (or in-memory for tests) and supports height-aware
historical state reconstruction through change sets.

**Cold storage** is append-only archival of full block data (transactions,
receipts, events). It uses a task-based async pattern with channel-separated
read/write handles and an LRU cache.

**Unified storage** (`UnifiedStorage`) combines both: hot writes are
synchronous database transactions, cold writes are dispatched asynchronously
(fire-and-forget). Hot errors are fatal; cold dispatch errors are recoverable.

## Trait Hierarchy

### Hot Storage

```
HotKv                          -- Transaction factory (reader/writer)
├── HotKvRead                  -- Raw + typed read operations
│   ├── HotDbRead (blanket)    -- Typed table accessors (get_header, get_account, etc.)
│   │   ├── HistoryRead (blanket)  -- History queries + height-aware reads
│   │   └── UnsafeHistoryWrite (blanket) -- Low-level history writes
│   └── RevmRead<T>            -- revm DatabaseRef/Database adapter
├── HotKvWrite (extends Read)  -- Queued writes + commit
│   ├── UnsafeDbWrite (blanket)    -- Typed table writes
│   ├── HistoryWrite (blanket)     -- append_blocks, unwind_above, load_genesis
│   └── RevmWrite<U>              -- revm TryDatabaseCommit adapter
```

### Cold Storage

```
ColdStorage                    -- Async backend trait
ColdStorageTask<B>             -- Event loop processing requests
ColdStorageHandle              -- Full read/write handle (Deref to ReadHandle)
ColdStorageReadHandle          -- Read-only handle
```

## Key Types

### `signet-storage-types`

- **`Account`** -- nonce, balance, optional bytecode_hash. Converts
  from/to `revm::state::AccountInfo`.
- **`ExecutedBlock`** -- Complete block output: header, bundle,
  transactions, receipts, signet_events, zenith_header.
- **`ExecutedBlockBuilder`** -- Builder for `ExecutedBlock`. Required:
  `header(SealedHeader)`, `bundle(BundleState)`. Optional: `transactions`,
  `receipts`, `signet_events`, `zenith_header`.
- **`SealedHeader`** -- Type alias for `Sealed<Header>`.
- **`ConfirmationMeta`** -- Block confirmation metadata (block_number,
  block_hash, transaction_index).
- **`Confirmed<T>`** -- Re-exported from `signet-cold`, pairs a value
  with `ConfirmationMeta`.
- **`TxLocation`** -- 16-byte fixed-size key (block number + tx index).
- **`BlockNumberList`** / **`IntegerList`** -- Roaring bitmap-backed lists.
- **`ShardedKey<T>`** -- Composite `(T, u64)` for sharded history tables.

### `signet-cold`

- **`ColdStorageTask::spawn(backend, cancel_token)`** -- Spawns the task
  loop and returns a `ColdStorageHandle`.
- **`ColdStorageHandle`** -- Derefs to `ColdStorageReadHandle`. Write
  methods: `append_block()`, `append_blocks()`, `truncate_above()`.
  Fire-and-forget: `dispatch_append_blocks()`, `dispatch_truncate_above()`.
- **`ColdStorageReadHandle`** -- Read methods:
  `get_header_by_number`, `get_header_by_hash`,
  `get_tx_by_hash`, `get_tx_by_block_and_index`,
  `get_receipt_by_hash`, `get_receipt_by_block_and_index`,
  `get_txs_in_block`, `get_tx_count`,
  `get_receipts_in_block`,
  `get_signet_events_by_number`, `get_zenith_header_by_number`,
  `get_latest_block`.

### `signet-hot`

- **`HotKv`** -- `reader()`, `writer()`, `revm_reader()`, `revm_writer()`,
  `revm_reader_at_height(height)`.
- **`HotKvRead`** -- `raw_get`, `raw_get_dual`, `get<T: SingleKey>`,
  `get_dual<T: DualKey>`, `traverse<T>`, `traverse_dual<T>`.
- **`HotKvWrite`** -- `queue_put<T>`, `queue_put_dual<T>`,
  `queue_delete<T>`, `queue_delete_dual<T>`, `queue_clear<T>`,
  `raw_commit(self)`.
- **`HotDbRead`** (blanket) -- `get_header(n)`, `get_header_number(hash)`,
  `get_account(addr)`, `get_storage(addr, slot)`, `get_bytecode(hash)`.
- **`HistoryRead`** (blanket) -- `get_account_at_height(addr, h)`,
  `get_storage_at_height(addr, slot, h)`, `get_account_change(block, addr)`,
  `get_storage_change(block, addr, slot)`, `get_chain_tip()`,
  `get_execution_range()`, `last_block_number()`, `has_block(n)`,
  `check_height(h)`.
- **`HistoryWrite`** (blanket) -- `validate_chain_extension(headers)`,
  `append_blocks(iter)`, `unwind_above(block)`, `load_genesis(genesis, forks)`.
- **`RevmRead<T>`** -- `new(reader)` for current state,
  `at_height(reader, height)` for historical state. Implements revm
  `Database` + `DatabaseRef`.
- **`RevmWrite<U>`** -- `new(writer)`, `persist(self)`. Implements revm
  `Database` + `DatabaseRef` + `TryDatabaseCommit`.

### `signet-hot-mdbx`

- **`DatabaseArguments::new()`** -- Builder with `.with_geometry_max_size()`,
  `.with_sync_mode()`, `.with_growth_step()`, `.with_exclusive()`,
  `.with_max_readers()`. Terminal: `.open_ro(path)`, `.open_rw(path)`.
- **`DatabaseEnv`** -- Implements `HotKv`.

### `signet-storage`

- **`UnifiedStorage::new(hot, cold_handle)`** -- `append_blocks(blocks)`,
  `unwind_above(block)`, `cold_lag()`, `replay_to_cold()`,
  `reader()`, `revm_reader()`, `revm_reader_at_height(h)`,
  `hot()`, `cold()`, `cold_reader()`.

## Hot Storage Tables

| Table | Key | Key2 | Value | Kind |
|-------|-----|------|-------|------|
| `Headers` | `BlockNumber` | -- | `Header` | SingleKey |
| `HeaderNumbers` | `B256` | -- | `BlockNumber` | SingleKey |
| `Bytecodes` | `B256` | -- | `Bytecode` | SingleKey |
| `PlainAccountState` | `Address` | -- | `Account` | SingleKey |
| `PlainStorageState` | `Address` | `U256` | `U256` | DualKey |
| `AccountsHistory` | `Address` | `u64` | `BlockNumberList` | DualKey |
| `AccountChangeSets` | `BlockNumber` | `Address` | `Account` | DualKey |
| `StorageHistory` | `Address` | `ShardedKey<U256>` | `BlockNumberList` | DualKey |
| `StorageChangeSets` | `(u64, Address)` | `U256` | `U256` | DualKey |

## Cold Storage Tables (MDBX)

| Table | Key | Key2 | Value |
|-------|-----|------|-------|
| `ColdHeaders` | `BlockNumber` | -- | `Header` |
| `ColdTransactions` | `BlockNumber` | `u64` | `TransactionSigned` |
| `ColdReceipts` | `BlockNumber` | `u64` | `Receipt` |
| `ColdSignetEvents` | `BlockNumber` | `u64` | `DbSignetEvent` |
| `ColdZenithHeaders` | `BlockNumber` | -- | `DbZenithHeader` |
| `ColdBlockHashIndex` | `B256` | -- | `BlockNumber` |
| `ColdTxHashIndex` | `B256` | -- | `TxLocation` |
| `ColdMetadata` | `MetadataKey` | -- | `BlockNumber` |

## Feature Flags

| Crate | Feature | Effect |
|-------|---------|--------|
| `signet-cold` | `in-memory` | `mem::MemColdBackend` |
| `signet-cold` | `test-utils` | Conformance tests + in-memory |
| `signet-hot` | `in-memory` | `mem::MemKv` |
| `signet-hot` | `test-utils` | Conformance tests + in-memory |
| `signet-hot-mdbx` | `test-utils` | Test utilities + tempfile |
| `signet-hot-mdbx` | `disable-lock` | Disables file-based storage lock |
| `signet-cold-mdbx` | `test-utils` | Cold test utilities |
| `signet-storage` | `test-utils` | Enables all sub-crate test-utils |

## Error Types

- **`HotKvError`** -- `Inner(E)`, `Deser`, `WriteLocked`, `NoBlocks`,
  `HeightOutOfRange`.
- **`HistoryError<E>`** -- `NonContiguousBlock`, `ParentHashMismatch`,
  `DbNotEmpty`, `EmptyRange`, `NoBlocks`, `HeightOutOfRange`, `Db(E)`,
  `IntList`.
- **`ColdStorageError`** -- `Backend(E)`, `NotFound`, `Cancelled`,
  `Backpressure`, `TaskTerminated`.
- **`StorageError`** -- `Hot(HistoryError<HotKvError>)` |
  `Cold(ColdStorageError)`.

## Usage Patterns

### Setting up unified storage

```rust
use signet_storage::{UnifiedStorage, ColdStorageTask};
use signet_hot_mdbx::DatabaseArguments;
use signet_cold_mdbx::MdbxColdBackend;
use tokio_util::sync::CancellationToken;

// Open hot storage
let hot = DatabaseArguments::new().open_rw(&hot_path).unwrap();

// Open cold backend and spawn task
let cold_backend = MdbxColdBackend::open_rw(&cold_path).unwrap();
let cancel = CancellationToken::new();
let cold_handle = ColdStorageTask::spawn(cold_backend, cancel.clone());

// Create unified storage
let storage = UnifiedStorage::new(hot, cold_handle);
```

### Reading current state via revm

```rust
let reader = storage.revm_reader().unwrap();
let account = reader.basic_ref(address).unwrap();
let value = reader.storage_ref(address, slot).unwrap();
```

### Reading historical state

```rust
let reader = storage.revm_reader_at_height(100).unwrap();
let account = reader.basic_ref(address).unwrap();
```

### Writing blocks

```rust
use signet_storage_types::ExecutedBlockBuilder;

let block = ExecutedBlockBuilder::new()
    .header(sealed_header)
    .bundle(bundle_state)
    .transactions(txs)
    .receipts(receipts)
    .build()
    .unwrap();

storage.append_blocks(vec![block]).unwrap();
```

### Querying cold storage directly

```rust
let cold = storage.cold_reader();
let header = cold.get_header_by_number(42).await.unwrap();
let tx = cold.get_tx_by_hash(tx_hash).await.unwrap();
let receipts = cold.get_receipts_in_block(42).await.unwrap();
```

### Handling reorgs

```rust
// Unwind hot state above block 100, truncate cold
storage.unwind_above(100).unwrap();
```

### Using hot storage directly

```rust
use signet_hot::{HistoryRead, HotKv};

let hot = storage.hot();
let reader = hot.reader().unwrap();
let tip = reader.get_chain_tip().unwrap();
let account = reader.get_account_at_height(address, 50).unwrap();
```
