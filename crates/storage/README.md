# signet-storage

Unified storage interface combining hot and cold backends.

This crate provides [`UnifiedStorage`], a single API for writing execution data
to both hot storage (fast state access) and cold storage (historical archival).

## Architecture

```text
                    UnifiedStorage
                         │
           ┌─────────────┴─────────────┐
           ▼                           ▼
      Hot Storage                Cold Storage
   (synchronous writes)      (async dispatch)
           │                           │
    ┌──────┴──────┐             ┌──────┴──────┐
    │   Headers   │             │   Headers   │
    │    State    │             │    Txs      │
    │  Changesets │             │  Receipts   │
    │   History   │             │   Events    │
    └─────────────┘             └─────────────┘
```

- **Hot storage**: Headers and state changes for fast EVM access
- **Cold storage**: Full block data (transactions, receipts, events)

## Write Semantics

- Hot writes are **synchronous** (database transactions)
- Cold writes are **dispatched** (non-blocking, fire-and-forget)
- Hot storage is **authoritative**; cold may lag behind

## Usage

```rust,ignore
use signet_storage::UnifiedStorage;

// Create from hot and cold backends
let storage = UnifiedStorage::new(hot_db, cold_handle);

// Write executed blocks (hot first, then cold)
storage.append_blocks(blocks)?;

// Handle reorgs
storage.unwind_above(reorg_block)?;

// Check if cold is behind
if let Some(first_missing) = storage.cold_lag().await? {
    // Replay missing blocks to cold storage
    storage.replay_to_cold(missing_blocks).await?;
}
```

## Error Handling

- `StorageError::Hot`: Hot storage failed. No data written.
- `StorageError::Cold`: Hot succeeded, cold dispatch failed. Data is safe in
  hot storage and can be recovered via `replay_to_cold`.

Cold dispatch errors indicate either:
- `Backpressure`: Channel full, task alive. Transient.
- `TaskTerminated`: Task stopped. Requires restart.

## Re-exports

Key types are re-exported for convenience:

- `ExecutedBlock`, `ExecutedBlockBuilder` - Block data structures
- `HotKv`, `HistoryRead`, `HistoryWrite` - Hot storage traits
- `ColdStorageHandle`, `ColdStorageError` - Cold storage types
