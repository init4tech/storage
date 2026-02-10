# signet-cold

Async cold storage engine for historical Ethereum data.

Cold storage is optimized for append-only writes with block-ordered data,
efficient bulk reads by block number or index, and truncation for reorg
handling.

## Architecture

The cold storage engine uses a task-based architecture with separate channels
for reads and writes:

- `ColdStorage` trait defines the backend interface
- `ColdStorageTask` processes requests from channels
- `ColdStorageHandle` provides full read/write access
- `ColdStorageReadHandle` provides read-only access

### Channel Separation

Reads and writes use **separate channels**:

- **Read channel**: Shared between `ColdStorageHandle` and
  `ColdStorageReadHandle`. Reads are processed concurrently (up to 64 in
  flight).
- **Write channel**: Exclusive to `ColdStorageHandle`. Writes are processed
  sequentially to maintain ordering.

This design allows read-heavy workloads to proceed without being blocked by
write operations, while ensuring write ordering is preserved.

## Core Types

- **Handles**: `ColdStorageHandle` (read/write), `ColdStorageReadHandle`
  (read-only)
- **Specifiers**: `HeaderSpecifier`, `TransactionSpecifier`, `ReceiptSpecifier`
  for querying by block number, hash, or other criteria
- **Requests**: `ColdReadRequest`, `ColdWriteRequest`, `AppendBlockRequest`
- **Errors**: `ColdStorageError`, `ColdResult`

## Consistency Model

Cold storage is **eventually consistent** with hot storage. Hot storage is
always authoritative.

- **Normal operation**: Writes are dispatched asynchronously. Cold may be a few
  blocks behind hot.
- **Backpressure**: If cold cannot keep up, dispatch returns
  `ColdStorageError::Backpressure`.
- **Task failure**: If the task stops, dispatch returns
  `ColdStorageError::TaskTerminated`.

Use `UnifiedStorage::cold_lag()` to detect gaps and `replay_to_cold()` to
recover.

## Feature Flags

- `in-memory`: Enables the in-memory backend implementation
- `test-utils`: Enables test utilities and conformance tests (implies `in-memory`)
