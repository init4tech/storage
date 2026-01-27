# signet-cold

Async cold storage engine for historical Ethereum data.

Cold storage is optimized for append-only writes with block-ordered data,
efficient bulk reads by block number or index, and truncation for reorg
handling.

## Architecture

The cold storage engine uses a task-based architecture:

- `ColdStorage` trait defines the backend interface
- `ColdStorageTask` processes requests from a channel
- `ColdStorageHandle` provides an ergonomic API for sending requests

## Core Types

- **Specifiers**: `HeaderSpecifier`, `TransactionSpecifier`, `ReceiptSpecifier`
  for querying by block number, hash, or other criteria
- **Requests**: `ColdReadRequest`, `ColdWriteRequest`, `AppendBlockRequest`
- **Errors**: `ColdStorageError`, `ColdResult`

## Feature Flags

- `in-memory`: Enables the in-memory backend implementation
- `test-utils`: Enables test utilities and conformance tests (implies `in-memory`)
