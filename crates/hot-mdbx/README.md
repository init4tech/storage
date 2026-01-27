# signet-hot-mdbx

MDBX implementation of the `signet-hot` traits.

This crate provides an MDBX-backed implementation of the `HotKv` trait for
production use.

## Usage

```rust,ignore
use signet_hot_mdbx::{DatabaseArguments, DatabaseEnvKind, DatabaseEnv};

let db = DatabaseArguments::new()
    .with_geometry_max_size(Some(8 * TERABYTE))
    .with_sync_mode(Some(SyncMode::Durable))
    .open_rw(path)?;
```

## Core Types

- `DatabaseEnv`: MDBX environment implementing `HotKv`
- `DatabaseArguments`: Builder for database configuration
- `Tx<RO>` / `Tx<RW>`: Read-only and read-write transactions

## Configuration

Key configuration options via `DatabaseArguments`:

- `with_geometry_max_size`: Maximum database size
- `with_sync_mode`: `Durable` (crash-safe) or `SafeNoSync` (faster)
- `with_exclusive`: Exclusive/monopolistic mode
- `with_max_readers`: Maximum concurrent readers

## Feature Flags

- `test-utils`: Enables test utilities
- `disable-lock`: Disables the storage lock file
