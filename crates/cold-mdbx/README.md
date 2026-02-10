# signet-cold-mdbx

MDBX backend implementation for cold storage.

This crate provides table definitions and an MDBX-based backend for storing
historical blockchain data.

## Tables

### Primary Data Tables

| Table | Key | Value |
|-------|-----|-------|
| `ColdHeaders` | Block number | Header |
| `ColdTransactions` | (Block number, tx index) | Transaction |
| `ColdReceipts` | (Block number, tx index) | Receipt |
| `ColdSignetEvents` | (Block number, event index) | Signet event |
| `ColdZenithHeaders` | Block number | Zenith header |

### Index Tables

| Table | Key | Value |
|-------|-----|-------|
| `ColdBlockHashIndex` | Block hash | Block number |
| `ColdTxHashIndex` | Tx hash | (Block number, tx index) |

### Metadata Tables

| Table | Key | Value |
|-------|-----|-------|
| `ColdMetadata` | Metadata key | Block number |

Metadata keys: `Latest`, `Finalized`, `Safe`, `Earliest`.

## Usage

```rust,ignore
use signet_cold_mdbx::{MdbxColdBackend, DatabaseArguments, DatabaseEnvKind};

// Open database
let backend = MdbxColdBackend::open(path, DatabaseEnvKind::RW, DatabaseArguments::new())?;

// Use with cold storage task
let (handle, task) = ColdStorageTask::new(backend, 64);
tokio::spawn(task.run());
```

## Re-exports

- `DatabaseArguments`, `DatabaseEnvKind` from `signet-hot-mdbx`
