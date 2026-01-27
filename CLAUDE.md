# Signet Storage

Storage system for the Signet project with hot/cold architecture.

## Crates

| Crate | Purpose |
|-------|---------|
| `signet-storage-types` | Shared primitive types (adapted from Reth) |
| `signet-cold` | Append-only cold storage for historical blockchain data |
| `signet-hot` | Trait-based hot storage abstractions |
| `signet-hot-mdbx` | MDBX implementation of hot storage |

## Architecture

**Hot storage** (`signet-hot`, `signet-hot-mdbx`): Fast key-value access for
frequently used data. Trait-based design allows different backends.

**Cold storage** (`signet-cold`): Append-only storage for historical data
indexed by block. Uses task-based async pattern with handles.

## Key Traits

- `ColdStorage`: Backend interface for cold storage
- `HotKv`, `HotKvRead`, `HotKvWrite`: Hot storage abstractions
- `HistoryRead`, `HistoryWrite`: Higher-level table operations

## Feature Flags

Common pattern across crates:
- `in-memory`: In-memory backend for testing
- `test-utils`: Test utilities and conformance tests
