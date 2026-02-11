# Changelog

## 0.1.0

Initial release of the Signet storage workspace.

### Crates

- **signet-storage-types**: Shared primitive types adapted from Reth
- **signet-hot**: Trait-based hot storage abstractions
- **signet-hot-mdbx**: MDBX implementation of hot storage
- **signet-cold**: Append-only cold storage for historical data
- **signet-cold-mdbx**: MDBX table definitions for cold storage
- **signet-storage**: Unified storage interface combining hot and cold backends
