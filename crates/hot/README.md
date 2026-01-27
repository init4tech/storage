# signet-hot

Hot storage abstractions for fast read/write access to frequently used data.

Hot storage provides trait-based abstractions over key-value storage backends,
with opinionated serialization and predefined tables for blockchain state.

## Trait Model

- `HotKv`: Creates read and write transactions
- `HotKvRead`: Transactional read-only access
- `HotKvWrite`: Transactional read-write access
- `HistoryRead` / `HistoryWrite`: Higher-level abstractions for predefined
  tables

## Serialization

Hot storage is opinionated about serialization. Each table defines key and
value types that implement `KeySer` and `ValSer` traits.

## Tables

Predefined tables are in the `tables` module. The `Table` and `DualKey` traits
define the interface for tables.

## Feature Flags

- `in-memory`: Enables the in-memory backend implementation
- `test-utils`: Enables test utilities and conformance tests (implies `in-memory`)

# Provenance

Significant portions of this code were originally developed for the
[Reth](https://github.com/paradigmxyz/reth) project by Paradigm. They have been
adapted and extended for use in the Signet project, with contributions from the
Signet development team.

Reth is available under choice of MIT or Apache-2.0 License. This crate is
licensed the same. Please refer to the license information in the
[Apache](https://github.com/paradigmxyz/reth/blob/main/LICENSE-APACHE) or
[MIT](https://github.com/paradigmxyz/reth/blob/main/LICENSE-MIT) files for more
information.
