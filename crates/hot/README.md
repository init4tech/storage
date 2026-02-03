# signet-hot

Hot storage abstractions for fast read/write access to frequently used data.

Hot storage provides trait-based abstractions over key-value storage backends,
with opinionated serialization and predefined tables for blockchain state.

## Usage

```rust,ignore
use signet_hot::{HotKv, HistoryRead, HistoryWrite};

fn example<D: HotKv>(db: &D) -> Result<(), signet_hot::db::HotKvError> {
    // Read operations
    let reader = db.reader()?;
    let tip = reader.get_chain_tip()?;
    let account = reader.get_account(&address)?;

    // Write operations (pass iterator of (&header, &bundle) tuples)
    let writer = db.writer()?;
    writer.append_blocks(blocks.iter().map(|(h, b)| (h, b)))?;
    writer.commit()?;
    Ok(())
}
```

For a concrete implementation, see the `signet-hot-mdbx` crate.

## Trait Hierarchy

```text
HotKv                        ← Transaction factory
  ├─ reader() → HotKvRead        ← Read-only transactions
  │              └─ HotDbRead         ← Typed accessors (blanket impl)
  │                   └─ HistoryRead      ← History queries (blanket impl)
  └─ writer() → HotKvWrite       ← Read-write transactions
                 └─ UnsafeDbWrite        ← Low-level writes (blanket impl)
                      └─ HistoryWrite     ← Safe chain operations (blanket impl)
```

## Serialization

Hot storage is opinionated about serialization. Each table defines key and
value types that implement `KeySer` and `ValSer` traits:

- **Keys**: Fixed-size, lexicographically ordered (max 64 bytes)
- **Values**: Variable-size, self-describing (optional fixed-size optimization)

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
