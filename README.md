# Signet Storage

High-level APIs for Signet's storage layer.

## Design Overview

We divide the storage system into two main components:

1. Hot storage, used in the critical consensus path.
2. Cold storage, used for historical data, RPC queries, and archival.

Hot and cold storage have different designs because they serve different
purposes:

- **Mutability**: Hot state changes constantly during block execution; cold
  data is finalized history that only grows (or truncates during reorgs).
- **Access patterns**: State execution requires fast point lookups; historical
  queries are block-centric and sequential.
- **Consistency**: Hot storage needs ACID transactions to maintain consistent
  state mid-block; cold storage can use eventual consistency via async ops.

This separation allows us to optimize each layer for its specific access
patterns and performance requirements. Hot storage needs to be fast and mutable,
while cold storage can be optimized for bulk writes, and asynchronous access.

See the crate documentation for `hot` and `cold` for more details on each
design.

```ignore,bash
cargo doc --no-deps --open -p signet-hot
cargo doc --no-deps --open -p signet-cold
```

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
