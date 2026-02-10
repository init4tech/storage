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

## Commands

- `cargo +nightly fmt`
- `cargo clippy -p <crate> --all-features --all-targets`
- `cargo clippy -p <crate> --no-default-features --all-targets`
- `cargo t -p <crate>`

### Pre-commit

- NEVER use `cargo check` or `cargo build`.
- ALWAYS run clippy with both `--all-features` and `--no-default-features`.
- ALWAYS run `cargo +nightly fmt`.
- Run tests per-crate (`-p <crate>`) before running repo-wide.

## Research

- ALWAYS prefer building crate docs and reading them over grep/find/GitHub.

## Code Style

- Match local patterns over personal preferences.
- Functional combinators over imperative control flow. Avoid nesting.
- Small, focused functions and types. Concise rustdoc and comments.
- NEVER add incomplete code or `TODO`s for core logic.
- `TODO` is acceptable only for perf improvements or non-critical features.
- New signet crates MUST use `signet-` prefix.

### Imports

- No glob imports. No blank lines between imports.
- Group imports from the same crate into one `use` statement.

### Visibility

- Default private. `pub(crate)` for internal use. `pub` only for public API.
- NEVER use `pub(super)`.

### Error Handling

- `thiserror` for libraries, `eyre` for applications. NEVER `anyhow`.
- Propagate with `?`. Convert with `map_err`.

### Options & Results

```rust
// NEVER:
if let Some(a) = option { Thing::do(a); } else { return; }
// Preferred:
option.map(Thing::do);
// Or:
let Some(a) = option else { return; };
Thing::do(a);
```

### Structs

- Builder pattern if >4 fields or multiple fields of same type. ASK if unclear.

### Tracing

- Use `tracing`. Spans for bounded work items, not long-lived tasks.
- `skip(self)` in instrumented methods; add needed fields manually.
- Levels: TRACE (rare/verbose), DEBUG (low-level), INFO (default), WARN
  (ignorable errors), ERROR (fatal).
- Propagate spans through task boundaries with `Instrument`.

### Async

- `tokio` conventions. No blocking in async.
- Long-lived tasks: return a spawnable future, don't run directly.
- Short-lived spawned tasks: propagate spans with `Instrument`.

### Unsafe

- ALWAYS include `// SAFETY:` comments.

### Tests

- `mod tests` at bottom of file. Integration tests in `tests/`.
- Fail fast: `unwrap()` directly, never return `Result` from tests.
- No unnecessary setup. No checking before unwrapping.

### Rustdoc

- Doc all public items with concise examples.
- Hide scaffolding with `#` lines. Include implementation guides for traits.
- Consult other `lib.rs` files for common linter directives.

## GitHub

- PRs on fresh branches off `main`. Descriptive branch names.
- Claude Code comments MUST start with `**[Claude Code]**`.
