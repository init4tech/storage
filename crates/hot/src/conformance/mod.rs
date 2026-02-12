mod cursor;
mod edge_cases;
mod history;
mod range;
mod roundtrip;
mod unwind;

pub use cursor::*;
pub use edge_cases::*;
pub use history::*;
pub use range::*;
pub use roundtrip::*;
pub use unwind::*;

use crate::model::HotKv;

/// Run all conformance tests against a [`HotKv`] implementation.
///
/// Tests share the provided store instance. Additional test functions
/// (cursor, edge-case, history, range) are exported for use in isolation
/// with a fresh store.
pub fn conformance<T: HotKv>(hot_kv: &T) {
    test_header_roundtrip(hot_kv);
    test_account_roundtrip(hot_kv);
    test_storage_roundtrip(hot_kv);
    test_storage_update_replaces(hot_kv);
    test_bytecode_roundtrip(hot_kv);
    test_account_history(hot_kv);
    test_storage_history(hot_kv);
    test_account_changes(hot_kv);
    test_storage_changes(hot_kv);
    test_missing_reads(hot_kv);
    test_cursor_iter_k2(hot_kv);
    test_cursor_iter_k2_single(hot_kv);
    test_cursor_iter_k2_empty(hot_kv);
}
