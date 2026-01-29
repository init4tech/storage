#![allow(dead_code)]

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
use alloy::{
    consensus::{Header, Sealable},
    primitives::B256,
};
use signet_storage_types::SealedHeader;

/// Run all conformance tests against a [`HotKv`] implementation.
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
}

/// Helper to create a sealed header at a given height with specific parent
pub(crate) fn make_header(number: u64, parent_hash: B256) -> SealedHeader {
    let header = Header { number, parent_hash, gas_limit: 1_000_000, ..Default::default() };
    header.seal_slow()
}
