//! Types used by Signet's storage crates.
//!
//! These are typically low-level types that are shared between multiple
//! storage backends, such as key and value types for various tables.

#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    missing_docs,
    unreachable_pub,
    clippy::missing_const_for_fn,
    rustdoc::all
)]
#![cfg_attr(not(test), warn(unused_crate_dependencies))]
#![deny(unused_must_use, rust_2018_idioms)]
#![cfg_attr(docsrs, feature(doc_cfg))]

mod execution;
pub use ::alloy::consensus::transaction::Recovered;
pub use execution::{ExecutedBlock, ExecutedBlockBuilder, MissingFieldError};

mod alloy;
pub use alloy::TransactionSigned;
mod events;
pub use events::{DbSignetEvent, DbZenithHeader};
mod indexed_receipt;
pub use indexed_receipt::IndexedReceipt;
mod int_list;
pub use int_list::{BlockNumberList, IntegerList, IntegerListError};
mod sharded;
pub use sharded::ShardedKey;
pub use signet_evm::{Account, EthereumHardfork, genesis_header};
pub use signet_types::{ConfirmationMeta, Confirmed, Receipt, TxLocation};

use ::alloy::consensus::{Header, Sealed};

/// A recovered transaction type.
pub type RecoveredTx = Recovered<TransactionSigned>;

/// A sealed header type.
pub type SealedHeader = Sealed<Header>;
