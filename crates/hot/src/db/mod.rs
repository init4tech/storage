//! Primary access traits for hot storage backends.

mod errors;
pub use errors::{HistoryError, HistoryResult};

pub mod history;
pub use history::{HistoryRead, HistoryWrite};

mod inconsistent;
pub use inconsistent::{BundleInit, LegacyUnsafeHistoryWrite, UnsafeDbWrite};

mod read;
pub use read::{HotDbRead, LegacyHistoryRead};

pub(crate) mod sealed {
    use crate::model::HotKvRead;

    /// Sealed trait to prevent external implementations of hot database traits.
    #[allow(dead_code, unreachable_pub)]
    pub trait Sealed {}
    impl<T> Sealed for T where T: HotKvRead {}
}
