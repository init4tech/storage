//! Type definitions for dual-key traversal.

use crate::tables::DualKey;
use std::borrow::Cow;

/// Raw k2-value pair: (key2, value).
///
/// This is returned by `iter_k2()` on dual-keyed tables. The caller already
/// knows k1 (they passed it in), so we don't return it redundantly.
pub type RawK2Value<'a> = (Cow<'a, [u8]>, super::RawValue<'a>);

/// Typed k2-value pair for a dual-keyed table.
pub type K2Value<T> = (<T as DualKey>::Key2, <T as crate::tables::Table>::Value);

/// An item from a dual-key iterator.
///
/// This enum avoids cloning k1 for every value when iterating
/// over dual-keyed tables. K1 is only provided when it changes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DualKeyItem<K1, K2, V> {
    /// First entry for a new k1.
    NewK1(K1, K2, V),
    /// Additional k2/value for the current k1.
    SameK1(K2, V),
}

impl<K1, K2, V> DualKeyItem<K1, K2, V> {
    /// Returns the value, consuming self.
    pub fn into_value(self) -> V {
        match self {
            Self::NewK1(_, _, v) | Self::SameK1(_, v) => v,
        }
    }

    /// Returns a reference to the value.
    pub const fn value(&self) -> &V {
        match self {
            Self::NewK1(_, _, v) | Self::SameK1(_, v) => v,
        }
    }

    /// Returns the k2, consuming self.
    pub fn into_k2(self) -> K2 {
        match self {
            Self::NewK1(_, k2, _) | Self::SameK1(k2, _) => k2,
        }
    }

    /// Returns a reference to k2.
    pub const fn k2(&self) -> &K2 {
        match self {
            Self::NewK1(_, k2, _) | Self::SameK1(k2, _) => k2,
        }
    }

    /// Returns k1 if this is a NewK1 entry.
    pub const fn k1(&self) -> Option<&K1> {
        match self {
            Self::NewK1(k1, _, _) => Some(k1),
            Self::SameK1(_, _) => None,
        }
    }

    /// Returns true if this item represents a new k1.
    pub const fn is_new_k1(&self) -> bool {
        matches!(self, Self::NewK1(..))
    }

    /// Convert to a tuple, requiring k1 to be provided for SameK1 variants.
    pub fn into_tuple(self, current_k1: K1) -> (K1, K2, V) {
        match self {
            Self::NewK1(k1, k2, v) => (k1, k2, v),
            Self::SameK1(k2, v) => (current_k1, k2, v),
        }
    }
}

/// Raw dual-key item: (k1?, k2, value) where k1 is only present on NewK1.
pub type RawDualKeyItem<'a> = DualKeyItem<Cow<'a, [u8]>, Cow<'a, [u8]>, Cow<'a, [u8]>>;
