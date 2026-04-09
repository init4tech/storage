use bytes::Buf;
use parking_lot::RwLock;
use signet_hot::{ValSer, tables::NUM_TABLES};
use std::{collections::HashMap, sync::Arc};

/// Inner storage for the two-tier FSI cache.
///
/// The `known` array holds pre-populated entries for the standard tables,
/// searched via lock-free linear scan. The `dynamic` map holds entries for
/// tables created at runtime.
#[derive(Debug)]
struct FsiCacheInner {
    /// Pre-populated at open time. Lock-free linear scan.
    known: [(&'static str, FixedSizeInfo); NUM_TABLES],
    /// Locking fallback for dynamically created tables.
    dynamic: RwLock<HashMap<&'static str, FixedSizeInfo>>,
}

/// Two-tier cache for [`FixedSizeInfo`].
///
/// The fast path is a lock-free linear scan over the known table entries.
/// The slow path acquires a `RwLock` for dynamically created tables.
#[derive(Debug, Clone)]
pub(crate) struct FsiCache(Arc<FsiCacheInner>);

impl Default for FsiCache {
    fn default() -> Self {
        Self::new([("", FixedSizeInfo::None); NUM_TABLES])
    }
}

impl FsiCache {
    /// Create a new `FsiCache` pre-populated with the known table entries.
    pub(crate) fn new(known: [(&'static str, FixedSizeInfo); NUM_TABLES]) -> Self {
        Self(Arc::new(FsiCacheInner { known, dynamic: RwLock::new(HashMap::new()) }))
    }

    /// Look up a table's [`FixedSizeInfo`].
    ///
    /// Checks the lock-free known array first, then the locked dynamic map.
    /// Returns `None` if the table is not cached.
    pub(crate) fn get(&self, name: &str) -> Option<FixedSizeInfo> {
        // Fast path: linear scan over known tables (no lock).
        for &(known_name, fsi) in &self.0.known {
            if known_name == name {
                return Some(fsi);
            }
        }
        // Slow path: check dynamic map.
        self.0.dynamic.read().get(name).copied()
    }

    /// Insert a dynamically created table's [`FixedSizeInfo`].
    pub(crate) fn insert_dynamic(&self, name: &'static str, fsi: FixedSizeInfo) {
        self.0.dynamic.write().insert(name, fsi);
    }
}

/// Information about fixed size values in a database.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum FixedSizeInfo {
    /// Not a DUPSORT table.
    #[default]
    None,
    /// DUPSORT table without DUP_FIXED (variable value size).
    DupSort {
        /// Size of key2 in bytes.
        key2_size: usize,
    },
    /// DUP_FIXED table with known key2 and total size.
    DupFixed {
        /// Size of key2 in bytes.
        key2_size: usize,
        /// Total fixed size (key2 + value).
        total_size: usize,
    },
}

impl FixedSizeInfo {
    /// Returns true if this is a DUP_FIXED table with known total size.
    pub const fn is_dup_fixed(&self) -> bool {
        matches!(self, Self::DupFixed { .. })
    }

    /// Returns true if there is no fixed size (not a DUPSORT table).
    pub const fn is_none(&self) -> bool {
        matches!(self, Self::None)
    }

    /// Returns true if this is a DUPSORT table (with or without DUP_FIXED).
    pub const fn is_dupsort(&self) -> bool {
        matches!(self, Self::DupSort { .. } | Self::DupFixed { .. })
    }

    /// Returns the key2 size if known (for DUPSORT tables).
    pub const fn key2_size(&self) -> Option<usize> {
        match self {
            Self::DupSort { key2_size } => Some(*key2_size),
            Self::DupFixed { key2_size, .. } => Some(*key2_size),
            Self::None => None,
        }
    }

    /// Returns the total stored size (key2 + value) if known (only for DUP_FIXED tables).
    pub const fn total_size(&self) -> Option<usize> {
        match self {
            Self::DupFixed { total_size, .. } => Some(*total_size),
            _ => None,
        }
    }
}

impl ValSer for FixedSizeInfo {
    fn encoded_size(&self) -> usize {
        8 // two u32s: key2_size and total_size
    }

    fn encode_value_to<B>(&self, buf: &mut B)
    where
        B: bytes::BufMut + AsMut<[u8]>,
    {
        match self {
            FixedSizeInfo::None => {
                buf.put_u32(0);
                buf.put_u32(0);
            }
            FixedSizeInfo::DupSort { key2_size } => {
                buf.put_u32(*key2_size as u32);
                buf.put_u32(0); // total_size = 0 means variable value
            }
            FixedSizeInfo::DupFixed { key2_size, total_size } => {
                buf.put_u32(*key2_size as u32);
                buf.put_u32(*total_size as u32);
            }
        }
    }

    fn decode_value(data: &[u8]) -> Result<Self, signet_hot::DeserError>
    where
        Self: Sized,
    {
        if data.len() < 8 {
            return Err(signet_hot::DeserError::InsufficientData {
                needed: 8,
                available: data.len(),
            });
        }
        let mut buf = data;
        let key2_size = buf.get_u32() as usize;
        let total_size = buf.get_u32() as usize;
        if key2_size == 0 {
            Ok(FixedSizeInfo::None)
        } else if total_size == 0 {
            Ok(FixedSizeInfo::DupSort { key2_size })
        } else {
            Ok(FixedSizeInfo::DupFixed { key2_size, total_size })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(fsi: FixedSizeInfo) {
        let mut buf = [0u8; 8];
        fsi.encode_value_to(&mut buf.as_mut_slice());
        let decoded = FixedSizeInfo::decode_value(&buf).unwrap();
        assert_eq!(fsi, decoded);
    }

    #[test]
    fn fsi_roundtrip_none() {
        roundtrip(FixedSizeInfo::None);
    }

    #[test]
    fn fsi_roundtrip_dupsort() {
        roundtrip(FixedSizeInfo::DupSort { key2_size: 32 });
    }

    #[test]
    fn fsi_roundtrip_dupfixed() {
        roundtrip(FixedSizeInfo::DupFixed { key2_size: 32, total_size: 64 });
    }

    #[test]
    fn fsi_decode_too_short() {
        let data = [0u8; 7];
        match FixedSizeInfo::decode_value(&data) {
            Err(signet_hot::DeserError::InsufficientData { needed, available }) => {
                assert_eq!(needed, 8);
                assert_eq!(available, 7);
            }
            other => panic!("expected InsufficientData, got: {other:?}"),
        }
    }

    #[test]
    fn fsi_cache_known_path() {
        let known = [
            ("TableA", FixedSizeInfo::None),
            ("TableB", FixedSizeInfo::DupSort { key2_size: 32 }),
            ("TableC", FixedSizeInfo::DupFixed { key2_size: 32, total_size: 64 }),
            ("TableD", FixedSizeInfo::None),
            ("TableE", FixedSizeInfo::None),
            ("TableF", FixedSizeInfo::None),
            ("TableG", FixedSizeInfo::None),
            ("TableH", FixedSizeInfo::None),
            ("TableI", FixedSizeInfo::None),
        ];
        let cache = FsiCache::new(known);

        assert_eq!(cache.get("TableA"), Some(FixedSizeInfo::None));
        assert_eq!(cache.get("TableB"), Some(FixedSizeInfo::DupSort { key2_size: 32 }));
        assert_eq!(
            cache.get("TableC"),
            Some(FixedSizeInfo::DupFixed { key2_size: 32, total_size: 64 })
        );
        // Unknown table returns None
        assert_eq!(cache.get("Unknown"), None);
    }

    #[test]
    fn fsi_cache_dynamic_path() {
        let known = [
            ("T1", FixedSizeInfo::None),
            ("T2", FixedSizeInfo::None),
            ("T3", FixedSizeInfo::None),
            ("T4", FixedSizeInfo::None),
            ("T5", FixedSizeInfo::None),
            ("T6", FixedSizeInfo::None),
            ("T7", FixedSizeInfo::None),
            ("T8", FixedSizeInfo::None),
            ("T9", FixedSizeInfo::None),
        ];
        let cache = FsiCache::new(known);

        // Not in known set
        assert_eq!(cache.get("DynTable"), None);

        // Insert dynamically
        let fsi = FixedSizeInfo::DupSort { key2_size: 20 };
        cache.insert_dynamic("DynTable", fsi);
        assert_eq!(cache.get("DynTable"), Some(fsi));
    }
}
