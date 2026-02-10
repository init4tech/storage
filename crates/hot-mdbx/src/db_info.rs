use bytes::Buf;
use parking_lot::RwLock;
use signet_hot::ValSer;
use std::collections::HashMap;

/// Type alias for the FixedSizeInfo cache.
pub type FsiCache = std::sync::Arc<RwLock<HashMap<&'static str, FixedSizeInfo>>>;

/// Information about fixed size values in a database.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FixedSizeInfo {
    /// Not a DUPSORT table.
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
}
