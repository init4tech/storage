//! Conversion helpers between Rust types and SQL column values.
//!
//! All fixed-size cryptographic types (B256, Address, U256, u128) are stored
//! as big-endian byte slices in BLOB columns. Scalar integers (u64) that fit
//! in SQL INTEGER are stored as i64.

use crate::SqlColdError;
use alloy::{
    consensus::{Receipt as AlloyReceipt, TxType},
    eips::{
        eip2930::{AccessList, AccessListItem},
        eip7702::{Authorization, SignedAuthorization},
    },
    primitives::{Address, B256, Log, TxKind, U256},
};
use signet_storage_types::Receipt;

// ============================================================================
// Integer conversions
// ============================================================================

/// Convert u64 to i64 for SQL storage.
pub(crate) const fn to_i64(v: u64) -> i64 {
    v as i64
}

/// Convert i64 from SQL back to u64.
pub(crate) const fn from_i64(v: i64) -> u64 {
    v as u64
}

// ============================================================================
// Fixed-size type encoding
// ============================================================================

/// Encode a U256 as 32 big-endian bytes.
pub(crate) const fn encode_u256(v: &U256) -> [u8; 32] {
    v.to_be_bytes::<32>()
}

/// Decode a U256 from big-endian bytes.
pub(crate) fn decode_u256(data: &[u8]) -> Result<U256, SqlColdError> {
    if data.len() < 32 {
        return Err(SqlColdError::Convert(format!("U256 requires 32 bytes, got {}", data.len())));
    }
    Ok(U256::from_be_slice(data))
}

/// Encode a u128 as 16 big-endian bytes.
pub(crate) const fn encode_u128(v: u128) -> [u8; 16] {
    v.to_be_bytes()
}

/// Decode a u128 from big-endian bytes.
pub(crate) fn decode_u128(data: &[u8]) -> Result<u128, SqlColdError> {
    let arr: [u8; 16] = data.try_into().map_err(|_| {
        SqlColdError::Convert(format!("u128 requires 16 bytes, got {}", data.len()))
    })?;
    Ok(u128::from_be_bytes(arr))
}

// ============================================================================
// Address / TxKind helpers
// ============================================================================

/// Encode a [`TxKind`] to an optional address blob for SQL.
pub(crate) fn to_address(kind: &TxKind) -> Option<Vec<u8>> {
    match kind {
        TxKind::Call(addr) => Some(addr.as_slice().to_vec()),
        TxKind::Create => None,
    }
}

/// Decode an optional address blob back to a [`TxKind`].
pub(crate) fn from_address(data: Option<&[u8]>) -> TxKind {
    data.map_or(TxKind::Create, |b| TxKind::Call(Address::from_slice(b)))
}

// ============================================================================
// Nullable field helpers
// ============================================================================

/// Decode a required u128 from a nullable blob column.
pub(crate) fn decode_u128_required(
    data: &Option<Vec<u8>>,
    field: &str,
) -> Result<u128, SqlColdError> {
    data.as_ref()
        .ok_or_else(|| SqlColdError::Convert(format!("{field} is required")))
        .and_then(|b| decode_u128(b))
}

/// Decode an access list from a nullable blob column, defaulting to empty.
pub(crate) fn decode_access_list_or_empty(
    data: &Option<Vec<u8>>,
) -> Result<AccessList, SqlColdError> {
    data.as_ref().map(|b| decode_access_list(b)).transpose().map(|opt| opt.unwrap_or_default())
}

// ============================================================================
// AccessList encoding (compact binary)
// ============================================================================

/// Encode an access list as a compact binary blob.
///
/// Format: u16 item count, then for each item:
///   - 20 bytes address
///   - u16 key count
///   - 32 bytes per key
pub(crate) fn encode_access_list(list: &AccessList) -> Vec<u8> {
    let items: &[AccessListItem] = &list.0;
    let mut buf = Vec::new();
    buf.extend_from_slice(&(items.len() as u16).to_be_bytes());
    for item in items {
        buf.extend_from_slice(item.address.as_slice());
        buf.extend_from_slice(&(item.storage_keys.len() as u16).to_be_bytes());
        for key in &item.storage_keys {
            buf.extend_from_slice(key.as_slice());
        }
    }
    buf
}

fn decode_access_list(data: &[u8]) -> Result<AccessList, SqlColdError> {
    if data.len() < 2 {
        return Err(SqlColdError::Convert("access_list too short".into()));
    }
    let count = u16::from_be_bytes([data[0], data[1]]) as usize;
    let mut offset = 2;
    let mut items = Vec::with_capacity(count);
    for _ in 0..count {
        if offset + 22 > data.len() {
            return Err(SqlColdError::Convert("access_list truncated".into()));
        }
        let address = Address::from_slice(&data[offset..offset + 20]);
        offset += 20;
        let key_count = u16::from_be_bytes([data[offset], data[offset + 1]]) as usize;
        offset += 2;
        let mut storage_keys = Vec::with_capacity(key_count);
        for _ in 0..key_count {
            if offset + 32 > data.len() {
                return Err(SqlColdError::Convert("access_list keys truncated".into()));
            }
            storage_keys.push(B256::from_slice(&data[offset..offset + 32]));
            offset += 32;
        }
        items.push(AccessListItem { address, storage_keys });
    }
    Ok(AccessList(items))
}

// ============================================================================
// Authorization list encoding (compact binary)
// ============================================================================

/// Encode an authorization list as a compact binary blob.
///
/// Format: u16 count, then for each SignedAuthorization:
///   - 32 bytes chain_id (U256)
///   - 20 bytes address
///   - 8 bytes nonce (u64 BE)
///   - 1 byte y_parity
///   - 32 bytes r (U256)
///   - 32 bytes s (U256)
pub(crate) fn encode_authorization_list(list: &[SignedAuthorization]) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(&(list.len() as u16).to_be_bytes());
    for auth in list {
        let inner = auth.inner();
        buf.extend_from_slice(&inner.chain_id.to_be_bytes::<32>());
        buf.extend_from_slice(inner.address.as_slice());
        buf.extend_from_slice(&inner.nonce.to_be_bytes());
        buf.push(auth.y_parity());
        buf.extend_from_slice(&auth.r().to_be_bytes::<32>());
        buf.extend_from_slice(&auth.s().to_be_bytes::<32>());
    }
    buf
}

pub(crate) fn decode_authorization_list(
    data: &[u8],
) -> Result<Vec<SignedAuthorization>, SqlColdError> {
    if data.is_empty() {
        return Ok(Vec::new());
    }
    if data.len() < 2 {
        return Err(SqlColdError::Convert("authorization_list too short".into()));
    }
    let count = u16::from_be_bytes([data[0], data[1]]) as usize;
    let mut offset = 2;
    let entry_size = 32 + 20 + 8 + 1 + 32 + 32; // 125 bytes
    let mut result = Vec::with_capacity(count);
    for _ in 0..count {
        if offset + entry_size > data.len() {
            return Err(SqlColdError::Convert("authorization_list truncated".into()));
        }
        let chain_id = U256::from_be_slice(&data[offset..offset + 32]);
        offset += 32;
        let address = Address::from_slice(&data[offset..offset + 20]);
        offset += 20;
        let nonce = u64::from_be_bytes(data[offset..offset + 8].try_into().unwrap());
        offset += 8;
        let y_parity = data[offset];
        offset += 1;
        let r = U256::from_be_slice(&data[offset..offset + 32]);
        offset += 32;
        let s = U256::from_be_slice(&data[offset..offset + 32]);
        offset += 32;

        let auth = Authorization { chain_id, address, nonce };
        result.push(SignedAuthorization::new_unchecked(auth, y_parity, r, s));
    }
    Ok(result)
}

// ============================================================================
// B256 vec encoding
// ============================================================================

pub(crate) fn encode_b256_vec(hashes: &[B256]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(2 + hashes.len() * 32);
    buf.extend_from_slice(&(hashes.len() as u16).to_be_bytes());
    for h in hashes {
        buf.extend_from_slice(h.as_slice());
    }
    buf
}

pub(crate) fn decode_b256_vec(data: &[u8]) -> Vec<B256> {
    if data.len() < 2 {
        return Vec::new();
    }
    let count = u16::from_be_bytes([data[0], data[1]]) as usize;
    let mut result = Vec::with_capacity(count);
    for i in 0..count {
        let start = 2 + i * 32;
        if start + 32 <= data.len() {
            result.push(B256::from_slice(&data[start..start + 32]));
        }
    }
    result
}

// ============================================================================
// Receipt builder
// ============================================================================

/// Reconstruct a [`Receipt`] from primitive column values and decoded logs.
pub(crate) fn build_receipt(
    tx_type: i16,
    success: bool,
    cumulative_gas_used: i64,
    logs: Vec<Log>,
) -> Result<Receipt, SqlColdError> {
    let tx_type = TxType::try_from(tx_type as u8)
        .map_err(|_| SqlColdError::Convert(format!("invalid tx_type: {tx_type}")))?;
    Ok(Receipt {
        tx_type,
        inner: AlloyReceipt {
            status: success.into(),
            cumulative_gas_used: from_i64(cumulative_gas_used),
            logs,
        },
    })
}

// ============================================================================
// Signet event constants
// ============================================================================

/// Event type discriminant for Transact.
pub(crate) const EVENT_TRANSACT: i16 = 0;
/// Event type discriminant for Enter.
pub(crate) const EVENT_ENTER: i16 = 1;
/// Event type discriminant for EnterToken.
pub(crate) const EVENT_ENTER_TOKEN: i16 = 2;
