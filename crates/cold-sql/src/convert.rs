//! Conversion helpers between Rust types and SQL column values.
//!
//! All fixed-size cryptographic types (B256, Address, U256, u128) are stored
//! as big-endian byte slices in BLOB columns. Scalar integers (u64) that fit
//! in SQL INTEGER are stored as i64.

use crate::SqlColdError;
use alloy::{
    consensus::{
        Header, Receipt as AlloyReceipt, Signed, TxEip1559, TxEip2930, TxEip4844, TxEip7702,
        TxLegacy, TxType,
    },
    eips::{
        eip2930::{AccessList, AccessListItem},
        eip7702::{Authorization, SignedAuthorization},
    },
    primitives::{Address, B256, Bloom, Bytes, Log, LogData, Signature, TxKind, U256},
};
use signet_storage_types::{
    DbSignetEvent, DbZenithHeader, Receipt, SealedHeader, TransactionSigned,
};
use signet_zenith::{
    Passage::{Enter, EnterToken},
    Transactor::Transact,
    Zenith,
};

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
pub(crate) fn encode_u256(v: &U256) -> Vec<u8> {
    v.to_be_bytes::<32>().to_vec()
}

/// Decode a U256 from big-endian bytes.
pub(crate) fn decode_u256(data: &[u8]) -> Result<U256, SqlColdError> {
    if data.len() < 32 {
        return Err(SqlColdError::Convert(format!("U256 requires 32 bytes, got {}", data.len())));
    }
    Ok(U256::from_be_slice(data))
}

/// Encode a u128 as 16 big-endian bytes.
pub(crate) fn encode_u128(v: u128) -> Vec<u8> {
    v.to_be_bytes().to_vec()
}

/// Decode a u128 from big-endian bytes.
pub(crate) fn decode_u128(data: &[u8]) -> Result<u128, SqlColdError> {
    let arr: [u8; 16] = data.try_into().map_err(|_| {
        SqlColdError::Convert(format!("u128 requires 16 bytes, got {}", data.len()))
    })?;
    Ok(u128::from_be_bytes(arr))
}

// ============================================================================
// Header conversion
// ============================================================================

/// Row data for a header in SQL.
pub(crate) struct HeaderRow {
    pub block_number: i64,
    pub block_hash: Vec<u8>,
    pub parent_hash: Vec<u8>,
    pub ommers_hash: Vec<u8>,
    pub beneficiary: Vec<u8>,
    pub state_root: Vec<u8>,
    pub transactions_root: Vec<u8>,
    pub receipts_root: Vec<u8>,
    pub logs_bloom: Vec<u8>,
    pub difficulty: Vec<u8>,
    pub gas_limit: i64,
    pub gas_used: i64,
    pub timestamp: i64,
    pub extra_data: Vec<u8>,
    pub mix_hash: Vec<u8>,
    pub nonce: Vec<u8>,
    pub base_fee_per_gas: Option<i64>,
    pub withdrawals_root: Option<Vec<u8>>,
    pub blob_gas_used: Option<i64>,
    pub excess_blob_gas: Option<i64>,
    pub parent_beacon_block_root: Option<Vec<u8>>,
    pub requests_hash: Option<Vec<u8>>,
}

impl HeaderRow {
    /// Convert a sealed header to a SQL row.
    pub(crate) fn from_header(header: &SealedHeader) -> Self {
        Self {
            block_number: to_i64(header.number),
            block_hash: header.hash().as_slice().to_vec(),
            parent_hash: header.parent_hash.as_slice().to_vec(),
            ommers_hash: header.ommers_hash.as_slice().to_vec(),
            beneficiary: header.beneficiary.as_slice().to_vec(),
            state_root: header.state_root.as_slice().to_vec(),
            transactions_root: header.transactions_root.as_slice().to_vec(),
            receipts_root: header.receipts_root.as_slice().to_vec(),
            logs_bloom: header.logs_bloom.as_slice().to_vec(),
            difficulty: encode_u256(&header.difficulty),
            gas_limit: to_i64(header.gas_limit),
            gas_used: to_i64(header.gas_used),
            timestamp: to_i64(header.timestamp),
            extra_data: header.extra_data.to_vec(),
            mix_hash: header.mix_hash.as_slice().to_vec(),
            nonce: header.nonce.as_slice().to_vec(),
            base_fee_per_gas: header.base_fee_per_gas.map(to_i64),
            withdrawals_root: header.withdrawals_root.map(|r| r.as_slice().to_vec()),
            blob_gas_used: header.blob_gas_used.map(to_i64),
            excess_blob_gas: header.excess_blob_gas.map(to_i64),
            parent_beacon_block_root: header
                .parent_beacon_block_root
                .map(|r| r.as_slice().to_vec()),
            requests_hash: header.requests_hash.map(|r| r.as_slice().to_vec()),
        }
    }

    /// Convert a SQL row back to a header.
    pub(crate) fn into_header(self) -> Result<Header, SqlColdError> {
        Ok(Header {
            parent_hash: B256::from_slice(&self.parent_hash),
            ommers_hash: B256::from_slice(&self.ommers_hash),
            beneficiary: Address::from_slice(&self.beneficiary),
            state_root: B256::from_slice(&self.state_root),
            transactions_root: B256::from_slice(&self.transactions_root),
            receipts_root: B256::from_slice(&self.receipts_root),
            logs_bloom: Bloom::from_slice(&self.logs_bloom),
            difficulty: decode_u256(&self.difficulty)?,
            number: from_i64(self.block_number),
            gas_limit: from_i64(self.gas_limit),
            gas_used: from_i64(self.gas_used),
            timestamp: from_i64(self.timestamp),
            extra_data: Bytes::from(self.extra_data),
            mix_hash: B256::from_slice(&self.mix_hash),
            nonce: alloy::primitives::B64::from_slice(&self.nonce),
            base_fee_per_gas: self.base_fee_per_gas.map(from_i64),
            withdrawals_root: self.withdrawals_root.map(|b| B256::from_slice(&b)),
            blob_gas_used: self.blob_gas_used.map(from_i64),
            excess_blob_gas: self.excess_blob_gas.map(from_i64),
            parent_beacon_block_root: self.parent_beacon_block_root.map(|b| B256::from_slice(&b)),
            requests_hash: self.requests_hash.map(|b| B256::from_slice(&b)),
        })
    }
}

// ============================================================================
// Transaction conversion
// ============================================================================

/// Row data for a transaction in SQL.
pub(crate) struct TxRow {
    pub block_number: i64,
    pub tx_index: i64,
    pub tx_hash: Vec<u8>,
    pub tx_type: i16,
    pub sig_y_parity: bool,
    pub sig_r: Vec<u8>,
    pub sig_s: Vec<u8>,
    pub chain_id: Option<i64>,
    pub nonce: i64,
    pub gas_limit: i64,
    pub to_address: Option<Vec<u8>>,
    pub value: Vec<u8>,
    pub input: Vec<u8>,
    pub gas_price: Option<Vec<u8>>,
    pub max_fee_per_gas: Option<Vec<u8>>,
    pub max_priority_fee_per_gas: Option<Vec<u8>>,
    pub max_fee_per_blob_gas: Option<Vec<u8>>,
    pub blob_versioned_hashes: Option<Vec<u8>>,
    pub access_list: Option<Vec<u8>>,
    pub authorization_list: Option<Vec<u8>>,
}

impl TxRow {
    /// Convert a signed transaction to a SQL row.
    pub(crate) fn from_tx(
        tx: &TransactionSigned,
        block_number: i64,
        tx_index: i64,
    ) -> Result<Self, SqlColdError> {
        use alloy::consensus::EthereumTxEnvelope;

        let tx_hash = tx.tx_hash().as_slice().to_vec();
        let tx_type = tx.tx_type() as i16;

        match tx {
            EthereumTxEnvelope::Legacy(signed) => {
                let sig = signed.signature();
                let inner = signed.tx();
                Ok(Self {
                    block_number,
                    tx_index,
                    tx_hash,
                    tx_type,
                    sig_y_parity: sig.v(),
                    sig_r: encode_u256(&sig.r()),
                    sig_s: encode_u256(&sig.s()),
                    chain_id: inner.chain_id.map(to_i64),
                    nonce: to_i64(inner.nonce),
                    gas_limit: to_i64(inner.gas_limit),
                    to_address: to_address(&inner.to),
                    value: encode_u256(&inner.value),
                    input: inner.input.to_vec(),
                    gas_price: Some(encode_u128(inner.gas_price)),
                    max_fee_per_gas: None,
                    max_priority_fee_per_gas: None,
                    max_fee_per_blob_gas: None,
                    blob_versioned_hashes: None,
                    access_list: None,
                    authorization_list: None,
                })
            }
            EthereumTxEnvelope::Eip2930(signed) => {
                let sig = signed.signature();
                let inner = signed.tx();
                Ok(Self {
                    block_number,
                    tx_index,
                    tx_hash,
                    tx_type,
                    sig_y_parity: sig.v(),
                    sig_r: encode_u256(&sig.r()),
                    sig_s: encode_u256(&sig.s()),
                    chain_id: Some(to_i64(inner.chain_id)),
                    nonce: to_i64(inner.nonce),
                    gas_limit: to_i64(inner.gas_limit),
                    to_address: to_address(&inner.to),
                    value: encode_u256(&inner.value),
                    input: inner.input.to_vec(),
                    gas_price: Some(encode_u128(inner.gas_price)),
                    max_fee_per_gas: None,
                    max_priority_fee_per_gas: None,
                    max_fee_per_blob_gas: None,
                    blob_versioned_hashes: None,
                    access_list: Some(encode_access_list(&inner.access_list)),
                    authorization_list: None,
                })
            }
            EthereumTxEnvelope::Eip1559(signed) => {
                let sig = signed.signature();
                let inner = signed.tx();
                Ok(Self {
                    block_number,
                    tx_index,
                    tx_hash,
                    tx_type,
                    sig_y_parity: sig.v(),
                    sig_r: encode_u256(&sig.r()),
                    sig_s: encode_u256(&sig.s()),
                    chain_id: Some(to_i64(inner.chain_id)),
                    nonce: to_i64(inner.nonce),
                    gas_limit: to_i64(inner.gas_limit),
                    to_address: to_address(&inner.to),
                    value: encode_u256(&inner.value),
                    input: inner.input.to_vec(),
                    gas_price: None,
                    max_fee_per_gas: Some(encode_u128(inner.max_fee_per_gas)),
                    max_priority_fee_per_gas: Some(encode_u128(inner.max_priority_fee_per_gas)),
                    max_fee_per_blob_gas: None,
                    blob_versioned_hashes: None,
                    access_list: Some(encode_access_list(&inner.access_list)),
                    authorization_list: None,
                })
            }
            EthereumTxEnvelope::Eip4844(signed) => {
                let sig = signed.signature();
                let inner = signed.tx();
                Ok(Self {
                    block_number,
                    tx_index,
                    tx_hash,
                    tx_type,
                    sig_y_parity: sig.v(),
                    sig_r: encode_u256(&sig.r()),
                    sig_s: encode_u256(&sig.s()),
                    chain_id: Some(to_i64(inner.chain_id)),
                    nonce: to_i64(inner.nonce),
                    gas_limit: to_i64(inner.gas_limit),
                    to_address: Some(inner.to.as_slice().to_vec()),
                    value: encode_u256(&inner.value),
                    input: inner.input.to_vec(),
                    gas_price: None,
                    max_fee_per_gas: Some(encode_u128(inner.max_fee_per_gas)),
                    max_priority_fee_per_gas: Some(encode_u128(inner.max_priority_fee_per_gas)),
                    max_fee_per_blob_gas: Some(encode_u128(inner.max_fee_per_blob_gas)),
                    blob_versioned_hashes: Some(encode_b256_vec(&inner.blob_versioned_hashes)),
                    access_list: Some(encode_access_list(&inner.access_list)),
                    authorization_list: None,
                })
            }
            EthereumTxEnvelope::Eip7702(signed) => {
                let sig = signed.signature();
                let inner = signed.tx();
                Ok(Self {
                    block_number,
                    tx_index,
                    tx_hash,
                    tx_type,
                    sig_y_parity: sig.v(),
                    sig_r: encode_u256(&sig.r()),
                    sig_s: encode_u256(&sig.s()),
                    chain_id: Some(to_i64(inner.chain_id)),
                    nonce: to_i64(inner.nonce),
                    gas_limit: to_i64(inner.gas_limit),
                    to_address: Some(inner.to.as_slice().to_vec()),
                    value: encode_u256(&inner.value),
                    input: inner.input.to_vec(),
                    gas_price: None,
                    max_fee_per_gas: Some(encode_u128(inner.max_fee_per_gas)),
                    max_priority_fee_per_gas: Some(encode_u128(inner.max_priority_fee_per_gas)),
                    max_fee_per_blob_gas: None,
                    blob_versioned_hashes: None,
                    access_list: Some(encode_access_list(&inner.access_list)),
                    authorization_list: Some(encode_authorization_list(&inner.authorization_list)),
                })
            }
        }
    }

    /// Convert a SQL row back to a signed transaction.
    pub(crate) fn into_tx(self) -> Result<TransactionSigned, SqlColdError> {
        use alloy::consensus::EthereumTxEnvelope;

        let sig =
            Signature::new(decode_u256(&self.sig_r)?, decode_u256(&self.sig_s)?, self.sig_y_parity);

        let tx_type = TxType::try_from(self.tx_type as u8)
            .map_err(|_| SqlColdError::Convert(format!("invalid tx_type: {}", self.tx_type)))?;

        match tx_type {
            TxType::Legacy => {
                let tx = TxLegacy {
                    chain_id: self.chain_id.map(from_i64),
                    nonce: from_i64(self.nonce),
                    gas_price: decode_u128_required(&self.gas_price, "gas_price")?,
                    gas_limit: from_i64(self.gas_limit),
                    to: from_address(&self.to_address),
                    value: decode_u256(&self.value)?,
                    input: Bytes::from(self.input),
                };
                Ok(EthereumTxEnvelope::Legacy(Signed::new_unhashed(tx, sig)))
            }
            TxType::Eip2930 => {
                let tx = TxEip2930 {
                    chain_id: from_i64(self.chain_id.unwrap_or(0)),
                    nonce: from_i64(self.nonce),
                    gas_price: decode_u128_required(&self.gas_price, "gas_price")?,
                    gas_limit: from_i64(self.gas_limit),
                    to: from_address(&self.to_address),
                    value: decode_u256(&self.value)?,
                    input: Bytes::from(self.input),
                    access_list: decode_access_list_or_empty(&self.access_list)?,
                };
                Ok(EthereumTxEnvelope::Eip2930(Signed::new_unhashed(tx, sig)))
            }
            TxType::Eip1559 => {
                let tx = TxEip1559 {
                    chain_id: from_i64(self.chain_id.unwrap_or(0)),
                    nonce: from_i64(self.nonce),
                    gas_limit: from_i64(self.gas_limit),
                    max_fee_per_gas: decode_u128_required(
                        &self.max_fee_per_gas,
                        "max_fee_per_gas",
                    )?,
                    max_priority_fee_per_gas: decode_u128_required(
                        &self.max_priority_fee_per_gas,
                        "max_priority_fee_per_gas",
                    )?,
                    to: from_address(&self.to_address),
                    value: decode_u256(&self.value)?,
                    input: Bytes::from(self.input),
                    access_list: decode_access_list_or_empty(&self.access_list)?,
                };
                Ok(EthereumTxEnvelope::Eip1559(Signed::new_unhashed(tx, sig)))
            }
            TxType::Eip4844 => {
                let tx = TxEip4844 {
                    chain_id: from_i64(self.chain_id.unwrap_or(0)),
                    nonce: from_i64(self.nonce),
                    gas_limit: from_i64(self.gas_limit),
                    max_fee_per_gas: decode_u128_required(
                        &self.max_fee_per_gas,
                        "max_fee_per_gas",
                    )?,
                    max_priority_fee_per_gas: decode_u128_required(
                        &self.max_priority_fee_per_gas,
                        "max_priority_fee_per_gas",
                    )?,
                    to: Address::from_slice(self.to_address.as_deref().ok_or_else(|| {
                        SqlColdError::Convert("EIP4844 requires to_address".into())
                    })?),
                    value: decode_u256(&self.value)?,
                    input: Bytes::from(self.input),
                    access_list: decode_access_list_or_empty(&self.access_list)?,
                    blob_versioned_hashes: decode_b256_vec(
                        self.blob_versioned_hashes.as_deref().unwrap_or_default(),
                    ),
                    max_fee_per_blob_gas: decode_u128_required(
                        &self.max_fee_per_blob_gas,
                        "max_fee_per_blob_gas",
                    )?,
                };
                Ok(EthereumTxEnvelope::Eip4844(Signed::new_unhashed(tx, sig)))
            }
            TxType::Eip7702 => {
                let tx = TxEip7702 {
                    chain_id: from_i64(self.chain_id.unwrap_or(0)),
                    nonce: from_i64(self.nonce),
                    gas_limit: from_i64(self.gas_limit),
                    max_fee_per_gas: decode_u128_required(
                        &self.max_fee_per_gas,
                        "max_fee_per_gas",
                    )?,
                    max_priority_fee_per_gas: decode_u128_required(
                        &self.max_priority_fee_per_gas,
                        "max_priority_fee_per_gas",
                    )?,
                    to: Address::from_slice(self.to_address.as_deref().ok_or_else(|| {
                        SqlColdError::Convert("EIP7702 requires to_address".into())
                    })?),
                    value: decode_u256(&self.value)?,
                    input: Bytes::from(self.input),
                    access_list: decode_access_list_or_empty(&self.access_list)?,
                    authorization_list: decode_authorization_list(
                        self.authorization_list.as_deref().unwrap_or_default(),
                    )?,
                };
                Ok(EthereumTxEnvelope::Eip7702(Signed::new_unhashed(tx, sig)))
            }
        }
    }
}

fn to_address(kind: &TxKind) -> Option<Vec<u8>> {
    match kind {
        TxKind::Call(addr) => Some(addr.as_slice().to_vec()),
        TxKind::Create => None,
    }
}

fn from_address(data: &Option<Vec<u8>>) -> TxKind {
    data.as_ref().map(|b| TxKind::Call(Address::from_slice(b))).unwrap_or(TxKind::Create)
}

fn decode_u128_required(data: &Option<Vec<u8>>, field: &str) -> Result<u128, SqlColdError> {
    data.as_ref()
        .ok_or_else(|| SqlColdError::Convert(format!("{field} is required")))
        .and_then(|b| decode_u128(b))
}

fn decode_access_list_or_empty(data: &Option<Vec<u8>>) -> Result<AccessList, SqlColdError> {
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
fn encode_access_list(list: &AccessList) -> Vec<u8> {
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
fn encode_authorization_list(list: &[SignedAuthorization]) -> Vec<u8> {
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

fn decode_authorization_list(data: &[u8]) -> Result<Vec<SignedAuthorization>, SqlColdError> {
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

fn encode_b256_vec(hashes: &[B256]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(2 + hashes.len() * 32);
    buf.extend_from_slice(&(hashes.len() as u16).to_be_bytes());
    for h in hashes {
        buf.extend_from_slice(h.as_slice());
    }
    buf
}

fn decode_b256_vec(data: &[u8]) -> Vec<B256> {
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
// Receipt conversion
// ============================================================================

/// Row data for a receipt in SQL.
pub(crate) struct ReceiptRow {
    pub block_number: i64,
    pub tx_index: i64,
    pub tx_type: i16,
    pub success: bool,
    pub cumulative_gas_used: i64,
}

impl ReceiptRow {
    /// Convert a receipt to a SQL row.
    pub(crate) const fn from_receipt(receipt: &Receipt, block_number: i64, tx_index: i64) -> Self {
        Self {
            block_number,
            tx_index,
            tx_type: receipt.tx_type as i16,
            success: receipt.inner.status.coerce_status(),
            cumulative_gas_used: to_i64(receipt.inner.cumulative_gas_used),
        }
    }
}

/// Row data for a log entry in SQL.
pub(crate) struct LogRow {
    pub block_number: i64,
    pub tx_index: i64,
    pub log_index: i64,
    pub address: Vec<u8>,
    pub topic0: Option<Vec<u8>>,
    pub topic1: Option<Vec<u8>>,
    pub topic2: Option<Vec<u8>>,
    pub topic3: Option<Vec<u8>>,
    pub data: Vec<u8>,
}

impl LogRow {
    /// Convert a log to a SQL row.
    pub(crate) fn from_log(log: &Log, block_number: i64, tx_index: i64, log_index: i64) -> Self {
        let topics = log.topics();
        Self {
            block_number,
            tx_index,
            log_index,
            address: log.address.as_slice().to_vec(),
            topic0: topics.first().map(|t| t.as_slice().to_vec()),
            topic1: topics.get(1).map(|t| t.as_slice().to_vec()),
            topic2: topics.get(2).map(|t| t.as_slice().to_vec()),
            topic3: topics.get(3).map(|t| t.as_slice().to_vec()),
            data: log.data.data.to_vec(),
        }
    }

    /// Convert a SQL row back to a log.
    pub(crate) fn into_log(self) -> Log {
        let mut topics = Vec::with_capacity(4);
        if let Some(t) = self.topic0 {
            topics.push(B256::from_slice(&t));
        }
        if let Some(t) = self.topic1 {
            topics.push(B256::from_slice(&t));
        }
        if let Some(t) = self.topic2 {
            topics.push(B256::from_slice(&t));
        }
        if let Some(t) = self.topic3 {
            topics.push(B256::from_slice(&t));
        }
        Log {
            address: Address::from_slice(&self.address),
            data: LogData::new_unchecked(topics, Bytes::from(self.data)),
        }
    }
}

/// Reconstruct a [`Receipt`] from a receipt row and its log rows.
pub(crate) fn receipt_from_rows(
    receipt_row: ReceiptRow,
    log_rows: Vec<LogRow>,
) -> Result<Receipt, SqlColdError> {
    let tx_type = TxType::try_from(receipt_row.tx_type as u8)
        .map_err(|_| SqlColdError::Convert(format!("invalid tx_type: {}", receipt_row.tx_type)))?;
    let logs = log_rows.into_iter().map(LogRow::into_log).collect();
    Ok(Receipt {
        tx_type,
        inner: AlloyReceipt {
            status: receipt_row.success.into(),
            cumulative_gas_used: from_i64(receipt_row.cumulative_gas_used),
            logs,
        },
    })
}

// ============================================================================
// Signet event conversion
// ============================================================================

/// Event type discriminants for SQL.
pub(crate) const EVENT_TRANSACT: i16 = 0;
/// Enter event discriminant.
pub(crate) const EVENT_ENTER: i16 = 1;
/// EnterToken event discriminant.
pub(crate) const EVENT_ENTER_TOKEN: i16 = 2;

/// Row data for a signet event in SQL.
pub(crate) struct SignetEventRow {
    pub block_number: i64,
    pub event_index: i64,
    pub event_type: i16,
    pub order_index: i64,
    pub rollup_chain_id: Vec<u8>,
    pub sender: Option<Vec<u8>>,
    pub to_address: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
    pub gas: Option<Vec<u8>>,
    pub max_fee_per_gas: Option<Vec<u8>>,
    pub data: Option<Vec<u8>>,
    pub rollup_recipient: Option<Vec<u8>>,
    pub amount: Option<Vec<u8>>,
    pub token: Option<Vec<u8>>,
}

impl SignetEventRow {
    /// Convert a signet event to a SQL row.
    pub(crate) fn from_event(event: &DbSignetEvent, block_number: i64, event_index: i64) -> Self {
        match event {
            DbSignetEvent::Transact(order, t) => Self {
                block_number,
                event_index,
                event_type: EVENT_TRANSACT,
                order_index: to_i64(*order),
                rollup_chain_id: encode_u256(&t.rollupChainId),
                sender: Some(t.sender.as_slice().to_vec()),
                to_address: Some(t.to.as_slice().to_vec()),
                value: Some(encode_u256(&t.value)),
                gas: Some(encode_u256(&t.gas)),
                max_fee_per_gas: Some(encode_u256(&t.maxFeePerGas)),
                data: Some(t.data.to_vec()),
                rollup_recipient: None,
                amount: None,
                token: None,
            },
            DbSignetEvent::Enter(order, e) => Self {
                block_number,
                event_index,
                event_type: EVENT_ENTER,
                order_index: to_i64(*order),
                rollup_chain_id: encode_u256(&e.rollupChainId),
                sender: None,
                to_address: None,
                value: None,
                gas: None,
                max_fee_per_gas: None,
                data: None,
                rollup_recipient: Some(e.rollupRecipient.as_slice().to_vec()),
                amount: Some(encode_u256(&e.amount)),
                token: None,
            },
            DbSignetEvent::EnterToken(order, e) => Self {
                block_number,
                event_index,
                event_type: EVENT_ENTER_TOKEN,
                order_index: to_i64(*order),
                rollup_chain_id: encode_u256(&e.rollupChainId),
                sender: None,
                to_address: None,
                value: None,
                gas: None,
                max_fee_per_gas: None,
                data: None,
                rollup_recipient: Some(e.rollupRecipient.as_slice().to_vec()),
                amount: Some(encode_u256(&e.amount)),
                token: Some(e.token.as_slice().to_vec()),
            },
        }
    }

    /// Convert a SQL row back to a signet event.
    pub(crate) fn into_event(self) -> Result<DbSignetEvent, SqlColdError> {
        let order = from_i64(self.order_index);
        let rollup_chain_id = decode_u256(&self.rollup_chain_id)?;

        match self.event_type {
            EVENT_TRANSACT => {
                let sender = Address::from_slice(
                    self.sender
                        .as_deref()
                        .ok_or_else(|| SqlColdError::Convert("Transact requires sender".into()))?,
                );
                let to = Address::from_slice(
                    self.to_address
                        .as_deref()
                        .ok_or_else(|| SqlColdError::Convert("Transact requires to".into()))?,
                );
                let value = decode_u256(
                    self.value
                        .as_deref()
                        .ok_or_else(|| SqlColdError::Convert("Transact requires value".into()))?,
                )?;
                let gas = decode_u256(
                    self.gas
                        .as_deref()
                        .ok_or_else(|| SqlColdError::Convert("Transact requires gas".into()))?,
                )?;
                let max_fee = decode_u256(self.max_fee_per_gas.as_deref().ok_or_else(|| {
                    SqlColdError::Convert("Transact requires max_fee_per_gas".into())
                })?)?;
                let data = Bytes::from(self.data.unwrap_or_default());

                Ok(DbSignetEvent::Transact(
                    order,
                    Transact {
                        rollupChainId: rollup_chain_id,
                        sender,
                        to,
                        value,
                        gas,
                        maxFeePerGas: max_fee,
                        data,
                    },
                ))
            }
            EVENT_ENTER => {
                let recipient =
                    Address::from_slice(self.rollup_recipient.as_deref().ok_or_else(|| {
                        SqlColdError::Convert("Enter requires rollup_recipient".into())
                    })?);
                let amount = decode_u256(
                    self.amount
                        .as_deref()
                        .ok_or_else(|| SqlColdError::Convert("Enter requires amount".into()))?,
                )?;

                Ok(DbSignetEvent::Enter(
                    order,
                    Enter { rollupChainId: rollup_chain_id, rollupRecipient: recipient, amount },
                ))
            }
            EVENT_ENTER_TOKEN => {
                let token =
                    Address::from_slice(self.token.as_deref().ok_or_else(|| {
                        SqlColdError::Convert("EnterToken requires token".into())
                    })?);
                let recipient =
                    Address::from_slice(self.rollup_recipient.as_deref().ok_or_else(|| {
                        SqlColdError::Convert("EnterToken requires rollup_recipient".into())
                    })?);
                let amount =
                    decode_u256(self.amount.as_deref().ok_or_else(|| {
                        SqlColdError::Convert("EnterToken requires amount".into())
                    })?)?;

                Ok(DbSignetEvent::EnterToken(
                    order,
                    EnterToken {
                        rollupChainId: rollup_chain_id,
                        token,
                        rollupRecipient: recipient,
                        amount,
                    },
                ))
            }
            _ => Err(SqlColdError::Convert(format!("invalid event_type: {}", self.event_type))),
        }
    }
}

// ============================================================================
// Zenith header conversion
// ============================================================================

/// Row data for a zenith header in SQL.
pub(crate) struct ZenithHeaderRow {
    pub block_number: i64,
    pub host_block_number: Vec<u8>,
    pub rollup_chain_id: Vec<u8>,
    pub gas_limit: Vec<u8>,
    pub reward_address: Vec<u8>,
    pub block_data_hash: Vec<u8>,
}

impl ZenithHeaderRow {
    /// Convert a zenith header to a SQL row.
    pub(crate) fn from_zenith(header: &DbZenithHeader, block_number: i64) -> Self {
        let h = &header.0;
        Self {
            block_number,
            host_block_number: encode_u256(&h.hostBlockNumber),
            rollup_chain_id: encode_u256(&h.rollupChainId),
            gas_limit: encode_u256(&h.gasLimit),
            reward_address: h.rewardAddress.as_slice().to_vec(),
            block_data_hash: h.blockDataHash.as_slice().to_vec(),
        }
    }

    /// Convert a SQL row back to a zenith header.
    pub(crate) fn into_zenith(self) -> Result<DbZenithHeader, SqlColdError> {
        Ok(DbZenithHeader(Zenith::BlockHeader {
            hostBlockNumber: decode_u256(&self.host_block_number)?,
            rollupChainId: decode_u256(&self.rollup_chain_id)?,
            gasLimit: decode_u256(&self.gas_limit)?,
            rewardAddress: Address::from_slice(&self.reward_address),
            blockDataHash: alloy::primitives::FixedBytes::<32>::from_slice(&self.block_data_hash),
        }))
    }
}
