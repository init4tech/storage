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
use signet_storage_types::{DbSignetEvent, DbZenithHeader, Receipt, TransactionSigned};
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
// Header conversion
// ============================================================================

/// Row data for a header read from SQL.
pub(crate) struct HeaderRow {
    pub block_number: i64,
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

impl TryFrom<HeaderRow> for Header {
    type Error = SqlColdError;

    fn try_from(row: HeaderRow) -> Result<Self, Self::Error> {
        Ok(Self {
            parent_hash: B256::from_slice(&row.parent_hash),
            ommers_hash: B256::from_slice(&row.ommers_hash),
            beneficiary: Address::from_slice(&row.beneficiary),
            state_root: B256::from_slice(&row.state_root),
            transactions_root: B256::from_slice(&row.transactions_root),
            receipts_root: B256::from_slice(&row.receipts_root),
            logs_bloom: Bloom::from_slice(&row.logs_bloom),
            difficulty: decode_u256(&row.difficulty)?,
            number: from_i64(row.block_number),
            gas_limit: from_i64(row.gas_limit),
            gas_used: from_i64(row.gas_used),
            timestamp: from_i64(row.timestamp),
            extra_data: Bytes::from(row.extra_data),
            mix_hash: B256::from_slice(&row.mix_hash),
            nonce: alloy::primitives::B64::from_slice(&row.nonce),
            base_fee_per_gas: row.base_fee_per_gas.map(from_i64),
            withdrawals_root: row.withdrawals_root.map(|b| B256::from_slice(&b)),
            blob_gas_used: row.blob_gas_used.map(from_i64),
            excess_blob_gas: row.excess_blob_gas.map(from_i64),
            parent_beacon_block_root: row.parent_beacon_block_root.map(|b| B256::from_slice(&b)),
            requests_hash: row.requests_hash.map(|b| B256::from_slice(&b)),
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
                    sig_r: encode_u256(&sig.r()).to_vec(),
                    sig_s: encode_u256(&sig.s()).to_vec(),
                    chain_id: inner.chain_id.map(to_i64),
                    nonce: to_i64(inner.nonce),
                    gas_limit: to_i64(inner.gas_limit),
                    to_address: to_address(&inner.to),
                    value: encode_u256(&inner.value).to_vec(),
                    input: inner.input.to_vec(),
                    gas_price: Some(encode_u128(inner.gas_price).to_vec()),
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
                    sig_r: encode_u256(&sig.r()).to_vec(),
                    sig_s: encode_u256(&sig.s()).to_vec(),
                    chain_id: Some(to_i64(inner.chain_id)),
                    nonce: to_i64(inner.nonce),
                    gas_limit: to_i64(inner.gas_limit),
                    to_address: to_address(&inner.to),
                    value: encode_u256(&inner.value).to_vec(),
                    input: inner.input.to_vec(),
                    gas_price: Some(encode_u128(inner.gas_price).to_vec()),
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
                    sig_r: encode_u256(&sig.r()).to_vec(),
                    sig_s: encode_u256(&sig.s()).to_vec(),
                    chain_id: Some(to_i64(inner.chain_id)),
                    nonce: to_i64(inner.nonce),
                    gas_limit: to_i64(inner.gas_limit),
                    to_address: to_address(&inner.to),
                    value: encode_u256(&inner.value).to_vec(),
                    input: inner.input.to_vec(),
                    gas_price: None,
                    max_fee_per_gas: Some(encode_u128(inner.max_fee_per_gas).to_vec()),
                    max_priority_fee_per_gas: Some(
                        encode_u128(inner.max_priority_fee_per_gas).to_vec(),
                    ),
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
                    sig_r: encode_u256(&sig.r()).to_vec(),
                    sig_s: encode_u256(&sig.s()).to_vec(),
                    chain_id: Some(to_i64(inner.chain_id)),
                    nonce: to_i64(inner.nonce),
                    gas_limit: to_i64(inner.gas_limit),
                    to_address: Some(inner.to.as_slice().to_vec()),
                    value: encode_u256(&inner.value).to_vec(),
                    input: inner.input.to_vec(),
                    gas_price: None,
                    max_fee_per_gas: Some(encode_u128(inner.max_fee_per_gas).to_vec()),
                    max_priority_fee_per_gas: Some(
                        encode_u128(inner.max_priority_fee_per_gas).to_vec(),
                    ),
                    max_fee_per_blob_gas: Some(encode_u128(inner.max_fee_per_blob_gas).to_vec()),
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
                    sig_r: encode_u256(&sig.r()).to_vec(),
                    sig_s: encode_u256(&sig.s()).to_vec(),
                    chain_id: Some(to_i64(inner.chain_id)),
                    nonce: to_i64(inner.nonce),
                    gas_limit: to_i64(inner.gas_limit),
                    to_address: Some(inner.to.as_slice().to_vec()),
                    value: encode_u256(&inner.value).to_vec(),
                    input: inner.input.to_vec(),
                    gas_price: None,
                    max_fee_per_gas: Some(encode_u128(inner.max_fee_per_gas).to_vec()),
                    max_priority_fee_per_gas: Some(
                        encode_u128(inner.max_priority_fee_per_gas).to_vec(),
                    ),
                    max_fee_per_blob_gas: None,
                    blob_versioned_hashes: None,
                    access_list: Some(encode_access_list(&inner.access_list)),
                    authorization_list: Some(encode_authorization_list(&inner.authorization_list)),
                })
            }
        }
    }
}

impl TryFrom<TxRow> for TransactionSigned {
    type Error = SqlColdError;

    fn try_from(row: TxRow) -> Result<Self, Self::Error> {
        use alloy::consensus::EthereumTxEnvelope;

        let sig =
            Signature::new(decode_u256(&row.sig_r)?, decode_u256(&row.sig_s)?, row.sig_y_parity);

        let tx_type = TxType::try_from(row.tx_type as u8)
            .map_err(|_| SqlColdError::Convert(format!("invalid tx_type: {}", row.tx_type)))?;

        match tx_type {
            TxType::Legacy => {
                let tx = TxLegacy {
                    chain_id: row.chain_id.map(from_i64),
                    nonce: from_i64(row.nonce),
                    gas_price: decode_u128_required(&row.gas_price, "gas_price")?,
                    gas_limit: from_i64(row.gas_limit),
                    to: from_address(row.to_address.as_deref()),
                    value: decode_u256(&row.value)?,
                    input: Bytes::from(row.input),
                };
                Ok(EthereumTxEnvelope::Legacy(Signed::new_unhashed(tx, sig)))
            }
            TxType::Eip2930 => {
                let tx = TxEip2930 {
                    chain_id: from_i64(row.chain_id.unwrap_or(0)),
                    nonce: from_i64(row.nonce),
                    gas_price: decode_u128_required(&row.gas_price, "gas_price")?,
                    gas_limit: from_i64(row.gas_limit),
                    to: from_address(row.to_address.as_deref()),
                    value: decode_u256(&row.value)?,
                    input: Bytes::from(row.input),
                    access_list: decode_access_list_or_empty(&row.access_list)?,
                };
                Ok(EthereumTxEnvelope::Eip2930(Signed::new_unhashed(tx, sig)))
            }
            TxType::Eip1559 => {
                let tx = TxEip1559 {
                    chain_id: from_i64(row.chain_id.unwrap_or(0)),
                    nonce: from_i64(row.nonce),
                    gas_limit: from_i64(row.gas_limit),
                    max_fee_per_gas: decode_u128_required(&row.max_fee_per_gas, "max_fee_per_gas")?,
                    max_priority_fee_per_gas: decode_u128_required(
                        &row.max_priority_fee_per_gas,
                        "max_priority_fee_per_gas",
                    )?,
                    to: from_address(row.to_address.as_deref()),
                    value: decode_u256(&row.value)?,
                    input: Bytes::from(row.input),
                    access_list: decode_access_list_or_empty(&row.access_list)?,
                };
                Ok(EthereumTxEnvelope::Eip1559(Signed::new_unhashed(tx, sig)))
            }
            TxType::Eip4844 => {
                let tx = TxEip4844 {
                    chain_id: from_i64(row.chain_id.unwrap_or(0)),
                    nonce: from_i64(row.nonce),
                    gas_limit: from_i64(row.gas_limit),
                    max_fee_per_gas: decode_u128_required(&row.max_fee_per_gas, "max_fee_per_gas")?,
                    max_priority_fee_per_gas: decode_u128_required(
                        &row.max_priority_fee_per_gas,
                        "max_priority_fee_per_gas",
                    )?,
                    to: Address::from_slice(row.to_address.as_deref().ok_or_else(|| {
                        SqlColdError::Convert("EIP4844 requires to_address".into())
                    })?),
                    value: decode_u256(&row.value)?,
                    input: Bytes::from(row.input),
                    access_list: decode_access_list_or_empty(&row.access_list)?,
                    blob_versioned_hashes: decode_b256_vec(
                        row.blob_versioned_hashes.as_deref().unwrap_or_default(),
                    ),
                    max_fee_per_blob_gas: decode_u128_required(
                        &row.max_fee_per_blob_gas,
                        "max_fee_per_blob_gas",
                    )?,
                };
                Ok(EthereumTxEnvelope::Eip4844(Signed::new_unhashed(tx, sig)))
            }
            TxType::Eip7702 => {
                let tx = TxEip7702 {
                    chain_id: from_i64(row.chain_id.unwrap_or(0)),
                    nonce: from_i64(row.nonce),
                    gas_limit: from_i64(row.gas_limit),
                    max_fee_per_gas: decode_u128_required(&row.max_fee_per_gas, "max_fee_per_gas")?,
                    max_priority_fee_per_gas: decode_u128_required(
                        &row.max_priority_fee_per_gas,
                        "max_priority_fee_per_gas",
                    )?,
                    to: Address::from_slice(row.to_address.as_deref().ok_or_else(|| {
                        SqlColdError::Convert("EIP7702 requires to_address".into())
                    })?),
                    value: decode_u256(&row.value)?,
                    input: Bytes::from(row.input),
                    access_list: decode_access_list_or_empty(&row.access_list)?,
                    authorization_list: decode_authorization_list(
                        row.authorization_list.as_deref().unwrap_or_default(),
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

fn from_address(data: Option<&[u8]>) -> TxKind {
    data.map_or(TxKind::Create, |b| TxKind::Call(Address::from_slice(b)))
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

/// Row data for a log entry read from SQL.
pub(crate) struct LogRow {
    pub address: Vec<u8>,
    pub topic0: Option<Vec<u8>>,
    pub topic1: Option<Vec<u8>>,
    pub topic2: Option<Vec<u8>>,
    pub topic3: Option<Vec<u8>>,
    pub data: Vec<u8>,
}

impl From<LogRow> for Log {
    fn from(row: LogRow) -> Self {
        let topics = [row.topic0, row.topic1, row.topic2, row.topic3]
            .into_iter()
            .flatten()
            .map(|t| B256::from_slice(&t))
            .collect();
        Self {
            address: Address::from_slice(&row.address),
            data: LogData::new_unchecked(topics, Bytes::from(row.data)),
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
    let logs = log_rows.into_iter().map(Log::from).collect();
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

/// Row data for a signet event read from SQL.
pub(crate) struct SignetEventRow {
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

impl TryFrom<SignetEventRow> for DbSignetEvent {
    type Error = SqlColdError;

    fn try_from(row: SignetEventRow) -> Result<Self, Self::Error> {
        let order = from_i64(row.order_index);
        let rollup_chain_id = decode_u256(&row.rollup_chain_id)?;

        match row.event_type {
            EVENT_TRANSACT => {
                let sender = Address::from_slice(
                    row.sender
                        .as_deref()
                        .ok_or_else(|| SqlColdError::Convert("Transact requires sender".into()))?,
                );
                let to = Address::from_slice(
                    row.to_address
                        .as_deref()
                        .ok_or_else(|| SqlColdError::Convert("Transact requires to".into()))?,
                );
                let value = decode_u256(
                    row.value
                        .as_deref()
                        .ok_or_else(|| SqlColdError::Convert("Transact requires value".into()))?,
                )?;
                let gas = decode_u256(
                    row.gas
                        .as_deref()
                        .ok_or_else(|| SqlColdError::Convert("Transact requires gas".into()))?,
                )?;
                let max_fee = decode_u256(row.max_fee_per_gas.as_deref().ok_or_else(|| {
                    SqlColdError::Convert("Transact requires max_fee_per_gas".into())
                })?)?;
                let data = Bytes::from(row.data.unwrap_or_default());

                Ok(Self::Transact(
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
                    Address::from_slice(row.rollup_recipient.as_deref().ok_or_else(|| {
                        SqlColdError::Convert("Enter requires rollup_recipient".into())
                    })?);
                let amount = decode_u256(
                    row.amount
                        .as_deref()
                        .ok_or_else(|| SqlColdError::Convert("Enter requires amount".into()))?,
                )?;

                Ok(Self::Enter(
                    order,
                    Enter { rollupChainId: rollup_chain_id, rollupRecipient: recipient, amount },
                ))
            }
            EVENT_ENTER_TOKEN => {
                let token =
                    Address::from_slice(row.token.as_deref().ok_or_else(|| {
                        SqlColdError::Convert("EnterToken requires token".into())
                    })?);
                let recipient =
                    Address::from_slice(row.rollup_recipient.as_deref().ok_or_else(|| {
                        SqlColdError::Convert("EnterToken requires rollup_recipient".into())
                    })?);
                let amount =
                    decode_u256(row.amount.as_deref().ok_or_else(|| {
                        SqlColdError::Convert("EnterToken requires amount".into())
                    })?)?;

                Ok(Self::EnterToken(
                    order,
                    EnterToken {
                        rollupChainId: rollup_chain_id,
                        token,
                        rollupRecipient: recipient,
                        amount,
                    },
                ))
            }
            _ => Err(SqlColdError::Convert(format!("invalid event_type: {}", row.event_type))),
        }
    }
}

// ============================================================================
// Zenith header conversion
// ============================================================================

/// Row data for a zenith header read from SQL.
pub(crate) struct ZenithHeaderRow {
    pub host_block_number: Vec<u8>,
    pub rollup_chain_id: Vec<u8>,
    pub gas_limit: Vec<u8>,
    pub reward_address: Vec<u8>,
    pub block_data_hash: Vec<u8>,
}

impl TryFrom<ZenithHeaderRow> for DbZenithHeader {
    type Error = SqlColdError;

    fn try_from(row: ZenithHeaderRow) -> Result<Self, Self::Error> {
        Ok(Self(Zenith::BlockHeader {
            hostBlockNumber: decode_u256(&row.host_block_number)?,
            rollupChainId: decode_u256(&row.rollup_chain_id)?,
            gasLimit: decode_u256(&row.gas_limit)?,
            rewardAddress: Address::from_slice(&row.reward_address),
            blockDataHash: alloy::primitives::FixedBytes::<32>::from_slice(&row.block_data_hash),
        }))
    }
}
