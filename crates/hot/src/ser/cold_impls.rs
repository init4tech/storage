//! Serialization implementations for cold storage types.
//!
//! This module provides [`KeySer`] and [`ValSer`] implementations for types
//! used in cold storage that are defined in [`signet_storage_types`].

use alloy::primitives::{Address, Bytes, FixedBytes, U256};
use bytes::BufMut;
use signet_storage_types::{DbSignetEvent, DbZenithHeader, IndexedReceipt, Receipt, TxLocation};
use signet_zenith::{
    Passage::{Enter, EnterToken},
    Transactor::Transact,
    Zenith,
};

use super::{DeserError, KeySer, MAX_KEY_SIZE, ValSer};

// ============================================================================
// TxLocation - 16 bytes fixed size (block: u64 + index: u64)
// ============================================================================

impl KeySer for TxLocation {
    const SIZE: usize = 16;

    fn encode_key<'a: 'c, 'b: 'c, 'c>(&'a self, buf: &'b mut [u8; MAX_KEY_SIZE]) -> &'c [u8] {
        buf[0..8].copy_from_slice(&self.block.to_be_bytes());
        buf[8..16].copy_from_slice(&self.index.to_be_bytes());
        &buf[..Self::SIZE]
    }

    fn decode_key(data: &[u8]) -> Result<Self, DeserError> {
        if data.len() < Self::SIZE {
            return Err(DeserError::InsufficientData { needed: Self::SIZE, available: data.len() });
        }
        let block = u64::from_be_bytes(data[0..8].try_into().unwrap());
        let index = u64::from_be_bytes(data[8..16].try_into().unwrap());
        Ok(TxLocation::new(block, index))
    }
}

impl ValSer for TxLocation {
    const FIXED_SIZE: Option<usize> = Some(16);

    fn encoded_size(&self) -> usize {
        16
    }

    fn encode_value_to<B>(&self, buf: &mut B)
    where
        B: BufMut + AsMut<[u8]>,
    {
        buf.put_u64(self.block);
        buf.put_u64(self.index);
    }

    fn decode_value(data: &[u8]) -> Result<Self, DeserError>
    where
        Self: Sized,
    {
        if data.len() < 16 {
            return Err(DeserError::InsufficientData { needed: 16, available: data.len() });
        }
        let block = u64::from_be_bytes(data[0..8].try_into().unwrap());
        let index = u64::from_be_bytes(data[8..16].try_into().unwrap());
        Ok(TxLocation::new(block, index))
    }
}

// ============================================================================
// DbSignetEvent - Variable size signet event
// ============================================================================

/// Event type discriminant for serialization.
const EVENT_TRANSACT: u8 = 0;
const EVENT_ENTER: u8 = 1;
const EVENT_ENTER_TOKEN: u8 = 2;

impl ValSer for DbSignetEvent {
    fn encoded_size(&self) -> usize {
        // 1 byte discriminant + 8 bytes event index + event-specific data
        1 + 8
            + match self {
                DbSignetEvent::Transact(_, t) => encoded_size_transact(t),
                DbSignetEvent::Enter(_, e) => encoded_size_enter(e),
                DbSignetEvent::EnterToken(_, e) => encoded_size_enter_token(e),
            }
    }

    fn encode_value_to<B>(&self, buf: &mut B)
    where
        B: BufMut + AsMut<[u8]>,
    {
        match self {
            DbSignetEvent::Transact(idx, t) => {
                buf.put_u8(EVENT_TRANSACT);
                buf.put_u64(*idx);
                encode_transact(t, buf);
            }
            DbSignetEvent::Enter(idx, e) => {
                buf.put_u8(EVENT_ENTER);
                buf.put_u64(*idx);
                encode_enter(e, buf);
            }
            DbSignetEvent::EnterToken(idx, e) => {
                buf.put_u8(EVENT_ENTER_TOKEN);
                buf.put_u64(*idx);
                encode_enter_token(e, buf);
            }
        }
    }

    fn decode_value(data: &[u8]) -> Result<Self, DeserError>
    where
        Self: Sized,
    {
        if data.len() < 9 {
            return Err(DeserError::InsufficientData { needed: 9, available: data.len() });
        }

        let discriminant = data[0];
        let idx = u64::from_be_bytes(data[1..9].try_into().unwrap());
        let rest = &data[9..];

        match discriminant {
            EVENT_TRANSACT => {
                let t = decode_transact(rest)?;
                Ok(DbSignetEvent::Transact(idx, t))
            }
            EVENT_ENTER => {
                let e = decode_enter(rest)?;
                Ok(DbSignetEvent::Enter(idx, e))
            }
            EVENT_ENTER_TOKEN => {
                let e = decode_enter_token(rest)?;
                Ok(DbSignetEvent::EnterToken(idx, e))
            }
            _ => Err(DeserError::String(format!(
                "Invalid DbSignetEvent discriminant: {}",
                discriminant
            ))),
        }
    }
}

// ============================================================================
// Transact event helpers
// Transact fields: rollupChainId (U256), sender (Address), to (Address),
//                  value (U256), gas (U256), maxFeePerGas (U256), data (Bytes)
// ============================================================================

fn encoded_size_transact(t: &Transact) -> usize {
    // rollupChainId: 32 bytes
    // sender: 20 bytes
    // to: 20 bytes
    // value: 32 bytes
    // gas: 32 bytes (U256)
    // maxFeePerGas: 32 bytes
    // data: 4 bytes length prefix + variable data
    32 + 20 + 20 + 32 + 32 + 32 + 4 + t.data.len()
}

fn encode_transact<B: BufMut>(t: &Transact, buf: &mut B) {
    buf.put_slice(&t.rollupChainId.to_be_bytes::<32>());
    buf.put_slice(t.sender.as_slice());
    buf.put_slice(t.to.as_slice());
    buf.put_slice(&t.value.to_be_bytes::<32>());
    buf.put_slice(&t.gas.to_be_bytes::<32>());
    buf.put_slice(&t.maxFeePerGas.to_be_bytes::<32>());
    buf.put_u32(t.data.len() as u32);
    buf.put_slice(&t.data);
}

fn decode_transact(data: &[u8]) -> Result<Transact, DeserError> {
    // rollupChainId: 32, sender: 20, to: 20, value: 32, gas: 32, maxFeePerGas: 32, data_len: 4
    const MIN_SIZE: usize = 32 + 20 + 20 + 32 + 32 + 32 + 4;
    if data.len() < MIN_SIZE {
        return Err(DeserError::InsufficientData { needed: MIN_SIZE, available: data.len() });
    }

    let rollup_chain_id = U256::from_be_slice(&data[0..32]);
    let sender = Address::from_slice(&data[32..52]);
    let to = Address::from_slice(&data[52..72]);
    let value = U256::from_be_slice(&data[72..104]);
    let gas = U256::from_be_slice(&data[104..136]);
    let max_fee_per_gas = U256::from_be_slice(&data[136..168]);
    let data_len = u32::from_be_bytes(data[168..172].try_into().unwrap()) as usize;

    if data.len() < MIN_SIZE + data_len {
        return Err(DeserError::InsufficientData {
            needed: MIN_SIZE + data_len,
            available: data.len(),
        });
    }

    let tx_data = Bytes::copy_from_slice(&data[172..172 + data_len]);

    Ok(Transact {
        rollupChainId: rollup_chain_id,
        sender,
        to,
        value,
        gas,
        maxFeePerGas: max_fee_per_gas,
        data: tx_data,
    })
}

// ============================================================================
// Enter event helpers
// Enter fields: rollupChainId (U256), rollupRecipient (Address), amount (U256)
// Note: Enter does NOT have a token field
// ============================================================================

const fn encoded_size_enter(_e: &Enter) -> usize {
    // rollupChainId: 32 bytes
    // rollupRecipient: 20 bytes
    // amount: 32 bytes
    32 + 20 + 32
}

fn encode_enter<B: BufMut>(e: &Enter, buf: &mut B) {
    buf.put_slice(&e.rollupChainId.to_be_bytes::<32>());
    buf.put_slice(e.rollupRecipient.as_slice());
    buf.put_slice(&e.amount.to_be_bytes::<32>());
}

fn decode_enter(data: &[u8]) -> Result<Enter, DeserError> {
    const SIZE: usize = 32 + 20 + 32;
    if data.len() < SIZE {
        return Err(DeserError::InsufficientData { needed: SIZE, available: data.len() });
    }

    let rollup_chain_id = U256::from_be_slice(&data[0..32]);
    let rollup_recipient = Address::from_slice(&data[32..52]);
    let amount = U256::from_be_slice(&data[52..84]);

    Ok(Enter { rollupChainId: rollup_chain_id, rollupRecipient: rollup_recipient, amount })
}

// ============================================================================
// EnterToken event helpers
// EnterToken fields: rollupChainId (U256), token (Address),
//                    rollupRecipient (Address), amount (U256)
// ============================================================================

const fn encoded_size_enter_token(_e: &EnterToken) -> usize {
    // rollupChainId: 32 bytes
    // token: 20 bytes
    // rollupRecipient: 20 bytes
    // amount: 32 bytes
    32 + 20 + 20 + 32
}

fn encode_enter_token<B: BufMut>(e: &EnterToken, buf: &mut B) {
    buf.put_slice(&e.rollupChainId.to_be_bytes::<32>());
    buf.put_slice(e.token.as_slice());
    buf.put_slice(e.rollupRecipient.as_slice());
    buf.put_slice(&e.amount.to_be_bytes::<32>());
}

fn decode_enter_token(data: &[u8]) -> Result<EnterToken, DeserError> {
    const SIZE: usize = 32 + 20 + 20 + 32;
    if data.len() < SIZE {
        return Err(DeserError::InsufficientData { needed: SIZE, available: data.len() });
    }

    let rollup_chain_id = U256::from_be_slice(&data[0..32]);
    let token = Address::from_slice(&data[32..52]);
    let rollup_recipient = Address::from_slice(&data[52..72]);
    let amount = U256::from_be_slice(&data[72..104]);

    Ok(EnterToken {
        rollupChainId: rollup_chain_id,
        token,
        rollupRecipient: rollup_recipient,
        amount,
    })
}

// ============================================================================
// DbZenithHeader - Zenith block header
// BlockHeader fields: hostBlockNumber (U256), rollupChainId (U256),
//                     gasLimit (U256), rewardAddress (Address),
//                     blockDataHash (FixedBytes<32>)
// ============================================================================

impl ValSer for DbZenithHeader {
    fn encoded_size(&self) -> usize {
        // hostBlockNumber: 32 bytes (U256)
        // rollupChainId: 32 bytes (U256)
        // gasLimit: 32 bytes (U256)
        // rewardAddress: 20 bytes
        // blockDataHash: 32 bytes (FixedBytes<32>)
        32 + 32 + 32 + 20 + 32
    }

    fn encode_value_to<B>(&self, buf: &mut B)
    where
        B: BufMut + AsMut<[u8]>,
    {
        let h = &self.0;
        buf.put_slice(&h.hostBlockNumber.to_be_bytes::<32>());
        buf.put_slice(&h.rollupChainId.to_be_bytes::<32>());
        buf.put_slice(&h.gasLimit.to_be_bytes::<32>());
        buf.put_slice(h.rewardAddress.as_slice());
        buf.put_slice(h.blockDataHash.as_slice());
    }

    fn decode_value(data: &[u8]) -> Result<Self, DeserError>
    where
        Self: Sized,
    {
        const SIZE: usize = 32 + 32 + 32 + 20 + 32;
        if data.len() < SIZE {
            return Err(DeserError::InsufficientData { needed: SIZE, available: data.len() });
        }

        let host_block_number = U256::from_be_slice(&data[0..32]);
        let rollup_chain_id = U256::from_be_slice(&data[32..64]);
        let gas_limit = U256::from_be_slice(&data[64..96]);
        let reward_address = Address::from_slice(&data[96..116]);
        let block_data_hash = FixedBytes::<32>::from_slice(&data[116..148]);

        Ok(DbZenithHeader(Zenith::BlockHeader {
            hostBlockNumber: host_block_number,
            rollupChainId: rollup_chain_id,
            gasLimit: gas_limit,
            rewardAddress: reward_address,
            blockDataHash: block_data_hash,
        }))
    }
}

// ============================================================================
// Receipt - Transaction receipt
// ============================================================================

use alloy::consensus::{Receipt as AlloyReceipt, TxType};
use alloy::primitives::{Log, LogData};

impl ValSer for Receipt {
    fn encoded_size(&self) -> usize {
        // tx_type: 1 byte
        // status: 1 byte (bool)
        // cumulative_gas_used: 8 bytes
        // logs: 2 bytes length + variable
        1 + 1 + 8 + 2 + self.inner.logs.iter().map(encoded_size_log).sum::<usize>()
    }

    fn encode_value_to<B>(&self, buf: &mut B)
    where
        B: BufMut + AsMut<[u8]>,
    {
        buf.put_u8(self.tx_type as u8);
        buf.put_u8(self.inner.status.coerce_status() as u8);
        buf.put_u64(self.inner.cumulative_gas_used);
        buf.put_u16(self.inner.logs.len() as u16);
        for log in &self.inner.logs {
            encode_log(log, buf);
        }
    }

    fn decode_value(data: &[u8]) -> Result<Self, DeserError>
    where
        Self: Sized,
    {
        if data.len() < 12 {
            return Err(DeserError::InsufficientData { needed: 12, available: data.len() });
        }

        let tx_type = TxType::try_from(data[0])
            .map_err(|_| DeserError::String(format!("Invalid TxType: {}", data[0])))?;
        let status = data[1] != 0;
        let cumulative_gas_used = u64::from_be_bytes(data[2..10].try_into().unwrap());
        let logs_len = u16::from_be_bytes(data[10..12].try_into().unwrap()) as usize;

        let mut offset = 12;
        let mut logs = Vec::with_capacity(logs_len);
        for _ in 0..logs_len {
            let (log, consumed) = decode_log(&data[offset..])?;
            logs.push(log);
            offset += consumed;
        }

        Ok(Receipt {
            tx_type,
            inner: AlloyReceipt { status: status.into(), cumulative_gas_used, logs },
        })
    }
}

impl ValSer for IndexedReceipt {
    fn encoded_size(&self) -> usize {
        // tx_hash: 32 bytes
        // first_log_index: 8 bytes
        // gas_used: 8 bytes
        // sender: 20 bytes
        // receipt: variable
        32 + 8 + 8 + 20 + self.receipt.encoded_size()
    }

    fn encode_value_to<B>(&self, buf: &mut B)
    where
        B: BufMut + AsMut<[u8]>,
    {
        buf.put_slice(self.tx_hash.as_slice());
        buf.put_u64(self.first_log_index);
        buf.put_u64(self.gas_used);
        buf.put_slice(self.sender.as_slice());
        self.receipt.encode_value_to(buf);
    }

    fn decode_value(data: &[u8]) -> Result<Self, DeserError>
    where
        Self: Sized,
    {
        if data.len() < 68 {
            return Err(DeserError::InsufficientData { needed: 68, available: data.len() });
        }
        let tx_hash = FixedBytes::from_slice(&data[..32]);
        let first_log_index = u64::from_be_bytes(data[32..40].try_into().unwrap());
        let gas_used = u64::from_be_bytes(data[40..48].try_into().unwrap());
        let sender = Address::from_slice(&data[48..68]);
        let receipt = Receipt::decode_value(&data[68..])?;
        Ok(Self { receipt, tx_hash, first_log_index, gas_used, sender })
    }
}

fn encoded_size_log(log: &Log) -> usize {
    // address: 20 bytes
    // topics count: 1 byte
    // topics: 32 bytes each
    // data length: 4 bytes
    // data: variable
    20 + 1 + log.topics().len() * 32 + 4 + log.data.data.len()
}

fn encode_log<B: BufMut>(log: &Log, buf: &mut B) {
    buf.put_slice(log.address.as_slice());
    buf.put_u8(log.topics().len() as u8);
    for topic in log.topics() {
        buf.put_slice(topic.as_slice());
    }
    buf.put_u32(log.data.data.len() as u32);
    buf.put_slice(&log.data.data);
}

fn decode_log(data: &[u8]) -> Result<(Log, usize), DeserError> {
    if data.len() < 21 {
        return Err(DeserError::InsufficientData { needed: 21, available: data.len() });
    }

    let address = Address::from_slice(&data[0..20]);
    let topics_len = data[20] as usize;

    let topics_start = 21;
    let topics_end = topics_start + topics_len * 32;
    if data.len() < topics_end + 4 {
        return Err(DeserError::InsufficientData { needed: topics_end + 4, available: data.len() });
    }

    let mut topics = Vec::with_capacity(topics_len);
    for i in 0..topics_len {
        let start = topics_start + i * 32;
        let topic = alloy::primitives::B256::from_slice(&data[start..start + 32]);
        topics.push(topic);
    }

    let data_len =
        u32::from_be_bytes(data[topics_end..topics_end + 4].try_into().unwrap()) as usize;
    let data_start = topics_end + 4;
    let data_end = data_start + data_len;

    if data.len() < data_end {
        return Err(DeserError::InsufficientData { needed: data_end, available: data.len() });
    }

    let log_data = Bytes::copy_from_slice(&data[data_start..data_end]);

    let log = Log { address, data: LogData::new_unchecked(topics, log_data) };

    Ok((log, data_end))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tx_location_key_roundtrip() {
        let loc = TxLocation::new(12345, 42);
        let mut buf = [0u8; MAX_KEY_SIZE];
        let encoded = loc.encode_key(&mut buf);
        assert_eq!(encoded.len(), TxLocation::SIZE);

        let decoded = TxLocation::decode_key(encoded).unwrap();
        assert_eq!(loc, decoded);
    }

    #[test]
    fn test_tx_location_val_roundtrip() {
        let loc = TxLocation::new(12345, 42);
        let mut buf = bytes::BytesMut::new();
        loc.encode_value_to(&mut buf);
        assert_eq!(buf.len(), 16);

        let decoded = TxLocation::decode_value(&buf).unwrap();
        assert_eq!(loc, decoded);
    }

    #[test]
    fn test_db_signet_event_transact_roundtrip() {
        let event = DbSignetEvent::Transact(
            5,
            Transact {
                rollupChainId: U256::from(42u64),
                sender: Address::repeat_byte(0x11),
                to: Address::repeat_byte(0x22),
                value: U256::from(1000u64),
                gas: U256::from(21000u64),
                maxFeePerGas: U256::from(20_000_000_000u64),
                data: Bytes::from_static(b"hello"),
            },
        );

        let mut buf = bytes::BytesMut::new();
        event.encode_value_to(&mut buf);
        assert_eq!(buf.len(), event.encoded_size());

        let decoded = DbSignetEvent::decode_value(&buf).unwrap();
        assert_eq!(event, decoded);
    }

    #[test]
    fn test_db_signet_event_enter_roundtrip() {
        let event = DbSignetEvent::Enter(
            3,
            Enter {
                rollupChainId: U256::from(42u64),
                rollupRecipient: Address::repeat_byte(0x44),
                amount: U256::from(500u64),
            },
        );

        let mut buf = bytes::BytesMut::new();
        event.encode_value_to(&mut buf);
        assert_eq!(buf.len(), event.encoded_size());

        let decoded = DbSignetEvent::decode_value(&buf).unwrap();
        assert_eq!(event, decoded);
    }

    #[test]
    fn test_db_signet_event_enter_token_roundtrip() {
        let event = DbSignetEvent::EnterToken(
            7,
            EnterToken {
                rollupChainId: U256::from(100u64),
                token: Address::repeat_byte(0x55),
                rollupRecipient: Address::repeat_byte(0x66),
                amount: U256::from(1000000u64),
            },
        );

        let mut buf = bytes::BytesMut::new();
        event.encode_value_to(&mut buf);
        assert_eq!(buf.len(), event.encoded_size());

        let decoded = DbSignetEvent::decode_value(&buf).unwrap();
        assert_eq!(event, decoded);
    }

    #[test]
    fn test_db_zenith_header_roundtrip() {
        let header = DbZenithHeader(Zenith::BlockHeader {
            hostBlockNumber: U256::from(12345u64),
            rollupChainId: U256::from(42u64),
            gasLimit: U256::from(30_000_000u64),
            rewardAddress: Address::repeat_byte(0x77),
            blockDataHash: FixedBytes::repeat_byte(0x88),
        });

        let mut buf = bytes::BytesMut::new();
        header.encode_value_to(&mut buf);
        assert_eq!(buf.len(), header.encoded_size());

        let decoded = DbZenithHeader::decode_value(&buf).unwrap();
        assert_eq!(header.0.hostBlockNumber, decoded.0.hostBlockNumber);
        assert_eq!(header.0.rollupChainId, decoded.0.rollupChainId);
        assert_eq!(header.0.gasLimit, decoded.0.gasLimit);
        assert_eq!(header.0.rewardAddress, decoded.0.rewardAddress);
        assert_eq!(header.0.blockDataHash, decoded.0.blockDataHash);
    }

    #[test]
    fn test_receipt_roundtrip() {
        let receipt = Receipt {
            tx_type: TxType::Eip1559,
            inner: AlloyReceipt {
                status: true.into(),
                cumulative_gas_used: 100000,
                logs: vec![Log {
                    address: Address::repeat_byte(0x88),
                    data: LogData::new_unchecked(
                        vec![
                            alloy::primitives::B256::repeat_byte(0x11),
                            alloy::primitives::B256::repeat_byte(0x22),
                        ],
                        Bytes::from_static(b"log data"),
                    ),
                }],
            },
        };

        let mut buf = bytes::BytesMut::new();
        receipt.encode_value_to(&mut buf);
        assert_eq!(buf.len(), receipt.encoded_size());

        let decoded = Receipt::decode_value(&buf).unwrap();
        assert_eq!(receipt.tx_type, decoded.tx_type);
        assert_eq!(receipt.inner.status, decoded.inner.status);
        assert_eq!(receipt.inner.cumulative_gas_used, decoded.inner.cumulative_gas_used);
        assert_eq!(receipt.inner.logs.len(), decoded.inner.logs.len());
    }

    #[test]
    fn test_receipt_empty_logs_roundtrip() {
        let receipt = Receipt {
            tx_type: TxType::Legacy,
            inner: AlloyReceipt { status: false.into(), cumulative_gas_used: 21000, logs: vec![] },
        };

        let mut buf = bytes::BytesMut::new();
        receipt.encode_value_to(&mut buf);
        assert_eq!(buf.len(), receipt.encoded_size());

        let decoded = Receipt::decode_value(&buf).unwrap();
        assert_eq!(receipt.tx_type, decoded.tx_type);
        assert_eq!(receipt.inner.logs.len(), 0);
    }
}
