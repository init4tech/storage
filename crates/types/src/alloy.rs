use alloy::consensus::{EthereumTxEnvelope, Receipt as AlloyReceipt, TxEip4844, TxType};

/// Signed transaction.
pub type TransactionSigned = EthereumTxEnvelope<TxEip4844>;

/// Typed ethereum transaction receipt.
/// Receipt containing result of transaction execution.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct Receipt {
    /// Receipt type.
    pub tx_type: TxType,

    /// The actual receipt data.
    pub inner: AlloyReceipt,
}
