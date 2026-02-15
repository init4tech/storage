use alloy::consensus::{EthereumTxEnvelope, TxEip4844};

/// Signed transaction.
pub type TransactionSigned = EthereumTxEnvelope<TxEip4844>;
