//! MDBX backend implementation for [`ColdStorageBackend`].
//!
//! This module provides an MDBX-based implementation of the cold storage
//! backend. It uses the table definitions from this crate and the MDBX
//! database environment from `signet-hot-mdbx`.

use crate::{
    ColdBlockHashIndex, ColdHeaders, ColdReceipts, ColdSignetEvents, ColdTransactions,
    ColdTxHashIndex, ColdTxSenders, ColdZenithHeaders, MdbxColdError,
};
use alloy::{consensus::transaction::Recovered, primitives::BlockNumber};
use signet_cold::{
    BlockData, ColdReceipt, ColdResult, ColdStorageBackend, ColdStorageError, ColdStorageRead,
    ColdStorageWrite, Confirmed, Filter, HeaderSpecifier, ReceiptSpecifier, RpcLog,
    SignetEventsSpecifier, TransactionSpecifier, ZenithHeaderSpecifier,
};
use signet_hot::{
    KeySer, MAX_KEY_SIZE, ValSer,
    model::{HotKvRead, HotKvWrite, KvTraverse},
    tables::Table,
};
use signet_hot_mdbx::{DatabaseArguments, DatabaseEnv, DatabaseEnvKind};
use signet_storage_types::{
    ConfirmationMeta, DbSignetEvent, DbZenithHeader, IndexedReceipt, RecoveredTx, SealedHeader,
    TransactionSigned, TxLocation,
};
use std::{
    path::Path,
    time::{Duration, Instant},
};

/// Default read deadline for MDBX read operations.
pub(crate) const DEFAULT_READ_TIMEOUT: Duration = Duration::from_millis(500);
/// Default advisory write deadline for MDBX write operations.
pub(crate) const DEFAULT_WRITE_TIMEOUT: Duration = Duration::from_secs(2);

/// Write a single block's data into an open read-write transaction.
///
/// Uses `MDBX_APPEND` / `MDBX_APPENDDUP` for block-number-keyed tables,
/// skipping B-tree traversal. Blocks MUST be appended in ascending order.
fn write_block_to_tx(
    tx: &signet_hot_mdbx::Tx<signet_libmdbx::Rw>,
    data: BlockData,
) -> Result<(), MdbxColdError> {
    let block = data.block_number();

    tx.queue_append::<ColdHeaders>(&block, &data.header)?;
    // Hash-keyed indices use put (keys are not sequential)
    tx.queue_put::<ColdBlockHashIndex>(&data.header.hash(), &block)?;

    // Store transactions, senders, hash indices, and receipts in a single
    // pass to avoid an intermediate Vec of (tx_hash, sender) tuples.
    let mut first_log_index = 0u64;
    let mut prior_cumulative_gas = 0u64;
    debug_assert_eq!(data.transactions.len(), data.receipts.len());
    for (idx, (recovered_tx, receipt)) in data.transactions.iter().zip(data.receipts).enumerate() {
        let tx_idx = idx as u64;
        let sender = recovered_tx.signer();
        let tx_signed: &TransactionSigned = recovered_tx;
        tx.queue_append_dual::<ColdTransactions>(&block, &tx_idx, tx_signed)?;
        tx.queue_append_dual::<ColdTxSenders>(&block, &tx_idx, &sender)?;
        tx.queue_put::<ColdTxHashIndex>(tx_signed.hash(), &TxLocation::new(block, tx_idx))?;

        let tx_hash = *tx_signed.hash();
        let gas_used = receipt.inner.cumulative_gas_used - prior_cumulative_gas;
        prior_cumulative_gas = receipt.inner.cumulative_gas_used;
        let ir = IndexedReceipt { receipt, tx_hash, first_log_index, gas_used, sender };
        first_log_index += ir.receipt.inner.logs.len() as u64;
        tx.queue_append_dual::<ColdReceipts>(&block, &tx_idx, &ir)?;
    }

    for (idx, event) in data.signet_events.iter().enumerate() {
        tx.queue_append_dual::<ColdSignetEvents>(&block, &(idx as u64), event)?;
    }

    if let Some(zh) = &data.zenith_header {
        tx.queue_append::<ColdZenithHeaders>(&block, zh)?;
    }

    Ok(())
}

/// Unwrap a `Result` or send the error through the stream and return.
macro_rules! try_stream {
    ($sender:expr, $expr:expr) => {
        match $expr {
            Ok(v) => v,
            Err(e) => {
                let _ =
                    $sender.blocking_send(Err(ColdStorageError::backend(MdbxColdError::from(e))));
                return;
            }
        }
    };
}

/// Produce a log stream using a single MDBX read transaction.
///
/// Runs synchronously on a blocking thread. The `Tx<Ro>` snapshot
/// provides MVCC consistency — the snapshot is self-consistent and
/// no reorg detection is needed within it.
fn produce_log_stream_blocking(
    env: DatabaseEnv,
    filter: Filter,
    from: BlockNumber,
    to: BlockNumber,
    max_logs: usize,
    sender: tokio::sync::mpsc::Sender<ColdResult<RpcLog>>,
    deadline: std::time::Instant,
) {
    // Open a read-only transaction. MDBX's MVCC guarantees a
    // consistent snapshot for the lifetime of this transaction,
    // so no reorg detection is needed.
    let tx = try_stream!(sender, env.tx());

    // Reuse cursors across blocks to avoid re-opening them on
    // every iteration (same pattern as get_logs_inner).
    let mut header_cursor = try_stream!(sender, tx.traverse::<ColdHeaders>());
    let mut receipt_cursor = try_stream!(sender, tx.traverse_dual::<ColdReceipts>());

    let mut total = 0usize;

    // Walk through blocks one at a time, filtering and sending
    // matching logs over the channel.
    for block_num in from..=to {
        // Check the deadline before starting each block so we
        // don't begin reading after the caller's timeout.
        if std::time::Instant::now() > deadline {
            let _ = sender.blocking_send(Err(ColdStorageError::StreamDeadlineExceeded));
            return;
        }

        // Look up the block header for its hash and timestamp,
        // which are attached to every emitted RpcLog.
        let sealed = match try_stream!(sender, header_cursor.exact(&block_num)) {
            Some(v) => v,
            None => continue,
        };
        let block_hash = sealed.hash();
        let block_timestamp = sealed.timestamp;

        // Iterate over all receipts (and their embedded logs) for
        // this block, applying the caller's address/topic filter.
        let iter = try_stream!(sender, receipt_cursor.iter_k2(&block_num));

        let remaining = max_logs.saturating_sub(total);
        let mut block_count = 0usize;

        for result in iter {
            let (tx_idx, ir): (u64, IndexedReceipt) = try_stream!(sender, result);
            let tx_hash = ir.tx_hash;
            let first_log_index = ir.first_log_index;
            for (log_idx, log) in ir.receipt.inner.logs.into_iter().enumerate() {
                if !filter.matches(&log) {
                    continue;
                }
                // Enforce the global log limit across all blocks.
                block_count += 1;
                if block_count > remaining {
                    let _ = sender
                        .blocking_send(Err(ColdStorageError::TooManyLogs { limit: max_logs }));
                    return;
                }
                let rpc_log = RpcLog {
                    inner: log,
                    block_hash: Some(block_hash),
                    block_number: Some(block_num),
                    block_timestamp: Some(block_timestamp),
                    transaction_hash: Some(tx_hash),
                    transaction_index: Some(tx_idx),
                    log_index: Some(first_log_index + log_idx as u64),
                    removed: false,
                };
                // Send the log to the caller via blocking_send
                // (we're on a spawn_blocking thread, not async).
                if sender.blocking_send(Ok(rpc_log)).is_err() {
                    return; // receiver dropped
                }
            }
        }

        // Early exit if we've already hit the limit — no need to
        // read the next block.
        total += block_count;
        if total >= max_logs {
            return;
        }
    }
}

/// MDBX-based cold storage backend.
///
/// This backend stores historical blockchain data in an MDBX database.
/// It implements the [`ColdStorageBackend`] trait for use with the cold storage
/// task runner.
#[derive(Clone)]
pub struct MdbxColdBackend {
    /// The MDBX environment.
    env: DatabaseEnv,
    /// Wall-clock deadline for read operations; checked between per-block
    /// iterations on iterator reads.
    read_timeout: Duration,
    /// Advisory deadline for write operations. Writes that exceed this are
    /// logged via [`tracing::warn!`] but still report success.
    write_timeout: Duration,
}

impl std::fmt::Debug for MdbxColdBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MdbxColdBackend").finish_non_exhaustive()
    }
}

impl MdbxColdBackend {
    /// Create a new backend from an existing MDBX environment.
    const fn from_env(env: DatabaseEnv) -> Self {
        Self { env, read_timeout: DEFAULT_READ_TIMEOUT, write_timeout: DEFAULT_WRITE_TIMEOUT }
    }

    /// Set the read deadline. Applied between per-block iterations on
    /// iterator reads; point lookups do not check this deadline.
    #[must_use]
    pub const fn with_read_timeout(mut self, read_timeout: Duration) -> Self {
        self.read_timeout = read_timeout;
        self
    }

    /// Set the advisory write deadline. Writes exceeding this threshold
    /// emit a [`tracing::warn!`] but still report success to the caller;
    /// MDBX commits are uninterruptible.
    #[must_use]
    pub const fn with_write_timeout(mut self, write_timeout: Duration) -> Self {
        self.write_timeout = write_timeout;
        self
    }

    /// Open an existing MDBX cold storage database in read-only mode.
    pub fn open_ro(path: &Path) -> Result<Self, MdbxColdError> {
        let env = DatabaseArguments::new().open_ro(path)?;
        Ok(Self::from_env(env))
    }

    /// Open or create an MDBX cold storage database in read-write mode.
    pub fn open_rw(path: &Path) -> Result<Self, MdbxColdError> {
        let env = DatabaseArguments::new().open_rw(path)?;
        let backend = Self::from_env(env);
        backend.create_tables()?;
        Ok(backend)
    }

    /// Open an MDBX cold storage database with custom arguments.
    pub fn open(
        path: &Path,
        kind: DatabaseEnvKind,
        args: DatabaseArguments,
    ) -> Result<Self, MdbxColdError> {
        let env = DatabaseEnv::open(path, kind, args)?;
        let backend = Self::from_env(env);
        if kind.is_rw() {
            backend.create_tables()?;
        }
        Ok(backend)
    }

    fn create_tables(&self) -> Result<(), MdbxColdError> {
        let tx = self.env.tx_rw()?;

        for (name, dual_key_size, fixed_val_size) in [
            (ColdHeaders::NAME, ColdHeaders::DUAL_KEY_SIZE, ColdHeaders::FIXED_VAL_SIZE),
            (
                ColdZenithHeaders::NAME,
                ColdZenithHeaders::DUAL_KEY_SIZE,
                ColdZenithHeaders::FIXED_VAL_SIZE,
            ),
            (
                ColdBlockHashIndex::NAME,
                ColdBlockHashIndex::DUAL_KEY_SIZE,
                ColdBlockHashIndex::FIXED_VAL_SIZE,
            ),
            (
                ColdTxHashIndex::NAME,
                ColdTxHashIndex::DUAL_KEY_SIZE,
                ColdTxHashIndex::FIXED_VAL_SIZE,
            ),
            (
                ColdTransactions::NAME,
                ColdTransactions::DUAL_KEY_SIZE,
                ColdTransactions::FIXED_VAL_SIZE,
            ),
            (ColdTxSenders::NAME, ColdTxSenders::DUAL_KEY_SIZE, ColdTxSenders::FIXED_VAL_SIZE),
            (ColdReceipts::NAME, ColdReceipts::DUAL_KEY_SIZE, ColdReceipts::FIXED_VAL_SIZE),
            (
                ColdSignetEvents::NAME,
                ColdSignetEvents::DUAL_KEY_SIZE,
                ColdSignetEvents::FIXED_VAL_SIZE,
            ),
        ] {
            tx.queue_raw_create(name, dual_key_size, fixed_val_size)?;
        }

        tx.raw_commit()?;
        Ok(())
    }

    fn get_header_inner(
        env: &DatabaseEnv,
        spec: HeaderSpecifier,
    ) -> Result<Option<SealedHeader>, MdbxColdError> {
        let tx = env.tx()?;
        let block_num = match spec {
            HeaderSpecifier::Number(n) => n,
            HeaderSpecifier::Hash(h) => {
                let Some(n) = tx.traverse::<ColdBlockHashIndex>()?.exact(&h)? else {
                    return Ok(None);
                };
                n
            }
        };
        Ok(tx.traverse::<ColdHeaders>()?.exact(&block_num)?)
    }

    fn get_headers_inner(
        env: &DatabaseEnv,
        specs: Vec<HeaderSpecifier>,
        deadline: Instant,
        read_timeout: Duration,
    ) -> Result<Vec<Option<SealedHeader>>, MdbxColdError> {
        let tx = env.tx()?;
        let mut out = Vec::with_capacity(specs.len());
        for spec in specs {
            if Instant::now() > deadline {
                return Err(MdbxColdError::Timeout(read_timeout));
            }
            let block_num = match spec {
                HeaderSpecifier::Number(n) => Some(n),
                HeaderSpecifier::Hash(h) => tx.traverse::<ColdBlockHashIndex>()?.exact(&h)?,
            };
            let header = match block_num {
                Some(n) => tx.traverse::<ColdHeaders>()?.exact(&n)?,
                None => None,
            };
            out.push(header);
        }
        Ok(out)
    }

    fn get_transaction_inner(
        env: &DatabaseEnv,
        spec: TransactionSpecifier,
    ) -> Result<Option<Confirmed<RecoveredTx>>, MdbxColdError> {
        let tx = env.tx()?;
        let (block, index) = match spec {
            TransactionSpecifier::Hash(h) => {
                let Some(loc) = tx.traverse::<ColdTxHashIndex>()?.exact(&h)? else {
                    return Ok(None);
                };
                (loc.block, loc.index)
            }
            TransactionSpecifier::BlockAndIndex { block, index } => (block, index),
            TransactionSpecifier::BlockHashAndIndex { block_hash, index } => {
                let Some(block) = tx.traverse::<ColdBlockHashIndex>()?.exact(&block_hash)? else {
                    return Ok(None);
                };
                (block, index)
            }
        };
        let Some(signed_tx) = tx.traverse_dual::<ColdTransactions>()?.exact_dual(&block, &index)?
        else {
            return Ok(None);
        };
        let Some(sender) = tx.traverse_dual::<ColdTxSenders>()?.exact_dual(&block, &index)? else {
            return Ok(None);
        };
        let Some(sealed) = tx.traverse::<ColdHeaders>()?.exact(&block)? else {
            return Ok(None);
        };
        let meta = ConfirmationMeta::new(block, sealed.hash(), index);
        // SAFETY: the sender was recovered at append time and stored alongside the transaction.
        let recovered = Recovered::new_unchecked(signed_tx, sender);
        Ok(Some(Confirmed::new(recovered, meta)))
    }

    fn get_receipt_inner(
        env: &DatabaseEnv,
        spec: ReceiptSpecifier,
    ) -> Result<Option<ColdReceipt>, MdbxColdError> {
        let tx = env.tx()?;
        let (block, index) = match spec {
            ReceiptSpecifier::TxHash(h) => {
                let Some(loc) = tx.traverse::<ColdTxHashIndex>()?.exact(&h)? else {
                    return Ok(None);
                };
                (loc.block, loc.index)
            }
            ReceiptSpecifier::BlockAndIndex { block, index } => (block, index),
        };
        let Some(sealed) = tx.traverse::<ColdHeaders>()?.exact(&block)? else {
            return Ok(None);
        };
        let Some(ir) = tx.traverse_dual::<ColdReceipts>()?.exact_dual(&block, &index)? else {
            return Ok(None);
        };
        Ok(Some(ColdReceipt::new(ir, &sealed, index)))
    }

    fn get_zenith_header_by_number(
        env: &DatabaseEnv,
        block: BlockNumber,
    ) -> Result<Option<DbZenithHeader>, MdbxColdError> {
        let tx = env.tx()?;
        Ok(tx.traverse::<ColdZenithHeaders>()?.exact(&block)?)
    }

    fn collect_transactions_in_block(
        env: &DatabaseEnv,
        block: BlockNumber,
    ) -> Result<Vec<RecoveredTx>, MdbxColdError> {
        let tx = env.tx()?;
        tx.traverse_dual::<ColdTransactions>()?
            .iter_k2(&block)?
            .zip(tx.traverse_dual::<ColdTxSenders>()?.iter_k2(&block)?)
            .map(|(tx_item, sender_item)| -> Result<_, MdbxColdError> {
                let (_, signed_tx) = tx_item?;
                let (_, sender) = sender_item?;
                // SAFETY: the sender was recovered at append time.
                Ok(Recovered::new_unchecked(signed_tx, sender))
            })
            .collect()
    }

    fn count_transactions_in_block(
        env: &DatabaseEnv,
        block: BlockNumber,
    ) -> Result<u64, MdbxColdError> {
        let tx = env.tx()?;
        let mut count = 0u64;
        for item in tx.traverse_dual::<ColdTransactions>()?.iter_k2(&block)? {
            item?;
            count += 1;
        }
        Ok(count)
    }

    fn collect_receipts_in_block(
        env: &DatabaseEnv,
        block: BlockNumber,
    ) -> Result<Vec<ColdReceipt>, MdbxColdError> {
        let tx = env.tx()?;
        let Some(sealed) = tx.traverse::<ColdHeaders>()?.exact(&block)? else {
            return Ok(Vec::new());
        };
        tx.traverse_dual::<ColdReceipts>()?
            .iter_k2(&block)?
            .map(|item| {
                let (idx, ir) = item?;
                Ok::<_, MdbxColdError>(ColdReceipt::new(ir, &sealed, idx))
            })
            .collect()
    }

    fn collect_signet_events_in_block(
        env: &DatabaseEnv,
        block: BlockNumber,
    ) -> Result<Vec<DbSignetEvent>, MdbxColdError> {
        let tx = env.tx()?;
        tx.traverse_dual::<ColdSignetEvents>()?
            .iter_k2(&block)?
            .map(|item| item.map(|(_, v)| v))
            .collect::<Result<_, _>>()
            .map_err(Into::into)
    }

    fn collect_signet_events_in_range(
        env: &DatabaseEnv,
        start: BlockNumber,
        end: BlockNumber,
        deadline: Instant,
        read_timeout: Duration,
    ) -> Result<Vec<DbSignetEvent>, MdbxColdError> {
        let tx = env.tx()?;
        let mut cursor = tx.traverse_dual::<ColdSignetEvents>()?;
        let mut events = Vec::new();
        for block in start..=end {
            if Instant::now() > deadline {
                return Err(MdbxColdError::Timeout(read_timeout));
            }
            for item in cursor.iter_k2(&block)? {
                events.push(item?.1);
            }
        }
        Ok(events)
    }

    fn collect_zenith_headers_in_range(
        env: &DatabaseEnv,
        start: BlockNumber,
        end: BlockNumber,
        deadline: Instant,
        read_timeout: Duration,
    ) -> Result<Vec<DbZenithHeader>, MdbxColdError> {
        let tx = env.tx()?;
        let mut cursor = tx.new_cursor::<ColdZenithHeaders>()?;
        let mut headers = Vec::new();

        let mut key_buf = [0u8; MAX_KEY_SIZE];
        let key_bytes = start.encode_key(&mut key_buf);

        let Some((key, value)) = cursor.lower_bound(key_bytes)? else {
            return Ok(headers);
        };

        let block_num = BlockNumber::decode_key(&key)?;
        if block_num <= end {
            headers.push(DbZenithHeader::decode_value(&value)?);
        }

        while let Some((key, value)) = cursor.read_next()? {
            if Instant::now() > deadline {
                return Err(MdbxColdError::Timeout(read_timeout));
            }
            let block_num = BlockNumber::decode_key(&key)?;
            if block_num > end {
                break;
            }
            headers.push(DbZenithHeader::decode_value(&value)?);
        }

        Ok(headers)
    }

    fn append_block_inner(&self, data: BlockData) -> Result<(), MdbxColdError> {
        let tx = self.env.tx_rw()?;
        write_block_to_tx(&tx, data)?;
        tx.raw_commit()?;
        Ok(())
    }

    fn append_blocks_inner(&self, data: Vec<BlockData>) -> Result<(), MdbxColdError> {
        let tx = self.env.tx_rw()?;
        for block_data in data {
            write_block_to_tx(&tx, block_data)?;
        }
        tx.raw_commit()?;
        Ok(())
    }

    /// Collect all sealed headers above `block` from the given transaction.
    fn collect_headers_above(
        tx: &signet_hot_mdbx::Tx<signet_libmdbx::Rw>,
        block: BlockNumber,
    ) -> Result<Vec<(BlockNumber, SealedHeader)>, MdbxColdError> {
        let mut cursor = tx.new_cursor::<ColdHeaders>()?;
        let mut headers: Vec<(BlockNumber, SealedHeader)> = Vec::new();

        let start_block = block + 1;
        let mut key_buf = [0u8; MAX_KEY_SIZE];
        let key_bytes = start_block.encode_key(&mut key_buf);

        if let Some((key, value)) = cursor.lower_bound(key_bytes)? {
            headers.push((BlockNumber::decode_key(&key)?, SealedHeader::decode_value(&value)?));

            while let Some((key, value)) = cursor.read_next()? {
                headers.push((BlockNumber::decode_key(&key)?, SealedHeader::decode_value(&value)?));
            }
        }
        Ok(headers)
    }

    /// Delete all data for the given blocks from the transaction.
    fn delete_blocks(
        tx: &signet_hot_mdbx::Tx<signet_libmdbx::Rw>,
        headers: &[(BlockNumber, SealedHeader)],
    ) -> Result<(), MdbxColdError> {
        let mut tx_cursor = tx.traverse_dual::<ColdTransactions>()?;
        for (block_num, sealed) in headers {
            for item in tx_cursor.iter_k2(block_num)? {
                let (_, tx_signed) = item?;
                tx.queue_delete::<ColdTxHashIndex>(tx_signed.hash())?;
            }

            tx.queue_delete::<ColdHeaders>(block_num)?;
            tx.queue_delete::<ColdBlockHashIndex>(&sealed.hash())?;
            tx.clear_k1_for::<ColdTransactions>(block_num)?;
            tx.clear_k1_for::<ColdTxSenders>(block_num)?;
            tx.clear_k1_for::<ColdReceipts>(block_num)?;
            tx.clear_k1_for::<ColdSignetEvents>(block_num)?;
            tx.queue_delete::<ColdZenithHeaders>(block_num)?;
        }
        Ok(())
    }

    fn truncate_above_inner(&self, block: BlockNumber) -> Result<(), MdbxColdError> {
        let tx = self.env.tx_rw()?;
        let headers = Self::collect_headers_above(&tx, block)?;
        if headers.is_empty() {
            return Ok(());
        }
        Self::delete_blocks(&tx, &headers)?;
        tx.raw_commit()?;
        Ok(())
    }

    fn drain_above_inner(
        &self,
        block: BlockNumber,
    ) -> Result<Vec<Vec<ColdReceipt>>, MdbxColdError> {
        let tx = self.env.tx_rw()?;
        let headers = Self::collect_headers_above(&tx, block)?;
        if headers.is_empty() {
            return Ok(Vec::new());
        }

        // Read receipts before deleting
        let mut all_receipts = Vec::with_capacity(headers.len());
        let mut receipt_cursor = tx.traverse_dual::<ColdReceipts>()?;
        for (block_num, sealed) in &headers {
            let block_receipts: Vec<ColdReceipt> = receipt_cursor
                .iter_k2(block_num)?
                .map(|item| {
                    let (idx, ir) = item?;
                    Ok::<_, MdbxColdError>(ColdReceipt::new(ir, sealed, idx))
                })
                .collect::<Result<_, _>>()?;
            all_receipts.push(block_receipts);
        }
        drop(receipt_cursor);

        Self::delete_blocks(&tx, &headers)?;
        tx.raw_commit()?;
        Ok(all_receipts)
    }

    fn get_logs_inner(
        env: &DatabaseEnv,
        filter: &Filter,
        max_logs: usize,
        deadline: Instant,
        read_timeout: Duration,
    ) -> Result<Vec<signet_cold::RpcLog>, MdbxColdError> {
        let tx = env.tx()?;
        let mut results = Vec::new();

        let from = filter.get_from_block().unwrap_or(0);
        let to = match filter.get_to_block() {
            Some(to) => to,
            None => {
                let mut cursor = tx.new_cursor::<ColdHeaders>()?;
                let Some((key, _)) = cursor.last()? else {
                    return Ok(results);
                };
                BlockNumber::decode_key(&key)?
            }
        };

        let mut header_cursor = tx.traverse::<ColdHeaders>()?;
        let mut receipt_cursor = tx.traverse_dual::<ColdReceipts>()?;

        for block_num in from..=to {
            if Instant::now() > deadline {
                return Err(MdbxColdError::Timeout(read_timeout));
            }
            let Some(sealed) = header_cursor.exact(&block_num)? else {
                continue;
            };
            let block_hash = sealed.hash();
            let block_timestamp = sealed.timestamp;

            for item in receipt_cursor.iter_k2(&block_num)? {
                let (tx_idx, ir) = item?;
                let tx_hash = ir.tx_hash;
                let first_log_index = ir.first_log_index;
                for (log_idx, log) in ir.receipt.inner.logs.into_iter().enumerate() {
                    if !filter.matches(&log) {
                        continue;
                    }
                    if results.len() >= max_logs {
                        return Err(MdbxColdError::TooManyLogs(max_logs));
                    }
                    results.push(signet_cold::RpcLog {
                        inner: log,
                        block_hash: Some(block_hash),
                        block_number: Some(block_num),
                        block_timestamp: Some(block_timestamp),
                        transaction_hash: Some(tx_hash),
                        transaction_index: Some(tx_idx),
                        log_index: Some(first_log_index + log_idx as u64),
                        removed: false,
                    });
                }
            }
        }

        Ok(results)
    }
}

impl ColdStorageRead for MdbxColdBackend {
    async fn get_header(&self, spec: HeaderSpecifier) -> ColdResult<Option<SealedHeader>> {
        let env = self.env.clone();
        tokio::task::spawn_blocking(move || Self::get_header_inner(&env, spec))
            .await
            .map_err(|_| ColdStorageError::TaskTerminated)?
            .map_err(ColdStorageError::backend)
    }

    async fn get_headers(
        &self,
        specs: Vec<HeaderSpecifier>,
    ) -> ColdResult<Vec<Option<SealedHeader>>> {
        let env = self.env.clone();
        let read_timeout = self.read_timeout;
        tokio::task::spawn_blocking(move || {
            let deadline = Instant::now() + read_timeout;
            Self::get_headers_inner(&env, specs, deadline, read_timeout)
        })
        .await
        .map_err(|_| ColdStorageError::TaskTerminated)?
        .map_err(ColdStorageError::backend)
    }

    async fn get_transaction(
        &self,
        spec: TransactionSpecifier,
    ) -> ColdResult<Option<Confirmed<RecoveredTx>>> {
        let env = self.env.clone();
        tokio::task::spawn_blocking(move || Self::get_transaction_inner(&env, spec))
            .await
            .map_err(|_| ColdStorageError::TaskTerminated)?
            .map_err(ColdStorageError::backend)
    }

    async fn get_transactions_in_block(&self, block: BlockNumber) -> ColdResult<Vec<RecoveredTx>> {
        let env = self.env.clone();
        tokio::task::spawn_blocking(move || Self::collect_transactions_in_block(&env, block))
            .await
            .map_err(|_| ColdStorageError::TaskTerminated)?
            .map_err(ColdStorageError::backend)
    }

    async fn get_transaction_count(&self, block: BlockNumber) -> ColdResult<u64> {
        let env = self.env.clone();
        tokio::task::spawn_blocking(move || Self::count_transactions_in_block(&env, block))
            .await
            .map_err(|_| ColdStorageError::TaskTerminated)?
            .map_err(ColdStorageError::backend)
    }

    async fn get_receipt(&self, spec: ReceiptSpecifier) -> ColdResult<Option<ColdReceipt>> {
        let env = self.env.clone();
        tokio::task::spawn_blocking(move || Self::get_receipt_inner(&env, spec))
            .await
            .map_err(|_| ColdStorageError::TaskTerminated)?
            .map_err(ColdStorageError::backend)
    }

    async fn get_receipts_in_block(&self, block: BlockNumber) -> ColdResult<Vec<ColdReceipt>> {
        let env = self.env.clone();
        tokio::task::spawn_blocking(move || Self::collect_receipts_in_block(&env, block))
            .await
            .map_err(|_| ColdStorageError::TaskTerminated)?
            .map_err(ColdStorageError::backend)
    }

    async fn get_signet_events(
        &self,
        spec: SignetEventsSpecifier,
    ) -> ColdResult<Vec<DbSignetEvent>> {
        let env = self.env.clone();
        let read_timeout = self.read_timeout;
        tokio::task::spawn_blocking(move || {
            let deadline = Instant::now() + read_timeout;
            match spec {
                SignetEventsSpecifier::Block(block) => {
                    Self::collect_signet_events_in_block(&env, block)
                }
                SignetEventsSpecifier::BlockRange { start, end } => {
                    Self::collect_signet_events_in_range(&env, start, end, deadline, read_timeout)
                }
            }
        })
        .await
        .map_err(|_| ColdStorageError::TaskTerminated)?
        .map_err(ColdStorageError::backend)
    }

    async fn get_zenith_header(
        &self,
        spec: ZenithHeaderSpecifier,
    ) -> ColdResult<Option<DbZenithHeader>> {
        let env = self.env.clone();
        let block = match spec {
            ZenithHeaderSpecifier::Number(n) => n,
            ZenithHeaderSpecifier::Range { start, .. } => start,
        };
        tokio::task::spawn_blocking(move || Self::get_zenith_header_by_number(&env, block))
            .await
            .map_err(|_| ColdStorageError::TaskTerminated)?
            .map_err(ColdStorageError::backend)
    }

    async fn get_zenith_headers(
        &self,
        spec: ZenithHeaderSpecifier,
    ) -> ColdResult<Vec<DbZenithHeader>> {
        let env = self.env.clone();
        let read_timeout = self.read_timeout;
        tokio::task::spawn_blocking(move || {
            let deadline = Instant::now() + read_timeout;
            match spec {
                ZenithHeaderSpecifier::Number(n) => {
                    Ok(Self::get_zenith_header_by_number(&env, n)?.into_iter().collect())
                }
                ZenithHeaderSpecifier::Range { start, end } => {
                    Self::collect_zenith_headers_in_range(&env, start, end, deadline, read_timeout)
                }
            }
        })
        .await
        .map_err(|_| ColdStorageError::TaskTerminated)?
        .map_err(ColdStorageError::backend)
    }

    async fn get_logs(
        &self,
        filter: &Filter,
        max_logs: usize,
    ) -> ColdResult<Vec<signet_cold::RpcLog>> {
        let env = self.env.clone();
        let read_timeout = self.read_timeout;
        let filter = filter.clone();
        tokio::task::spawn_blocking(move || {
            let deadline = Instant::now() + read_timeout;
            Self::get_logs_inner(&env, &filter, max_logs, deadline, read_timeout)
        })
        .await
        .map_err(|_| ColdStorageError::TaskTerminated)?
        .map_err(ColdStorageError::backend)
    }

    async fn produce_log_stream(&self, filter: &Filter, params: signet_cold::StreamParams) {
        let env = self.env.clone();
        // ENG-2036: clone required to move into spawn_blocking. Eliminating
        // this would require changing the ColdStorageBackend trait to pass owned
        // Filter, which is a cross-crate API change.
        let filter = filter.clone();
        let std_deadline = params.deadline.into_std();
        let _ = tokio::task::spawn_blocking(move || {
            produce_log_stream_blocking(
                env,
                filter,
                params.from,
                params.to,
                params.max_logs,
                params.sender,
                std_deadline,
            );
        })
        .await;
    }

    async fn get_latest_block(&self) -> ColdResult<Option<BlockNumber>> {
        let env = self.env.clone();
        tokio::task::spawn_blocking(move || -> Result<Option<BlockNumber>, MdbxColdError> {
            let tx = env.tx()?;
            let mut cursor = tx.new_cursor::<ColdHeaders>()?;
            cursor
                .last()?
                .map(|(key, _)| BlockNumber::decode_key(&key))
                .transpose()
                .map_err(MdbxColdError::from)
        })
        .await
        .map_err(|_| ColdStorageError::TaskTerminated)?
        .map_err(ColdStorageError::backend)
    }
}

impl ColdStorageWrite for MdbxColdBackend {
    async fn append_block(&self, data: BlockData) -> ColdResult<()> {
        let threshold = self.write_timeout;
        let start = Instant::now();
        let result = tokio::task::block_in_place(|| self.append_block_inner(data));
        let elapsed = start.elapsed();
        if elapsed > threshold {
            tracing::warn!(
                elapsed_ms = elapsed.as_millis() as u64,
                threshold_ms = threshold.as_millis() as u64,
                "mdbx append_block exceeded advisory write timeout",
            );
        }
        Ok(result?)
    }

    async fn append_blocks(&self, data: Vec<BlockData>) -> ColdResult<()> {
        let threshold = self.write_timeout;
        let start = Instant::now();
        let result = tokio::task::block_in_place(|| self.append_blocks_inner(data));
        let elapsed = start.elapsed();
        if elapsed > threshold {
            tracing::warn!(
                elapsed_ms = elapsed.as_millis() as u64,
                threshold_ms = threshold.as_millis() as u64,
                "mdbx append_blocks exceeded advisory write timeout",
            );
        }
        Ok(result?)
    }

    async fn truncate_above(&self, block: BlockNumber) -> ColdResult<()> {
        let threshold = self.write_timeout;
        let start = Instant::now();
        let result = tokio::task::block_in_place(|| self.truncate_above_inner(block));
        let elapsed = start.elapsed();
        if elapsed > threshold {
            tracing::warn!(
                elapsed_ms = elapsed.as_millis() as u64,
                threshold_ms = threshold.as_millis() as u64,
                "mdbx truncate_above exceeded advisory write timeout",
            );
        }
        Ok(result?)
    }
}

impl ColdStorageBackend for MdbxColdBackend {
    async fn drain_above(&self, block: BlockNumber) -> ColdResult<Vec<Vec<ColdReceipt>>> {
        let threshold = self.write_timeout;
        let start = Instant::now();
        let result = tokio::task::block_in_place(|| self.drain_above_inner(block));
        let elapsed = start.elapsed();
        if elapsed > threshold {
            tracing::warn!(
                elapsed_ms = elapsed.as_millis() as u64,
                threshold_ms = threshold.as_millis() as u64,
                "mdbx drain_above exceeded advisory write timeout",
            );
        }
        Ok(result?)
    }
}

#[cfg(all(test, feature = "test-utils"))]
mod tests {
    use super::*;
    use signet_cold::conformance::conformance;
    use tempfile::tempdir;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn mdbx_backend_conformance() {
        let dir = tempdir().unwrap();
        let backend = MdbxColdBackend::open_rw(dir.path()).unwrap();
        conformance(backend).await.unwrap();
    }
}
