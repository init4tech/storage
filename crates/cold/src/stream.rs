//! Log-streaming helper for backends without snapshot semantics.

use crate::{ColdResult, ColdStorage, ColdStorageError, Filter, HeaderSpecifier, RpcLog};
use alloy::{primitives::BlockNumber, rpc::types::FilterBlockOption};
use tokio::sync::mpsc;

/// Parameters for a log-streaming request.
///
/// Bundles the block range, limits, channel, and deadline that every
/// [`ColdStorage::produce_log_stream`] implementation needs.
#[derive(Debug)]
pub struct StreamParams {
    /// First block in range (inclusive).
    pub from: BlockNumber,
    /// Last block in range (inclusive).
    pub to: BlockNumber,
    /// Maximum number of matching logs before aborting with
    /// [`ColdStorageError::TooManyLogs`].
    pub max_logs: usize,
    /// Channel for sending log results.
    pub sender: mpsc::Sender<ColdResult<RpcLog>>,
    /// Deadline after which the stream is aborted with
    /// [`ColdStorageError::StreamDeadlineExceeded`].
    pub deadline: tokio::time::Instant,
}

/// Log-streaming implementation for backends without snapshot semantics.
///
/// Captures an anchor hash from the `to` block at the start and
/// re-checks it before each block to detect reorgs. Uses
/// [`ColdStorage::get_header`] for anchor checks and
/// [`ColdStorage::get_logs`] with single-block filters per block.
///
/// Backends that hold a consistent read snapshot (MDBX, PostgreSQL
/// with REPEATABLE READ) should provide their own
/// [`ColdStorage::produce_log_stream`] implementation instead.
pub async fn produce_log_stream_default<B: ColdStorage + ?Sized>(
    backend: &B,
    filter: &Filter,
    params: StreamParams,
) {
    let StreamParams { from, to, max_logs, sender, deadline } = params;

    // Capture the hash of the `to` block before we start iterating.
    // Without snapshot isolation we have no guarantee that the
    // underlying data stays consistent, so we re-check this hash
    // before each block to detect reorgs.
    let anchor_hash = match backend.get_header(HeaderSpecifier::Number(to)).await {
        Ok(Some(h)) => h.hash(),
        Ok(None) => return, // block doesn't exist; empty stream
        Err(e) => {
            let _ = sender.send(Err(e)).await;
            return;
        }
    };

    let mut total = 0usize;

    // Clone the filter once; we reuse it across blocks by mutating
    // only the block range, avoiding per-block clones of the address
    // and topic arrays.
    let mut block_filter = filter.clone();

    // Walk through blocks one at a time, fetching matching logs from
    // each block and sending them over the channel individually.
    for block_num in from..=to {
        // Check the deadline before starting each block so we
        // don't begin a new query after the caller's timeout.
        if tokio::time::Instant::now() > deadline {
            let _ = sender.send(Err(ColdStorageError::StreamDeadlineExceeded)).await;
            return;
        }

        // Re-check the anchor hash to detect reorgs that may have
        // occurred since we started streaming.
        match backend.get_header(HeaderSpecifier::Number(to)).await {
            Ok(Some(h)) if h.hash() == anchor_hash => {}
            Ok(_) => {
                let _ = sender.send(Err(ColdStorageError::ReorgDetected)).await;
                return;
            }
            Err(e) => {
                let _ = sender.send(Err(e)).await;
                return;
            }
        }

        // Fetch all matching logs for this single block. The
        // remaining budget shrinks as we accumulate results so
        // `get_logs` can reject early if a single block overflows.
        let remaining = max_logs.saturating_sub(total);
        block_filter.block_option = FilterBlockOption::Range {
            from_block: Some(block_num.into()),
            to_block: Some(block_num.into()),
        };
        let block_logs = match backend.get_logs(&block_filter, remaining).await {
            Ok(logs) => logs,
            Err(ColdStorageError::TooManyLogs { .. }) => {
                let _ = sender.send(Err(ColdStorageError::TooManyLogs { limit: max_logs })).await;
                return;
            }
            Err(e) => {
                let _ = sender.send(Err(e)).await;
                return;
            }
        };

        total += block_logs.len();

        // Send each log individually over the channel. The timeout
        // ensures we stop if the deadline passes while back-pressured
        // by a slow receiver.
        for log in block_logs {
            match tokio::time::timeout_at(deadline, sender.send(Ok(log))).await {
                Ok(Ok(())) => {}
                Ok(Err(_)) => return, // receiver dropped
                Err(_) => {
                    let _ = sender.send(Err(ColdStorageError::StreamDeadlineExceeded)).await;
                    return;
                }
            }
        }

        // Early exit if we've already hit the limit — no need to
        // query the next block.
        if total >= max_logs {
            return;
        }
    }
}
