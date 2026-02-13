//! Filter type for log queries following `eth_getLogs` semantics.

use alloy::primitives::{Address, B256, BlockNumber, Log};

/// Filter for log queries, following `eth_getLogs` semantics.
///
/// All filter fields are optional except the block range, which is
/// required to prevent unbounded scans.
///
/// # Topic Matching
///
/// Each topic position is independently filtered:
/// - `None` matches any value at that position.
/// - `Some(vec![a, b])` matches if the topic at that position equals
///   `a` **or** `b` (OR within a position).
///
/// Positions are combined with AND: a log must satisfy *all* non-`None`
/// topic filters simultaneously.
#[derive(Debug, Clone, Default)]
pub struct LogFilter {
    /// Start block number (inclusive).
    pub from_block: BlockNumber,
    /// End block number (inclusive).
    pub to_block: BlockNumber,
    /// Filter by emitting contract address. `None` matches any address.
    pub address: Option<Vec<Address>>,
    /// Topic filters for positions 0–3.
    pub topics: [Option<Vec<B256>>; 4],
}

impl LogFilter {
    /// Returns `true` if the given log matches this filter's address and
    /// topic criteria. Block range is **not** checked.
    pub fn matches_log(&self, log: &Log) -> bool {
        if let Some(ref addrs) = self.address
            && !addrs.contains(&log.address)
        {
            return false;
        }
        self.topics.iter().enumerate().all(|(i, topic_filter)| {
            let Some(acceptable) = topic_filter else { return true };
            log.topics().get(i).is_some_and(|actual| acceptable.contains(actual))
        })
    }
}
