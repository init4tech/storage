//! Metrics for cold storage operations.
//!
//! Metric names, help strings, and helper recording functions are exposed as
//! `pub(crate)` items. Helpers force the shared [`LazyLock`] describe block on
//! first use, so descriptions are registered exactly once before any value is
//! recorded.

use metrics::{counter, gauge, histogram};
use std::{sync::LazyLock, time::Duration};

/// Gauge: number of read operations currently in flight against the backend.
pub(crate) const READS_IN_FLIGHT: &str = "cold.reads_in_flight";
pub(crate) const READS_IN_FLIGHT_HELP: &str =
    "Number of cold storage read operations currently in flight against the backend";

/// Gauge: number of write operations currently in flight against the backend.
pub(crate) const WRITES_IN_FLIGHT: &str = "cold.writes_in_flight";
pub(crate) const WRITES_IN_FLIGHT_HELP: &str =
    "Number of cold storage write operations currently in flight against the backend";

/// Gauge: number of log streams currently active.
pub(crate) const STREAMS_ACTIVE: &str = "cold.streams_active";
pub(crate) const STREAMS_ACTIVE_HELP: &str = "Number of cold storage log streams currently active";

/// Histogram (microseconds): per-operation duration, labeled by `op`.
pub(crate) const OP_DURATION_US: &str = "cold.op_duration_us";
pub(crate) const OP_DURATION_US_HELP: &str =
    "Duration of cold storage operations in microseconds, labeled by op";

/// Histogram (microseconds): time spent waiting for a concurrency permit,
/// labeled by `sem`.
pub(crate) const PERMIT_WAIT_US: &str = "cold.permit_wait_us";
pub(crate) const PERMIT_WAIT_US_HELP: &str =
    "Time spent waiting for a cold storage concurrency permit in microseconds, labeled by sem";

/// Counter: cold storage operation errors, labeled by `op` and `error`.
pub(crate) const OP_ERRORS_TOTAL: &str = "cold.op_errors_total";
pub(crate) const OP_ERRORS_TOTAL_HELP: &str =
    "Number of cold storage operation errors, labeled by op and error kind";

/// Histogram (milliseconds): lifetime of a log stream from spawn to end.
pub(crate) const STREAM_LIFETIME_MS: &str = "cold.stream_lifetime_ms";
pub(crate) const STREAM_LIFETIME_MS_HELP: &str =
    "Lifetime of cold storage log streams in milliseconds";

static DESCRIBE: LazyLock<()> = LazyLock::new(|| {
    metrics::describe_gauge!(READS_IN_FLIGHT, metrics::Unit::Count, READS_IN_FLIGHT_HELP);
    metrics::describe_gauge!(WRITES_IN_FLIGHT, metrics::Unit::Count, WRITES_IN_FLIGHT_HELP);
    metrics::describe_gauge!(STREAMS_ACTIVE, metrics::Unit::Count, STREAMS_ACTIVE_HELP);
    metrics::describe_histogram!(OP_DURATION_US, metrics::Unit::Microseconds, OP_DURATION_US_HELP);
    metrics::describe_histogram!(PERMIT_WAIT_US, metrics::Unit::Microseconds, PERMIT_WAIT_US_HELP);
    metrics::describe_counter!(OP_ERRORS_TOTAL, metrics::Unit::Count, OP_ERRORS_TOTAL_HELP);
    metrics::describe_histogram!(
        STREAM_LIFETIME_MS,
        metrics::Unit::Milliseconds,
        STREAM_LIFETIME_MS_HELP
    );
});

/// Force the one-time describe block to run.
fn ensure_described() {
    LazyLock::force(&DESCRIBE);
}

/// Resolve the in-flight gauge for the given pool name.
fn in_flight_gauge(which: &'static str) -> metrics::Gauge {
    match which {
        "read" => gauge!(READS_IN_FLIGHT),
        "write" => gauge!(WRITES_IN_FLIGHT),
        "stream" => gauge!(STREAMS_ACTIVE),
        _ => unreachable!("unknown in-flight pool: {which}"),
    }
}

/// Increment the in-flight gauge for a pool. `which` is one of
/// `"read"`, `"write"`, `"stream"`.
pub(crate) fn inc_in_flight(which: &'static str) {
    ensure_described();
    in_flight_gauge(which).increment(1);
}

/// Decrement the in-flight gauge for a pool. `which` is one of
/// `"read"`, `"write"`, `"stream"`.
pub(crate) fn dec_in_flight(which: &'static str) {
    ensure_described();
    in_flight_gauge(which).decrement(1);
}

/// Record an operation duration, in microseconds, labeled by `op`.
pub(crate) fn record_op_duration(op: &'static str, d: Duration) {
    ensure_described();
    histogram!(OP_DURATION_US, "op" => op).record(d.as_micros() as f64);
}

/// Record a permit-wait duration, in microseconds, labeled by `sem`.
/// `sem` is one of `"read"`, `"write"`, `"drain"`, `"stream"`.
pub(crate) fn record_permit_wait(sem: &'static str, d: Duration) {
    ensure_described();
    histogram!(PERMIT_WAIT_US, "sem" => sem).record(d.as_micros() as f64);
}

/// Record an operation error, labeled by `op` and `error` kind.
pub(crate) fn record_op_error(op: &'static str, error: &'static str) {
    ensure_described();
    counter!(OP_ERRORS_TOTAL, "op" => op, "error" => error).increment(1);
}

/// Record a stream lifetime, in milliseconds.
pub(crate) fn record_stream_lifetime(d: Duration) {
    ensure_described();
    histogram!(STREAM_LIFETIME_MS).record(d.as_millis() as f64);
}
