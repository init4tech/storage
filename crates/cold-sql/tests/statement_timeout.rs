//! Postgres `statement_timeout` is honoured by `begin_read` / `begin_write`.
//!
//! Gated on the `postgres` feature. Skipped at runtime if
//! `DATABASE_URL` is not set (mirrors the existing `pg_conformance`
//! test convention).

#![cfg(all(feature = "postgres", feature = "test-utils"))]

use signet_cold_sql::SqlColdBackend;
use std::time::Duration;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pg_statement_timeout_trips() {
    let Ok(url) = std::env::var("DATABASE_URL") else {
        eprintln!("skipping pg_statement_timeout_trips: DATABASE_URL not set");
        return;
    };

    let backend =
        SqlColdBackend::connect(&url).await.unwrap().with_read_timeout(Duration::from_millis(50));

    // pg_sleep(1s) must trip the 50 ms statement_timeout.
    let err = backend
        .debug_pg_sleep(Duration::from_secs(1))
        .await
        .expect_err("pg_sleep should trip statement_timeout");

    let msg = err.to_string().to_lowercase();
    assert!(
        msg.contains("canceling") || msg.contains("57014") || msg.contains("timeout"),
        "expected statement_timeout error, got: {err}"
    );
}
