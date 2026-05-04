//! Postgres `statement_timeout` is honoured by `begin_read` / `begin_write`.
//!
//! Gated on the `postgres` feature. Skipped at runtime if
//! `DATABASE_URL` is not set (mirrors the existing `pg_conformance`
//! test convention).

#![cfg(all(feature = "postgres", feature = "test-utils"))]

use signet_cold_sql::{SqlColdBackend, SqlColdError};
use std::time::Duration;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pg_statement_timeout_trips() {
    let Ok(url) = std::env::var("DATABASE_URL") else {
        eprintln!("skipping pg_statement_timeout_trips: DATABASE_URL not set");
        return;
    };

    let backend =
        SqlColdBackend::connect(&url).await.unwrap().with_read_timeout(Duration::from_millis(50));

    // pg_sleep(1s) must trip the 50 ms statement_timeout and surface
    // as the dedicated `Timeout` variant (SQLSTATE 57014), not as a
    // generic `Sqlx` wrap. A future refactor that drops 57014
    // detection should fail this test.
    let err = backend
        .debug_pg_sleep(Duration::from_secs(1))
        .await
        .expect_err("pg_sleep should trip statement_timeout");

    assert!(matches!(err, SqlColdError::Timeout), "expected SqlColdError::Timeout, got: {err}");
}

/// At the handle boundary, the same condition must surface as
/// [`signet_cold::ColdStorageError::DeadlineExceeded`] rather than
/// `Backend(...)`, so MDBX and SQL deadline expiry classify
/// symmetrically and `cold.op_errors_total{error="deadline"}` covers
/// both backends.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pg_statement_timeout_maps_to_deadline_exceeded() {
    let Ok(url) = std::env::var("DATABASE_URL") else {
        eprintln!("skipping pg_statement_timeout_maps_to_deadline_exceeded: DATABASE_URL not set");
        return;
    };

    let backend =
        SqlColdBackend::connect(&url).await.unwrap().with_read_timeout(Duration::from_millis(50));

    let sql_err = backend.debug_pg_sleep(Duration::from_secs(1)).await.unwrap_err();
    let cold_err: signet_cold::ColdStorageError = sql_err.into();
    assert!(
        matches!(cold_err, signet_cold::ColdStorageError::DeadlineExceeded(_)),
        "expected DeadlineExceeded, got: {cold_err}"
    );
}
