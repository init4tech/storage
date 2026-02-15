//! Conformance tests for the in-memory cold storage backend.

use signet_cold::{conformance::conformance, mem::MemColdBackend};

#[tokio::test]
async fn mem_backend_conformance() {
    let backend = MemColdBackend::new();
    conformance(backend).await.unwrap();
}
