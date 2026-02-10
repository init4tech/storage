//! RPC error types built on [`ajj`]'s error types.

use ajj::ResponsePayload;

/// Result type for RPC handlers.
pub type RpcResult<T> = ResponsePayload<T, String>;

/// Error code for methods not supported by this server.
pub(crate) const METHOD_NOT_SUPPORTED: i64 = -32004;
