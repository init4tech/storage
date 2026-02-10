//! RPC error helpers built on [`ajj`]'s error types.
//!
//! This module provides thin convenience functions over [`ErrorPayload`] and
//! [`ResponsePayload`] for constructing common JSON-RPC error responses.

use ajj::{ErrorPayload, ResponsePayload};
use std::borrow::Cow;

/// Result type for RPC handlers.
pub type RpcResult<T> = ResponsePayload<T, String>;

/// Error code for methods not supported by this server.
const METHOD_NOT_SUPPORTED: i64 = -32004;

/// Error code for invalid parameters.
const INVALID_PARAMS: i64 = -32602;

/// Wrap a successful value in an RPC result.
pub const fn rpc_ok<T>(value: T) -> RpcResult<T> {
    ResponsePayload(Ok(value))
}

/// Create a "method not supported" response (code -32004).
pub fn method_not_supported<T>(method: &str) -> RpcResult<T> {
    ResponsePayload(Err(ErrorPayload {
        code: METHOD_NOT_SUPPORTED,
        message: Cow::Owned(format!("Method '{method}' is not supported")),
        data: None,
    }))
}

/// Create an "invalid params" response (code -32602).
pub fn invalid_params<T>(msg: impl Into<Cow<'static, str>>) -> RpcResult<T> {
    ResponsePayload(Err(ErrorPayload { code: INVALID_PARAMS, message: msg.into(), data: None }))
}

/// Create an internal error response (code -32603).
pub fn internal_err<T>(msg: impl std::fmt::Display) -> RpcResult<T> {
    ResponsePayload::internal_error_message(Cow::Owned(msg.to_string()))
}

/// Create an internal error response with additional data (code -32603).
pub fn internal_err_with_data<T>(
    msg: impl Into<Cow<'static, str>>,
    data: impl Into<String>,
) -> RpcResult<T> {
    ResponsePayload::internal_error_with_message_and_obj(msg.into(), data.into())
}
