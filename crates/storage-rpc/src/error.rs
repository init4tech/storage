//! RPC error types following Ethereum JSON-RPC spec.
//!
//! Error codes follow the JSON-RPC 2.0 specification and Ethereum conventions:
//! - `-32700`: Parse error
//! - `-32600`: Invalid Request
//! - `-32601`: Method not found
//! - `-32602`: Invalid params
//! - `-32603`: Internal error
//! - `-32000` to `-32099`: Server error (reserved for implementation-defined errors)

use serde::{Deserialize, Serialize};

/// Standard JSON-RPC 2.0 error codes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum ErrorCode {
    /// Invalid JSON was received.
    ParseError = -32700,
    /// The JSON sent is not a valid Request object.
    InvalidRequest = -32600,
    /// The method does not exist / is not available.
    MethodNotFound = -32601,
    /// Invalid method parameter(s).
    InvalidParams = -32602,
    /// Internal JSON-RPC error.
    InternalError = -32603,
    /// Method is not supported by this server.
    MethodNotSupported = -32004,
}

impl ErrorCode {
    /// Get the error code as an i32.
    pub const fn code(self) -> i32 {
        self as i32
    }

    /// Get the default message for this error code.
    pub const fn message(self) -> &'static str {
        match self {
            Self::ParseError => "Parse error",
            Self::InvalidRequest => "Invalid Request",
            Self::MethodNotFound => "Method not found",
            Self::InvalidParams => "Invalid params",
            Self::InternalError => "Internal error",
            Self::MethodNotSupported => "Method not supported",
        }
    }
}

/// RPC error type for handler results.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcError {
    /// Error code.
    pub code: i32,
    /// Error message.
    pub message: String,
    /// Optional additional data.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<String>,
}

impl RpcError {
    /// Create a new RPC error.
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code: code.code(),
            message: message.into(),
            data: None,
        }
    }

    /// Create a new RPC error with additional data.
    pub fn with_data(code: ErrorCode, message: impl Into<String>, data: impl Into<String>) -> Self {
        Self {
            code: code.code(),
            message: message.into(),
            data: Some(data.into()),
        }
    }

    /// Create a "method not supported" error.
    pub fn method_not_supported(method: &str) -> Self {
        Self::new(
            ErrorCode::MethodNotSupported,
            format!("Method '{}' is not supported", method),
        )
    }

    /// Create an "invalid params" error.
    pub fn invalid_params(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::InvalidParams, message)
    }

    /// Create an "internal error".
    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::InternalError, message)
    }
}

impl std::fmt::Display for RpcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}] {}", self.code, self.message)
    }
}

impl std::error::Error for RpcError {}

/// Result type for RPC handlers.
pub type RpcResult<T> = Result<T, RpcError>;
