//! Cold storage task and handles.
//!
//! This module provides the task-based architecture for cold storage:
//!
//! - [`ColdStorageTask`] processes requests from channels
//! - [`ColdStorageHandle`] provides full read/write access
//! - [`ColdStorageReadHandle`] provides read-only access

mod handle;
pub use handle::{ColdStorageHandle, ColdStorageReadHandle};

mod runner;
pub use runner::ColdStorageTask;
