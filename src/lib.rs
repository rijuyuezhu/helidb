//! A simple SQL database engine implementation in Rust.
//!
//! This crate provides core database functionality including:
//! - SQL parsing and execution
//! - Data storage and retrieval
//! - Error handling

pub mod core;
pub mod error;
pub mod interface;
pub mod utils;

pub use interface::SQLExecConfig;
pub use utils::WriteHandle;
