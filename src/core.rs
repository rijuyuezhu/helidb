//! Core database engine components.
//!
//! Organized into:
//! - Data structures (`Database`, `Table`, `Value`)
//! - SQL execution pipeline
//! - Storage management

pub mod data_structure;
pub mod executor;
pub mod parser;
pub mod storage;
