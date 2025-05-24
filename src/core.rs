//! Core database functionality including parsing, execution and data structures.
//!
//! This module contains:
//! - Data structures for tables, columns and values
//! - SQL statement parsing
//! - Query execution logic
//! - Storage management for persistent data

pub mod data_structure;
pub mod executor;
pub mod parser;
pub mod storage;
