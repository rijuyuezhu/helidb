//! Core data structures for the database system.
//!
//! Contains definitions for:
//! - Database, table and column metadata
//! - Value types and operations

pub mod column_info;
pub mod database;
pub mod table;
pub mod table_manager;
pub mod value;

pub use column_info::{ColumnInfo, ColumnTypeSpecific};
pub use database::Database;
pub use table::Table;
pub use value::{Value, ValueNotNull};
