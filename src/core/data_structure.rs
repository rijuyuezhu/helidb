//! Core database structures - Database, Table, and Value types.
//!
//! # Examples
//! ```
//! # use helidb::core::data_structure::{Database, ColumnInfo, ColumnTypeSpecific};
//! #
//! // Create a simple database table
//! let mut db = Database::new();
//! let columns = vec![
//!     ColumnInfo {name: "id".into(), nullable: false, unique: true, type_specific: ColumnTypeSpecific::Int { display_width: None }},
//!     ColumnInfo {name: "name".into(), nullable: true, unique: false, type_specific: ColumnTypeSpecific::Varchar { max_length: 255 }},
//! ];
//!
//! db.create_table("users".into(), columns);
//! assert!(db.get_table("users").is_some());
//! ```

pub mod column_info;
pub mod database;
pub mod table;
pub mod value;

pub use column_info::{ColumnInfo, ColumnTypeSpecific};
pub use database::Database;
pub use table::Table;
pub use value::{Value, ValueNotNull};
