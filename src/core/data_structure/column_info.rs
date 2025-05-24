//! Column type definitions and metadata.
//!
//! Provides types for representing column definitions and data types.

use crate::error::{DBResult, DBSingleError};
use sqlparser::ast;

/// Specific type information for database columns.
#[derive(Debug, Clone, Copy)]
pub enum ColumnTypeSpecific {
    /// Integer type with optional display width
    Int { display_width: Option<u64> },
    /// Variable-length string with maximum length
    Varchar { max_length: u64 },
    /// Generic/unknown type
    Any,
}

/// Converts SQL parser character length to internal representation.
///
/// # Arguments
/// * `length` - SQL parser character length specification
///
/// # Returns
/// Maximum length as u64 (u64::MAX for unlimited)
fn varchar_length_convert(length: Option<ast::CharacterLength>) -> DBResult<u64> {
    match length {
        Some(ast::CharacterLength::IntegerLength { length, .. }) => Ok(length),
        Some(ast::CharacterLength::Max) => Ok(u64::MAX),
        None => Ok(u64::MAX),
    }
}

impl ColumnTypeSpecific {
    /// Creates ColumnTypeSpecific from SQL parser column definition.
    ///
    /// # Arguments
    /// * `def` - SQL parser column definition
    ///
    /// # Returns
    /// ColumnTypeSpecific or error if type is unsupported
    pub fn from_column_def(def: &ast::ColumnDef) -> DBResult<Self> {
        Ok(match def.data_type {
            ast::DataType::Int(width) => ColumnTypeSpecific::Int {
                display_width: width,
            },
            ast::DataType::Integer(width) => ColumnTypeSpecific::Int {
                display_width: width,
            },
            ast::DataType::Varchar(length) => ColumnTypeSpecific::Varchar {
                max_length: varchar_length_convert(length)?,
            },
            _ => Err(DBSingleError::UnsupportedOPError(format!(
                "unsupported type {}",
                def.data_type
            )))?,
        })
    }
}

/// Metadata about a database column.
#[derive(Debug, Clone)]
pub struct ColumnInfo {
    /// Name of the column
    pub name: String,
    /// Whether the column allows NULL values
    pub nullable: bool,
    /// Whether the column has a UNIQUE constraint
    pub unique: bool,
    /// Type-specific information and constraints
    pub type_specific: ColumnTypeSpecific,
}
