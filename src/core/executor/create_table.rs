//! CREATE TABLE statement execution.
//!
//! Handles parsing and execution of CREATE TABLE statements.

use super::SQLExecutor;
use crate::core::data_structure::{ColumnInfo, ColumnTypeSpecific};
use crate::error::{DBResult, DBSingleError};
use sqlparser::ast;

/// Extracts column constraints (nullable, unique) from SQL options.
///
/// # Arguments
/// * `opts` - Column option definitions from SQL
///
/// # Returns
/// Tuple of `(nullable, unique)` flags
///
/// # Errors
/// Returns error for unsupported column options
fn get_column_info(opts: &[ast::ColumnOptionDef]) -> DBResult<(bool, bool)> {
    let mut nullable = true;
    let mut unique = false;
    for opt in opts {
        match opt.option {
            ast::ColumnOption::NotNull => nullable = false,
            ast::ColumnOption::Unique {
                is_primary: true, ..
            } => {
                unique = true;
                nullable = false;
            }
            ast::ColumnOption::Unique {
                is_primary: false, ..
            } => unique = true,
            _ => Err(DBSingleError::OtherError(format!(
                "unsupported column option {:?}",
                opt.option
            )))?,
        };
    }
    Ok((nullable, unique))
}

impl SQLExecutor<'_, '_> {
    /// Executes a CREATE TABLE statement.
    ///
    /// # Arguments
    /// * `create_table` - Parsed CREATE TABLE statement
    ///
    /// # Errors
    /// Returns error for:
    /// - Duplicate table names
    /// - Unsupported column types/options
    /// - Invalid column definitions
    pub(super) fn execute_create_table(&mut self, create_table: &ast::CreateTable) -> DBResult<()> {
        let table_name = create_table.name.to_string();

        if self.database.tables.contains_key(&table_name) {
            Err(DBSingleError::OtherError(format!(
                "table name {} already exists",
                table_name
            )))?;
        }

        let mut column_info = vec![];
        for col in &create_table.columns {
            let name = col.name.to_string();
            let type_specific = ColumnTypeSpecific::from_column_def(col)?;
            let (nullable, unique) = get_column_info(&col.options)?;
            column_info.push(ColumnInfo {
                name,
                nullable,
                unique,
                type_specific,
            });
        }

        self.database.create_table(table_name, column_info);
        Ok(())
    }
}
