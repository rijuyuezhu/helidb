//! INSERT statement execution.
//!
//! Handles parsing and execution of INSERT statements, including
//! column reordering and value validation.

use super::SQLExecutor;
use crate::core::data_structure::{Table, Value};
use crate::error::{DBResult, DBSingleError};
use sqlparser::ast;

/// Parses a single SQL expression into a Value.
///
/// # Arguments
/// * `expr` - SQL expression to parse
///
/// # Returns
/// Parsed Value or error
fn parse_expr(expr: &ast::Expr) -> DBResult<Value> {
    Table::get_dummy().calc_expr_for_row(&[], expr)
}

pub fn parse_raw_row(raw_row: &[ast::Expr]) -> DBResult<Vec<Value>> {
    raw_row.iter().map(parse_expr).collect::<DBResult<Vec<_>>>()
}

impl SQLExecutor {
    /// Executes an INSERT statement.
    ///
    /// # Arguments
    /// * `insert` - Parsed INSERT statement
    pub(super) fn execute_insert(&mut self, insert: &ast::Insert) -> DBResult<()> {
        let table_object = &insert.table;
        let ast::TableObject::TableName(table_name) = table_object else {
            Err(DBSingleError::UnsupportedOPError(
                "only support TableName".into(),
            ))?
        };
        let table_name = table_name.to_string();
        let table = self
            .database
            .get_table_mut(&table_name)
            .ok_or_else(|| DBSingleError::OtherError(format!("table not found: {}", table_name)))?;
        let query = insert
            .source
            .as_ref()
            .ok_or_else(|| DBSingleError::UnsupportedOPError("insert without query".into()))?
            .as_ref();
        let columns_indicator = insert
            .columns
            .iter()
            .map(|c| c.to_string())
            .collect::<Vec<_>>();
        let ast::SetExpr::Values(values) = query.body.as_ref() else {
            Err(DBSingleError::UnsupportedOPError(
                "only support values".into(),
            ))?
        };
        let raw_rows = &values.rows;
        self.table_manager
            .insert_rows(table, raw_rows, columns_indicator)?;
        Ok(())
    }
}
