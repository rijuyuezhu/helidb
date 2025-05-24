//! INSERT statement execution.
//!
//! Handles parsing and execution of INSERT statements, including
//! column reordering and value validation.

use super::SQLExecutor;
use crate::core::data_structure::{Table, Value};
use crate::error::{DBResult, DBSingleError};
use sqlparser::ast;
use std::collections::HashSet;

/// Parses a single SQL expression into a Value.
///
/// # Arguments
/// * `expr` - SQL expression to parse
///
/// # Returns
/// Parsed Value or error
///
/// # Errors
/// Returns error for unsupported expression types
fn insert_parse_expr(expr: &ast::Expr) -> DBResult<Value> {
    Table::get_dummy().calc_expr_for_row(&[], expr)
}

/// Parses a VALUES clause into rows of Values.
///
/// # Arguments
/// * `query` - SQL query containing VALUES
///
/// # Returns
/// Vector of rows (each a vector of Values)
///
/// # Errors
/// Returns error for:
/// - Non-VALUES queries
/// - Invalid expressions in VALUES
fn insert_parse_query(query: &ast::Query) -> DBResult<Vec<Vec<Value>>> {
    let ast::SetExpr::Values(values) = query.body.as_ref() else {
        Err(DBSingleError::UnsupportedOPError(
            "only support values".into(),
        ))?
    };
    let mut rows = vec![];
    for row in &values.rows {
        let mut res_row = vec![];
        for entry in row {
            res_row.push(insert_parse_expr(entry)?);
        }
        rows.push(res_row);
    }
    Ok(rows)
}

impl SQLExecutor {
    /// Parses an INSERT statement into its components.
    ///
    /// # Arguments
    /// * `insert` - Parsed INSERT statement
    ///
    /// # Returns
    /// Tuple of:
    /// - Table to insert into
    /// - Query containing values
    /// - Column names specified in INSERT
    ///
    /// # Errors
    /// Returns error for:
    /// - Unsupported table types
    /// - Missing source query
    /// - Table not found
    fn parse_insert<'a, 'b>(
        &'a mut self,
        insert: &'b ast::Insert,
    ) -> DBResult<(&'a mut Table, &'b ast::Query, Vec<String>)> {
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
        Ok((table, query, columns_indicator))
    }

    /// Executes an INSERT statement.
    ///
    /// # Arguments
    /// * `insert` - Parsed INSERT statement
    ///
    /// # Errors
    /// Returns error for:
    /// - Column count/value count mismatch
    /// - Duplicate columns
    /// - Invalid values
    /// - Constraint violations
    pub(super) fn execute_insert(&mut self, insert: &ast::Insert) -> DBResult<()> {
        let (table, query, columns_indicator) = self.parse_insert(insert)?;

        let rows = insert_parse_query(query)?;

        if columns_indicator.is_empty() {
            // on columns_indicator in the INSERT statement
            for row in rows {
                table.insert_row(row)?;
            }
        } else {
            // reorder the values according to the columns_indicator
            for mut insert_values in rows {
                if insert_values.len() != columns_indicator.len() {
                    Err(DBSingleError::OtherError(format!(
                        "number of columns given {} does not match number of values {}",
                        columns_indicator.len(),
                        insert_values.len()
                    )))?
                }
                let mut row = vec![Value::from_null(); table.get_column_num()];
                let mut index_used = HashSet::new();
                for i in 0..columns_indicator.len() {
                    let column_name = &columns_indicator[i];
                    let index = table.get_column_index(column_name).ok_or_else(|| {
                        DBSingleError::OtherError(format!("column {} not found", column_name))
                    })?;

                    if index_used.contains(&index) {
                        Err(DBSingleError::OtherError(format!(
                            "column {} is duplicated",
                            column_name
                        )))?
                    }
                    index_used.insert(index);

                    std::mem::swap(&mut row[index], &mut insert_values[i]);
                }
                table.insert_row(row)?;
            }
        }
        Ok(())
    }
}
