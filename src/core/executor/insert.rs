//! INSERT statement execution.
//!
//! Handles parsing and execution of INSERT statements, including
//! column reordering and value validation.

use super::SQLExecutor;
use crate::core::data_structure::{Table, Value};
use crate::error::{DBResult, DBSingleError};
use sqlparser::ast;
use std::collections::HashSet;

fn parse_expr(expr: &ast::Expr) -> DBResult<Value> {
    Table::get_dummy().calc_expr_for_row(&[], expr)
}

pub fn parse_raw_row_and_rearrange(
    table: &Table,
    raw_row: &[ast::Expr],
    columns_indicator: &[String],
) -> DBResult<Vec<Value>> {
    let mut insert_values = vec![];
    for expr in raw_row {
        insert_values.push(parse_expr(expr)?);
    }
    if columns_indicator.is_empty() {
        Ok(insert_values)
    } else {
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
        Ok(row)
    }
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
