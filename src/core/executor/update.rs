//! UPDATE statement execution.
//!
//! Handles parsing and execution of UPDATE statements.

use super::SQLExecutor;
use crate::core::data_structure::Table;
use crate::error::{DBResult, DBSingleError};
use sqlparser::ast;

impl SQLExecutor<'_, '_> {
    /// Parses and validates the target table from UPDATE statement.
    ///
    /// # Arguments
    /// * `table` - Table reference from UPDATE statement
    ///
    /// # Returns
    /// Mutable reference to the table
    ///
    /// # Errors
    /// Returns error for:
    /// - Unsupported table types
    /// - Table not found
    fn parse_table_in_ast(&mut self, table: &ast::TableWithJoins) -> DBResult<&mut Table> {
        let ast::TableFactor::Table {
            name: ref table_name,
            ..
        } = table.relation
        else {
            Err(DBSingleError::UnsupportedOPError(
                "only support table".into(),
            ))?
        };
        let table_name = table_name.to_string();

        let table = self
            .database
            .get_table_mut(&table_name)
            .ok_or_else(|| DBSingleError::OtherError(format!("table not found: {}", table_name)))?;
        Ok(table)
    }

    /// Executes an UPDATE statement.
    ///
    /// # Arguments
    /// * `update_statement` - Parsed UPDATE statement
    ///
    /// # Errors
    /// Returns error for:
    /// - Invalid table references
    /// - Column not found
    /// - Invalid value assignments
    /// - Constraint violations
    pub(super) fn execute_update(&mut self, update_statement: &ast::Statement) -> DBResult<()> {
        let ast::Statement::Update {
            table,
            assignments,
            selection,
            ..
        } = update_statement
        else {
            // This should never happen, as we have entered into this function
            panic!("Should not reach here");
        };

        let table = self.parse_table_in_ast(table)?;
        let row_selected = table.get_row_satisfying_cond(selection.as_ref())?;

        for row_idx in row_selected {
            let orig_row = table.rows[row_idx].clone();
            for ast::Assignment {
                target,
                value: expr,
            } in assignments
            {
                let ast::AssignmentTarget::ColumnName(column_name) = target else {
                    Err(DBSingleError::UnsupportedOPError(
                        "only support column name".into(),
                    ))?
                };
                let column_name = column_name.to_string();

                let index = table.get_column_index(&column_name).ok_or_else(|| {
                    DBSingleError::OtherError(format!("column not found: {}", column_name))
                })?;

                let value = table.calc_expr_for_row(&orig_row, expr)?;
                table.check_column_with_value(index, &value, Some(row_idx))?;
                table.rows[row_idx][index] = value;
            }
        }

        Ok(())
    }
}
