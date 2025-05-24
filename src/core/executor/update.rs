//! UPDATE statement execution.
//!
//! Handles parsing and execution of UPDATE statements.

use super::SQLExecutor;
use crate::core::data_structure::Table;
use crate::error::{DBResult, DBSingleError};
use sqlparser::ast;

impl SQLExecutor {
    /// Parses and validates the target table from UPDATE statement.
    ///
    /// # Arguments
    /// * `table` - Table reference from UPDATE statement
    ///
    /// # Returns
    /// Mutable reference to the table
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
        table.update_rows(&row_selected, assignments)?;

        Ok(())
    }
}
