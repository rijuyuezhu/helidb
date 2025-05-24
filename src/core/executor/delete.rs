//! DELETE statement execution.
//!
//! Handles parsing and execution of DELETE statements.

use super::SQLExecutor;
use crate::error::{DBResult, DBSingleError};
use sqlparser::ast;

impl SQLExecutor {
    /// Executes a DELETE statement.
    ///
    /// # Arguments
    /// * `delete` - Parsed DELETE statement
    pub(super) fn execute_delete(&mut self, delete: &ast::Delete) -> DBResult<()> {
        let tables = match &delete.from {
            ast::FromTable::WithFromKeyword(tables) => tables,
            ast::FromTable::WithoutKeyword(tables) => tables,
        };
        for table in tables {
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
            let table = self.database.get_table_mut(&table_name).ok_or_else(|| {
                DBSingleError::OtherError(format!("table not found: {}", table_name))
            })?;
            self.table_manager
                .delete_rows(table, delete.selection.as_ref())?;
        }

        Ok(())
    }
}
