use super::SQLExecutor;
use crate::error::{DBResult, DBSingleError};
use sqlparser::ast;

impl SQLExecutor {
    pub(super) fn execute_delete(&mut self, delete: &ast::Delete) -> DBResult<()> {
        if delete.tables.len() != 1 {
            Err(DBSingleError::UnsupportedOPError(
                "only support one table".into(),
            ))?;
        }
        let table = &delete.tables[0].to_string();
        let Some(table) = self.database.get_table_mut(table) else {
            Err(DBSingleError::OtherError(format!(
                "table not found: {}",
                table
            )))?
        };
        let row_to_delete = table.get_row_by_condition(delete.selection.as_ref())?;
        table.delete_row(&row_to_delete)?;

        Ok(())
    }
}
