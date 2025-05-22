use super::SQLExecutor;
use crate::error::{DBResult, DBSingleError};
use sqlparser::ast;

impl SQLExecutor<'_, '_> {
    pub(super) fn execute_delete(&mut self, delete: &ast::Delete) -> DBResult<()> {
        let table = match &delete.from {
            ast::FromTable::WithFromKeyword(table) => table,
            ast::FromTable::WithoutKeyword(table) => table,
        };
        if table.len() != 1 {
            Err(DBSingleError::UnsupportedOPError(
                "only support one table".into(),
            ))?
        }
        let table = &table[0];
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
        let Some(table) = self.database.get_table_mut(&table_name) else {
            Err(DBSingleError::OtherError(format!(
                "table not found: {}",
                table_name
            )))?
        };
        let row_to_delete = table.get_row_by_condition(delete.selection.as_ref())?;
        table.delete_row(&row_to_delete)?;

        Ok(())
    }
}
