use super::SQLExecutor;
use sqlparser::ast;
use crate::error::{DBResult, DBSingleError, join_result};
use crate::core::data_structure::{ColumnTypeSpecific, ColumnInfo};

impl SQLExecutor {
    pub(super) fn execute_create_table(&mut self, create_table: &ast::CreateTable) -> DBResult<()> {
        let table_name = create_table.name.to_string();
        if self.database.tables.contains_key(&table_name) {
            return Err(DBSingleError::Other("table already exists".into()))?;
        }
        let mut column_info = vec![];
        let mut result = Ok(());
        for col in &create_table.columns {
            let name = col.name.to_string();
            let type_specific = match ColumnTypeSpecific::from_column_def(col) {
                Ok(type_specific) => type_specific,
                Err(e) => {
                    result = join_result(result, Err(e));
                    continue;
                }
            };
            column_info.push(ColumnInfo {
                name,
                type_specific,
            });
        }
        self.database.create_table(table_name, column_info);
        result
    }
}
