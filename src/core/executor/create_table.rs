use super::SQLExecutor;
use crate::core::data_structure::{ColumnInfo, ColumnTypeSpecific};
use crate::error::{DBResult, DBSingleError};
use sqlparser::ast;

impl SQLExecutor<'_> {
    pub(super) fn execute_create_table(&mut self, create_table: &ast::CreateTable) -> DBResult<()> {
        let table_name = create_table.name.to_string();
        if self.database.tables.contains_key(&table_name) {
            return Err(DBSingleError::OtherError("table already exists".into()))?;
        }
        let mut column_info = vec![];
        for col in &create_table.columns {
            let name = col.name.to_string();
            let type_specific = ColumnTypeSpecific::from_column_def(col)?;
            let mut nullable = true;
            let mut unique = false;
            for opt in &col.options {
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
                }
            }
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
