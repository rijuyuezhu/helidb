use super::SQLExecutor;
use crate::error::{DBResult, DBSingleError};
use sqlparser::ast;

impl SQLExecutor<'_, '_> {
    pub(super) fn execute_update(&mut self, update_statement: &ast::Statement) -> DBResult<()> {
        let ast::Statement::Update {
            table,
            assignments,
            selection,
            ..
        } = update_statement
        else {
            panic!("Should not reach here");
        };
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

        let row_selected = table.get_row_by_condition(selection.as_ref())?;
        for row_idx in row_selected {
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
                let Some(col) = table.get_column_index(&column_name) else {
                    Err(DBSingleError::OtherError(format!(
                        "column not found: {}",
                        column_name
                    )))?
                };
                let orig_row = &table.rows[row_idx];
                let value = table.calc_expr_for_row(orig_row, expr)?;
                table.check_column_with_value(col, &value, Some(row_idx))?;
                let orig_row = &mut table.rows[row_idx];
                orig_row[col] = value;
            }
        }

        Ok(())
    }
}
