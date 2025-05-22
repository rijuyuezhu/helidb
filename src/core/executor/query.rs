use super::SQLExecutor;
use crate::core::data_structure::{ColumnInfo, ColumnTypeSpecific, Table, Value};
use crate::error::{DBResult, DBSingleError};
use sqlparser::ast;
use std::fmt::Write;

impl SQLExecutor<'_> {
    pub(super) fn execute_query(&mut self, query: &ast::Query) -> DBResult<()> {
        let ast::SetExpr::Select(select) = query.body.as_ref() else {
            Err(DBSingleError::UnsupportedOPError(
                "only support select".into(),
            ))?
        };
        if select.from.len() > 1 {
            Err(DBSingleError::UnsupportedOPError(
                "only support zero or one table".into(),
            ))?;
        }
        let table = if select.from.len() == 1 {
            let table = &select.from[0];
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
            let Some(table) = self.database.get_table(&table_name) else {
                Err(DBSingleError::OtherError(format!(
                    "table not found: {}",
                    table_name
                )))?
            };
            table
        } else {
            &Table::new_dummy_for_empty_select()
        };
        // table got

        let mut new_column_infos = vec![];
        type CalcFunc<'a> = Box<dyn Fn(&[Value]) -> DBResult<Value> + 'a>;
        let mut calc_funcs: Vec<CalcFunc> = vec![];

        for select_item in &select.projection {
            use ast::SelectItem::*;
            match select_item {
                Wildcard(_) => {
                    for (i, column) in table.column_info.iter().enumerate() {
                        new_column_infos.push(column.clone());
                        calc_funcs.push(Box::new(move |row| Ok(row[i].clone())));
                    }
                }
                UnnamedExpr(expr) => {
                    let column_name = expr.to_string();
                    new_column_infos.push(ColumnInfo {
                        name: column_name,
                        nullable: false,
                        unique: false,
                        type_specific: ColumnTypeSpecific::Int {
                            display_width: None,
                        },
                    });
                    calc_funcs.push(Box::new(|row| table.calc_expr_for_row(row, expr)));
                }
                _ => Err(DBSingleError::UnsupportedOPError(format!(
                    "Not support select item {:?}",
                    select_item
                )))?,
            }
        }
        let mut new_table = Table::new(new_column_infos);

        let row_selected = table.get_row_by_condition(select.selection.as_ref())?;
        row_selected
            .into_iter()
            .map(|idx| &table.rows[idx])
            .try_for_each(|row| {
                let mut new_row = vec![];
                for calc_func in &calc_funcs {
                    new_row.push(calc_func(row)?);
                }
                new_table.insert_row_unchecked(new_row)?;
                DBResult::Ok(())
            })?;
        if self.output_count > 0 {
            writeln!(self.output_target)?;
        }
        write!(self.output_target, "{}", new_table)?;
        self.output_count += 1;
        Ok(())
    }
}
