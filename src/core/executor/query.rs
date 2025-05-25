//! SELECT query execution.
//!
//! Handles parsing and execution of SELECT queries including:
//! - Projection
//! - Filtering
//! - Ordering
//! - Result output

use super::{SQLExecutor, SQLExecutorState};
use crate::core::data_structure::{ColumnInfo, ColumnTypeSpecific, Table};
use crate::core::executor::table_manager::CalcFunc;
use crate::error::{DBResult, DBSingleError};
use sqlparser::ast::{self, Spanned};
use std::fmt::Write;

impl SQLExecutor {
    /// Applies ORDER BY clauses to a table.
    ///
    /// # Arguments
    /// * `table` - Table to sort
    /// * `order_by` - Optional ORDER BY clauses
    fn execute_order_by(&self, table: &mut Table, order_by: &Option<ast::OrderBy>) -> DBResult<()> {
        let order_by = match order_by.as_ref().map(|x| &x.kind) {
            Some(x) => x,
            None => return Ok(()),
        };

        let ast::OrderByKind::Expressions(order_by_exprs) = order_by else {
            Err(DBSingleError::UnsupportedOPError(
                "only support order by expressions".into(),
            ))?
        };

        let keys = order_by_exprs
            .iter()
            .map(|order_by_expr| {
                let expr = &order_by_expr.expr;
                let is_asc = order_by_expr.options.asc.unwrap_or(true);
                (expr, is_asc)
            })
            .collect::<Vec<_>>();

        self.table_manager.convert_order_by(table, &keys)?;
        Ok(())
    }
    /// Gets the source table for a SELECT query.
    ///
    /// # Arguments
    /// * `select` - Parsed SELECT statement
    ///
    /// # Returns
    /// Reference to source table
    fn parse_table_from_select(&self, select: &ast::Select) -> DBResult<&Table> {
        match select.from.len() {
            0 => Ok(Table::get_dummy()),
            1 => {
                let table = &select.from[0];
                let ast::TableFactor::Table {
                    name: ref table_name,
                    ..
                } = table.relation
                else {
                    Err(DBSingleError::UnsupportedOPError(
                        "only support table in relation".into(),
                    ))?
                };
                let table_name = table_name.to_string();

                self.database.get_table(&table_name).ok_or_else(|| {
                    DBSingleError::OtherError(format!("table not found: {}", table_name)).into()
                })
            }
            _ => Err(DBSingleError::UnsupportedOPError(
                "only support zero or one table".into(),
            ))?,
        }
    }

    /// Constructs result table from SELECT query.
    ///
    /// # Arguments
    /// * `table` - Source table
    /// * `select` - Parsed SELECT statement
    /// * `executor_state` - Current executor state for evaluation context
    ///
    /// # Returns
    /// New table containing query results
    fn get_query_table(
        &self,
        table: &Table,
        select: &ast::Select,
        executor_state: &SQLExecutorState,
    ) -> DBResult<Table> {
        let mut columns_info = vec![];
        let mut calc_funcs: Vec<CalcFunc> = vec![];

        for select_item in &select.projection {
            use ast::SelectItem::*;
            match select_item {
                Wildcard(_) => {
                    for (i, column) in table.columns_info.iter().enumerate() {
                        columns_info.push(column.clone());
                        calc_funcs.push(Box::new(move |row| Ok(row[i].clone())));
                    }
                }
                UnnamedExpr(expr) => {
                    let column_name = self
                        .get_content_from_span(expr.span(), executor_state)
                        .unwrap_or_else(|| expr.to_string());
                    columns_info.push(ColumnInfo {
                        name: column_name,
                        nullable: true,                         // dummy setting
                        unique: false,                          // dummy setting
                        type_specific: ColumnTypeSpecific::Any, // dummy setting
                    });
                    calc_funcs.push(Box::new(|row| table.calc_expr_for_row(row, expr)));
                }
                _ => Err(DBSingleError::UnsupportedOPError(format!(
                    "Not support select item {:?}",
                    select_item
                )))?,
            }
        }
        let new_table = self.table_manager.construct_table_from_calc_func(
            table,
            columns_info,
            calc_funcs,
            select.selection.as_ref(),
        )?;
        Ok(new_table)
    }

    /// Executes a SELECT query.
    ///
    /// # Arguments
    /// * `query` - Parsed query to execute
    /// * `executor_state` - Current executor state for evaluation context
    pub(super) fn execute_query(
        &mut self,
        query: &ast::Query,
        executor_state: &mut SQLExecutorState,
    ) -> DBResult<()> {
        let ast::SetExpr::Select(select) = query.body.as_ref() else {
            Err(DBSingleError::UnsupportedOPError(
                "only support select".into(),
            ))?
        };

        let table = self.parse_table_from_select(select)?;
        let mut new_table = self.get_query_table(table, select, executor_state)?;
        self.execute_order_by(&mut new_table, &query.order_by)?;

        // output
        if new_table.get_row_num() > 0 {
            // output the new_table
            if executor_state.output_count > 0 {
                writeln!(executor_state.output_buffer)?;
            }
            write!(executor_state.output_buffer, "{}", new_table)?;
            executor_state.output_count += 1;
        }

        Ok(())
    }
}
