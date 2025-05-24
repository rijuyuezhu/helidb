//! SELECT query execution.
//!
//! Handles parsing and execution of SELECT queries including:
//! - Projection
//! - Filtering
//! - Ordering
//! - Result output

use super::SQLExecutor;
use crate::core::data_structure::{ColumnInfo, ColumnTypeSpecific, Table, Value};
use crate::error::{DBResult, DBSingleError};
use sqlparser::ast::{self, Spanned};
use std::fmt::Write;

/// Applies ORDER BY clauses to a table.
///
/// # Arguments
/// * `table` - Table to sort
/// * `order_by` - Optional ORDER BY clauses
///
/// # Errors
/// Returns error for:
/// - Unsupported ORDER BY types
/// - Invalid sort expressions
fn execute_order_by(table: &mut Table, order_by: &Option<ast::OrderBy>) -> DBResult<()> {
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

    table.convert_order_by(&keys)?;
    Ok(())
}

impl SQLExecutor<'_, '_> {
    /// Gets the source table for a SELECT query.
    ///
    /// # Arguments
    /// * `select` - Parsed SELECT statement
    ///
    /// # Returns
    /// Reference to source table
    ///
    /// # Errors
    /// Returns error for:
    /// - Multiple tables in FROM
    /// - Unsupported table types
    /// - Table not found
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
    ///
    /// # Returns
    /// New table containing query results
    ///
    /// # Errors
    /// Returns error for:
    /// - Unsupported projection types
    /// - Invalid expressions
    /// - Filter evaluation failures
    fn get_query_table(&self, table: &Table, select: &ast::Select) -> DBResult<Table> {
        type CalcFunc<'a> = Box<dyn Fn(&[Value]) -> DBResult<Value> + 'a>;

        let mut new_column_infos = vec![];
        let mut calc_funcs: Vec<CalcFunc> = vec![];

        for select_item in &select.projection {
            use ast::SelectItem::*;
            match select_item {
                Wildcard(_) => {
                    for (i, column) in table.columns_info.iter().enumerate() {
                        new_column_infos.push(column.clone());
                        calc_funcs.push(Box::new(move |row| Ok(row[i].clone())));
                    }
                }
                UnnamedExpr(expr) => {
                    let column_name = self
                        .get_content_from_span(expr.span())
                        .unwrap_or_else(|| expr.to_string());
                    new_column_infos.push(ColumnInfo {
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

        let mut new_table = Table::new(new_column_infos);

        let rows = table
            .get_row_satisfying_cond(select.selection.as_ref())?
            .into_iter()
            .map(|idx| &table.rows[idx]);

        for row in rows {
            let mut new_row = vec![];
            for calc_func in &calc_funcs {
                new_row.push(calc_func(row)?);
            }
            new_table.insert_row_unchecked(new_row)?;
        }

        Ok(new_table)
    }

    /// Executes a SELECT query.
    ///
    /// # Arguments
    /// * `query` - Parsed query to execute
    ///
    /// # Errors
    /// Returns error for:
    /// - Non-SELECT queries
    /// - Unsupported query features
    /// - Evaluation failures
    pub(super) fn execute_query(&mut self, query: &ast::Query) -> DBResult<()> {
        let ast::SetExpr::Select(select) = query.body.as_ref() else {
            Err(DBSingleError::UnsupportedOPError(
                "only support select".into(),
            ))?
        };

        let table = self.parse_table_from_select(select)?;
        let mut new_table = self.get_query_table(table, select)?;

        execute_order_by(&mut new_table, &query.order_by)?;

        if new_table.get_row_num() > 0 {
            // output the new_table
            if self.output_count > 0 {
                writeln!(self.output_target)?;
            }
            write!(self.output_target, "{}", new_table)?;
            self.output_count += 1;
        }

        Ok(())
    }
}
