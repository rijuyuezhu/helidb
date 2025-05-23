use std::collections::HashSet;

use super::SQLExecutor;
use crate::core::data_structure::{Value, ValueNotNull};
use crate::error::{DBResult, DBSingleError};
use sqlparser::ast;

fn insert_parse_expr(expr: &ast::Expr) -> DBResult<Value> {
    use ast::Expr;
    Ok(match expr {
        Expr::Identifier(ident) => Some(ValueNotNull::Varchar(ident.value.clone())).into(),

        Expr::Value(ast::ValueWithSpan {
            value: ast::Value::Number(num, ..),
            ..
        }) => {
            let num = num.parse::<i32>().map_err(|_| {
                DBSingleError::OtherError(format!("failed to parse number: {}", num))
            })?;
            Some(ValueNotNull::Int(num)).into()
        }

        Expr::Value(ast::ValueWithSpan {
            value: ast::Value::Null,
            ..
        }) => None.into(),

        _ => Err(DBSingleError::UnsupportedOPError(format!(
            "unsupported expression: {:?}",
            expr
        )))?,
    })
}

fn insert_parse_values(values: &ast::Values) -> DBResult<Vec<Vec<Value>>> {
    let mut result = vec![];
    for row in values.rows.iter() {
        let mut res_row = vec![];
        for entry in row {
            res_row.push(insert_parse_expr(entry)?);
        }
        result.push(res_row);
    }
    Ok(result)
}

fn insert_parse_query(query: &ast::Query) -> DBResult<Vec<Vec<Value>>> {
    let ast::SetExpr::Values(values) = query.body.as_ref() else {
        Err(DBSingleError::UnsupportedOPError(
            "only support values".into(),
        ))?
    };
    insert_parse_values(values)
}

impl SQLExecutor<'_, '_> {
    pub(super) fn execute_insert(&mut self, insert: &ast::Insert) -> DBResult<()> {
        let table = &insert.table;
        let ast::TableObject::TableName(table_name) = table else {
            Err(DBSingleError::UnsupportedOPError(
                "only support TableName".into(),
            ))?
        };
        let Some(table) = self.database.get_table_mut(&table_name.to_string()) else {
            Err(DBSingleError::OtherError(format!(
                "table not found: {}",
                table_name
            )))?
        };
        let Some(ref query) = insert.source else {
            Err(DBSingleError::UnsupportedOPError(
                "insert without query".into(),
            ))?
        };
        let columns_given = insert
            .columns
            .iter()
            .map(|c| c.to_string())
            .collect::<Vec<_>>();

        let num_insert_col = columns_given.len();

        let rows = insert_parse_query(query)?;
        for mut row in rows {
            let new_row = if columns_given.is_empty() {
                row
            } else {
                if row.len() != num_insert_col {
                    Err(DBSingleError::OtherError(format!(
                        "number of columns given {} does not match number of values {}",
                        num_insert_col,
                        row.len()
                    )))?
                }
                let mut used_index = HashSet::new();
                let mut new_row = vec![None.into(); table.columns_info.len()];
                for i in 0..num_insert_col {
                    let column_name = &columns_given[i];
                    let Some(index) = table.get_column_index(column_name) else {
                        Err(DBSingleError::OtherError(format!(
                            "column {} not found",
                            column_name
                        )))?
                    };
                    if used_index.contains(&index) {
                        Err(DBSingleError::OtherError(format!(
                            "column {} is duplicated",
                            column_name
                        )))?
                    }
                    used_index.insert(index);
                    std::mem::swap(&mut new_row[index], &mut row[i]);
                }
                drop(row);
                new_row
            };
            table.insert_row(new_row)?;
        }
        Ok(())
    }
}
