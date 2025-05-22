use super::SQLExecutor;
use crate::core::data_structure::{Table, Value, ValueNotNull};
use crate::error::{DBResult, DBSingleError};
use sqlparser::ast;

fn insert_parse_expr(expr: &ast::Expr) -> DBResult<Value> {
    use ast::Expr;
    Ok(match expr {
        Expr::Identifier(ident) => Some(ValueNotNull::Varchar(ident.value.clone())),
        Expr::Value(ast::ValueWithSpan {
            value: ast::Value::Number(num, ..),
            ..
        }) => {
            let num = num
                .parse::<i32>()
                .map_err(|_| DBSingleError::Other(format!("failed to parse number: {}", num)))?;
            Some(ValueNotNull::Int(num))
        }
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

fn check_row(row: Vec<Vec<Value>>, table: &mut Table) -> DBResult<()> {
    // for r in row {
    //     for v in r {
    //         match v {
    //             Value::Int(_) => {}
    //             Value::Varchar(_) => {}
    //             _ => Err(DBSingleError::UnsupportedOPError(
    //                 "unsupported value type".into(),
    //             ))?,
    //         }
    //     }
    // }
    Ok(())
}

impl SQLExecutor {
    pub(super) fn execute_insert(&mut self, insert: &ast::Insert) -> DBResult<()> {
        let table = &insert.table;
        let ast::TableObject::TableName(name) = table else {
            Err(DBSingleError::UnsupportedOPError(
                "only support TableName".into(),
            ))?
        };
        let table = match self.database.get_table_mut(&name.to_string()) {
            Some(table) => table,
            None => Err(DBSingleError::Other(format!("table not found: {}", name)))?,
        };
        let Some(ref query) = insert.source else {
            Err(DBSingleError::UnsupportedOPError(
                "insert without query".into(),
            ))?
        };
        let row = insert_parse_query(query)?;
        check_row(row, table)?;
        Ok(())
    }
}
