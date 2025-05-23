use super::{ColumnInfo, Value, ValueNotNull};
use crate::error::{DBResult, DBSingleError};
use sqlparser::ast;
use std::collections::HashMap;

type TableRow = Vec<Value>;

#[derive(Debug, Clone)]
pub struct Table {
    pub rows: Vec<TableRow>,

    pub columns_info: Vec<ColumnInfo>,
    pub column_rmap: HashMap<String, usize>,
}

impl Table {
    pub fn new(columns_info: Vec<ColumnInfo>) -> Self {
        let column_rmap = columns_info
            .iter()
            .enumerate()
            .map(|(i, col)| (col.name.clone(), i))
            .collect();
        Table {
            rows: Vec::new(),
            columns_info,
            column_rmap,
        }
    }

    pub fn new_dummy_for_empty_select() -> Self {
        Table {
            rows: vec![TableRow::new()],
            columns_info: Vec::new(),
            column_rmap: HashMap::new(),
        }
    }

    pub fn get_column_index(&self, column_name: &str) -> Option<usize> {
        self.column_rmap.get(column_name).copied()
    }

    pub fn get_column_info(&self, column_index: usize) -> &ColumnInfo {
        &self.columns_info[column_index]
    }

    pub fn calc_expr_for_row(&self, row: &[Value], expr: &ast::Expr) -> DBResult<Value> {
        use ast::Expr;
        Ok(match expr {
            Expr::Identifier(name) => {
                if name.quote_style.is_some() {
                    Value::from_varchar(name.value.clone())
                } else {
                    let index = self.get_column_index(&name.value).ok_or_else(|| {
                        DBSingleError::OtherError(format!("column {} not found", name))
                    })?;
                    row[index].clone()
                }
            }

            Expr::Value(ast::ValueWithSpan {
                value: ast::Value::Number(num, ..),
                ..
            }) => {
                let num = num.parse::<i32>().map_err(|_| {
                    DBSingleError::OtherError(format!("failed to parse number: {}", num))
                })?;
                Value::from_int(num)
            }

            Expr::Value(ast::ValueWithSpan {
                value: ast::Value::Null,
                ..
            }) => Value::from_null(),

            Expr::IsFalse(expr) => Value::from_bool(
                self.calc_expr_for_row(row, expr)?
                    .try_to_bool()?
                    .map(|b| !b)
                    .unwrap_or(false),
            ),
            Expr::IsTrue(expr) => Value::from_bool(
                self.calc_expr_for_row(row, expr)?
                    .try_to_bool()?
                    .unwrap_or(false),
            ),
            Expr::IsNotTrue(expr) => Value::from_bool(
                self.calc_expr_for_row(row, expr)?
                    .try_to_bool()?
                    .map(|b| !b)
                    .unwrap_or(true),
            ),
            Expr::IsNotFalse(expr) => Value::from_bool(
                self.calc_expr_for_row(row, expr)?
                    .try_to_bool()?
                    .unwrap_or(true),
            ),
            Expr::IsNull(expr) => Value::from_bool(self.calc_expr_for_row(row, expr)?.is_null()),
            Expr::IsNotNull(expr) => {
                Value::from_bool(!self.calc_expr_for_row(row, expr)?.is_null())
            }
            Expr::BinaryOp { left, op, right } => {
                let left = self.calc_expr_for_row(row, left)?.0;
                let right = self.calc_expr_for_row(row, right)?.0;
                match (left, right) {
                    (Some(ValueNotNull::Int(left)), Some(ValueNotNull::Int(right))) => {
                        use ast::BinaryOperator::*;
                        match op {
                            Plus => Value::from_int(left + right),
                            Minus => Value::from_int(left - right),
                            Multiply => Value::from_int(left * right),
                            Divide => Value::from_int(left / right),
                            Modulo => Value::from_int(left % right),
                            Gt => Value::from_bool(left > right),
                            Lt => Value::from_bool(left < right),
                            GtEq => Value::from_bool(left >= right),
                            LtEq => Value::from_bool(left <= right),
                            Eq => Value::from_bool(left == right),
                            NotEq => Value::from_bool(left != right),
                            And => Value::from_bool(left != 0 && right != 0),
                            Or => Value::from_bool(left != 0 || right != 0),
                            _ => Err(DBSingleError::UnsupportedOPError(format!(
                                "unsupported binary operator {:?}",
                                op
                            )))?,
                        }
                    }
                    (
                        Some(ValueNotNull::Varchar(ref left)),
                        Some(ValueNotNull::Varchar(ref right)),
                    ) => {
                        use ast::BinaryOperator::*;
                        match op {
                            Eq => Value::from_bool(left == right),
                            _ => Err(DBSingleError::UnsupportedOPError(format!(
                                "unsupported binary operator {:?}",
                                op
                            )))?,
                        }
                    }
                    (left, right) => Err(DBSingleError::UnsupportedOPError(format!(
                        "unsupported binary operator {:?} {:?} {:?}",
                        op, left, right
                    )))?,
                }
            }

            _ => Err(DBSingleError::UnsupportedOPError(format!(
                "unsupported expression {:?}",
                expr
            )))?,
        })
    }

    pub fn get_row_by_condition(&self, cond: Option<&ast::Expr>) -> DBResult<Vec<usize>> {
        if cond.is_none() {
            return Ok((0..self.rows.len()).collect());
        }
        let cond = cond.unwrap();

        let mut result = vec![];
        for (i, row) in self.rows.iter().enumerate() {
            if self
                .calc_expr_for_row(row, cond)?
                .try_to_bool()?
                .is_some_and(|v| v)
            {
                result.push(i)
            }
        }
        Ok(result)
    }

    pub fn insert_row_unchecked(&mut self, row: Vec<Value>) -> DBResult<usize> {
        let row_number = self.rows.len();
        self.rows.push(row);
        Ok(row_number)
    }

    pub fn check_column_with_value(
        &self,
        col_idx: usize,
        value: &Value,
        skip_row: Option<usize>,
    ) -> DBResult<()> {
        if !self.columns_info[col_idx].nullable && value.is_null() {
            Err(DBSingleError::RequiredError(format!(
                "Field '{}' doesn't have a default value",
                self.columns_info[col_idx].name
            )))?
        }
        if self.columns_info[col_idx].unique {
            for (i, orig_row) in self.rows.iter().enumerate() {
                if skip_row.is_some_and(|s| s == i) {
                    continue;
                }
                if orig_row[col_idx] == *value {
                    Err(DBSingleError::RequiredError(format!(
                        "Duplicate entry '{}' for key 'PRIMARY'",
                        value.to_string(),
                    )))?
                }
            }
        }
        Ok(())
    }

    pub fn insert_row(&mut self, row: Vec<Value>) -> DBResult<usize> {
        if row.len() != self.columns_info.len() {
            Err(DBSingleError::OtherError(format!(
                "row length {} not match columns num {}",
                row.len(),
                self.columns_info.len()
            )))?
        }
        for (i, elem) in row.iter().enumerate() {
            self.check_column_with_value(i, elem, None)?;
        }
        self.insert_row_unchecked(row)
    }

    pub fn delete_row(&mut self, row_idxs: &[usize]) -> DBResult<()> {
        let row_idxs_to_delete = row_idxs
            .iter()
            .copied()
            .collect::<std::collections::HashSet<_>>();

        let rows = std::mem::take(&mut self.rows);
        self.rows = rows
            .into_iter()
            .enumerate()
            .filter(|(i, _)| !row_idxs_to_delete.contains(i))
            .map(|(_, v)| v)
            .collect();
        Ok(())
    }

    pub fn convert_order_by(&mut self, keys: &[(&ast::Expr, bool)]) -> DBResult<()> {
        // Safety:
        // 1. self.rows borrows self mutably; however, it only changes self.rows;
        // 2. self.calc_expr_for_row borrows self immutably; however, it only reads self.columns_rmap (in self.get_column_index);
        // Hence the operation below is safe; we use tricks to bypass Rust's borrow check.
        let self_ptr = unsafe { &*(self as *const Table) };

        // beforehand check: to avoid panic when sorting
        for &(expr, _) in keys {
            let mut prev_value = None;
            for row in &self.rows {
                let v = self.calc_expr_for_row(row, expr)?;
                if prev_value
                    .as_ref()
                    .is_none_or(|prev: &Value| prev.partial_cmp(&v).is_some())
                {
                    prev_value = Some(v);
                } else {
                    Err(DBSingleError::OtherError(format!(
                        "invalid value type for order by: {:?} {:?}",
                        prev_value, v
                    )))?;
                }
            }
        }

        self.rows.sort_by(|a, b| {
            for &(expr, is_asc) in keys {
                let av = self_ptr.calc_expr_for_row(a, expr).unwrap();
                let bv = self_ptr.calc_expr_for_row(b, expr).unwrap();
                let mut ord = av.partial_cmp(&bv).unwrap();
                if !is_asc {
                    ord = ord.reverse();
                }
                if ord != std::cmp::Ordering::Equal {
                    return ord;
                }
            }
            std::cmp::Ordering::Equal
        });
        Ok(())
    }
}

impl std::fmt::Display for Table {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let col_num = self.columns_info.len();
        let mut max_width = vec![];
        for i in 0..col_num {
            let mut width = std::cmp::max(3, self.columns_info[i].name.len());
            for row in &self.rows {
                width = std::cmp::max(width, row[i].to_string().len());
            }
            max_width.push(width);
        }

        for (col_info, width) in self.columns_info.iter().zip(&max_width) {
            write!(f, "| {:<width$} ", col_info.name, width = width)?;
        }
        writeln!(f, "|")?;

        for width in max_width.iter().copied() {
            write!(
                f,
                "| {:<width$} ",
                std::iter::repeat_n("-", width).collect::<String>(),
                width = width
            )?;
        }
        writeln!(f, "|")?;

        for row in &self.rows {
            for (entry, width) in row.iter().zip(&max_width) {
                write!(f, "| {:<width$} ", entry.to_string(), width = width)?;
            }
            writeln!(f, "|")?;
        }

        Ok(())
    }
}
