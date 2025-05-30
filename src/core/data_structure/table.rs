//! Table structure and operations.
//!
//! Contains the Table type that manages rows and columns of data.

use super::{ColumnInfo, Value, ValueNotNull};
use crate::error::{DBResult, DBSingleError};
use bincode::{Decode, Encode};
use lazy_static::lazy_static;
use sqlparser::ast;
use std::collections::{BTreeMap, HashMap, HashSet};

/// Represents a database table with rows and columns.
#[derive(Debug, Clone, Decode, Encode)]
pub struct Table {
    /// All rows in the table, with row number increasing. None indicates a deleted row.
    pub rows: BTreeMap<usize, Option<Vec<Value>>>,
    /// Accumulator for the next row index to be used.
    pub row_idx_acc: usize,
    /// Total number of rows currently in the table.
    pub row_num: usize,
    /// Set of unique values for each column, used for indexing and constraints.
    pub columns_values: Vec<HashSet<Value>>,
    /// Metadata about each column
    pub columns_info: Vec<ColumnInfo>,
    /// Mapping from column names to their indices
    pub column_rmap: HashMap<String, usize>,
}

impl Table {
    /// Creates a new empty table with the given column definitions.
    ///
    /// # Arguments
    /// * `columns_info` - Column metadata definitions
    pub fn new(columns_info: Vec<ColumnInfo>) -> Self {
        let column_rmap = columns_info
            .iter()
            .enumerate()
            .map(|(i, col)| (col.name.clone(), i))
            .collect();
        Table {
            rows: BTreeMap::new(),
            row_idx_acc: 0,
            row_num: 0,
            columns_values: vec![HashSet::new(); columns_info.len()],
            columns_info,
            column_rmap,
        }
    }

    /// Gets a static dummy table instance (one line) for testing/placeholder purposes.
    pub fn get_dummy() -> &'static Self {
        lazy_static! {
            static ref DUMMY: Table = Table {
                rows: [(0, Some(vec![]))].into_iter().collect(),
                row_idx_acc: 1,
                row_num: 1,
                columns_values: vec![],
                columns_info: vec![],
                column_rmap: HashMap::new(),
            };
        }
        &DUMMY
    }

    /// Gets the number of rows in the table.
    pub fn get_row_num(&self) -> usize {
        self.row_num
    }

    /// Gets the number of columns in the table.
    pub fn get_column_num(&self) -> usize {
        self.columns_info.len()
    }

    /// Gets the index of a column by name.
    ///
    /// # Arguments
    /// * `column_name` - Name of the column to look up
    ///
    /// # Returns
    /// The index of the column if found, None otherwise
    pub fn get_column_index(&self, column_name: &str) -> Option<usize> {
        self.column_rmap.get(column_name).copied()
    }

    /// Gets column metadata by index.
    ///
    /// # Arguments
    /// * `column_index` - Index of the column
    pub fn get_column_info(&self, column_index: usize) -> &ColumnInfo {
        &self.columns_info[column_index]
    }

    /// Evaluates a SQL expression against a row of values.
    /// In fact only `self.columns_rmap` is used to determine the column index,
    ///
    /// # Arguments
    /// * `row` - Row values to evaluate against
    /// * `expr` - SQL expression to evaluate
    ///
    /// # Returns
    /// The evaluated [`Value`].
    pub fn calc_expr_for_row(&self, row: &[Value], expr: &ast::Expr) -> DBResult<Value> {
        use ast::Expr;
        Ok(match expr {
            Expr::Nested(expr) => self.calc_expr_for_row(row, expr)?,
            Expr::Identifier(name) => {
                if name.quote_style.is_some() {
                    Value::from_varchar(name.value.clone())
                } else {
                    match self.get_column_index(&name.value) {
                        Some(index) => row[index].clone(),
                        None => Value::from_varchar(name.value.clone()),
                    }
                }
            }

            Expr::Value(val) => match &val.value {
                ast::Value::Number(num, ..) => {
                    Value::from_int(num.parse::<i32>().map_err(|_| {
                        DBSingleError::OtherError(format!("invalid number {}", num))
                    })?)
                }
                ast::Value::Boolean(b) => Value::from_bool(*b),
                ast::Value::Null => Value::from_null(),
                ast::Value::SingleQuotedString(s) => Value::from_varchar(s.clone()),
                ast::Value::DoubleQuotedString(s) => Value::from_varchar(s.clone()),
                _ => Err(DBSingleError::UnsupportedOPError(format!(
                    "unsupported value type {:?}",
                    val
                )))?,
            },

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

    /// Checks if a row satisfies a given condition (SQL expression).
    /// In fact only `self.columns_rmap` is used to determine the column index.
    ///
    /// # Arguments
    /// * `row` - Row values to check against the condition
    /// * `cond` - Optional SQL expression to evaluate as the condition
    ///
    /// # Returns
    /// True if the row satisfies the condition, false otherwise.
    ///
    /// If `cond` is None, always returns true.
    pub fn is_row_satisfy_cond(&self, row: &[Value], cond: Option<&ast::Expr>) -> DBResult<bool> {
        Ok(match cond {
            Some(expr) => self
                .calc_expr_for_row(row, expr)?
                .try_to_bool()?
                .unwrap_or(false),
            None => true,
        })
    }

    /// Iterates over existing rows (non-deleted).
    ///
    /// # Returns
    /// An iterator over rows that are not None.
    pub fn existed_rows(&self) -> impl Iterator<Item = &Vec<Value>> {
        self.rows.values().filter_map(|r| r.as_ref())
    }

    /// Iterates over existing rows (non-deleted) with mutable access.
    ///
    /// # Returns
    /// An iterator over mutable references to rows that are not None.
    pub fn existed_rows_mut(&mut self) -> impl Iterator<Item = &mut Vec<Value>> {
        self.rows.values_mut().filter_map(|r| r.as_mut())
    }

    /// Iterates over existing indexed rows (non-deleted) with their indices.
    ///
    /// # Returns
    /// An iterator over tuples of `(index, &row)` for non-deleted rows.
    pub fn existed_indexed_rows(&self) -> impl Iterator<Item = (usize, &Vec<Value>)> {
        self.rows
            .iter()
            .filter_map(|(idx, r)| r.as_ref().map(|v| (*idx, v)))
    }

    /// Iterates over existing indexed rows (non-deleted) with mutable access to the rows.
    ///
    /// # Returns
    /// An iterator over tuples of `(index, &mut row)` for non-deleted rows.
    pub fn existed_indexed_rows_mut(&mut self) -> impl Iterator<Item = (usize, &mut Vec<Value>)> {
        self.rows
            .iter_mut()
            .filter_map(|(idx, r)| r.as_mut().map(|v| (*idx, v)))
    }
}

impl std::fmt::Display for Table {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let col_num = self.columns_info.len();
        let mut max_width = vec![];
        for i in 0..col_num {
            let mut width = std::cmp::max(3, self.columns_info[i].name.len());
            for row in self.rows.values().filter_map(|r| r.as_ref()) {
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

        for row in self.rows.values().filter_map(|r| r.as_ref()) {
            for (entry, width) in row.iter().zip(&max_width) {
                write!(f, "| {:<width$} ", entry.to_string(), width = width)?;
            }
            writeln!(f, "|")?;
        }

        Ok(())
    }
}
