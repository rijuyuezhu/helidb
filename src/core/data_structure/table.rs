//! Table structure and operations.
//!
//! Contains the Table type that manages rows and columns of data.

use super::{ColumnInfo, Value, ValueNotNull};
use crate::error::{DBResult, DBSingleError};
use bincode::{Decode, Encode};
use lazy_static::lazy_static;
use sqlparser::ast;
use std::collections::{BTreeMap, HashMap};

/// Represents a database table with rows and columns.
#[derive(Debug, Clone, Decode, Encode)]
pub struct Table {
    /// All rows in the table, with row number increasing
    pub rows: BTreeMap<usize, Vec<Value>>,
    pub row_idx_acc: usize,

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
            columns_info,
            column_rmap,
        }
    }

    /// Gets a static dummy table instance for testing/placeholder purposes.
    pub fn get_dummy() -> &'static Self {
        lazy_static! {
            static ref DUMMY: Table = Table {
                rows: [(0, vec![])].into_iter().collect(),
                row_idx_acc: 1,
                columns_info: vec![],
                column_rmap: HashMap::new(),
            };
        }
        &DUMMY
    }

    /// Gets the number of rows in the table.
    pub fn get_row_num(&self) -> usize {
        self.rows.len()
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
    ///
    /// # Panics
    /// If index is out of bounds
    pub fn get_column_info(&self, column_index: usize) -> &ColumnInfo {
        &self.columns_info[column_index]
    }

    /// Evaluates a SQL expression against a row of values.
    ///
    /// # Arguments
    /// * `row` - Row values to evaluate against
    /// * `expr` - SQL expression to evaluate
    ///
    /// # Returns
    /// Result containing the evaluated Value or error
    pub fn calc_expr_for_row(&self, row: &[Value], expr: &ast::Expr) -> DBResult<Value> {
        use ast::Expr;
        Ok(match expr {
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

    /// Gets indices of rows matching a condition.
    ///
    /// # Arguments
    /// * `cond` - Optional SQL condition expression
    ///
    /// # Returns
    /// Vector of row indices matching the condition
    ///
    /// # Note
    /// Returns all row indices if cond is None
    pub fn get_row_satisfying_cond(&self, cond: Option<&ast::Expr>) -> DBResult<Vec<usize>> {
        if cond.is_none() {
            return Ok(self.rows.keys().copied().collect());
        }
        let cond = cond.unwrap();

        let mut result = vec![];
        for (&i, row) in self.rows.iter() {
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

    /// Inserts a row without validation checks.
    ///
    /// # Arguments
    /// * `row` - Row values to insert
    ///
    /// # Returns
    /// Index of the newly inserted row
    ///
    /// # Safety
    /// Does not validate constraints - caller must ensure validity
    pub fn insert_row_unchecked(&mut self, row: Vec<Value>) -> DBResult<usize> {
        let row_number = self.row_idx_acc;
        self.row_idx_acc += 1;
        self.rows.insert(row_number, row);
        Ok(row_number)
    }

    /// Validates a value against column constraints.
    ///
    /// # Arguments
    /// * `col_idx` - Column index to validate against
    /// * `value` - Value to validate
    /// * `skip_row` - Optional row index to skip during uniqueness check
    ///
    /// # Returns
    /// Ok(()) if valid, Err if constraints violated
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
            for (&i, orig_row) in self.rows.iter() {
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

    /// Inserts a row with full validation.
    ///
    /// # Arguments
    /// * `row` - Row values to insert
    ///
    /// # Returns
    /// Index of the newly inserted row
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

    /// Deletes rows by their indices.
    ///
    /// # Arguments
    /// * `row_idxs` - Indices of rows to delete
    pub fn delete_rows(&mut self, row_idxs: &[usize]) -> DBResult<()> {
        for row_idx in row_idxs {
            if self.rows.remove(row_idx).is_none() {
                Err(DBSingleError::OtherError(format!(
                    "row index {} not found",
                    row_idx
                )))?;
            }
        }
        Ok(())
    }

    /// Updates rows by their indices.
    ///
    /// # Arguments
    /// * `row_idxs` - Indices of rows to update
    /// * `assignments` - List of assignments to apply
    pub fn update_rows(
        &mut self,
        row_idxs: &[usize],
        assignments: &[ast::Assignment],
    ) -> DBResult<()> {
        for &row_idx in row_idxs {
            // safety here: any self is used to calculate expressions,
            // where only the columns_info are used.
            let any_self = unsafe { &*(self as *const Table) };

            let row = self.rows.get_mut(&row_idx).ok_or_else(|| {
                DBSingleError::OtherError(format!("row index {} not found", row_idx))
            })?;
            let orig_row = row.clone();

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

                let index = any_self.get_column_index(&column_name).ok_or_else(|| {
                    DBSingleError::OtherError(format!("column not found: {}", column_name))
                })?;

                let value = any_self.calc_expr_for_row(&orig_row, expr)?;
                any_self.check_column_with_value(index, &value, Some(row_idx))?;
                row[index] = value;
            }
        }
        Ok(())
    }

    /// Sorts table rows according to ORDER BY clauses.
    ///
    /// # Arguments
    /// * `keys` - Pairs of (expression, is_ascending) defining sort order
    pub fn convert_order_by(&mut self, keys: &[(&ast::Expr, bool)]) -> DBResult<()> {
        let mut rows = std::mem::take(&mut self.rows)
            .into_values()
            .collect::<Vec<_>>();

        let mut cached_entries = vec![];

        // beforehand check: to avoid panic when sorting
        for &(expr, _) in keys {
            let mut row_entries = vec![];
            for row in &rows {
                let v = self.calc_expr_for_row(row, expr)?;
                if row_entries
                    .last()
                    .is_some_and(|prev: &Value| prev.partial_cmp(&v).is_none())
                {
                    Err(DBSingleError::OtherError(format!(
                        "invalid value type for order by: {:?}",
                        v
                    )))?;
                }
                row_entries.push(v);
            }
            cached_entries.push(row_entries);
        }
        let row_start = &rows[0] as *const Vec<Value>;

        rows.sort_by(|a, b| {
            let a_idx = unsafe { (a as *const Vec<Value>).offset_from(row_start) } as usize;
            let b_idx = unsafe { (b as *const Vec<Value>).offset_from(row_start) } as usize;
            for (expr_idx, &(_, is_asc)) in keys.iter().enumerate() {
                let av = &cached_entries[expr_idx][a_idx];
                let bv = &cached_entries[expr_idx][b_idx];
                let mut ord = av.partial_cmp(bv).unwrap();
                if !is_asc {
                    ord = ord.reverse();
                }
                if ord != std::cmp::Ordering::Equal {
                    return ord;
                }
            }
            std::cmp::Ordering::Equal
        });

        self.rows = rows.into_iter().enumerate().collect();
        self.row_idx_acc = self.rows.len();

        Ok(())
    }
}

impl std::fmt::Display for Table {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let col_num = self.columns_info.len();
        let mut max_width = vec![];
        for i in 0..col_num {
            let mut width = std::cmp::max(3, self.columns_info[i].name.len());
            for row in self.rows.values() {
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

        for row in self.rows.values() {
            for (entry, width) in row.iter().zip(&max_width) {
                write!(f, "| {:<width$} ", entry.to_string(), width = width)?;
            }
            writeln!(f, "|")?;
        }

        Ok(())
    }
}
