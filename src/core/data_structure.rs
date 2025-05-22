use crate::error::{DBResult, DBSingleError};
use sqlparser::ast;
use std::{borrow::Cow, collections::HashMap};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValueNotNull {
    Int(i32),
    Varchar(String),
}

impl std::fmt::Display for ValueNotNull {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ValueNotNull::Int(i) => write!(f, "{}", i),
            ValueNotNull::Varchar(s) => write!(f, "{}", s),
        }
    }
}

pub type Value = Option<ValueNotNull>;

pub fn value_to_string(value: &Value) -> Cow<'_, str> {
    match value {
        Some(ValueNotNull::Int(x)) => x.to_string().into(),
        Some(ValueNotNull::Varchar(s)) => s.into(),
        None => "".into(),
    }
}

#[derive(Debug, Clone, Copy)]
pub enum ColumnTypeSpecific {
    Int { display_width: Option<u64> },
    Varchar { max_length: u64 },
}

impl ColumnTypeSpecific {
    pub fn from_column_def(def: &ast::ColumnDef) -> DBResult<Self> {
        pub fn varchar_length_convert(length: Option<ast::CharacterLength>) -> DBResult<u64> {
            match length {
                Some(ast::CharacterLength::IntegerLength { length, .. }) => Ok(length),
                Some(ast::CharacterLength::Max) => Ok(u64::MAX),
                None => Ok(u64::MAX),
            }
        }

        Ok(match def.data_type {
            ast::DataType::Int(width) => ColumnTypeSpecific::Int {
                display_width: width,
            },
            ast::DataType::Varchar(length) => ColumnTypeSpecific::Varchar {
                max_length: varchar_length_convert(length)?,
            },
            _ => Err(DBSingleError::OtherError(format!(
                "unsupported type {:?}",
                def.data_type
            )))?,
        })
    }
}

#[derive(Debug, Clone)]
pub struct ColumnInfo {
    pub name: String,
    pub nullable: bool,
    pub unique: bool,
    pub type_specific: ColumnTypeSpecific,
}

#[derive(Debug, Clone)]
pub struct Table {
    /// line number to line content
    pub rows: Vec<Vec<Value>>,

    pub column_info: Vec<ColumnInfo>,
    pub column_rmap: HashMap<String, usize>,
}

impl Table {
    pub fn new(column_info: Vec<ColumnInfo>) -> Self {
        let column_rmap = column_info
            .iter()
            .enumerate()
            .map(|(i, col)| (col.name.clone(), i))
            .collect();
        Table {
            rows: Vec::new(),
            column_info,
            column_rmap,
        }
    }

    pub fn new_dummy_for_empty_select() -> Self {
        Table {
            rows: vec![vec![]],
            column_info: Vec::new(),
            column_rmap: HashMap::new(),
        }
    }

    pub fn get_column_index(&self, column_name: &str) -> Option<usize> {
        self.column_rmap.get(column_name).copied()
    }

    pub fn get_column_info(&self, column_index: usize) -> &ColumnInfo {
        &self.column_info[column_index]
    }

    pub fn calc_expr_for_row(&self, row: &[Value], expr: &ast::Expr) -> DBResult<Value> {
        use ast::Expr::*;
        Ok(match expr {
            Identifier(name) => {
                if name.quote_style.is_some() {
                    Some(ValueNotNull::Varchar(name.value.clone()))
                } else {
                    let Some(index) = self.get_column_index(&name.value) else {
                        Err(DBSingleError::OtherError(format!(
                            "column {} not found",
                            name
                        )))?
                    };
                    row[index].clone()
                }
            }
            Value(ast::ValueWithSpan {
                value: ast::Value::Number(num, ..),
                ..
            }) => {
                let num = num.parse::<i32>().map_err(|_| {
                    DBSingleError::OtherError(format!("failed to parse number: {}", num))
                })?;
                Some(ValueNotNull::Int(num))
            }

            Value(ast::ValueWithSpan {
                value: ast::Value::Null,
                ..
            }) => None,
            IsFalse(expr) => {
                let value = self.calc_expr_for_row(row, expr)?;
                match value {
                    Some(ValueNotNull::Int(i)) if i != 0 => Some(ValueNotNull::Int(1)),
                    _ => Some(ValueNotNull::Int(0)),
                }
            }
            IsTrue(expr) => {
                let value = self.calc_expr_for_row(row, expr)?;
                match value {
                    Some(ValueNotNull::Int(i)) if i != 0 => Some(ValueNotNull::Int(0)),
                    _ => Some(ValueNotNull::Int(1)),
                }
            }
            IsNotFalse(expr) => {
                let value = self.calc_expr_for_row(row, expr)?;
                match value {
                    Some(ValueNotNull::Int(i)) if i != 0 => Some(ValueNotNull::Int(0)),
                    _ => Some(ValueNotNull::Int(1)),
                }
            }
            IsNotTrue(expr) => {
                let value = self.calc_expr_for_row(row, expr)?;
                match value {
                    Some(ValueNotNull::Int(i)) if i != 0 => Some(ValueNotNull::Int(1)),
                    _ => Some(ValueNotNull::Int(0)),
                }
            }
            IsNull(expr) => {
                let value = self.calc_expr_for_row(row, expr)?;
                match value {
                    Some(_) => Some(ValueNotNull::Int(0)),
                    None => Some(ValueNotNull::Int(1)),
                }
            }
            IsNotNull(expr) => {
                let value = self.calc_expr_for_row(row, expr)?;
                match value {
                    Some(_) => Some(ValueNotNull::Int(1)),
                    None => Some(ValueNotNull::Int(0)),
                }
            }
            BinaryOp { left, op, right } => {
                let left = self.calc_expr_for_row(row, left)?;
                let right = self.calc_expr_for_row(row, right)?;
                match (left, right) {
                    (Some(ValueNotNull::Int(left)), Some(ValueNotNull::Int(right))) => {
                        use ast::BinaryOperator::*;
                        match op {
                            Plus => Some(ValueNotNull::Int(left + right)),
                            Minus => Some(ValueNotNull::Int(left - right)),
                            Multiply => Some(ValueNotNull::Int(left * right)),
                            Divide => Some(ValueNotNull::Int(left / right)),
                            Modulo => Some(ValueNotNull::Int(left % right)),
                            Gt => Some(ValueNotNull::Int(if left > right { 1 } else { 0 })),
                            Lt => Some(ValueNotNull::Int(if left < right { 1 } else { 0 })),
                            GtEq => Some(ValueNotNull::Int(if left >= right { 1 } else { 0 })),
                            LtEq => Some(ValueNotNull::Int(if left <= right { 1 } else { 0 })),
                            Eq => Some(ValueNotNull::Int(if left == right { 1 } else { 0 })),
                            NotEq => Some(ValueNotNull::Int(if left != right { 1 } else { 0 })),
                            And => Some(ValueNotNull::Int(if left != 0 && right != 0 { 1 } else { 0 })),
                            Or => Some(ValueNotNull::Int(if left != 0 || right != 0 { 1 } else { 0 })),
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
                            Eq => Some(ValueNotNull::Int(if left == right { 1 } else { 0 })),
                            _ => Err(DBSingleError::UnsupportedOPError(format!(
                                "unsupported binary operator {:?} for {:?} and {:?}",
                                op, left, right
                            )))?,
                        }
                    }
                    (left, right) => Err(DBSingleError::UnsupportedOPError(format!(
                        "unsupported binary operator {:?} for {:?} and {:?}",
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
            let value = self.calc_expr_for_row(row, cond)?;
            match value {
                Some(ValueNotNull::Int(v)) => {
                    if v != 0 {
                        result.push(i)
                    }
                }
                _ => Err(DBSingleError::OtherError(format!(
                    "condition value {:?} is not int",
                    value
                )))?,
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
        if !self.column_info[col_idx].nullable && value.is_none() {
            Err(DBSingleError::RequiredError(format!(
                "Field '{}' doesn't have a default value",
                self.column_info[col_idx].name
            )))?
        }
        if self.column_info[col_idx].unique {
            for (i, orig_row) in self.rows.iter().enumerate() {
                if skip_row.is_some_and(|x| x == i) {
                    continue;
                }
                if orig_row[col_idx] == *value {
                    Err(DBSingleError::RequiredError(format!(
                        "Duplicate entry '{}' for key 'PRIMARY'",
                        value.as_ref().unwrap(),
                    )))?
                }
            }
        }
        Ok(())
    }

    pub fn insert_row(&mut self, row: Vec<Value>) -> DBResult<usize> {
        if row.len() != self.column_info.len() {
            Err(DBSingleError::OtherError(format!(
                "row length {} not match columns num {}",
                row.len(),
                self.column_info.len()
            )))?
        }
        for i in 0..self.column_info.len() {
            self.check_column_with_value(i, &row[i], None)?;
        }
        self.insert_row_unchecked(row)
    }

    pub fn delete_row(&mut self, row_idx: &[usize]) -> DBResult<()> {
        let rows = std::mem::take(&mut self.rows);
        self.rows = rows
            .into_iter()
            .enumerate()
            .filter(|(i, _)| !row_idx.contains(i))
            .map(|(_, v)| v)
            .collect();
        Ok(())
    }
}

impl std::fmt::Display for Table {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let col_num = self.column_info.len();
        let mut max_width = vec![];
        for i in 0..col_num {
            let mut width = std::cmp::max(3, self.column_info[i].name.len());
            for row in &self.rows {
                width = std::cmp::max(width, value_to_string(&row[i]).len());
            }
            max_width.push(width);
        }

        for i in 0..col_num {
            write!(
                f,
                "| {:<width$} ",
                self.column_info[i].name,
                width = max_width[i]
            )?;
        }
        writeln!(f, "|")?;
        for i in 0..col_num {
            write!(
                f,
                "| {:<width$} ",
                std::iter::repeat_n("-", max_width[i]).collect::<String>(),
                width = max_width[i]
            )?;
        }
        writeln!(f, "|")?;
        for row in &self.rows {
            for i in 0..col_num {
                write!(
                    f,
                    "| {:<width$} ",
                    value_to_string(&row[i]),
                    width = max_width[i]
                )?;
            }
            writeln!(f, "|")?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Default)]
pub struct Database {
    pub tables: HashMap<String, Table>,
}

impl Database {
    pub fn new() -> Self {
        Database {
            tables: HashMap::new(),
        }
    }
    pub fn create_table(&mut self, table_name: String, column_info: Vec<ColumnInfo>) {
        let table = Table::new(column_info);
        self.tables.insert(table_name, table);
    }

    pub fn drop_table(&mut self, table_name: &str) -> DBResult<()> {
        match self.tables.remove(table_name) {
            Some(_) => Ok(()),
            None => Err(DBSingleError::OtherError(format!(
                "table {} not found",
                table_name
            )))?,
        }
    }

    pub fn get_table(&self, table_name: &str) -> Option<&Table> {
        self.tables.get(table_name)
    }

    pub fn get_table_mut(&mut self, table_name: &str) -> Option<&mut Table> {
        self.tables.get_mut(table_name)
    }
}
