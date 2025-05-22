use crate::error::{DBResult, DBSingleError};
use sqlparser::ast::{self, ColumnDef};
use std::collections::HashMap;

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
#[derive(Debug, Clone, Copy)]
pub enum ColumnTypeSpecific {
    Int { display_width: Option<u64> },
    Varchar { max_length: u64 },
}

impl ColumnTypeSpecific {
    pub fn from_column_def(def: &ColumnDef) -> DBResult<Self> {
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

    pub fn get_column_index(&self, column_name: &str) -> Option<usize> {
        self.column_rmap.get(column_name).copied()
    }

    pub fn get_column_info(&self, column_index: usize) -> &ColumnInfo {
        &self.column_info[column_index]
    }
    pub fn insert_row(&mut self, row: Vec<Value>) -> DBResult<usize> {
        let row_number = self.rows.len();
        if row.len() != self.column_info.len() {
            Err(DBSingleError::OtherError(format!(
                "row length {} not match columns num {}",
                row.len(),
                self.column_info.len()
            )))?
        }
        for i in 0..self.column_info.len() {
            if !self.column_info[i].nullable && row[i].is_none() {
                Err(DBSingleError::RequiredError(format!(
                    "Field '{}' doesn't have a default value",
                    self.column_info[i].name
                )))?
            }
        }
        for i in 0..self.column_info.len() {
            if self.column_info[i].unique {
                for j in 0..row_number {
                    if self.rows[j][i] == row[i] {
                        Err(DBSingleError::OtherError(format!(
                            "Duplicate entry '{}' for key 'PRIMARY'",
                            row[i].as_ref().unwrap(),
                        )))?
                    }
                }
            }
        }
        self.rows.push(row);
        Ok(row_number)
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
