use crate::error::{DBResult, DBSingleError};
use std::collections::{BTreeMap, HashMap};

use sqlparser::ast::{self, ColumnDef};

#[derive(Debug, Clone)]
pub enum ValueNotNull {
    Int(i32),
    Varchar(String),
}

pub type Value = Option<ValueNotNull>;
#[derive(Debug, Clone, Copy)]
pub enum ColumnTypeSpecific {
    Int { display_width: Option<u64> },
    Varchar { max_length: u64 },
}

impl ColumnTypeSpecific {
    pub fn from_column_def(def: &ColumnDef) -> DBResult<Self> {
        fn varchar_length_convert(length: Option<ast::CharacterLength>) -> DBResult<u64> {
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
            _ => Err(DBSingleError::Other(format!(
                "unsupported type {:?}",
                def.data_type
            )))?,
        })
    }
}

#[derive(Debug, Clone)]
pub struct ColumnInfo {
    pub name: String,
    pub type_specific: ColumnTypeSpecific,
}

#[derive(Debug, Clone)]
pub struct Table {
    /// line number to line content
    pub rows: BTreeMap<usize, Vec<Value>>,

    pub columns: Vec<ColumnInfo>,
    pub column_rmap: HashMap<String, usize>,
}

impl Table {
    pub fn new(columns: Vec<ColumnInfo>) -> Self {
        let column_rmap = columns
            .iter()
            .enumerate()
            .map(|(i, col)| (col.name.clone(), i))
            .collect();
        Table {
            rows: BTreeMap::new(),
            columns,
            column_rmap,
        }
    }

    pub fn get_column_index(&self, column_name: &str) -> Option<usize> {
        self.column_rmap.get(column_name).copied()
    }

    pub fn get_column_info(&self, column_name: &str) -> Option<&ColumnInfo> {
        self.get_column_index(column_name)
            .map(|index| &self.columns[index])
    }
    pub fn get_row(&self, row_number: usize) -> Option<&Vec<Value>> {
        self.rows.get(&row_number)
    }
    pub fn get_row_mut(&mut self, row_number: usize) -> Option<&mut Vec<Value>> {
        self.rows.get_mut(&row_number)
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
            None => Err(DBSingleError::Other(format!(
                "table {} not found",
                table_name
            )))?,
        }
    }
}
