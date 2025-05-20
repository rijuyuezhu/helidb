use std::collections::{BTreeMap, HashMap};

#[derive(Debug, Clone)]
pub enum ValueNotNull {
    Int(i32),
    VarChar(String),
}

pub type Value = Option<ValueNotNull>;
#[derive(Debug, Clone, Copy)]
pub enum ColumnTypeSpecific {
    Int { display_width: usize },
    VarChar { max_length: usize },
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
}
