//! Database structure and operations.
//!
//! Contains the main Database type that manages all tables.

use super::{ColumnInfo, Table};
use crate::error::{DBResult, DBSingleError};
use bincode::{Decode, Encode};
use std::collections::HashMap;

/// Represents a database containing multiple tables.
#[derive(Debug, Clone, Default, Decode, Encode)]
pub struct Database {
    /// Map of table names to Table instances
    pub tables: HashMap<String, Table>,
}

impl Database {
    /// Creates a new empty Database.
    pub fn new() -> Self {
        Database {
            tables: HashMap::new(),
        }
    }
    /// Creates a new table in the database.
    ///
    /// # Arguments
    /// * `table_name` - Name of the table to create
    /// * `column_info` - Column definitions for the table
    ///
    /// # Panics
    /// If a table with the same name already exists.
    /// Check the existance of the table before creating it.
    pub fn create_table(&mut self, table_name: String, column_info: Vec<ColumnInfo>) {
        let table = Table::new(column_info);
        if self.tables.insert(table_name, table).is_some() {
            panic!(
                "table already exists; should not reach here. Check the existence of the table before creating it"
            );
        }
    }

    /// Removes a table from the database.
    ///
    /// # Arguments
    /// * `table_name` - Name of the table to remove
    pub fn drop_table(&mut self, table_name: &str) -> DBResult<()> {
        match self.tables.remove(table_name) {
            Some(_) => Ok(()),
            None => Err(DBSingleError::OtherError(format!(
                "table {} not found",
                table_name
            )))?,
        }
    }

    /// Gets an immutable reference to a table.
    ///
    /// # Arguments
    /// * `table_name` - Name of the table to retrieve
    ///
    /// # Returns
    /// The table if found, None otherwise
    pub fn get_table(&self, table_name: &str) -> Option<&Table> {
        self.tables.get(table_name)
    }

    /// Gets a mutable reference to a table.
    ///
    /// # Arguments
    /// * `table_name` - Name of the table to retrieve
    ///
    /// # Returns
    /// The table if found, None otherwise
    pub fn get_table_mut(&mut self, table_name: &str) -> Option<&mut Table> {
        self.tables.get_mut(table_name)
    }
}
