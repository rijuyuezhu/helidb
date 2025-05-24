//! SQL statement execution functionality.
//!
//! Contains the SQLExecutor that handles execution of parsed SQL statements
//! against the database.

mod create_table;
mod delete;
mod drop_table;
mod insert;
mod query;
mod update;
mod utils;

use crate::core::data_structure::Database;
use crate::error::{DBResult, DBSingleError};
use crate::utils::WriteHandle;
use sqlparser::ast;

/// Executes SQL statements against a database.
///
/// Manages:
/// - Database state
/// - Statement execution
/// - Result output
#[derive(Default)]
pub struct SQLExecutor<'a, 'b> {
    sql_statements: &'a str,
    database: Database,
    output_target: WriteHandle<'b>,
    output_count: usize,
}

impl<'a, 'b> SQLExecutor<'a, 'b> {
    /// Creates a new SQLExecutor instance.
    ///
    /// # Arguments
    /// * `sql_statements` - SQL statements to execute
    /// * `output_target` - Handle for writing execution results
    pub fn new(sql_statements: &'a str, output_target: WriteHandle<'b>) -> Self {
        SQLExecutor {
            sql_statements,
            database: Database::new(),
            output_target,
            output_count: 0,
        }
    }
}

impl SQLExecutor<'_, '_> {
    /// Executes a single SQL statement.
    ///
    /// # Arguments
    /// * `statement` - Parsed SQL statement to execute
    ///
    /// # Errors
    /// Returns error for:
    /// - Unsupported statement types
    /// - Execution failures
    pub fn execute_statement(&mut self, statement: &ast::Statement) -> DBResult<()> {
        use ast::Statement::*;
        match statement {
            CreateTable(create_table) => self.execute_create_table(create_table),
            Drop { .. } => self.execute_drop_table(statement),
            Insert(insert) => self.execute_insert(insert),
            Query(query) => self.execute_query(query),
            Update { .. } => self.execute_update(statement),
            Delete(delete) => self.execute_delete(delete),
            _ => Err(DBSingleError::UnsupportedOPError(format!(
                "statement {:?}",
                statement
            )))?,
        }
    }
    /// Gets the count of outputted tables from executed Query statements.
    pub fn get_output_count(&self) -> usize {
        self.output_count
    }
}
