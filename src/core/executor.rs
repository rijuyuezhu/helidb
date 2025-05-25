//! SQL statement execution and table management.

mod create_table;
mod delete;
mod drop_table;
mod insert;
mod query;
mod update;
mod utils;

pub mod table_manager;

use crate::core::data_structure::Database;
use crate::core::parser::SQLParser;
use crate::core::storage;
use crate::error::join_result;
use crate::error::{DBResult, DBSingleError};
use crate::interface::SQLExecConfig;
use sqlparser::ast;
use std::fmt::Write;
use table_manager::{ParallelTableManager, SequentialTableManager, TableManager};

/// SQLExecutor is responsible for executing SQL statements against a database.
///
/// It handles parsing, execution, and output formatting.
///
/// # Example
/// ```
/// use simple_db::{SQLExecConfig, SQLExecutor};
///
/// let config = SQLExecConfig::new();
/// let mut executor = SQLExecutor::build_from_config(config).unwrap();
///
/// // Execute multiple statements
/// let result = executor.execute_sql(
///     "CREATE TABLE users (id INT, name VARCHAR(255));
///      INSERT INTO users VALUES (1, 'Alice');
///      SELECT * FROM users;"
/// );
///
/// assert_eq!(result.unwrap(), "| id  | name  |\n\
///                              | --- | ----- |\n\
///                              | 1   | Alice |\n")
/// ```
pub struct SQLExecutor {
    /// The database instance to execute SQL statements against.
    database: Database,
    /// Configuration for SQL execution, including storage path and reinitialization options.
    config: SQLExecConfig,
    /// The table manager used for managing tables during execution.
    table_manager: Box<dyn TableManager>,
}

/// State for SQLExecutor to track execution progress and output.
///
/// This struct is used internally to manage the state during SQL execution,
/// including the SQL statements being executed and the accumulated output results.
#[derive(Default)]
struct SQLExecutorState<'a> {
    /// The SQL statements to execute.
    sql_statements: &'a str,
    /// The count of output results produced by the execution.
    output_count: usize,
    /// The buffer to accumulate output results.
    output_buffer: String,
}

impl SQLExecutor {
    /// Creates a new SQLExecutor instance with the provided configuration.
    ///
    /// # Arguments
    /// * `config` - Configuration for SQL execution, including storage path and reinitialization options.
    /// # Returns
    /// The SQLExecutor instance ready for executing SQL statements.
    pub fn build_from_config(config: SQLExecConfig) -> DBResult<Self> {
        let database = if config.reinit {
            Database::new()
        } else {
            config
                .storage_path
                .as_ref()
                .map_or_else(|| Ok(Database::new()), storage::load_database_from_path)?
        };

        let table_manager: Box<dyn TableManager> = if config.parallel {
            Box::new(ParallelTableManager)
        } else {
            Box::new(SequentialTableManager)
        };

        Ok(SQLExecutor {
            database,
            config,
            table_manager,
        })
    }
}

impl SQLExecutor {
    /// Executes a single SQL statement.
    ///
    /// # Arguments
    /// * `statement` - Parsed SQL statement to execute
    /// * `executor_state` - Mutable state to track execution progress and output
    fn execute_statement(
        &mut self,
        statement: &ast::Statement,
        executor_state: &mut SQLExecutorState,
    ) -> DBResult<()> {
        use ast::Statement::*;
        match statement {
            CreateTable(create_table) => self.execute_create_table(create_table),
            Drop { .. } => self.execute_drop_table(statement),
            Insert(insert) => self.execute_insert(insert),
            Query(query) => self.execute_query(query, executor_state),
            Update { .. } => self.execute_update(statement),
            Delete(delete) => self.execute_delete(delete),
            _ => Err(DBSingleError::UnsupportedOPError(format!(
                "statement {:?}",
                statement
            )))?,
        }
    }

    /// Executes a series of SQL statements and accumulates the output.
    ///
    /// # Arguments
    /// * `sql_statements` - A string containing multiple SQL statements to execute.
    ///
    /// # Returns
    /// A result containing the accumulated output of all executed statements.
    pub fn execute_sql(&mut self, sql_statements: &str) -> DBResult<String> {
        let mut execute_state = SQLExecutorState {
            sql_statements,
            output_count: 0,
            output_buffer: String::new(),
        };

        let statements = SQLParser::new().parse(sql_statements)?;

        let mut result = Ok(());
        for statement in statements.iter() {
            result = join_result(
                result,
                self.execute_statement(statement, &mut execute_state),
            );
        }
        if execute_state.output_count == 0 {
            writeln!(
                execute_state.output_buffer,
                "There are no results to be displayed."
            )?;
        }
        result?;
        self.write_back()?;
        Ok(execute_state.output_buffer)
    }

    /// Executes a series of SQL statements and returns a boolean indicating success or failure, along with the accumulated output.
    ///
    /// # Arguments
    /// * `sql_statements` - A string containing multiple SQL statements to execute.
    ///
    /// # Returns
    /// A tuple containing a boolean indicating success or failure, and the accumulated output of all executed statements.
    pub fn execute_sql_combine_outputs(&mut self, sql_statements: &str) -> (bool, String) {
        match self.execute_sql(sql_statements) {
            Ok(output) => (true, output),
            Err(e) => (false, e.to_string()),
        }
    }

    /// Write the current state of the database back to the storage path if write_back is enabled.
    ///
    /// This method is typically called after executing SQL statements to persist changes.
    pub fn write_back(&mut self) -> DBResult<()> {
        if !self.config.write_back {
            return Ok(());
        }
        let Some(path) = &self.config.storage_path else {
            return Ok(());
        };

        let file = std::fs::File::create(path)?;
        storage::write_database_to(file, &self.database)?;
        Ok(())
    }
}
