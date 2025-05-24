//! Public interfaces for executing SQL statements and handling results.

use crate::core::executor::SQLExecutor;
use crate::core::parser::SQLParser;
use crate::core::storage;
use crate::error::{DBResult, join_result};
use crate::utils::WriteHandle;
use std::fmt::Write;

/// Configuration for executing SQL statements and handling output.
///
/// Controls where query results and errors are written.
#[derive(Default)]
pub struct SQLExecConfig<'a> {
    /// Target for normal output (query results, status messages)
    output_target: WriteHandle<'a>,
    /// Target for error output (parse/execution errors)
    err_output_target: WriteHandle<'a>,
    /// Path to the storage file, or None if not using file storage
    storage_path: Option<String>,
}

impl<'a> SQLExecConfig<'a> {
    /// Creates a new SQLExecConfig with default values.
    ///
    /// Both output targets will write to stdout by default.
    pub fn new() -> Self {
        SQLExecConfig::default()
    }

    /// Sets the output target for query results.
    ///
    /// # Arguments
    /// * `output_target` - Where to write query results and status messages
    ///
    /// # Returns
    /// Mutable reference to self for method chaining
    pub fn output_target(&mut self, output_target: WriteHandle<'a>) -> &mut Self {
        self.output_target = output_target;
        self
    }

    /// Sets the error output target.
    ///
    /// # Arguments
    /// * `err_output_target` - Where to write error messages
    ///
    /// # Returns  
    /// Mutable reference to self for method chaining
    pub fn err_output_target(&mut self, err_output_target: WriteHandle<'a>) -> &mut Self {
        self.err_output_target = err_output_target;
        self
    }

    /// Sets the storage path for the database.
    ///
    /// # Arguments
    /// * `storage_path` - Path to the storage file (None if not using file storage)
    ///
    /// # Returns
    /// Mutable reference to self for method chaining
    pub fn storage_path(&mut self, storage_path: Option<String>) -> &mut Self {
        self.storage_path = storage_path;
        self
    }

    /// Executes one or more SQL statements.
    ///
    /// Normal output is written to the configured output target,
    ///
    /// # Arguments
    /// * `sql_statements` - String containing SQL to execute (can be multiple statements)
    ///
    /// # Returns
    /// Result indicating success or failure
    ///
    /// # Examples
    /// ```
    /// # use simple_db::SQLExecConfig;
    /// let mut config = SQLExecConfig::new();
    /// config.execute("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR);").unwrap();
    /// ```
    pub fn execute(&mut self, sql_statements: &str) -> DBResult<()> {
        let statements = {
            let parser = SQLParser::new();
            parser.parse(sql_statements)?
        };

        let mut executor = match &self.storage_path {
            Some(path) => match std::fs::File::open(path) {
                Ok(f) => {
                    let database = storage::load_database_from(f)?;
                    SQLExecutor::from_database(database, sql_statements, self.output_target.clone())
                }
                Err(e) => match e.kind() {
                    std::io::ErrorKind::NotFound => {
                        SQLExecutor::new(sql_statements, self.output_target.clone())
                    }
                    _ => {
                        writeln!(self.err_output_target, "Error opening storage file: {}", e)?;
                        return Err(e.into());
                    }
                },
            },
            None => SQLExecutor::new(sql_statements, self.output_target.clone()),
        };

        let mut result = Ok(());
        for statement in statements.iter() {
            result = join_result(result, executor.execute_statement(statement));
        }
        if executor.get_output_count() == 0 {
            writeln!(self.output_target, "There are no results to be displayed.")?;
        }
        if let Some(path) = &self.storage_path {
            storage::write_database_to(std::fs::File::create(path)?, executor.get_database())?;
        }
        result
    }
    /// Executes SQL statements and returns success/failure as boolean.
    ///
    /// Normal output is written to the configured output target,
    /// and Errors are automatically written to the configured error output.
    ///
    /// # Arguments
    /// * `sql_statements` - String containing SQL to execute
    ///
    /// # Returns
    /// true if execution succeeded, false if any errors occurred
    pub fn execute_sql(&mut self, sql_statements: &str) -> bool {
        if let Err(e) = self.execute(sql_statements) {
            write!(self.err_output_target, "{}", e).unwrap();
            false
        } else {
            true
        }
    }
}
