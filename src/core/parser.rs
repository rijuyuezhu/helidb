//! SQL statement parsing functionality.
//!
//! Wraps the sqlparser crate to provide SQL parsing capabilities
//! for the database system.

use crate::error::DBResult;
use sqlparser::ast::Statement;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

/// SQL parser that converts SQL strings into abstract syntax trees.
#[derive(Default, Debug)]
pub struct SQLParser {}

impl SQLParser {
    /// Creates a new SQLParser instance.
    pub fn new() -> Self {
        SQLParser::default()
    }

    /// Parses a SQL string into AST statements.
    ///
    /// # Arguments
    /// * `sql` - SQL string to parse
    ///
    /// # Returns
    /// Vector of parsed statements or error
    ///
    /// # Example
    /// ```
    /// # use simple_db::core::parser::SQLParser;
    /// #
    /// let parser = SQLParser::new();
    /// let statements = parser.parse("SELECT * FROM users").unwrap();
    /// ```
    pub fn parse(&self, sql: &str) -> DBResult<Vec<Statement>> {
        let dialect = GenericDialect {};
        Ok(Parser::parse_sql(&dialect, sql)?)
    }
}
