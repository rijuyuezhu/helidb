//! SQL statement parsing using sqlparser.
//!
//! # Example
//! ```
//! use helidb::core::parser::SQLParser;
//!
//! let parser = SQLParser::new();
//! let statements = parser.parse("SELECT * FROM users").unwrap();
//! ```

use crate::error::DBResult;
use sqlparser::ast::Statement;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

/// SQL parser that converts SQL strings into abstract syntax trees.
#[derive(Default, Debug)]
pub struct SQLParser {}

impl SQLParser {
    /// Creates a new SQLParser instance with default configuration.
    pub fn new() -> Self {
        SQLParser::default()
    }

    /// Parses a SQL string into AST statements.
    ///
    /// # Arguments
    /// * `sql` - SQL string to parse (can contain multiple statements)
    ///
    /// # Returns
    /// Vector of parsed `Statement` ASTs or error
    ///
    /// # Errors
    /// Returns `DBError` if parsing fails due to:
    /// - Syntax errors
    /// - Unsupported SQL features
    pub fn parse(&self, sql: &str) -> DBResult<Vec<Statement>> {
        let dialect = GenericDialect {};
        Ok(Parser::parse_sql(&dialect, sql)?)
    }
}
