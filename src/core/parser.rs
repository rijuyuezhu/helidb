use crate::error::DBResult;

use sqlparser::ast::Statement;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

pub struct SQLParser {}

impl SQLParser {
    pub fn new() -> Self {
        SQLParser {}
    }

    pub fn parse(self, sql: &str) -> DBResult<Vec<Statement>> {
        let dialect = GenericDialect {};
        Ok(Parser::parse_sql(&dialect, sql)?)
    }
}

impl Default for SQLParser {
    fn default() -> Self {
        SQLParser::new()
    }
}
