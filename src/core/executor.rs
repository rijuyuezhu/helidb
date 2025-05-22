mod create_table;
mod delete;
mod drop_table;
mod insert;
mod query;
mod update;

use crate::core::data_structure::Database;
use crate::error::{DBResult, DBSingleError};
use crate::utils::WriteHandle;
use sqlparser::ast;

#[derive(Default)]
pub struct SQLExecutor<'a, 'b> {
    sql_statements: &'a str,
    database: Database,
    output_target: WriteHandle<'b>,
    output_count: usize,
}

impl<'a, 'b> SQLExecutor<'a, 'b> {
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
    pub fn execute_statement(&mut self, statement: &ast::Statement) -> DBResult<()> {
        // println!("{:#?}\n", statement);
        use ast::Statement::*;
        match statement {
            CreateTable(create_table) => self.execute_create_table(create_table),
            Drop { .. } => self.execute_drop_table(statement),
            Insert(insert) => self.execute_insert(insert),
            Query(query) => self.execute_query(query),
            Update { .. } => self.execute_update(statement),
            Delete(delete) => self.execute_delete(delete),
            _ => Err(DBSingleError::UnsupportedOPError("main operator".into()))?,
        }
    }
    fn get_content_from_span(&self, span: sqlparser::tokenizer::Span) -> Option<String> {
        let start = span.start;
        let end = span.end;
        if start.line != end.line || start.column > end.column || start.line == 0 || end.line == 0 {
            return None;
        }
        let line = start.line as usize;
        let sql_line = self.sql_statements.lines().nth(line - 1)?;
        let start_column = start.column as usize - 1;
        let end_column = end.column as usize - 1;
        if sql_line.len() < end_column {
            return None;
        }
        Some(sql_line[start_column..end_column].to_string())
    }
}
