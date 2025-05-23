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
    pub fn get_output_count(&self) -> usize {
        self.output_count
    }
}
