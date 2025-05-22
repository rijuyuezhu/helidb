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
pub struct SQLExecutor {
    database: Database,
    output_target: WriteHandle,
    output_count: usize,
}

impl SQLExecutor {
    pub fn new(output_target: WriteHandle) -> Self {
        SQLExecutor {
            database: Database::new(),
            output_target,
            output_count: 0,
        }
    }
}

impl SQLExecutor {
    pub fn execute_statement(&mut self, statement: &ast::Statement) -> DBResult<()> {
        println!("{:#?}\n", statement);
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
}
