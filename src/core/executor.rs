mod create_table;
mod drop_table;
mod insert;
mod query;
mod update;
mod delete;

use crate::core::data_structure::Database;
use crate::error::{DBError, DBResult, DBSingleError};
use crate::utils::WriteHandle;
use sqlparser::ast;

#[derive(Default)]
pub struct SQLExecutor {
    database: Database,
    output_target: WriteHandle,
}

impl SQLExecutor {
    pub fn new(output_target: WriteHandle) -> Self {
        SQLExecutor {
            database: Database::new(),
            output_target,
        }
    }
}

impl SQLExecutor {
    pub fn execute_statement(&mut self, statement: &ast::Statement) -> DBResult<()> {
        println!("{:#?}\n", statement);
        match statement {
            ast::Statement::CreateTable(create_table) => self.execute_create_table(create_table),
            drop_statement @ ast::Statement::Drop { .. } => self.execute_drop_table(drop_statement),
            ast::Statement::Insert(insert) => self.execute_insert(insert),
            ast::Statement::Query(query) => self.execute_query(query),
            update_statement @ ast::Statement::Update { .. } => {
                self.execute_update(update_statement)
            }
            ast::Statement::Delete(delete) => self.execute_delete(delete),
            _ => Err(DBSingleError::UnsupportedOPError("main operator".into()))?,
        }
    }

    pub fn execute_statements<'b, I>(&mut self, statements: I) -> Vec<DBError>
    where
        I: IntoIterator<Item = &'b ast::Statement>,
    {
        statements
            .into_iter()
            .flat_map(|statement| self.execute_statement(statement).err())
            .collect()
    }
}
