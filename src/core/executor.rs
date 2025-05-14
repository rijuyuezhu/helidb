use crate::utils::{DBError, DBResult};
use sqlparser::ast::Statement;

pub struct SQLExecutor {}

impl SQLExecutor {
    pub fn new() -> Self {
        SQLExecutor {}
    }

    pub fn execute_statement(&mut self, statement: &Statement) -> DBResult<()> {
        Err(DBError::Other(format!("{:?}", statement)))
        // Ok(())
    }

    pub fn execute_statements<'a, I>(&mut self, statements: I) -> Vec<DBError>
    where
        I: IntoIterator<Item = &'a Statement>,
    {
        statements
            .into_iter()
            .flat_map(|statement| self.execute_statement(statement).err())
            .collect()
    }
}
impl Default for SQLExecutor {
    fn default() -> Self {
        SQLExecutor::new()
    }
}
