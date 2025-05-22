use super::SQLExecutor;
use crate::error::{DBResult, DBSingleError, join_result};
use sqlparser::ast;

impl SQLExecutor {
    pub(super) fn execute_drop_table(&mut self, drop_statement: &ast::Statement) -> DBResult<()> {
        let ast::Statement::Drop {
            object_type, names, ..
        } = drop_statement
        else {
            panic!()
        };

        if object_type != &ast::ObjectType::Table {
            return Err(DBSingleError::Other("only table drop is supported".into()))?;
        }
        let mut result = Ok(());
        for name in names {
            if let Err(e) = self.database.drop_table(&name.to_string()) {
                result = join_result(result, Err(e));
            }
        }
        result
    }
}
