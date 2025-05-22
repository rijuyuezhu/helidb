use super::SQLExecutor;
use crate::error::{DBResult, DBSingleError};
use sqlparser::ast;

impl SQLExecutor<'_, '_> {
    pub(super) fn execute_drop_table(&mut self, drop_statement: &ast::Statement) -> DBResult<()> {
        let ast::Statement::Drop {
            object_type, names, ..
        } = drop_statement
        else {
            panic!("Should not reach here");
        };

        if object_type != &ast::ObjectType::Table {
            return Err(DBSingleError::OtherError(
                "only table drop is supported".into(),
            ))?;
        }

        for name in names {
            self.database.drop_table(&name.to_string())?;
        }
        Ok(())
    }
}
