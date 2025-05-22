use super::SQLExecutor;
use crate::error::{DBResult, DBSingleError};
use sqlparser::ast;

impl SQLExecutor {
    pub(super) fn execute_update(&mut self, _update_statement: &ast::Statement) -> DBResult<()> {
        Err(DBSingleError::UnsupportedOPError("update".into()))?
    }
}
