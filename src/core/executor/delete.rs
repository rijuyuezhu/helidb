use super::SQLExecutor;
use crate::error::{DBResult, DBSingleError};
use sqlparser::ast;

impl SQLExecutor {
    pub(super) fn execute_delete(&mut self, _delete: &ast::Delete) -> DBResult<()> {
        Err(DBSingleError::UnsupportedOPError("delete".into()))?
    }
}
