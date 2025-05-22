use super::SQLExecutor;
use crate::error::{DBResult, DBSingleError};
use sqlparser::ast;

impl SQLExecutor {
    pub(super) fn execute_query(&mut self, _query: &ast::Query) -> DBResult<()> {
        Err(DBSingleError::UnsupportedOPError("query".into()))?
    }
}
