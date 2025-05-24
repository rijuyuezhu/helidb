//! DROP TABLE statement execution.
//!
//! Handles parsing and execution of DROP TABLE statements.

use super::SQLExecutor;
use crate::error::{DBResult, DBSingleError};
use sqlparser::ast;

impl SQLExecutor {
    /// Executes a DROP TABLE statement.
    ///
    /// # Arguments
    /// * `drop_statement` - Parsed DROP statement
    pub(super) fn execute_drop_table(&mut self, drop_statement: &ast::Statement) -> DBResult<()> {
        let ast::Statement::Drop {
            object_type, names, ..
        } = drop_statement
        else {
            // This should never happen, as we have entered into this function
            panic!("Should not reach here");
        };

        if *object_type != ast::ObjectType::Table {
            Err(DBSingleError::OtherError(
                "only table drop is supported".into(),
            ))?;
        }

        for name in names {
            self.database.drop_table(&name.to_string())?;
        }
        Ok(())
    }
}
