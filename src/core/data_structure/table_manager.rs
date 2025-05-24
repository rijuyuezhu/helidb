pub mod parallel;
pub mod sequential;

use super::Table;
use crate::error::DBResult;
pub use parallel::ParallelTableManager;
pub use sequential::SequentialTableManager;
use sqlparser::ast;

pub trait TableManager {
    /// Gets indices of rows matching a condition.
    ///
    /// # Arguments
    /// * `table` - The table to search
    /// * `cond` - Optional SQL condition expression
    ///
    /// # Returns
    /// Vector of row indices matching the condition
    ///
    /// # Note
    /// Returns all row indices if cond is None
    fn get_row_satisfying_cond(
        &self,
        table: &Table,
        cond: Option<&ast::Expr>,
    ) -> DBResult<Vec<usize>>;

    /// Deletes rows by their indices.
    ///
    /// # Arguments
    /// * `table` - The table from which to delete rows
    /// * `row_idxs` - Indices of rows to delete
    fn delete_rows(&self, table: &mut Table, row_idxs: &[usize]) -> DBResult<()>;

    /// Updates rows by their indices.
    ///
    /// # Arguments
    /// * `table` - The table to update
    /// * `row_idxs` - Indices of rows to update
    /// * `assignments` - List of assignments to apply
    fn update_rows(
        &self,
        table: &mut Table,
        row_idxs: &[usize],
        assignments: &[ast::Assignment],
    ) -> DBResult<()>;
}
