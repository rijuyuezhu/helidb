//! TableManager trait and its implementations for managing tables in a database.
//! It provides methods for inserting, deleting, updating rows, constructing new tables,
//! and converting ORDER BY clauses.

pub mod parallel;
pub mod sequential;

use crate::core::data_structure::{ColumnInfo, Table, Value};
use crate::error::DBResult;
pub use parallel::ParallelTableManager;
pub use sequential::SequentialTableManager;
use sqlparser::ast;

pub type CalcFunc<'a> = Box<dyn Fn(&[Value]) -> DBResult<Value> + Send + Sync + 'a>;

pub trait TableManager {
    /// Inserts rows into the table.
    ///
    /// # Arguments
    /// * `table` - The table to insert rows into
    /// * `raw_rows` - Rows to be inserted, each row is a vector of expressions
    /// * `columns_indicator` - List of column names corresponding to the expressions in `raw_rows`
    ///
    /// # Returns
    /// A result indicating success or failure of the operation
    fn insert_rows(
        &self,
        table: &mut Table,
        raw_rows: &[Vec<ast::Expr>],
        columns_indicator: Vec<String>,
    ) -> DBResult<()>;

    /// Deletes rows by their indices.
    ///
    /// # Arguments
    /// * `table` - The table from which to delete rows
    /// * `cond` - Optional condition to filter which rows to delete
    fn delete_rows(&self, table: &mut Table, cond: Option<&ast::Expr>) -> DBResult<()>;

    /// Updates rows by their indices.
    ///
    /// # Arguments
    /// * `table` - The table in which to update rows
    /// * `assignments` - List of assignments indicating which columns to update and their new values
    /// * `cond` - Optional condition to filter which rows to update
    fn update_rows(
        &self,
        table: &mut Table,
        assignments: &[ast::Assignment],
        cond: Option<&ast::Expr>,
    ) -> DBResult<()>;

    /// Constructs a new table based on the provided calculation functions.
    ///
    /// # Arguments
    /// * `table` - The original table to be transformed
    /// * `columns_info` - Information about the columns in the new table
    /// * `calc_funcs` - Functions to calculate values for the new table's columns
    /// * `cond` - Optional condition to filter which rows to include in the new table
    ///
    /// # Returns
    /// A result containing the newly constructed table or an error if the operation fails
    fn construct_table_from_calc_func(
        &self,
        table: &Table,
        columns_info: Vec<ColumnInfo>,
        calc_funcs: Vec<CalcFunc>,
        cond: Option<&ast::Expr>,
    ) -> DBResult<Table>;

    /// Converts the ORDER BY clause into a format suitable for the table manager.
    ///
    /// # Arguments
    /// * `table` - The table on which to apply the ORDER BY clause
    /// * `keys` - A list of expressions and their sort order (ascending/descending)
    fn convert_order_by(&self, table: &mut Table, keys: &[(&ast::Expr, bool)]) -> DBResult<()>;
}
