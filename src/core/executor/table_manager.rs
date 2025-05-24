pub mod parallel;
pub mod sequential;

use crate::core::data_structure::{ColumnInfo, Table, Value};
use crate::error::DBResult;
pub use parallel::ParallelTableManager;
pub use sequential::SequentialTableManager;
use sqlparser::ast;

pub type CalcFunc<'a> = Box<dyn Fn(&[Value]) -> DBResult<Value> + 'a>;

pub trait TableManager {
    fn insert_rows(
        &self,
        table: &mut Table,
        raw_rows: &[Vec<ast::Expr>],
        columns_indicator: Vec<String>,
    ) -> DBResult<()>;
    fn insert_row_unchecked(&self, table: &mut Table, row: Vec<Value>) -> DBResult<usize>;

    fn insert_row(&self, table: &mut Table, row: Vec<Value>) -> DBResult<usize>;
    /// Deletes rows by their indices.
    ///
    /// # Arguments
    /// * `table` - The table from which to delete rows
    /// * `cond` - Optional condition to filter which rows to delete
    fn delete_rows(&self, table: &mut Table, cond: Option<&ast::Expr>) -> DBResult<()>;

    /// Updates rows by their indices.
    ///
    /// # Arguments
    /// * `table` - The table to update
    /// * `row_idxs` - Indices of rows to update
    /// * `assignments` - List of assignments to apply
    /// * `cond` - Optional condition to filter which rows to update
    fn update_rows(
        &self,
        table: &mut Table,
        assignments: &[ast::Assignment],
        cond: Option<&ast::Expr>,
    ) -> DBResult<()>;

    fn construct_rows_from_calc_func(
        &self,
        table: &Table,
        columns_info: Vec<ColumnInfo>,
        calc_funcs: Vec<CalcFunc>,
        cond: Option<&ast::Expr>,
    ) -> DBResult<Table>;

    fn convert_order_by(&self, table: &mut Table, keys: &[(&ast::Expr, bool)]) -> DBResult<()>;
}
