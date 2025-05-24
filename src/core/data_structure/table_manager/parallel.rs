use super::TableManager;
use crate::core::data_structure::Table;
use crate::error::{DBResult, DBSingleError};
use rayon::prelude::*;
use sqlparser::ast;

pub struct ParallelTableManager;

impl TableManager for ParallelTableManager {
    fn get_row_satisfying_cond(
        &self,
        table: &Table,
        cond: Option<&ast::Expr>,
    ) -> DBResult<Vec<usize>> {
        if cond.is_none() {
            return Ok(table.rows.keys().copied().collect());
        }
        let cond = cond.unwrap();
        table
            .rows
            .par_iter()
            .map(|(&i, row)| -> DBResult<_> {
                if table
                    .calc_expr_for_row(row, cond)?
                    .try_to_bool()?
                    .is_some_and(|v| v)
                {
                    Ok(vec![i])
                } else {
                    Ok(vec![])
                }
            })
            .try_reduce(Vec::new, |mut a, b| {
                a.extend(b);
                Ok(a)
            })
    }

    fn delete_rows(&self, table: &mut Table, row_idxs: &[usize]) -> DBResult<()> {
        for &row_idx in row_idxs {
            if table.rows.remove(&row_idx).is_none() {
                Err(DBSingleError::OtherError(format!(
                    "row index {} not found",
                    row_idx
                )))?;
            }
        }
        Ok(())
    }

    fn update_rows(
        &self,
        table: &mut Table,
        row_idxs: &[usize],
        assignments: &[ast::Assignment],
    ) -> DBResult<()> {
        let table_ptr = table as *mut Table as usize;

        row_idxs
            .par_iter()
            .try_for_each(|&row_idx| -> DBResult<()> {
                // safety here: any self is used to calculate expressions,
                // where only the columns_info are used.
                let any_table = unsafe { &*(table_ptr as *const Table) };
                let table = unsafe { &mut *(table_ptr as *mut Table) };

                let row = table.rows.get_mut(&row_idx).ok_or_else(|| {
                    DBSingleError::OtherError(format!("row index {} not found", row_idx))
                })?;
                let orig_row = row.clone();

                for ast::Assignment {
                    target,
                    value: expr,
                } in assignments
                {
                    let ast::AssignmentTarget::ColumnName(column_name) = target else {
                        Err(DBSingleError::UnsupportedOPError(
                            "only support column name".into(),
                        ))?
                    };
                    let column_name = column_name.to_string();

                    let index = any_table.get_column_index(&column_name).ok_or_else(|| {
                        DBSingleError::OtherError(format!("column not found: {}", column_name))
                    })?;

                    let value = any_table.calc_expr_for_row(&orig_row, expr)?;
                    any_table.check_column_with_value(index, &value, Some(row_idx))?;
                    row[index] = value;
                }
                Ok(())
            })?;
        Ok(())
    }
}
