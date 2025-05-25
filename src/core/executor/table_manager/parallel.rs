use super::TableManager;
use crate::core::data_structure::{ColumnInfo, Table, Value};
use crate::error::{DBResult, DBSingleError};
use rayon::prelude::*;
use sqlparser::ast;
use std::collections::HashSet;
use std::sync::Mutex;

/// A parallel implementation of the `TableManager` trait.
/// This manager uses Rayon for parallel processing of table operations.
pub struct ParallelTableManager;

impl ParallelTableManager {
    fn update_column_values(
        &self,
        column_info: &ColumnInfo,
        column_values: &Mutex<&mut HashSet<Value>>,
        value_to_delete: Option<&Value>,
        value_to_add: Option<&Value>,
    ) -> DBResult<()> {
        let mut column_values = column_values.lock().unwrap();

        if value_to_add.is_none() {
            if let Some(value_to_delete) = value_to_delete {
                column_values.remove(value_to_delete);
            }
            return Ok(());
        }

        let value_to_add = value_to_add.unwrap();

        // First check nullable
        if !column_info.nullable && value_to_add.is_null() {
            Err(DBSingleError::RequiredError(format!(
                "Field '{}' doesn't have a default value",
                column_info.name
            )))?
        }

        // then check the uniqueness
        if column_info.unique {
            let is_duplicate;
            if value_to_delete
                .as_ref()
                .is_some_and(|&value_to_delete| *value_to_delete == *value_to_add)
            {
                is_duplicate = false;
            } else {
                if column_values.contains(value_to_add) {
                    is_duplicate = true;
                } else {
                    column_values.insert(value_to_add.clone());
                    is_duplicate = false;
                }
                if !is_duplicate {
                    if let Some(value_to_delete) = value_to_delete {
                        column_values.remove(value_to_delete);
                    }
                }
            }
            if is_duplicate {
                Err(DBSingleError::RequiredError(format!(
                    "Duplicate entry '{}' for key 'PRIMARY'",
                    value_to_add.to_string(),
                )))?
            }
        }
        Ok(())
    }
}

fn get_mutexed_columns_values(
    columns_values: &mut [HashSet<Value>],
) -> Vec<Mutex<&mut HashSet<Value>>> {
    columns_values.iter_mut().map(Mutex::new).collect()
}

impl TableManager for ParallelTableManager {
    fn insert_rows(
        &self,
        table: &mut Table,
        raw_rows: &[Vec<ast::Expr>],
        columns_indicator: Vec<String>,
    ) -> DBResult<()> {
        let base_row_idx = table.row_idx_acc;
        table.row_idx_acc += raw_rows.len();
        table.row_num += raw_rows.len();

        let table_confine_header = unsafe { &*(table as *const Table) };
        let column_values = get_mutexed_columns_values(&mut table.columns_values);
        let insert_rows = raw_rows
            .par_iter()
            .enumerate()
            .map(|(local_idx, raw_row)| -> DBResult<_> {
                let row_idx = base_row_idx + local_idx;
                let row = crate::core::executor::insert::parse_raw_row_and_rearrange(
                    table_confine_header,
                    raw_row,
                    &columns_indicator,
                )?;
                if row.len() != table.columns_info.len() {
                    Err(DBSingleError::OtherError(format!(
                        "row length {} not match columns num {}",
                        row.len(),
                        table.columns_info.len()
                    )))?
                }
                for (col_idx, value) in row.iter().enumerate() {
                    self.update_column_values(
                        &table.columns_info[col_idx],
                        &column_values[col_idx],
                        None,
                        Some(value),
                    )?;
                }
                Ok((row_idx, Some(row)))
            })
            .collect::<DBResult<Vec<_>>>()?;
        table.rows.par_extend(insert_rows.into_par_iter());
        Ok(())
    }

    fn delete_rows(&self, table: &mut Table, cond: Option<&ast::Expr>) -> DBResult<()> {
        let table_confine_header = unsafe { &*(table as *const Table) };
        let column_values = get_mutexed_columns_values(&mut table.columns_values);
        let deleted_num = table
            .rows
            .par_iter_mut()
            .map(|(_, opt_row)| -> DBResult<usize> {
                if opt_row.is_none() {
                    return Ok(0);
                }
                let row = opt_row.as_mut().unwrap();
                if !table_confine_header.is_row_satisfy_cond(row, cond)? {
                    return Ok(0);
                }
                for (col_idx, value) in row.iter().enumerate() {
                    self.update_column_values(
                        &table.columns_info[col_idx],
                        &column_values[col_idx],
                        Some(value),
                        None,
                    )?;
                }
                *opt_row = None;
                Ok(1)
            })
            .try_reduce(|| 0, |acc, res| Ok(acc + res))?;
        table.row_num -= deleted_num;
        Ok(())
    }

    fn update_rows(
        &self,
        table: &mut Table,
        assignments: &[ast::Assignment],
        cond: Option<&ast::Expr>,
    ) -> DBResult<()> {
        let table_confine_header = unsafe { &*(table as *const Table) };
        let column_values = get_mutexed_columns_values(&mut table.columns_values);

        table
            .rows
            .par_iter_mut()
            .try_for_each(|(_, opt_row)| -> DBResult<()> {
                if opt_row.is_none() {
                    return Ok(());
                }
                let row = opt_row.as_mut().unwrap();
                if !table_confine_header.is_row_satisfy_cond(row, cond)? {
                    return Ok(());
                }

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

                    let col_idx = table_confine_header
                        .get_column_index(&column_name)
                        .ok_or_else(|| {
                            DBSingleError::OtherError(format!("column not found: {}", column_name))
                        })?;

                    let value = table_confine_header.calc_expr_for_row(&orig_row, expr)?;
                    self.update_column_values(
                        &table_confine_header.columns_info[col_idx],
                        &column_values[col_idx],
                        Some(&row[col_idx]),
                        Some(&value),
                    )?;
                    row[col_idx] = value;
                }
                Ok(())
            })
    }

    fn construct_table_from_calc_func(
        &self,
        table: &Table,
        columns_info: Vec<ColumnInfo>,
        calc_funcs: Vec<super::CalcFunc>,
        cond: Option<&ast::Expr>,
    ) -> DBResult<Table> {
        let mut new_table = Table::new(columns_info);
        let insert_rows = table
            .rows
            .par_iter()
            .map(|(_, opt_row)| -> DBResult<_> {
                if opt_row.is_none() {
                    return Ok(None);
                }
                let row = opt_row.as_ref().unwrap();
                if !table.is_row_satisfy_cond(row, cond)? {
                    return Ok(None);
                }
                let mut new_row = vec![];
                for calc_func in &calc_funcs {
                    new_row.push(calc_func(row)?);
                }
                Ok(Some(new_row))
            })
            .collect::<DBResult<Vec<_>>>()?;
        new_table.rows.par_extend(
            insert_rows
                .into_par_iter()
                .enumerate()
                .filter(|(_, x)| x.is_some()),
        );
        new_table.row_idx_acc = new_table.rows.len();
        new_table.row_num = new_table.rows.len();
        Ok(new_table)
    }

    fn convert_order_by(&self, table: &mut Table, keys: &[(&ast::Expr, bool)]) -> DBResult<()> {
        let mut rows = std::mem::take(&mut table.rows)
            .into_values()
            .flatten()
            .collect::<Vec<_>>();

        let mut cached_entries = vec![];

        // beforehand check: to avoid panic when sorting
        for &(expr, _) in keys {
            let row_entries = rows
                .par_iter()
                .map(|row| table.calc_expr_for_row(row, expr))
                .collect::<DBResult<Vec<_>>>()?;
            cached_entries.push(row_entries);
        }

        for row_entries in &cached_entries {
            let Some(first) = row_entries.first() else {
                continue;
            };
            let not_match_count = row_entries
                .par_iter()
                .filter(|&v| v.partial_cmp(first).is_none())
                .count();
            if not_match_count > 0 {
                Err(DBSingleError::OtherError(format!(
                    "invalid value type for order by: {:?}",
                    first
                )))?;
            }
        }

        if rows.is_empty() {
            table.rows = Default::default();
            table.row_idx_acc = 0;
            table.row_num = 0;
            return Ok(());
        }

        let row_start = &rows[0] as *const Vec<Value> as usize;

        rows.par_sort_by(|a, b| {
            let row_start = row_start as *const Vec<Value>;
            let a_idx = unsafe { (a as *const Vec<Value>).offset_from(row_start) } as usize;
            let b_idx = unsafe { (b as *const Vec<Value>).offset_from(row_start) } as usize;
            for (expr_idx, &(_, is_asc)) in keys.iter().enumerate() {
                let av = &cached_entries[expr_idx][a_idx];
                let bv = &cached_entries[expr_idx][b_idx];
                let mut ord = av.partial_cmp(bv).unwrap();
                if !is_asc {
                    ord = ord.reverse();
                }
                if ord != std::cmp::Ordering::Equal {
                    return ord;
                }
            }
            std::cmp::Ordering::Equal
        });

        table.rows = rows.into_iter().map(Some).enumerate().collect();
        table.row_idx_acc = table.rows.len();

        Ok(())
    }
}
