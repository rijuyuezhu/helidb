// use super::TableManager;
// use crate::core::data_structure::{ColumnInfo, Table, Value};
// use crate::error::{DBResult, DBSingleError};
// use rayon::prelude::*;
// use sqlparser::ast;
// use std::collections::HashSet;

pub use crate::core::executor::table_manager::sequential::SequentialTableManager as ParallelTableManager;
// pub struct ParallelTableManager;

// impl TableManager for ParallelTableManager {
//     fn insert_rows(
//         &self,
//         table: &mut Table,
//         raw_rows: &[Vec<ast::Expr>],
//         columns_indicator: Vec<String>,
//     ) -> DBResult<()> {
//         use crate::core::executor::insert::parse_raw_row;
//         if columns_indicator.is_empty() {
//             // on columns_indicator in the INSERT statement
//             for raw_row in raw_rows {
//                 let row = parse_raw_row(raw_row)?;
//                 table.insert_row(row)?;
//             }
//         } else {
//             // reorder the values according to the columns_indicator
//             for raw_row in raw_rows {
//                 let mut insert_values = parse_raw_row(raw_row)?;
//                 if insert_values.len() != columns_indicator.len() {
//                     Err(DBSingleError::OtherError(format!(
//                         "number of columns given {} does not match number of values {}",
//                         columns_indicator.len(),
//                         insert_values.len()
//                     )))?
//                 }
//                 let mut row = vec![Value::from_null(); table.get_column_num()];
//                 let mut index_used = HashSet::new();
//                 for i in 0..columns_indicator.len() {
//                     let column_name = &columns_indicator[i];
//                     let index = table.get_column_index(column_name).ok_or_else(|| {
//                         DBSingleError::OtherError(format!("column {} not found", column_name))
//                     })?;
//
//                     if index_used.contains(&index) {
//                         Err(DBSingleError::OtherError(format!(
//                             "column {} is duplicated",
//                             column_name
//                         )))?
//                     }
//                     index_used.insert(index);
//
//                     std::mem::swap(&mut row[index], &mut insert_values[i]);
//                 }
//                 table.insert_row(row)?;
//             }
//         }
//         Ok(())
//     }
//
//     fn delete_rows(&self, table: &mut Table, cond: Option<&ast::Expr>) -> DBResult<()> {
//         let mut row_to_delete = vec![];
//         for (&row_idx, row) in &table.rows {
//             if table.is_row_satisfy_cond(row, cond)? {
//                 row_to_delete.push(row_idx);
//             }
//         }
//         for row_idx in &row_to_delete {
//             if table.rows.remove(row_idx).is_none() {
//                 Err(DBSingleError::OtherError(format!(
//                     "row index {} not found",
//                     row_idx
//                 )))?;
//             }
//         }
//         Ok(())
//     }
//
//     fn update_rows(
//         &self,
//         table: &mut Table,
//         assignments: &[ast::Assignment],
//         cond: Option<&ast::Expr>,
//     ) -> DBResult<()> {
//         let any_table = unsafe { &*(table as *const Table) };
//
//         table
//             .rows
//             .par_iter_mut()
//             .try_for_each(|(&row_idx, row)| -> DBResult<()> {
//                 // safety here: any self is used to calculate expressions,
//                 // where only the columns_info are used.
//
//                 if !any_table.is_row_satisfy_cond(row, cond)? {
//                     return Ok(());
//                 }
//
//                 let orig_row = row.clone();
//
//                 for ast::Assignment {
//                     target,
//                     value: expr,
//                 } in assignments
//                 {
//                     let ast::AssignmentTarget::ColumnName(column_name) = target else {
//                         Err(DBSingleError::UnsupportedOPError(
//                             "only support column name".into(),
//                         ))?
//                     };
//                     let column_name = column_name.to_string();
//
//                     let index = any_table.get_column_index(&column_name).ok_or_else(|| {
//                         DBSingleError::OtherError(format!("column not found: {}", column_name))
//                     })?;
//
//                     let value = any_table.calc_expr_for_row(&orig_row, expr)?;
//                     any_table.check_column_with_value(index, &value, Some(row_idx))?;
//                     row[index] = value;
//                 }
//                 Ok(())
//             })?;
//         Ok(())
//     }
//     fn construct_rows_from_calc_func(
//         &self,
//         table: &Table,
//         columns_info: Vec<ColumnInfo>,
//         calc_funcs: Vec<super::CalcFunc>,
//         cond: Option<&ast::Expr>,
//     ) -> DBResult<Table> {
//         let mut new_table = Table::new(columns_info);
//         for row in table.rows.values() {
//             if !table.is_row_satisfy_cond(row, cond)? {
//                 continue;
//             }
//             let mut new_row = vec![];
//             for calc_func in &calc_funcs {
//                 new_row.push(calc_func(row)?);
//             }
//             new_table.insert_row_unchecked(new_row)?;
//         }
//         Ok(new_table)
//     }
//
//     fn convert_order_by(&self, table: &mut Table, keys: &[(&ast::Expr, bool)]) -> DBResult<()> {
//         let mut rows = std::mem::take(&mut table.rows)
//             .into_values()
//             .collect::<Vec<_>>();
//
//         let mut cached_entries = vec![];
//
//         // beforehand check: to avoid panic when sorting
//         for &(expr, _) in keys {
//             let mut row_entries = vec![];
//             for row in &rows {
//                 let v = table.calc_expr_for_row(row, expr)?;
//                 if row_entries
//                     .last()
//                     .is_some_and(|prev: &Value| prev.partial_cmp(&v).is_none())
//                 {
//                     Err(DBSingleError::OtherError(format!(
//                         "invalid value type for order by: {:?}",
//                         v
//                     )))?;
//                 }
//                 row_entries.push(v);
//             }
//             cached_entries.push(row_entries);
//         }
//         let row_start = &rows[0] as *const Vec<Value>;
//
//         rows.sort_by(|a, b| {
//             let a_idx = unsafe { (a as *const Vec<Value>).offset_from(row_start) } as usize;
//             let b_idx = unsafe { (b as *const Vec<Value>).offset_from(row_start) } as usize;
//             for (expr_idx, &(_, is_asc)) in keys.iter().enumerate() {
//                 let av = &cached_entries[expr_idx][a_idx];
//                 let bv = &cached_entries[expr_idx][b_idx];
//                 let mut ord = av.partial_cmp(bv).unwrap();
//                 if !is_asc {
//                     ord = ord.reverse();
//                 }
//                 if ord != std::cmp::Ordering::Equal {
//                     return ord;
//                 }
//             }
//             std::cmp::Ordering::Equal
//         });
//
//         table.rows = rows.into_iter().enumerate().collect();
//         table.row_idx_acc = table.rows.len();
//
//         Ok(())
//     }
// }
