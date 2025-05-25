use super::TableManager;
use crate::core::data_structure::{ColumnInfo, Table, Value};
use crate::error::{DBResult, DBSingleError};
use sqlparser::ast;

/// A table manager that inserts rows sequentially.
pub struct SequentialTableManager;

impl SequentialTableManager {
    fn insert_row_unchecked(&self, table: &mut Table, row: Vec<Value>) -> DBResult<usize> {
        let row_number = table.row_idx_acc;
        table.row_idx_acc += 1;
        table.row_num += 1;
        table.rows.insert(row_number, Some(row));
        Ok(row_number)
    }

    fn insert_row(&self, table: &mut Table, row: Vec<Value>) -> DBResult<usize> {
        if row.len() != table.columns_info.len() {
            Err(DBSingleError::OtherError(format!(
                "row length {} not match columns num {}",
                row.len(),
                table.columns_info.len()
            )))?
        }
        for (col_idx, value) in row.iter().enumerate() {
            self.update_column_values(table, col_idx, None, Some(value))?;
        }
        self.insert_row_unchecked(table, row)
    }

    fn update_column_values(
        &self,
        table: &mut Table,
        col_idx: usize,
        value_to_delete: Option<&Value>,
        value_to_add: Option<&Value>,
    ) -> DBResult<()> {
        let column_info = &table.columns_info[col_idx];
        let column_values = &mut table.columns_values[col_idx];

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

impl TableManager for SequentialTableManager {
    fn insert_rows(
        &self,
        table: &mut Table,
        raw_rows: &[Vec<ast::Expr>],
        columns_indicator: Vec<String>,
    ) -> DBResult<()> {
        for raw_row in raw_rows {
            let row = crate::core::executor::insert::parse_raw_row_and_rearrange(
                table,
                raw_row,
                &columns_indicator,
            )?;
            self.insert_row(table, row)?;
        }
        Ok(())
    }

    fn delete_rows(&self, table: &mut Table, cond: Option<&ast::Expr>) -> DBResult<()> {
        let table_confine_header = unsafe { &mut *(table as *mut Table) };
        for opt_row in table.rows.values_mut() {
            if opt_row.is_none() {
                continue;
            }
            if !table_confine_header.is_row_satisfy_cond(opt_row.as_ref().unwrap(), cond)? {
                continue;
            }
            let row = opt_row.as_mut().unwrap();
            for (col_idx, value) in row.iter().enumerate() {
                self.update_column_values(table_confine_header, col_idx, Some(value), None)?;
            }
            *opt_row = None;
            table.row_num -= 1;
        }
        Ok(())
    }

    fn update_rows(
        &self,
        table: &mut Table,
        assignments: &[ast::Assignment],
        cond: Option<&ast::Expr>,
    ) -> DBResult<()> {
        let table_confine_header = unsafe { &mut *(table as *mut Table) };

        for row in table.existed_rows_mut() {
            if !table_confine_header.is_row_satisfy_cond(row, cond)? {
                continue;
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
                    table_confine_header,
                    col_idx,
                    Some(&row[col_idx]),
                    Some(&value),
                )?;
                row[col_idx] = value;
            }
        }
        Ok(())
    }

    fn construct_table_from_calc_func(
        &self,
        table: &Table,
        columns_info: Vec<ColumnInfo>,
        calc_funcs: Vec<super::CalcFunc>,
        cond: Option<&ast::Expr>,
    ) -> DBResult<Table> {
        let mut new_table = Table::new(columns_info);
        for row in table.existed_rows() {
            if !table.is_row_satisfy_cond(row, cond)? {
                continue;
            }
            let mut new_row = vec![];
            for calc_func in &calc_funcs {
                new_row.push(calc_func(row)?);
            }
            self.insert_row_unchecked(&mut new_table, new_row)?;
        }
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
            let mut row_entries = vec![];
            for row in rows.iter() {
                let v = table.calc_expr_for_row(row, expr)?;
                if row_entries
                    .last()
                    .is_some_and(|prev: &Value| prev.partial_cmp(&v).is_none())
                {
                    Err(DBSingleError::OtherError(format!(
                        "invalid value type for order by: {:?}",
                        v
                    )))?;
                }
                row_entries.push(v);
            }
            cached_entries.push(row_entries);
        }

        if rows.is_empty() {
            table.rows = Default::default();
            table.row_idx_acc = 0;
            table.row_num = 0;
            return Ok(());
        }

        let row_start = &rows[0] as *const Vec<Value>;

        rows.sort_by(|a, b| {
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
