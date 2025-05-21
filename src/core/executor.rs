#![allow(unused)]

use crate::core::data_structure::{ColumnInfo, Database};
use crate::error::{DBError, DBResult, DBSingleError};
use crate::utils::WriteHandle;
use sqlparser::ast;
use sqlparser::ast::{CreateTable, Delete, Insert, Query, Statement};
use std::cell::RefCell;
use std::io::Write;

use super::data_structure::ColumnTypeSpecific;

#[derive(Default)]
pub struct SQLExecutor {
    database: Database,
    output_target: WriteHandle,
}

impl SQLExecutor {
    pub fn new(output_target: WriteHandle) -> Self {
        SQLExecutor {
            database: Database::new(),
            output_target,
        }
    }

    pub fn execute_create_table(&mut self, create_table: &CreateTable) -> DBResult<()> {
        fn varchar_convert(length: Option<ast::CharacterLength>) -> Result<u64, DBError> {
            match length {
                Some(ast::CharacterLength::IntegerLength { length, .. }) => Ok(length),
                Some(ast::CharacterLength::Max) => Ok(u64::MAX),
                None => Ok(u64::MAX),
            }
        }

        let table_name = create_table.name.to_string();
        if self.database.tables.contains_key(&table_name) {
            return Err(DBSingleError::Other("table already exists".into()))?;
        }
        let column_info = create_table
            .columns
            .iter()
            .map(|col| -> DBResult<ColumnInfo> {
                let col_name = col.name.to_string();
                let type_specific = match col.data_type {
                    ast::DataType::Int(width) => ColumnTypeSpecific::Int {
                        display_width: width,
                    },
                    ast::DataType::Varchar(length) => ColumnTypeSpecific::Varchar {
                        max_length: varchar_convert(length)?,
                    },
                    _ => {
                        return Err(DBSingleError::Other("unsupported column type".into()))?;
                    }
                };
                Ok(ColumnInfo {
                    name: col_name,
                    type_specific,
                })
            })
            .collect::<Vec<_>>();
        todo!();
        Ok(())
    }

    pub fn execute_drop_table(&mut self, drop_statement: &Statement) -> DBResult<()> {
        Err(DBSingleError::UnsupportedOPError(String::from(
            "drop table",
        )))?
    }

    pub fn execute_insert(&mut self, insert: &Insert) -> DBResult<()> {
        Err(DBSingleError::UnsupportedOPError(String::from("insert")))?
    }

    pub fn execute_query(&mut self, query: &Query) -> DBResult<()> {
        Err(DBSingleError::UnsupportedOPError(String::from("query")))?
    }

    pub fn execute_update(&mut self, update_statement: &Statement) -> DBResult<()> {
        Err(DBSingleError::UnsupportedOPError(String::from("update")))?
    }

    pub fn execute_delete(&mut self, delete: &Delete) -> DBResult<()> {
        Err(DBSingleError::UnsupportedOPError(String::from("delete")))?
    }

    pub fn execute_statement(&mut self, statement: &Statement) -> DBResult<()> {
        println!("{:?}\n", statement);
        match statement {
            Statement::CreateTable(create_table) => self.execute_create_table(create_table),
            drop_statement @ Statement::Drop { .. } => self.execute_drop_table(drop_statement),
            Statement::Insert(insert) => self.execute_insert(insert),
            Statement::Query(query) => self.execute_query(query),
            update_statement @ Statement::Update { .. } => self.execute_update(update_statement),
            Statement::Delete(delete) => self.execute_delete(delete),
            _ => Err(DBSingleError::UnsupportedOPError(String::from(
                "main operator",
            )))?,
        }
    }

    pub fn execute_statements<'b, I>(&mut self, statements: I) -> Vec<DBError>
    where
        I: IntoIterator<Item = &'b Statement>,
    {
        statements
            .into_iter()
            .flat_map(|statement| self.execute_statement(statement).err())
            .collect()
    }
}
