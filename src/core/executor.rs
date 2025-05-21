#![allow(unused)]

use crate::core::data_structure::{ColumnInfo, Database};
use crate::error::{DBError, DBResult, DBSingleError, join_result};
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
        let table_name = create_table.name.to_string();
        if self.database.tables.contains_key(&table_name) {
            return Err(DBSingleError::Other("table already exists".into()))?;
        }
        let mut column_info = vec![];
        let mut result = Ok(());
        for col in &create_table.columns {
            let name = col.name.to_string();
            let type_specific = match ColumnTypeSpecific::from_column_def(col) {
                Ok(type_specific) => type_specific,
                Err(e) => {
                    result = join_result(result, Err(e));
                    continue;
                }
            };
            column_info.push(ColumnInfo {
                name,
                type_specific,
            });
        }
        self.database.create_table(table_name, column_info);
        result
    }

    pub fn execute_drop_table(&mut self, drop_statement: &Statement) -> DBResult<()> {
        let Statement::Drop {
            object_type, names, ..
        } = drop_statement
        else {
            panic!()
        };

        if object_type != &ast::ObjectType::Table {
            return Err(DBSingleError::Other("only table drop is supported".into()))?;
        }
        let mut result = Ok(());
        for name in names {
            if let Err(e) = self.database.drop_table(&name.to_string()) {
                result = join_result(result, Err(e));
            }
        }
        result
    }

    pub fn execute_insert(&mut self, insert: &Insert) -> DBResult<()> {
        Err(DBSingleError::UnsupportedOPError("insert".into()))?
    }

    pub fn execute_query(&mut self, query: &Query) -> DBResult<()> {
        Err(DBSingleError::UnsupportedOPError("query".into()))?
    }

    pub fn execute_update(&mut self, update_statement: &Statement) -> DBResult<()> {
        Err(DBSingleError::UnsupportedOPError("update".into()))?
    }

    pub fn execute_delete(&mut self, delete: &Delete) -> DBResult<()> {
        Err(DBSingleError::UnsupportedOPError("delete".into()))?
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
            _ => Err(DBSingleError::UnsupportedOPError("main operator".into()))?,
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
