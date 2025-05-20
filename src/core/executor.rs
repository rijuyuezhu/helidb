use crate::core::data_structure::Database;
use crate::error::{DBError, DBResult};
use sqlparser::ast::{CreateTable, Delete, Insert, Query, Statement};
use std::cell::RefCell;
use std::io::Write;

#[derive(Default)]
pub struct SQLExecutor {
    database: Database,
    output_target: Option<RefCell<Box<dyn Write>>>,
}

impl SQLExecutor {
    pub fn new(output_target: Option<RefCell<Box<dyn Write>>>) -> Self {
        SQLExecutor {
            database: Database::new(),
            output_target,
        }
    }

    pub fn execute_create_table(&mut self, create_table: &CreateTable) -> DBResult<()> {
        Err(DBError::UnsupportedOperationError(String::from(
            "create table",
        )))
    }

    pub fn execute_drop_table(&mut self, drop_statement: &Statement) -> DBResult<()> {
        Err(DBError::UnsupportedOperationError(String::from(
            "drop table",
        )))
    }

    pub fn execute_insert(&mut self, insert: &Insert) -> DBResult<()> {
        Err(DBError::UnsupportedOperationError(String::from("insert")))
    }

    pub fn execute_query(&mut self, query: &Query) -> DBResult<()> {
        Err(DBError::UnsupportedOperationError(String::from("query")))
    }

    pub fn execute_update(&mut self, update_statement: &Statement) -> DBResult<()> {
        Err(DBError::UnsupportedOperationError(String::from("update")))
    }

    pub fn execute_delete(&mut self, delete: &Delete) -> DBResult<()> {
        Err(DBError::UnsupportedOperationError(String::from("delete")))
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
            _ => Err(DBError::UnsupportedOperationError(String::from(
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
