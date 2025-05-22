use crate::core::executor::SQLExecutor;
use crate::core::parser::SQLParser;
use crate::error::{DBResult, join_result};
use crate::utils::WriteHandle;
use std::fmt::Write;

#[derive(Default)]
pub struct SQLExecConfig<'a> {
    pub output_target: WriteHandle<'a>,
    pub err_output_target: WriteHandle<'a>,
}

impl<'a> SQLExecConfig<'a> {
    pub fn new() -> Self {
        SQLExecConfig::default()
    }

    pub fn output_target(&mut self, output_target: WriteHandle<'a>) -> &mut Self {
        self.output_target = output_target;
        self
    }

    pub fn err_output_target(&mut self, err_output_target: WriteHandle<'a>) -> &mut Self {
        self.err_output_target = err_output_target;
        self
    }

    pub fn execute(&mut self, sql_statements: &str) -> DBResult<()> {
        let statements = {
            let parser = SQLParser::new();
            parser.parse(sql_statements)?
        };
        let mut executor = SQLExecutor::new(self.output_target.clone());

        let mut result = Ok(());
        for statement in statements.iter() {
            result = join_result(result, executor.execute_statement(statement));
        }
        result
    }
    pub fn execute_sql(&mut self, sql_statements: &str) -> bool {
        if let Err(e) = self.execute(sql_statements) {
            write!(self.err_output_target, "{}", e).unwrap();
            false
        } else {
            true
        }
    }
}
