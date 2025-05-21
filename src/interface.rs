use crate::core::executor::SQLExecutor;
use crate::core::parser::SQLParser;
use crate::error::{DBResult, join_result};
use crate::utils::WriteHandle;
use std::io::Write;

#[derive(Default)]
pub struct ExecConfig {
    pub output_target: WriteHandle,
    pub err_output_target: WriteHandle,
}

impl ExecConfig {
    pub fn new() -> Self {
        ExecConfig::default()
    }

    pub fn output_target(&mut self, output_target: Box<dyn Write>) -> &mut Self {
        self.output_target.set(output_target);
        self
    }

    pub fn err_output_target(&mut self, err_output_target: Box<dyn Write>) -> &mut Self {
        self.err_output_target.set(err_output_target);
        self
    }

    pub fn execute_sql(&mut self, sql_statements: &str) -> DBResult<()> {
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
}

pub fn execute_sql(sql_statements: &str) -> bool {
    if let Err(e) = ExecConfig::new().execute_sql(sql_statements) {
        print!("{}", e);
        false
    } else {
        true
    }
}
