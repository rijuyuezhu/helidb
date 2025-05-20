use crate::core::executor::SQLExecutor;
use crate::core::parser::SQLParser;
use crate::db_outputln;
use std::cell::RefCell;
use std::io::Write;

#[derive(Default)]
pub struct ExecConfig {
    pub output_target: Option<RefCell<Box<dyn Write>>>,
    pub err_output_target: Option<RefCell<Box<dyn Write>>>,
}

impl ExecConfig {
    pub fn new() -> Self {
        ExecConfig {
            output_target: None,
            err_output_target: None,
        }
    }

    pub fn output_target(&mut self, output_target: Box<dyn Write>) -> &mut Self {
        self.output_target = Some(RefCell::new(output_target));
        self
    }

    pub fn err_output_target(&mut self, err_output_target: Box<dyn Write>) -> &mut Self {
        self.err_output_target = Some(RefCell::new(err_output_target));
        self
    }

    pub fn execute_sql(self, sql_statements: &str) -> bool {
        let statements = {
            let parser = SQLParser::new();
            match parser.parse(sql_statements) {
                Ok(statements) => statements,
                Err(e) => {
                    db_outputln!(self.err_output_target, "{}", e);
                    // return early if the parsing fails
                    return false;
                }
            }
        };
        let mut executor = SQLExecutor::new(self.output_target);
        let mut has_failed = false;

        for statement in statements.iter() {
            if let Err(e) = executor.execute_statement(statement) {
                db_outputln!(self.err_output_target, "{}", e);
                has_failed = true;
            }
        }
        has_failed
    }
}

pub fn execute_sql(sql_statements: &str) -> bool {
    ExecConfig::new().execute_sql(sql_statements)
}
