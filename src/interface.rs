use std::io::Write;

use crate::core::executor::SQLExecutor;
use crate::core::parser::SQLParser;
use crate::utils::DBResult;

pub struct ExecConfig<'a> {
    output_target: Option<&'a mut dyn Write>,
}

impl<'a> ExecConfig<'a> {
    pub fn new() -> Self {
        ExecConfig {
            output_target: None,
        }
    }
    pub fn output_target(&mut self, output_target: &'a mut dyn Write) -> &mut Self {
        self.output_target = Some(output_target);
        self
    }

    pub fn execute_sql(&mut self, sql_statements: &str) -> DBResult<()> {
        let statements = {
            let parser = SQLParser::new();
            parser.parse(sql_statements)?
        };
        let mut executor = SQLExecutor::new();

        for statement in statements.iter() {
            if let Err(e) = executor.execute_statement(statement) {
                match self.output_target {
                    Some(ref mut output) => writeln!(output, "{}", e)?,
                    None => eprintln!("{}", e),
                }
            }
        }
        Ok(())
    }
}

impl Default for ExecConfig<'_> {
    fn default() -> Self {
        ExecConfig::new()
    }
}

pub fn execute_sql(sql_statements: &str) -> bool {
    if let Err(e) = ExecConfig::default().execute_sql(sql_statements) {
        eprintln!("{}", e);
        false
    } else {
        true
    }
}
