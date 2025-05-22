use simple_db::{SQLExecConfig, utils::WriteHandle};
use std::env;

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        println!("Usage: {} <sql_file>", args[0]);
        return;
    }
    let sql_statements = std::fs::read_to_string(&args[1]).expect("Unable to read file");
    let mut output = String::new();
    let mut err_output = String::new();
    let no_error = SQLExecConfig::new()
        .output_target(WriteHandle::from(Box::new(&mut output)))
        .err_output_target(WriteHandle::from(Box::new(&mut err_output)))
        .execute_sql(&sql_statements);
    if no_error {
        print!("{}", output);
    } else {
        print!("{}", err_output);
    }
}
