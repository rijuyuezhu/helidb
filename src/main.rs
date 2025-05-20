use simple_db::execute_sql;
use std::env;

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        println!("Usage: {} <sql_file>", args[0]);
        return;
    }
    let sql_statements = std::fs::read_to_string(&args[1]).expect("Unable to read file");
    execute_sql(&sql_statements);
}
