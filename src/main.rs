use clap::Parser;
use helidb::{SQLExecConfig, SQLExecutor};
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Cli {
    /// The SQL file to execute for OJ test. When not given, enter the REPL
    sql: Option<String>,

    /// Storage path
    #[arg(short = 's', long)]
    storage_path: Option<PathBuf>,

    /// Reinitialize the storage
    #[arg(long)]
    reinit: bool,

    /// Do not write back to the storage path
    #[arg(long)]
    no_write_back: bool,

    /// Execute queries in parallel
    #[arg(long)]
    parallel: bool,
}

impl Cli {
    fn get_executor(&self) -> SQLExecutor {
        SQLExecConfig::new()
            .storage_path(self.storage_path.clone())
            .reinit(self.reinit)
            .write_back(!self.no_write_back)
            .parallel(self.parallel)
            .connect()
            .expect("Failed to connect to database")
    }
}

fn oj_test(file_name: &str, mut handle: SQLExecutor) {
    let sql_statements = std::fs::read_to_string(file_name).expect("Unable to read file");
    let (_, output) = handle.execute_sql_combine_outputs(&sql_statements);
    print!("{}", output);
}

fn repl(mut handle: SQLExecutor) {
    let mut rl = rustyline::DefaultEditor::new().unwrap();
    loop {
        let readline = rl.readline("SQL> ");
        match readline {
            Ok(line) => {
                rl.add_history_entry(&line).unwrap();
                if line.trim().is_empty() {
                    continue;
                }
                let (_, output) = handle.execute_sql_combine_outputs(&line);
                print!("{}", output);
            }
            Err(rustyline::error::ReadlineError::Interrupted) => {
                println!("Ctrl-C pressed, exiting REPL.");
                break;
            }
            Err(rustyline::error::ReadlineError::Eof) => {
                break;
            }
            Err(err) => {
                eprintln!("Error reading line: {}", err);
            }
        }
    }
}

fn main() {
    let cli = Cli::parse();
    let handle = cli.get_executor();
    if let Some(ref file_name) = cli.sql {
        oj_test(file_name, handle);
    } else {
        println!("No file name provided. Entering REPL...");
        repl(handle);
    }
}
