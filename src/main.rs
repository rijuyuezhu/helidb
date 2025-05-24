use clap::Parser;
use simple_db::SQLExecConfig;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Cli {
    /// The SQL file to execute for OJ test. When not given, enter the REPL
    sql: Option<String>,

    /// Storage path (only for REPL)
    #[arg(short = 's', long)]
    storage_path: Option<PathBuf>,

    /// Reinitialize the storage (only for REPL)
    #[arg(long)]
    reinit: bool,

    /// Do not write back to the storage path (only for REPL)
    #[arg(long)]
    no_write_back: bool,

    /// Execute queries in parallel (only for REPL)
    #[arg(long)]
    parallel: bool,
}

fn oj_test(file_name: &str) {
    let sql_statements = std::fs::read_to_string(file_name).expect("Unable to read file");
    let mut handle = SQLExecConfig::new()
        .parallel(true)
        .connect()
        .expect("Failed to connect to database");
    let (_, output) = handle.execute_sql_combine_outputs(&sql_statements);
    print!("{}", output);
}

fn repl(cli: Cli) {
    let mut handle = SQLExecConfig::new()
        .storage_path(cli.storage_path)
        .reinit(cli.reinit)
        .write_back(!cli.no_write_back)
        .parallel(cli.parallel)
        .connect()
        .expect("Failed to connect to database");
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
    if let Some(file_name) = cli.sql {
        oj_test(&file_name);
    } else {
        println!("No file name provided. Entering REPL...");
        repl(cli);
    }
}
