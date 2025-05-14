use helidb::execute_sql;
use rustyline::{Result, error::ReadlineError};

const HISTORY_FILE: &str = "./.sql_history";

fn try_main() -> Result<()> {
    let mut rl = rustyline::DefaultEditor::new()?;
    let _ = rl.load_history(HISTORY_FILE);
    let result = loop {
        match rl.readline("helidb>> ") {
            Ok(ref line) => {
                rl.add_history_entry(line)?;
                execute_sql(line);
            }
            Err(ReadlineError::Eof) => break Ok(()),
            Err(e) => break Err(e),
        }
    };
    rl.save_history(HISTORY_FILE)?;
    result
}
fn main() {
    if let Err(e) = try_main() {
        eprintln!("{}", e);
    }
}
