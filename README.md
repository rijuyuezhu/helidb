# Simple SQL Database Engine

A lightweight SQL database engine implementation in Rust, developed as part of the Rust programming course (2025 Spring) at Nanjing University.

## Features

- SQL parsing and execution
- In-memory data storage with optional persistence
- Parallel query execution
- Comprehensive error handling
- Interactive REPL (Read-Eval-Print Loop) interface

## Supported SQL Operations

| Operation     | Syntax |
|---------------|--------|
| **Create table** | `CREATE TABLE <table> (<columns,>...);` |
| **Drop table**   | `DROP TABLE <table,>...;` |
| **Insert**       | `INSERT <table> VALUES (<values,>...);`<br>or<br>`INSERT <table> (<columns,>...) VALUES (<values,>...);` |
| **Query**        | `SELECT <columns,>... FROM <table> WHERE <condition>;` |
| **Update**       | `UPDATE <table> SET <column=value,>... WHERE <condition>;` |
| **Delete**       | `DELETE FROM <table> WHERE <condition>;` |

## Usage

### Library Usage

```rust
use simple_db::{SQLExecConfig, SQLExecutor};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Configure database
    let config = SQLExecConfig::new()
        .parallel(true);  // Enable parallel execution

    // Connect
    let mut executor = config.connect()?;

    // Execute SQL
    executor.execute_sql("CREATE TABLE users (id INT, name VARCHAR)")?;
    executor.execute_sql("INSERT INTO users VALUES (1, 'Alice')")?;
    
    let output = executor.execute_sql("SELECT * FROM users")?;
    println!("{}", output);

    Ok(())
}
```

### Command Line Interface

The project provides a REPL (Read-Eval-Print Loop) for interactive SQL execution:

```bash
cargo run
```

Available CLI options:
- `--sql <FILE>`: Execute SQL from a file instead of entering REPL
- `-s/--storage-path <PATH>`: Set path for persistent storage
- `--reinit`: Reinitialize storage (clear existing data)
- `--no-write-back`: Disable persisting changes to storage
- `--parallel`: Enable parallel query execution

## Testing

The project includes an extensive test suite with test cases organized in numbered directories under `tests/cases/`. Each test case consists of:
- `input.txt`: SQL statements to execute
- `output.txt`: Expected output

To run all tests:
```bash
cargo test
```

## Benchmarking

Benchmarks compare performance of sequential vs parallel execution:
```bash
cargo bench
```

The benchmark file `benches/seq_par_update.rs` measures performance of update operations in both modes.

## Project Structure

Key components:
- `src/lib.rs`: Core library implementation
- `src/main.rs`: REPL interface
- `src/core/`: Database engine implementation
  - `data_structure/`: Database, table, and value types
  - `executor/`: SQL operation implementations
  - `parser/`: SQL parsing
  - `storage/`: Data persistence
- `tests/`: Test cases and utilities
- `benches/`: Performance benchmarks

## Documentation

To generate and view the API documentation:

```bash
cargo doc --open
```

This will:
1. Generate documentation for all public items
2. Open the documentation in your default browser

The documentation includes:
- Detailed module and type documentation
- Usage examples
- Feature explanations

## LICENSE

MIT License
