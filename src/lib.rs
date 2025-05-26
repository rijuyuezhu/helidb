//! # Simple SQL Database Engine
//!
//! A lightweight SQL database engine implementation in Rust.
//!
//! ## Features
//!
//! - SQL parsing and execution
//! - In-memory data storage with optional persistence
//! - Parallel query execution
//! - Comprehensive error handling
//!
//! ## Supported SQL Operations
//!
//! | Operation     | Syntax |
//! |---------------|--------|
//! | **Create table** | `CREATE TABLE <table> (<columns,>...);` |
//! | **Drop table**   | `DROP TABLE <table,>...;` |
//! | **Insert**       | `INSERT <table> VALUES (<values,>...);`<br>or<br>`INSERT <table> (<columns,>...) VALUES (<values,>...);` |
//! | **Query**        | `SELECT <columns,>... FROM <table> WHERE <condition>;` |
//! | **Update**       | `UPDATE <table> SET <column=value,>... WHERE <condition>;` |
//! | **Delete**       | `DELETE FROM <table> WHERE <condition>;` |
//!
//! ## Data Model
//!
//! - **Database**: Contains multiple tables
//! - **Table**: Contains rows and columns with defined schema
//! - **Column**: Supports INT, VARCHAR, and NULL values
//!
//! ## Configuration ([`SQLExecConfig`])
//!
//! The main configuration struct uses a builder pattern for flexible setup:
//!
//! | Method | Description | Default |
//! |:-------|:------------|:-------:|
//! | [`storage_path`](SQLExecConfig::storage_path) | Path for persistent storage (None for in-memory only) | `None` |
//! | [`reinit`](SQLExecConfig::reinit) | Reinitialize storage (clear existing data) | `false` |
//! | [`write_back`](SQLExecConfig::write_back) | Persist changes to storage path | `true` |
//! | [`parallel`](SQLExecConfig::parallel) | Enable parallel query execution (uses RAYON_NUM_THREADS) | `false` |
//!
//! ### Configuration Example
//!
//! ```rust
//! use helidb::{SQLExecConfig, SQLExecutor};
//!
//! // Typical configuration
//! let config = SQLExecConfig::new()
//!     .storage_path(Some("/storage".into())) // Persistent storage
//!     .reinit(false)                         // Keep existing data
//!     .write_back(true)                      // Save changes
//!     .parallel(true);                       // Parallel execution
//! ```
//!
//! Connect to the database using the [`connect`](SQLExecConfig::connect) method:
//!
//! ```rust
//! # use helidb::{SQLExecConfig, SQLExecutor};
//! # let config = SQLExecConfig::new();
//! let mut executor = config.connect().unwrap();
//! ```
//!
//! ## [`SQLExecutor`] Interface
//!
//! The [`SQLExecutor`] provides methods for executing SQL statements:
//!
//! - [`execute_sql`](SQLExecutor::execute_sql): Executes a single SQL statement
//! - [`execute_sql_combine_outputs`](SQLExecutor::execute_sql_combine_outputs): Combines normal and error outputs
//!
//! Data persistence (if enabled in [`SQLExecConfig`]) occurs after SQL execution.
//!
//! ```rust
//! # use helidb::{SQLExecConfig, SQLExecutor};
//!
//! // Use default configuration
//! let mut executor = SQLExecConfig::new().connect().unwrap();
//!
//! // Execute a SQL statement
//! let output = executor.execute_sql("CREATE TABLE users (id INT, name VARCHAR)").unwrap();
//!
//! // Execute and combine normal/error output into one String
//! let (no_error, output): (bool, String) = executor
//!     .execute_sql_combine_outputs("CREATE TABLE users (id INT, name VARCHAR)");
//! ```
//!
//! ## Complete Example
//!
//! ```rust
//! use helidb::{SQLExecConfig, SQLExecutor};
//! use std::path::PathBuf;
//!
//! fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // 1. Configure database
//!     let config = SQLExecConfig::new()
//!         .parallel(true);
//!
//!     // 2. Connect
//!     let mut executor = config.connect()?;
//!
//!     // 3. Create schema
//!     executor.execute_sql("
//!         CREATE TABLE products (
//!             id INT PRIMARY KEY,
//!             name VARCHAR NOT NULL,
//!             price INT,
//!             in_stock INT
//!         );"
//!     )?;
//!
//!     // 4. Insert records
//!     let (no_error, output) = executor.execute_sql_combine_outputs("
//!         INSERT INTO products VALUES (1, 'Laptop', 999, 1);
//!         INSERT INTO products VALUES (2, 'Mouse', 25, 1);
//!         INSERT INTO products VALUES (3, 'Keyboard', 49, 0);"
//!     );
//!
//!     if !no_error {
//!         return Err(output.into());
//!     }
//!
//!     // 5. Query data
//!     let output = executor.execute_sql("
//!         SELECT name, price
//!         FROM products
//!         WHERE in_stock = 1
//!         ORDER BY price DESC;"
//!     )?;
//!
//!     // 6. Update data
//!     executor.execute_sql("
//!         UPDATE products
//!         SET price = price * 2
//!         WHERE in_stock = true"
//!     )?;
//!
//!     // 7. Drop table
//!     executor.execute_sql("DROP TABLE products")?;
//!
//!     Ok(())
//! }
//! ```

pub mod core;
pub mod error;
pub mod interface;

pub use interface::{SQLExecConfig, SQLExecutor};
