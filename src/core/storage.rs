//! Database persist storage using bincode serialization.
//!
//! # Example
//! ```
//! # use helidb::core::storage::{load_database_from, write_database_to};
//! use helidb::core::data_structure::Database;
//! use std::fs::File;
//!
//! let db = Database::new();
//! let mut mem_file = Vec::<u8>::new();
//! write_database_to(&mut mem_file, &db).unwrap();
//!
//! let loaded = load_database_from(&*mem_file).unwrap();
//! ```

use crate::core::data_structure::Database;
use crate::error::{DBResult, DBSingleError};
use bincode;

/// Loads a database from a binary reader.
///
/// # Arguments
/// * `reader` - Read source implementing std::io::Read
///
/// # Returns
/// Deserialized Database if no error occurs, otherwise an error.
pub fn load_database_from<R>(mut reader: R) -> DBResult<Database>
where
    R: std::io::Read,
{
    let mut buffer = Vec::new();
    reader.read_to_end(&mut buffer)?;
    let config = bincode::config::standard();
    let (database, _) = bincode::decode_from_slice(&buffer, config)
        .map_err(|e| DBSingleError::OtherError(format!("Failed to decode data: {}", e)))?;
    Ok(database)
}

/// Loads a database from a file at the specified path.
///
/// # Arguments
/// * `path` - The path to the file from which the database will be loaded.
///
/// # Returns
/// The loaded database or an error if the operation fails.
pub fn load_database_from_path<P>(path: P) -> DBResult<Database>
where
    P: AsRef<std::path::Path>,
{
    match std::fs::File::open(path) {
        Ok(f) => load_database_from(f),
        Err(e) => match e.kind() {
            std::io::ErrorKind::NotFound => Ok(Database::new()),
            _ => Err(DBSingleError::OtherError(format!(
                "Error opening storage file: {}",
                e
            )))?,
        },
    }
}

/// Writes a database to a binary format.
///
/// # Arguments
///
/// * `writer` - The writer to which the database will be written.
/// * `database` - The database to be written.
pub fn write_database_to<W>(mut writer: W, database: &Database) -> DBResult<()>
where
    W: std::io::Write,
{
    let config = bincode::config::standard();
    let buffer = bincode::encode_to_vec(database, config)
        .map_err(|e| DBSingleError::OtherError(format!("Failed to encode data: {}", e)))?;
    writer.write_all(&buffer)?;
    Ok(())
}
