//! This module provides functionality for loading and writing a database from/to a binary format.

use crate::core::data_structure::Database;
use crate::error::{DBResult, DBSingleError};
use bincode;

/// Loads a database from a binary format.
///
/// # Arguments
///
/// * `reader` - The reader from which the database will be loaded.
///
/// # Returns
///
/// The loaded database or an error if the operation fails.
///
/// # Errors
///
/// This function returns an error if reading from the provided reader fails or if decoding the data fails.
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

/// Writes a database to a binary format.
///
/// # Arguments
///
/// * `writer` - The writer to which the database will be written.
/// * `database` - The database to be written.
///
/// # Errors
///
/// This function returns an error if writing to the provided writer fails or if encoding the data fails.
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

