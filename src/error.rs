//! Database error types and handling.
//!
//! # Example
//! ```
//! use simple_db::error::{DBResult, DBSingleError};
//!
//! fn validate_input(input: &str) -> DBResult<()> {
//!     if input.is_empty() {
//!         Err(DBSingleError::RequiredError("Input cannot be empty".into()))?;
//!     }
//!     Ok(())
//! }
//!
//! assert!(validate_input("").is_err());
//! assert!(validate_input("test").is_ok());
//! ```

use sqlparser;
use sqlparser::parser::ParserError;

/// Represents a single database operation error.
#[derive(Debug)]
pub enum DBSingleError {
    /// IO error occurred during input/output operations
    IOError(std::io::Error),
    /// Formatting error occurred during output generation
    FmtError(std::fmt::Error),
    /// Errors required by the OJ system
    RequiredError(String),
    /// Attempted to use an unsupported operation
    UnsupportedOPError(String),
    /// Other miscellaneous database error
    OtherError(String),
}

impl std::fmt::Display for DBSingleError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use DBSingleError::*;
        match self {
            IOError(e) => write!(f, "IOError: {}", e),
            FmtError(e) => write!(f, "FmtError: {}", e),
            RequiredError(e) => write!(f, "Error: {}", e),
            UnsupportedOPError(e) => write!(f, "UnsupportedOPError: {}", e),
            OtherError(e) => write!(f, "OtherError: {}", e),
        }
    }
}

impl std::error::Error for DBSingleError {}

impl From<std::io::Error> for DBSingleError {
    fn from(e: std::io::Error) -> Self {
        DBSingleError::IOError(e)
    }
}

impl From<std::fmt::Error> for DBSingleError {
    fn from(e: std::fmt::Error) -> Self {
        DBSingleError::FmtError(e)
    }
}

impl From<ParserError> for DBSingleError {
    fn from(_: ParserError) -> Self {
        DBSingleError::RequiredError("Syntax error".into())
    }
}

/// Collection of multiple database errors.
///
/// Used when operations can fail in multiple ways and we want to
/// preserve all error information.
#[derive(Debug, Default)]
pub struct DBError {
    /// List of individual errors that occurred
    pub errors: Vec<DBSingleError>,
}

impl std::fmt::Display for DBError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for error in &self.errors {
            writeln!(f, "{}", error)?;
        }
        Ok(())
    }
}

impl<T: Into<DBSingleError>> From<T> for DBError {
    fn from(errors: T) -> Self {
        DBError {
            errors: vec![errors.into()],
        }
    }
}

impl DBError {
    /// Combines another DBError into this one.
    ///
    /// # Arguments
    /// * `other` - Error to merge into this one
    ///
    /// # Examples
    ///
    /// ```
    /// # use simple_db::error::{DBError, DBSingleError};
    /// let mut err1 = DBError::from(DBSingleError::RequiredError("First error".into()));
    /// let err2 = DBSingleError::OtherError("Second error".into());
    /// err1.join(err2.into());
    ///
    /// assert_eq!(err1.errors.len(), 2);
    /// assert_eq!(err1.to_string(), "Error: First error\nOtherError: Second error\n");
    /// ```
    pub fn join(&mut self, other: DBError) {
        self.errors.extend(other.errors);
    }
}

impl std::error::Error for DBError {}

/// Result type alias for database operations.
pub type DBResult<T> = Result<T, DBError>;

/// Combines two database results, preserving all errors.
///
/// If both results are errors, their errors are merged.
/// Otherwise returns the first error or success.
///
/// # Examples
/// ```
/// # use simple_db::error::{join_result, DBResult, DBSingleError};
/// let ok: DBResult<()> = Ok(());
/// let err: DBResult<()> = Err(DBSingleError::OtherError("test".into()).into());
///
/// assert!(join_result(ok, err).is_err());
/// ```
pub fn join_result(res1: DBResult<()>, res2: DBResult<()>) -> DBResult<()> {
    match res1 {
        Ok(()) => res2,
        Err(e) => {
            let mut err = e;
            if let Err(e) = res2 {
                err.join(e);
            }
            Err(err)
        }
    }
}
