use sqlparser;

use sqlparser::parser::ParserError;

#[derive(Debug)]
pub enum DBError {
    IOError(std::io::Error),
    ParserError(ParserError),
    RequiredReportError(String),
    UnsupportedOperationError(String),
    Other(String),
}
impl std::fmt::Display for DBError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use DBError::*;
        match self {
            IOError(e) => write!(f, "IO error: {}", e),
            ParserError(e) => write!(f, "Parser error: {}", e),
            RequiredReportError(e) => write!(f, "Error: {}", e),
            UnsupportedOperationError(e) => write!(f, "Unsupported operation: {}", e),
            Other(e) => write!(f, "Error: {}", e),
        }
    }
}
impl std::error::Error for DBError {}

impl From<std::io::Error> for DBError {
    fn from(e: std::io::Error) -> Self {
        DBError::IOError(e)
    }
}

impl From<ParserError> for DBError {
    fn from(e: ParserError) -> Self {
        DBError::ParserError(e)
    }
}

pub type DBResult<T> = Result<T, DBError>;
