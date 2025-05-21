use sqlparser;

use sqlparser::parser::ParserError;

#[derive(Debug)]
pub enum DBSingleError {
    IOError(std::io::Error),
    // required error:
    RequiredError(String),
    UnsupportedOPError(String),
    Other(String),
}
impl std::fmt::Display for DBSingleError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use DBSingleError::*;
        match self {
            IOError(e) => write!(f, "IOError: {}", e),
            RequiredError(e) => write!(f, "Error: {}", e),
            UnsupportedOPError(e) => write!(f, "UnsupportedOPError: {}", e),
            Other(e) => write!(f, "Other: {}", e),
        }
    }
}
impl std::error::Error for DBSingleError {}

impl From<std::io::Error> for DBSingleError {
    fn from(e: std::io::Error) -> Self {
        DBSingleError::IOError(e)
    }
}

impl From<ParserError> for DBSingleError {
    fn from(_: ParserError) -> Self {
        DBSingleError::RequiredError("Syntax error".into())
    }
}

#[derive(Debug, Default)]
pub struct DBError {
    pub errors: Vec<DBSingleError>,
}

impl std::fmt::Display for DBError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for error in &self.errors {
            write!(f, "{}\n", error)?;
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
    pub fn join(&mut self, other: DBError) {
        self.errors.extend(other.errors);
    }
}

impl std::error::Error for DBError {}

pub type DBResult<T> = Result<T, DBError>;

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
