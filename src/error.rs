use sqlparser;

use sqlparser::parser::ParserError;

#[derive(Debug)]
pub enum DBSingleError {
    FmtError(std::fmt::Error),
    RequiredError(String),
    UnsupportedOPError(String),
    OtherError(String),
}

impl std::fmt::Display for DBSingleError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use DBSingleError::*;
        match self {
            FmtError(e) => write!(f, "FmtError: {}", e),
            RequiredError(e) => write!(f, "Error: {}", e),
            UnsupportedOPError(e) => write!(f, "UnsupportedOPError: {}", e),
            OtherError(e) => write!(f, "OtherError: {}", e),
        }
    }
}

impl std::error::Error for DBSingleError {}

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

#[derive(Debug, Default)]
pub struct DBError {
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
