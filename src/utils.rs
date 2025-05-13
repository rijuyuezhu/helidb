#[derive(Debug)]
pub enum Error {
    IO(std::io::Error),
    Parse(String),
    Execute(String),
    Other(String),
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::IO(e) => write!(f, "IO error: {}", e),
            Error::Parse(e) => write!(f, "Parse error: {}", e),
            Error::Execute(e) => write!(f, "Execute error: {}", e),
            Error::Other(e) => write!(f, "Other error: {}", e),
        }
    }
}
impl std::error::Error for Error {}

pub type Result<T> = std::result::Result<T, Error>;
