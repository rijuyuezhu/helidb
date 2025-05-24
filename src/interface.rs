//! Public interfaces for executing SQL statements and handling results.

pub use crate::core::executor::SQLExecutor;
use crate::error::DBResult;
use std::path::PathBuf;

/// Configuration for executing SQL statements and handling output.
///
/// Controls where query results and errors are written.
pub struct SQLExecConfig {
    /// Path to the storage file, or None if not using file storage
    pub(crate) storage_path: Option<PathBuf>,
    /// Whether to reinitialize the storage
    pub(crate) reinit: bool,
    /// Whether to write back to the storage path
    pub(crate) write_back: bool,
    /// Whether to execute queries in parallel
    pub(crate) parallel: bool,
}

impl Default for SQLExecConfig {
    fn default() -> Self {
        SQLExecConfig {
            storage_path: None,
            reinit: false,
            write_back: true,
            parallel: false,
        }
    }
}

impl SQLExecConfig {
    /// Creates a new SQLExecConfig with default values.
    pub fn new() -> Self {
        Default::default()
    }

    /// Sets the storage path for the database.
    ///
    /// # Arguments
    /// * `storage_path` - Path to the storage file (None if not using file storage)
    ///
    /// # Returns
    /// Self for method chaining
    pub fn storage_path(mut self, storage_path: Option<PathBuf>) -> Self {
        self.storage_path = storage_path;
        self
    }

    /// Sets whether to reinitialize the storage.
    ///
    /// # Arguments
    /// * `reinit` - true to reinitialize, false otherwise
    ///
    /// # Returns
    /// Self for method chaining
    pub fn reinit(mut self, reinit: bool) -> Self {
        self.reinit = reinit;
        self
    }

    /// Sets whether to write back to the storage path.
    ///
    /// # Arguments
    /// * `write_back` - true to write back, false otherwise
    ///
    /// # Returns
    /// Self for method chaining
    pub fn write_back(mut self, write_back: bool) -> Self {
        self.write_back = write_back;
        self
    }

    /// Sets whether to execute queries in parallel.
    ///
    /// # Arguments
    /// * `parallel` - true to enable parallel execution, false otherwise
    ///
    /// # Returns
    /// Self for method chaining
    pub fn parallel(mut self, parallel: bool) -> Self {
        self.parallel = parallel;
        self
    }

    /// Connects to the database using the specified configuration.
    ///
    /// # Returns
    /// A `SQLExecutor` instance serving as the handle configured with the provided settings.
    pub fn connect(self) -> DBResult<SQLExecutor> {
        SQLExecutor::build_from_config(self)
    }
}
