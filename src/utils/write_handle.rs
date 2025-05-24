//! Handle for writing output with fallback to stdout.
//!
//! Provides a thread-unsafe but flexible way to redirect output streams.

use std::cell::RefCell;
use std::fmt::Write;
use std::rc::Rc;

/// A handle for writing output that can be redirected.
///
/// When no writer is set, falls back to writing to stdout.
/// Uses `Rc<RefCell>` internally to allow shared mutable access.
#[derive(Default, Clone)]
pub struct WriteHandle<'a>(Option<Rc<RefCell<Box<dyn Write + 'a>>>>);

impl<'a> WriteHandle<'a> {
    /// Creates a new WriteHandle that writes to stdout by default.
    pub fn new() -> Self {
        WriteHandle(None)
    }
    /// Creates a new WriteHandle wrapping the given writer.
    ///
    /// # Arguments
    /// * `writer` - The writer to use for output
    ///
    /// # Safety
    /// The writer must not be accessed from multiple threads.
    pub fn from(writer: Box<dyn Write + 'a>) -> Self {
        WriteHandle(Some(Rc::new(RefCell::new(writer))))
    }
    /// Sets the writer for this handle.
    ///
    /// # Arguments
    /// * `writer` - The writer to use for output
    ///
    /// # Safety
    /// The writer must not be accessed from multiple threads.
    pub fn set(&mut self, writer: Box<dyn Write + 'a>) {
        self.0 = Some(Rc::new(RefCell::new(writer)));
    }
}

impl Write for WriteHandle<'_> {
    fn write_str(&mut self, s: &str) -> std::fmt::Result {
        if let Some(ref writer) = self.0 {
            writer.borrow_mut().write_str(s)
        } else {
            print!("{}", s);
            Ok(())
        }
    }
}
