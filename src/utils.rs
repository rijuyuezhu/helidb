use std::cell::RefCell;
use std::io::{self, Write};
use std::rc::Rc;

#[derive(Default, Clone)]
pub struct WriteHandle(Option<Rc<RefCell<Box<dyn std::io::Write>>>>);

impl WriteHandle {
    pub fn new() -> Self {
        WriteHandle(None)
    }
    pub fn from(writer: Box<dyn Write>) -> Self {
        WriteHandle(Some(Rc::new(RefCell::new(writer))))
    }
    pub fn set(&mut self, writer: Box<dyn Write>) {
        self.0 = Some(Rc::new(RefCell::new(writer)));
    }
}

impl std::io::Write for WriteHandle {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self.0 {
            Some(ref writer) => writer.borrow_mut().write(buf),
            None => io::stdout().write(buf),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self.0 {
            Some(ref writer) => writer.borrow_mut().flush(),
            None => io::stdout().flush(),
        }
    }
}
