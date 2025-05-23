use std::cell::RefCell;
use std::fmt::Write;
use std::rc::Rc;

#[derive(Default, Clone)]
pub struct WriteHandle<'a>(Option<Rc<RefCell<Box<dyn Write + 'a>>>>);

impl<'a> WriteHandle<'a> {
    pub fn new() -> Self {
        WriteHandle(None)
    }
    pub fn from(writer: Box<dyn Write + 'a>) -> Self {
        WriteHandle(Some(Rc::new(RefCell::new(writer))))
    }
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
