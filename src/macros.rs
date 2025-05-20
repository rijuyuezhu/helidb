#[macro_export]
macro_rules! db_output {
    ($output:expr, $($arg:tt)*) => {
        match $output {
            Some(ref writer) => {
                let mut writer = writer.borrow_mut();
                write!(writer, $($arg)*).unwrap();
            },
            None => {
                print!($($arg)*);
            }
        }
    };
}

#[macro_export]
macro_rules! db_outputln {
    ($output:expr, $($arg:tt)*) => {
        match $output {
            Some(ref writer) => {
                let mut writer = writer.borrow_mut();
                writeln!(writer, $($arg)*).unwrap();
            },
            None => {
                println!($($arg)*);
            }
        }
    };
}
