use simple_db::{SQLExecConfig, WriteHandle};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TestResult {
    Normal(String),
    Error(String),
}

fn strip_str(a: &str) -> String {
    a.trim_end()
        .lines()
        .map(|t| t.trim_end())
        .collect::<Vec<_>>()
        .join("\n")
}

impl TestResult {
    pub fn as_str(&self) -> &str {
        let (TestResult::Normal(output) | TestResult::Error(output)) = self;
        output
    }

    pub fn is_error(&self) -> bool {
        matches!(self, TestResult::Error(_))
    }

    pub fn expect(&self, expect: &str) {
        assert_eq!(strip_str(self.as_str()), strip_str(expect));
    }

    pub fn expect_normal(&self, expect: &str) {
        assert!(!self.is_error(), "Expect normal, but got error: {}", self);
        self.expect(expect);
    }

    pub fn expect_error(&self, expect: &str) {
        assert!(self.is_error(), "Expect error, but got normal: {}", self);
        self.expect(expect);
    }
}

impl std::fmt::Display for TestResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

pub fn run_sql(sql: &str) -> TestResult {
    let mut output = String::new();
    let mut err_output = String::new();
    let no_error = SQLExecConfig::new()
        .output_target(WriteHandle::from(Box::new(&mut output)))
        .err_output_target(WriteHandle::from(Box::new(&mut err_output)))
        .execute_sql(sql);
    print!("{}", no_error);
    if no_error {
        TestResult::Normal(output)
    } else {
        TestResult::Error(err_output)
    }
}
