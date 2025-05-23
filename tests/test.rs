pub mod utils;

pub use utils::{TestResult, run_sql};

#[test]
fn test_cases() {
    let entries = std::fs::read_dir("./tests/cases")
        .unwrap()
        .map(|res| res.unwrap().path())
        .collect::<Vec<_>>();
    for entry in entries {
        // ./cases/1/input.txt
        let sql = std::fs::read_to_string(entry.join("input.txt")).unwrap();
        let expect = std::fs::read_to_string(entry.join("output.txt")).unwrap();
        println!("{}", entry.to_str().unwrap());
        run_sql(&sql).expect(&expect);
    }
}
