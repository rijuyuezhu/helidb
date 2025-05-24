#![feature(test)]

use simple_db::{SQLExecConfig, SQLExecutor};
extern crate test;

const STORAGE_PATH: &str = "/tmp/db_parallel_test";

fn maybe_load_large_database(parallel: bool) -> SQLExecutor {
    if std::path::Path::new(STORAGE_PATH).exists() {
        SQLExecConfig::new()
            .parallel(parallel)
            .storage_path(Some(STORAGE_PATH.into()))
            .write_back(false)
            .connect()
            .unwrap()
    } else {
        let mut handle = SQLExecConfig::new()
            .parallel(parallel)
            .storage_path(Some(STORAGE_PATH.into()))
            .connect()
            .unwrap();

        let _ = handle
            .execute_sql("CREATE TABLE test (id INT PRIMARY KEY, i INT NOT NULL, j INT NOT NULL);");

        for i in 0..100_000 {
            let _ = handle.execute_sql(&format!(
                "INSERT INTO test (id, i, j) VALUES ({i}, {i}, {i});"
            ));
        }
        handle
    }
}

fn bench(parallel: bool, b: &mut test::Bencher) {
    let mut handle = maybe_load_large_database(parallel);
    b.iter(|| {
        let _ = handle.execute_sql(
            "UPDATE test SET i = i + 2, j = j + 2 WHERE i % 2 = 0 AND j = i AND j % 2 = 0;",
        );
    });
}

#[bench]
fn bench_sequential(b: &mut test::Bencher) {
    bench(false, b);
}

#[bench]
fn bench_parallel(b: &mut test::Bencher) {
    bench(true, b);
}
