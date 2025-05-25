use criterion::{Criterion, criterion_group, criterion_main};
use lazy_static::lazy_static;
use simple_db::{SQLExecConfig, SQLExecutor};
use std::time::Instant;

const STORAGE_PATH: &str = "/tmp/db_parallel_test";
const ELEM_CNT: usize = 1_000_000;
const UPDATE_STMT: &str = "UPDATE test SET i = i + 2, j = j + 2 WHERE i % 2 = 0 AND j = i AND (j + 1) % 2 = 5 - 4 + 3 - 2 - 1 AND i + j = 2 * j;";
const DELETE_STMT: &str =
    "DELETE test WHERE i % 2 = 0 AND j = i AND (j + 1) % 2 = 5 - 4 + 3 - 2 - 1 AND i + j = 2 * j;";
const QUERY_STMT: &str = "SELECT * FROM test WHERE i % 2 = 0 AND j = i AND (j + 1) % 2 = 5 - 4 + 3 - 2 - 1 AND i + j = 2 * j;";
const QUERY_WITHORDER_STMT: &str = "SELECT * FROM test WHERE i % 2 = 0 AND j = i AND (j + 1) % 2 = 5 - 4 + 3 - 2 - 1 AND i + j = 2 * j ORDER BY i DESC, j ASC;";

macro_rules! function {
    () => {{
        fn f() {}
        fn type_name_of<T>(_: T) -> &'static str {
            std::any::type_name::<T>()
        }
        let name = type_name_of(f);

        // Find and cut the rest of the path
        match &name[..name.len() - 3].rfind(':') {
            Some(pos) => &name[pos + 1..name.len() - 3],
            None => &name[..name.len() - 3],
        }
    }};
}

lazy_static! {
    static ref INSERT_STMT: &'static str = {
        let insert_tuples = (ELEM_CNT..(ELEM_CNT + ELEM_CNT / 2))
            .map(|i| format!("({}, {}, {})", i, i, i))
            .collect::<Vec<_>>()
            .join(", ");
        format!("INSERT INTO test (id, i, j) VALUES {insert_tuples};").leak()
    };
}

fn maybe_load_large_database(parallel: bool) -> SQLExecutor {
    if !std::path::Path::new(STORAGE_PATH).exists() {
        let mut handle = SQLExecConfig::new()
            .parallel(true)
            .storage_path(Some(STORAGE_PATH.into()))
            .connect()
            .unwrap();

        let _ = handle.execute_sql("CREATE TABLE test (id INT, i INT NOT NULL, j INT NOT NULL);");

        let insert_tuples = (0..ELEM_CNT)
            .map(|i| format!("({}, {}, {})", i, i, i))
            .collect::<Vec<_>>()
            .join(", ");
        let _ = handle.execute_sql(&format!(
            "INSERT INTO test (id, i, j) VALUES {insert_tuples};"
        ));
    }

    SQLExecConfig::new()
        .parallel(parallel)
        .storage_path(Some(STORAGE_PATH.into()))
        .write_back(false) // do not write back, so we can run multiple times
        .connect()
        .unwrap()
}

fn bench(parallel: bool, bench_id: &str, stmt: &str, c: &mut Criterion) {
    c.bench_function(bench_id, |b| {
        b.iter_custom(|iters| {
            let mut handle = (0..iters)
                .map(|_| maybe_load_large_database(parallel))
                .collect::<Vec<_>>();
            let start = Instant::now();
            for i in 0..iters {
                let _ = handle[i as usize].execute_sql(stmt);
            }
            start.elapsed()
        })
    });
}

fn parallel_update(c: &mut Criterion) {
    bench(true, function!(), UPDATE_STMT, c);
}

fn sequential_update(c: &mut Criterion) {
    bench(false, function!(), UPDATE_STMT, c);
}

fn parallel_delete(c: &mut Criterion) {
    bench(true, function!(), DELETE_STMT, c);
}

fn sequential_delete(c: &mut Criterion) {
    bench(false, function!(), DELETE_STMT, c);
}

fn parallel_query(c: &mut Criterion) {
    bench(true, function!(), QUERY_STMT, c);
}

fn sequential_query(c: &mut Criterion) {
    bench(false, function!(), QUERY_STMT, c);
}

fn parallel_query_with_order(c: &mut Criterion) {
    bench(true, function!(), QUERY_WITHORDER_STMT, c);
}

fn sequential_query_with_order(c: &mut Criterion) {
    bench(false, function!(), QUERY_WITHORDER_STMT, c);
}

fn parallel_insert(c: &mut Criterion) {
    bench(true, function!(), *INSERT_STMT, c);
}

fn sequential_insert(c: &mut Criterion) {
    bench(false, function!(), *INSERT_STMT, c);
}

criterion_group!(
    benches,
    parallel_update,
    sequential_update,
    parallel_delete,
    sequential_delete,
    parallel_query,
    sequential_query,
    parallel_query_with_order,
    sequential_query_with_order,
    parallel_insert,
    sequential_insert
);
criterion_main!(benches);
