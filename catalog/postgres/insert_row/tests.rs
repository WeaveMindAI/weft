//! PostgresInsertRow self-tests: the built SQL (layer 1).

use weft::{NodeTest, WeftResult};

use super::insert_sql;

pub fn tests() -> Vec<NodeTest> {
    vec![NodeTest::basic("builds_a_quoted_parameterized_insert", builds)]
}

fn builds() -> WeftResult<()> {
    let sql = insert_sql("users", &["email".into(), "we\"ird".into()])?;
    assert_eq!(
        sql,
        "INSERT INTO \"users\" (\"email\", \"we\"\"ird\") VALUES ($1, $2) RETURNING *"
    );
    assert!(insert_sql("users", &[]).is_err(), "no columns refuses");
    Ok(())
}
