//! PostgresInsertRow self-tests: the built SQL (layer 1), and how a
//! table column it could not read back is refused.

use tokio_postgres::types::{Kind, Type};
use weft::{NodeTest, WeftError, WeftResult};

use super::super::postgres::{readable, WrittenBy};
use super::insert_sql;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::basic("builds_a_quoted_parameterized_insert", builds),
        NodeTest::basic("a_column_it_cannot_read_back_refuses_without_a_cast_hint", unreadable_column),
    ]
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

/// The node writes its own `RETURNING *`, so a refusal there cannot tell
/// the author to cast in a query they never wrote: it names the column
/// and its type, says nothing was written, and points at the node where
/// they write the SQL.
fn unreadable_column() -> WeftResult<()> {
    let vector = Type::new("vector".into(), 16_385, Kind::Simple, "public".into());
    let sparse = Type::new("sparsevec".into(), 16_400, Kind::Simple, "public".into());
    assert!(readable([("embedding", &vector)]).is_ok(), "a vector is read back");
    let err = readable([("id", &Type::INT4), ("weights", &sparse)]).expect_err("sparsevec refuses").refusal(WrittenBy::Node);
    assert!(matches!(err, WeftError::Input(_)), "the program's or table's shape: {err:?}");
    let err = err.to_string();
    assert!(err.contains("'weights'") && err.contains("sparsevec"), "{err}");
    assert!(err.contains("nothing was written") && err.contains("Postgres: Query"), "{err}");
    assert!(!err.contains("cast it in the query"), "no query of the author's to cast in: {err}");
    assert!(err.contains("RETURNING \"weights\"::text") && !err.contains("id,"), "names no column the table may lack: {err}");
    let err = readable([("user", &sparse)]).expect_err("sparsevec refuses").refusal(WrittenBy::Node).to_string();
    assert!(err.contains("RETURNING \"user\"::text"), "a name Postgres reads as something else is quoted: {err}");
    Ok(())
}
