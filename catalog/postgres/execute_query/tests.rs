//! PostgresExecuteQuery self-tests: the pure parameter/identifier
//! plumbing and everything the connection must refuse BEFORE a dial
//! (fake; the fake rig has no sockets). The query path itself needs a
//! real database and is exercised end to end there.

use serde_json::json;

use weft::{FakeRig, NodeTest, WeftResult};

use tokio_postgres::types::Type;

use super::super::postgres::{params_of, quote_ident};
use super::PostgresExecuteQueryNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::basic("params_map_every_json_shape", params_map),
        NodeTest::basic("identifiers_quote_and_refuse_empty", idents),
        NodeTest::fake("a_non_numeric_port_refuses_before_dialing", bad_port),
        NodeTest::fake("an_unknown_tls_setting_refuses_before_dialing", bad_tls),
    ]
}

/// Encoding a parameter against a column type is what proves the
/// binding: a wrong one refuses here exactly as Postgres would. A
/// count of parameters would pass whatever the bindings were.
fn binds(value: serde_json::Value, ty: &Type) -> bool {
    let mut buf = bytes::BytesMut::new();
    params_of(&[value])[0].to_sql_checked(ty, &mut buf).is_ok()
}

fn params_map() -> WeftResult<()> {
    assert!(binds(json!(true), &Type::BOOL), "a bool binds as bool");
    assert!(binds(json!("text"), &Type::TEXT), "a string binds as text");
    assert!(binds(json!({ "a": 1 }), &Type::JSONB), "an object binds as jsonb");

    // A number binds to the column it meets, because that is what the
    // author wrote it for. `id serial primary key` is int4, so a rule
    // that bound whole numbers as int8 alone would refuse the single
    // commonest query there is (`WHERE id = $1`).
    for ty in [Type::INT2, Type::INT4, Type::INT8, Type::FLOAT4, Type::FLOAT8] {
        assert!(binds(json!(7), &ty), "a whole number binds to a {ty} column");
    }
    assert!(binds(json!(4.5), &Type::FLOAT8), "a fractional number binds as float8");

    // A value the column cannot hold is REFUSED, never wrapped: an id
    // that arrived truncated would match or overwrite a different row.
    assert!(!binds(json!(70_000), &Type::INT2), "a value past int2 refuses");
    assert!(!binds(json!(4.5), &Type::INT4), "a fraction is not a whole number");

    // An id past what a float can hold exactly keeps its value.
    let exact = 9_007_199_254_740_993_i64; // 2^53 + 1
    let mut buf = bytes::BytesMut::new();
    params_of(&[json!(exact)])[0]
        .to_sql_checked(&Type::INT8, &mut buf)
        .expect("binds as int8");
    assert_eq!(i64::from_be_bytes(buf[..].try_into().expect("8 bytes")), exact);
    Ok(())
}

fn idents() -> WeftResult<()> {
    assert_eq!(quote_ident("users")?, "\"users\"");
    assert_eq!(quote_ident("we\"ird")?, "\"we\"\"ird\"");
    assert!(quote_ident("  ").is_err(), "an empty identifier refuses");
    Ok(())
}

/// The connection fields every dial needs, so each refusal test
/// varies exactly the one field it is about.
fn connection(rig: &FakeRig) {
    rig.connection_value("postgres", "host", "db.example.com");
    rig.connection_value("postgres", "database", "app");
    rig.connection_value("postgres", "user", "postgres");
    rig.connection_value("postgres", "password", "secret");
}

async fn run_query(rig: &FakeRig) -> weft::RunOutcome {
    rig.run(
        &PostgresExecuteQueryNode,
        json!({
            "account": rig.access("postgres"),
            "query": "SELECT 1",
        }),
    )
    .await
}

async fn bad_port(rig: FakeRig) -> WeftResult<()> {
    connection(&rig);
    rig.connection_value("postgres", "port", "not-a-number");
    let err = run_query(&rig).await.result.expect_err("a bad port must refuse").to_string();
    assert!(err.contains("port is not a number"), "{err}");
    Ok(())
}

async fn bad_tls(rig: FakeRig) -> WeftResult<()> {
    connection(&rig);
    rig.connection_value("postgres", "sslmode", "sometimes");
    let err = run_query(&rig).await.result.expect_err("a bad TLS setting must refuse").to_string();
    assert!(err.contains("TLS setting"), "{err}");
    Ok(())
}
