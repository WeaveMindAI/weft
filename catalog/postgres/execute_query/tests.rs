//! PostgresExecuteQuery self-tests: the pure parameter/identifier
//! plumbing, the placeholder reading that turns the node's ports into
//! the query's parameters, and everything the node must refuse BEFORE
//! a dial (fake; the fake rig has no sockets). The query path itself
//! needs a real database and is exercised end to end there.

use std::collections::BTreeMap;

use serde_json::{json, Value};

use weft::{FakeRig, NodeTest, WeftResult};

use tokio_postgres::types::Type;

use super::super::postgres::{params_of, placeholders, quote_ident};
use super::{plan, Plan, PostgresExecuteQueryNode};

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::basic("params_map_every_json_shape", params_map),
        NodeTest::basic("identifiers_quote_and_refuse_empty", idents),
        NodeTest::basic("placeholders_become_positions_in_first_use_order", placeholder_order),
        NodeTest::basic("placeholders_leave_quotes_comments_and_dollar_bodies_alone", placeholder_quoting),
        NodeTest::basic("placeholders_count_statements_and_refuse_positions", placeholder_statements),
        NodeTest::basic("plan_reads_the_query_as_a_query_or_a_script", plan_mismatches),
        NodeTest::fake("a_non_numeric_port_refuses_before_dialing", bad_port),
        NodeTest::fake("an_unknown_tls_setting_refuses_before_dialing", bad_tls),
        NodeTest::fake("an_unread_port_refuses_before_dialing", unread_port),
        NodeTest::fake("a_script_with_a_placeholder_refuses_before_dialing", script_with_placeholder),
        NodeTest::fake("a_placeholder_without_a_port_refuses_before_dialing", missing_port),
    ]
}

fn placeholder_order() -> WeftResult<()> {
    let p = placeholders("SELECT $b, $a, $b::int FROM t WHERE x = $a")?;
    assert_eq!(p.sql, "SELECT $1, $2, $1::int FROM t WHERE x = $2");
    assert_eq!(p.names, vec!["b", "a"]);
    assert_eq!(p.statements, 1);
    // No placeholder at all is a plain query.
    let none = placeholders("SELECT 1")?;
    assert_eq!((none.sql.as_str(), none.names.len(), none.statements), ("SELECT 1", 0, 1));
    Ok(())
}

fn placeholder_quoting() -> WeftResult<()> {
    // A `$` inside a string, a quoted identifier, either comment, a
    // dollar-quoted body, or an identifier is SQL, never a parameter;
    // only the real one moves.
    let sql = "SELECT '$not', \"$col\", x$y, E'\\'$no', $$ $body; $$, $fn$ $inner; $fn$ \
               -- $line; \n /* $block; /* nested */ */ FROM t WHERE id = $real";
    let p = placeholders(sql)?;
    assert_eq!(p.names, vec!["real"], "{}", p.sql);
    assert!(p.sql.ends_with("id = $1"), "{}", p.sql);
    assert!(p.sql.contains("'$not'") && p.sql.contains("\"$col\"") && p.sql.contains("x$y"));
    assert!(p.sql.contains("$$ $body; $$") && p.sql.contains("$fn$ $inner; $fn$"));
    assert_eq!(p.statements, 1, "the `;` inside quotes and comments separate nothing");
    // Unclosed quoting is a loud error, not a silent parameter.
    assert!(placeholders("SELECT $$ open").is_err());
    assert!(placeholders("SELECT 'open").is_err());
    Ok(())
}

fn placeholder_statements() -> WeftResult<()> {
    let script = placeholders("CREATE TABLE a (x int);\nCREATE TABLE b (y int);\n")?;
    assert_eq!(script.statements, 2, "a trailing `;` counts no empty statement");
    assert_eq!(placeholders("DO $$ BEGIN NULL; END $$;")?.statements, 1);
    let err = placeholders("SELECT * FROM t WHERE id = $1").expect_err("positions are gone").to_string();
    assert!(err.contains("$1") && err.contains("name it"), "{err}");
    // A name that could never be a port is the same refusal.
    let err = placeholders("SELECT * FROM t WHERE id = $1st").expect_err("not a port name").to_string();
    assert!(err.contains("$1st"), "{err}");
    // In a SCRIPT the SQL is sent verbatim, so `$1` is the author's
    // own (a PREPARE) and stays.
    let script = placeholders("PREPARE find AS SELECT * FROM t WHERE id = $1; EXECUTE find(3)")?;
    assert_eq!(script.statements, 2);
    assert!(script.sql.contains("$1") && script.names.is_empty(), "{}", script.sql);
    // Nothing to run is a refusal, not an empty answer.
    let err = placeholders("-- fill this in later").expect_err("an empty query is refused").to_string();
    assert!(err.contains("empty"), "{err}");
    Ok(())
}

fn plan_mismatches() -> WeftResult<()> {
    assert_eq!(
        plan("UPDATE t SET n = $n WHERE id = $id")?,
        Plan::Query {
            sql: "UPDATE t SET n = $1 WHERE id = $2".into(),
            names: vec!["n".into(), "id".into()],
        },
        "the ports bind in placeholder order, whatever order they arrived in"
    );
    assert!(matches!(
        plan("CREATE TABLE a (x int); CREATE TABLE b (y int)")?,
        Plan::Script { .. }
    ));
    let script_param = plan("CREATE TABLE a (x int); INSERT INTO a VALUES ($x)")
        .expect_err("a script cannot bind")
        .to_string();
    assert!(script_param.contains("script") && script_param.contains("$x"), "{script_param}");
    Ok(())
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

async fn unread_port(rig: FakeRig) -> WeftResult<()> {
    connection(&rig);
    let err = rig
        .run(
            &PostgresExecuteQueryNode,
            json!({ "account": rig.access("postgres"), "query": "SELECT 1", "who": "u1" }),
        )
        .await
        .result
        .expect_err("a wired port the SQL never reads must refuse")
        .to_string();
    assert!(err.contains("`who`") && err.contains("never reads"), "{err}");
    Ok(())
}

async fn script_with_placeholder(rig: FakeRig) -> WeftResult<()> {
    connection(&rig);
    let err = rig
        .run(
            &PostgresExecuteQueryNode,
            json!({
                "account": rig.access("postgres"),
                "query": "CREATE TABLE a (x int); INSERT INTO a VALUES ($x)",
                "x": 1,
            }),
        )
        .await
        .result
        .expect_err("a script cannot take a parameter")
        .to_string();
    assert!(err.contains("script") && err.contains("$x"), "{err}");
    Ok(())
}

/// A `$name` the ports never carried is named, with the header to
/// write. The mirror of `unread_port`.
async fn missing_port(rig: FakeRig) -> WeftResult<()> {
    connection(&rig);
    let err = rig
        .run(
            &PostgresExecuteQueryNode,
            json!({
                "account": rig.access("postgres"),
                "query": "SELECT * FROM users WHERE id = $user_id",
            }),
        )
        .await
        .result
        .expect_err("a placeholder with no port must refuse")
        .to_string();
    assert!(err.contains("`$user_id`") && err.contains("declare"), "{err}");
    Ok(())
}

async fn bad_tls(rig: FakeRig) -> WeftResult<()> {
    connection(&rig);
    rig.connection_value("postgres", "sslmode", "sometimes");
    let err = run_query(&rig).await.result.expect_err("a bad TLS setting must refuse").to_string();
    assert!(err.contains("TLS setting"), "{err}");
    Ok(())
}
