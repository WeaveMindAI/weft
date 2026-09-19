//! PostgresExecuteQuery self-tests: the pure parameter/identifier
//! plumbing, the placeholder reading that turns the node's ports into
//! the query's parameters, and everything the node must refuse BEFORE
//! a dial (fake; the fake rig has no sockets). The query path itself
//! needs a real database and is exercised end to end there.

use serde_json::{json, Value};

use weft::{FakeRig, NodeTest, WeftResult};

use tokio_postgres::types::Type;

use super::super::postgres::{
    params_of, placeholders, plan, quote_ident, refuse_shadowed_columns, refuse_unanswered_ports,
    spell_parameters, Plan, Statement,
};
use super::PostgresExecuteQueryNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::basic("params_map_every_json_shape", params_map),
        NodeTest::basic("identifiers_quote_and_refuse_empty", idents),
        NodeTest::basic("placeholders_become_positions_in_first_use_order", placeholder_order),
        NodeTest::basic("placeholders_leave_quotes_comments_and_dollar_bodies_alone", placeholder_quoting),
        NodeTest::basic("placeholders_count_statements_and_refuse_positions", placeholder_statements),
        NodeTest::basic("plan_reads_the_query_as_a_query_or_a_script", plan_mismatches),
        NodeTest::basic("a_refusal_names_the_port_not_the_position", spelled_parameters),
        NodeTest::basic("a_column_named_like_one_of_our_ports_is_refused", shadowed_columns),
        NodeTest::basic("a_port_no_column_answers_is_refused", unanswered_ports),
        NodeTest::fake("a_non_numeric_port_refuses_before_dialing", bad_port),
        NodeTest::fake("an_unknown_tls_setting_refuses_before_dialing", bad_tls),
        NodeTest::fake("an_unread_port_refuses_before_dialing", unread_port),
        NodeTest::fake("a_script_with_a_placeholder_still_needs_the_port", script_with_placeholder),
        NodeTest::fake("a_placeholder_without_a_port_refuses_before_dialing", missing_port),
        NodeTest::fake("a_declared_port_that_stayed_silent_is_not_a_refusal", silent_optional_port),
    ]
}

/// `photo?: File` on a card sent with no picture: the author said the
/// value may be absent, so the query binds SQL NULL instead of refusing.
/// The refusal is the thing under test, so this asserts the node got
/// PAST the hole check; the dial itself has no socket in a fake rig.
async fn silent_optional_port(rig: FakeRig) -> WeftResult<()> {
    connection(&rig);
    rig.input_type("photo", weft::WeftType::parse("String").expect("String parses"));
    let err = rig
        .run(
            &PostgresExecuteQueryNode,
            json!({
                "account": rig.access("postgres"),
                "query": "INSERT INTO cards (photo) VALUES ($photo)",
            }),
        )
        .await
        .result
        .expect_err("a fake rig has no socket to dial");
    let err = err.to_string();
    assert!(!err.contains("`$photo`"), "the hole check refused a declared port: {err}");
    Ok(())
}

fn placeholder_order() -> WeftResult<()> {
    let p = placeholders("SELECT $b, $a, $b::int FROM t WHERE x = $a")?;
    assert_eq!(p.statements.len(), 1);
    assert_eq!(p.statements[0].sql, "SELECT $1, $2, $1::int FROM t WHERE x = $2");
    assert_eq!(p.statements[0].names, vec!["b", "a"]);
    // No placeholder at all is a plain query.
    let none = placeholders("SELECT 1")?;
    assert_eq!(none.statements, vec![Statement { sql: "SELECT 1".into(), names: vec![] }]);
    // Each statement numbers its own ports from $1, and a port two
    // statements read binds once per statement.
    let two = placeholders("UPDATE t SET n = $n WHERE id = $id; SELECT * FROM t WHERE id = $id")?;
    assert_eq!(two.statements.len(), 2);
    assert_eq!(two.statements[0].sql, "UPDATE t SET n = $1 WHERE id = $2");
    assert_eq!(two.statements[0].names, vec!["n", "id"]);
    assert_eq!(two.statements[1].sql, "SELECT * FROM t WHERE id = $1");
    assert_eq!(two.statements[1].names, vec!["id"]);
    assert_eq!(two.names(), vec!["n", "id"]);
    Ok(())
}

fn placeholder_quoting() -> WeftResult<()> {
    // A `$` inside a string, a quoted identifier, either comment, a
    // dollar-quoted body, or an identifier is SQL, never a parameter;
    // only the real one moves.
    let sql = "SELECT '$not', \"$col\", x$y, E'\\'$no', $$ $body; $$, $fn$ $inner; $fn$ \
               -- $line; \n /* $block; /* nested */ */ FROM t WHERE id = $real";
    let p = placeholders(sql)?;
    assert_eq!(p.statements.len(), 1, "the `;` inside quotes and comments separate nothing");
    let s = &p.statements[0];
    assert_eq!(s.names, vec!["real"], "{}", s.sql);
    assert!(s.sql.ends_with("id = $1"), "{}", s.sql);
    assert!(s.sql.contains("'$not'") && s.sql.contains("\"$col\"") && s.sql.contains("x$y"));
    assert!(s.sql.contains("$$ $body; $$") && s.sql.contains("$fn$ $inner; $fn$"));
    assert!(s.sql.contains("-- $line;") && s.sql.contains("/* $block; /* nested */ */"));
    // Unclosed quoting is a loud error, not a silent parameter.
    assert!(placeholders("SELECT $$ open").is_err());
    assert!(placeholders("SELECT 'open").is_err());
    Ok(())
}

fn placeholder_statements() -> WeftResult<()> {
    let script = placeholders("CREATE TABLE a (x int);\nCREATE TABLE b (y int);\n")?;
    assert_eq!(script.statements.len(), 2, "a trailing `;` counts no empty statement");
    assert_eq!(placeholders("DO $$ BEGIN NULL; END $$;")?.statements.len(), 1);
    let err = placeholders("SELECT * FROM t WHERE id = $1").expect_err("positions are gone").to_string();
    assert!(err.contains("$1") && err.contains("name it"), "{err}");
    // A name that could never be a port is the same refusal.
    let err = placeholders("SELECT * FROM t WHERE id = $1st").expect_err("not a port name").to_string();
    assert!(err.contains("$1st"), "{err}");
    // In a SCRIPT that binds no port nothing is renumbered, so `$1` is
    // the author's own (a PREPARE) and stays.
    let script = placeholders("PREPARE find AS SELECT * FROM t WHERE id = $1; EXECUTE find(3)")?;
    assert_eq!(script.statements.len(), 2);
    assert!(
        script.statements[0].sql.contains("$1") && script.names().is_empty(),
        "{}",
        script.statements[0].sql
    );
    // A script that binds a port cannot also carry a `$1`.
    let err = placeholders("INSERT INTO t VALUES ($x); PREPARE f AS SELECT $1")
        .expect_err("mixed")
        .to_string();
    assert!(err.contains("mixes") && err.contains("$1"), "{err}");
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
    assert_eq!(
        plan("CREATE TABLE a (x int); CREATE TABLE b (y int)")?,
        Plan::Script {
            head: vec!["CREATE TABLE a (x int)".into()],
            last: "CREATE TABLE b (y int)".into(),
        },
        "no port named anywhere: the leading statements go whole, the last one alone so \
         its rows come back typed"
    );
    assert_eq!(
        plan("CREATE TABLE a (x int); INSERT INTO a VALUES ($x)")?,
        Plan::Steps(vec![
            Statement { sql: "CREATE TABLE a (x int)".into(), names: vec![] },
            Statement { sql: "INSERT INTO a VALUES ($1)".into(), names: vec!["x".into()] },
        ]),
        "a port named in a script: statement by statement, each with its own ports"
    );
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

    // A string binds against ANY column, in text format, so a cast
    // placeholder (`$when::timestamptz`) and a uuid column both take
    // it as Postgres takes a quoted literal: the bytes sent are the
    // text itself.
    for ty in [Type::TIMESTAMPTZ, Type::UUID, Type::NUMERIC, Type::JSONB, Type::DATE] {
        assert!(binds(json!("2024-05-01T10:00:00Z"), &ty), "a string binds to a {ty} column");
    }
    let mut buf = bytes::BytesMut::new();
    let text = params_of(&[json!("2024-05-01T10:00:00Z")]).remove(0);
    text.to_sql_checked(&Type::TIMESTAMPTZ, &mut buf).expect("binds");
    assert_eq!(&buf[..], b"2024-05-01T10:00:00Z", "sent as text, parsed by the database");
    assert!(matches!(text.encode_format(&Type::TIMESTAMPTZ), tokio_postgres::types::Format::Text));
    assert!(matches!(params_of(&[json!(1)]).remove(0).encode_format(&Type::INT4), tokio_postgres::types::Format::Binary), "numbers stay binary");

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

/// A script that names a port binds it like a single query does, so
/// the port has to arrive; with it missing the refusal names it, and
/// nothing is dialed either way (the fake rig has no sockets, so a
/// plan that passed the port check fails at the dial, which proves
/// the script was accepted).
async fn script_with_placeholder(rig: FakeRig) -> WeftResult<()> {
    connection(&rig);
    let err = rig
        .run(
            &PostgresExecuteQueryNode,
            json!({
                "account": rig.access("postgres"),
                "query": "CREATE TABLE a (x int); INSERT INTO a VALUES ($x)",
            }),
        )
        .await
        .result
        .expect_err("the named port must arrive")
        .to_string();
    assert!(err.contains("`$x`") && err.contains("declare"), "{err}");
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
        .expect_err("the fake rig has no database to dial")
        .to_string();
    assert!(!err.contains("script"), "the script itself was accepted: {err}");
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

/// A database refusal in the author's own words. Postgres counts
/// parameters; the author named them, and the number it prints appears
/// nowhere in their file. The one that costs the most time is a
/// parameter the server cannot place: it fails at run time, deep in
/// whatever loop was running, and the fix (a cast) is not guessable
/// from the text.
fn spelled_parameters() -> WeftResult<()> {
    let names: Vec<String> = ["id", "rude", "sure", "reason"].iter().map(|s| s.to_string()).collect();

    let cannot_place = spell_parameters(
        "db error: ERROR: could not determine data type of parameter $4",
        &names,
    );
    assert!(cannot_place.contains("$reason"), "{cannot_place}");
    assert!(!cannot_place.contains("$4"), "the number is replaced, not appended: {cannot_place}");
    assert!(cannot_place.contains("::text"), "the fix is named: {cannot_place}");

    // The other spelling, from a message about the VALUE rather than
    // the SQL. Same answer, so the author never meets a bare number.
    let serializing = spell_parameters("error serializing parameter 2", &names);
    assert_eq!(serializing, "error serializing parameter $rude");

    // A number that is not a parameter index stays exactly as written:
    // a dollar-quoted body, a position past the end, a plain count.
    let untouched = spell_parameters("syntax error near $$ and $9, 3 rows", &names);
    assert_eq!(untouched, "syntax error near $$ and $9, 3 rows");

    // Nothing named means nothing to say back, and the message has to
    // survive that unchanged rather than come out mangled.
    let no_ports = spell_parameters("ERROR: relation \"cards\" does not exist", &[]);
    assert_eq!(no_ports, "ERROR: relation \"cards\" does not exist");
    Ok(())
}

/// `select count(*) as count` asks for a port this node already
/// answers with the number of rows. Nothing downstream could tell the
/// two apart (both are Numbers, and the node's own value is written
/// last), so an API served `1` for a page of nine cards. It refuses
/// now, and says where to put the column instead.
/// The mirror: a declared port the SELECT answers no column for. It
/// used to close in silence and skip everything behind it.
fn unanswered_ports() -> WeftResult<()> {
    let declared = |names: &[&str]| -> std::collections::HashMap<String, weft::WeftType> {
        names
            .iter()
            .map(|n| ((*n).to_string(), weft::WeftType::parse("String").expect("String parses")))
            .collect()
    };
    let row = json!({ "id": 1, "name": "ada", "note": "hi" });

    let missed = refuse_unanswered_ports(&row, &declared(&["card", "rows", "count"]), &["rows", "count"])
        .expect_err("no column called 'card'");
    let said = missed.to_string();
    assert!(said.contains("'card'"), "it names the port: {said}");
    assert!(said.contains("'name'"), "it names what the query did answer: {said}");
    assert!(!said.contains("'rows'"), "the node's own ports are not columns: {said}");

    // A port that matches a column, and the node's own ports, are fine.
    refuse_unanswered_ports(&row, &declared(&["name", "rows", "count"]), &["rows", "count"])?;

    // No rows means every declared port closes, and that IS the right
    // reading of a query that answered nothing.
    refuse_unanswered_ports(&Value::Null, &declared(&["card"]), &["rows", "count"])?;
    Ok(())
}

fn shadowed_columns() -> WeftResult<()> {
    let clash = refuse_shadowed_columns(&json!({ "count": 9, "title": "ada" }), &["rows", "count"])
        .expect_err("a column called 'count' has nowhere to go");
    let said = clash.to_string();
    assert!(said.contains("'count'"), "it names the column: {said}");
    assert!(said.contains("as count_value"), "it names the fix: {said}");

    // Every other name is exactly what the added ports are for.
    refuse_shadowed_columns(&json!({ "total": 9, "title": "ada" }), &["rows", "count"])?;

    // A query that answered nothing has no columns to clash, and a
    // node with no ports of its own can never clash at all.
    refuse_shadowed_columns(&Value::Null, &["rows", "count"])?;
    refuse_shadowed_columns(&json!({ "count": 9 }), &[])?;
    Ok(())
}
