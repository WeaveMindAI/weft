//! PostgresWatchQuery self-tests: the pure pieces (what may be
//! watched, the cadence, what counts as a change) and every refusal
//! before a dial (fake; the fake rig has no sockets). The live feed
//! itself needs a real database and a run that is cancelled from the
//! outside, which the live rig cannot do yet; it is proven end to end
//! by a program.

use serde_json::json;

use weft::{FakeRig, NodeTest, WeftResult};

use super::{cadence, changed, watched, PostgresWatchQueryNode};

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::basic("only_one_statement_can_be_watched", one_statement),
        NodeTest::basic("the_cadence_is_at_least_a_second", cadence_floor),
        NodeTest::basic("the_first_result_and_every_move_count_as_a_change", changes),
        NodeTest::fake("a_script_refuses_before_dialing", script_refused),
        NodeTest::fake("a_placeholder_without_a_port_refuses_before_dialing", missing_port),
        NodeTest::fake("a_bad_cadence_refuses_before_dialing", bad_cadence),
        NodeTest::basic("a_declared_column_carries_that_column_of_the_first_row", first_row_split),
    ]
}

/// The live-counter shape: an output port named after a column carries
/// that column's VALUE, so a consumer reads `7` instead of unwrapping
/// `[{"count": 7}]`. A tick with no rows says nothing on those ports
/// rather than closing them, because the next tick may answer one and a
/// closed port would end the feed.
fn first_row_split() -> WeftResult<()> {
    let rows = vec![json!({ "count": 7, "name": "ada" }), json!({ "count": 9, "name": "bo" })];
    let cols = ["count".to_string(), "name".to_string()];
    assert_eq!(
        super::first_row_columns(&rows, &cols),
        vec![("count".to_string(), json!(7)), ("name".to_string(), json!("ada"))],
        "the FIRST row, not the last and not all of them"
    );

    assert!(super::first_row_columns(&[], &cols).is_empty(), "no rows, nothing to say");

    // Declaring nothing is the ordinary case, and it costs nothing.
    assert!(super::first_row_columns(&rows, &[]).is_empty());
    Ok(())
}

fn one_statement() -> WeftResult<()> {
    let (sql, names) = watched("SELECT * FROM cards WHERE owner = $owner")?;
    assert_eq!(sql, "SELECT * FROM cards WHERE owner = $1");
    assert_eq!(names, vec!["owner".to_string()]);
    let err = watched("CREATE TABLE a (x int); SELECT * FROM a").unwrap_err().to_string();
    assert!(err.contains("one statement"), "{err}");
    Ok(())
}

fn cadence_floor() -> WeftResult<()> {
    assert_eq!(cadence(5.0)?, std::time::Duration::from_secs(5));
    assert_eq!(cadence(1.5)?, std::time::Duration::from_millis(1500));
    assert!(cadence(0.2).is_err());
    assert!(cadence(f64::NAN).is_err());
    Ok(())
}

fn changes() -> WeftResult<()> {
    let rows = vec![json!({ "id": 1 })];
    assert!(changed(None, &rows), "the feed opens with what is there");
    assert!(changed(None, &[]), "an empty first result is still the first message");
    assert!(!changed(Some(&rows), &rows));
    assert!(changed(Some(&rows), &[json!({ "id": 1 }), json!({ "id": 2 })]));
    Ok(())
}

/// The connection fields every dial needs.
fn connection(rig: &FakeRig) {
    rig.connection_value("postgres", "host", "db.example.com");
    rig.connection_value("postgres", "database", "app");
    rig.connection_value("postgres", "user", "postgres");
    rig.connection_value("postgres", "password", "secret");
}

async fn script_refused(rig: FakeRig) -> WeftResult<()> {
    connection(&rig);
    let err = rig
        .run(
            &PostgresWatchQueryNode,
            json!({ "account": rig.access("postgres"), "query": "SELECT 1; SELECT 2", "intervalSecs": 5 }),
        )
        .await
        .result
        .expect_err("a script cannot be watched")
        .to_string();
    assert!(err.contains("one statement"), "{err}");
    Ok(())
}

async fn missing_port(rig: FakeRig) -> WeftResult<()> {
    connection(&rig);
    let err = rig
        .run(
            &PostgresWatchQueryNode,
            json!({ "account": rig.access("postgres"), "query": "SELECT * FROM t WHERE id = $id", "intervalSecs": 5 }),
        )
        .await
        .result
        .expect_err("the placeholder names a port that is not there")
        .to_string();
    assert!(err.contains("$id"), "{err}");
    Ok(())
}

async fn bad_cadence(rig: FakeRig) -> WeftResult<()> {
    connection(&rig);
    let err = rig
        .run(
            &PostgresWatchQueryNode,
            json!({ "account": rig.access("postgres"), "query": "SELECT 1", "intervalSecs": 0 }),
        )
        .await
        .result
        .expect_err("a cadence under a second refuses")
        .to_string();
    assert!(err.contains("intervalSecs must be at least 1"), "{err}");
    Ok(())
}
