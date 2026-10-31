//! PostgresWatchQuery self-tests: the pure pieces (what may be
//! watched, the cadence, what counts as a change) and every refusal
//! before a dial (fake; the fake rig has no sockets). The live feed
//! itself needs a real database and a run that is cancelled from the
//! outside, which the live rig cannot do yet; it is proven end to end
//! by a program.

use async_trait::async_trait;
use serde_json::json;

use weft::{Access, FakeRig, Node, NodeManifest, NodeMetadata, NodeTest, RunContext, WeftResult};

use super::super::postgres::watch_config;
use super::super::postgres_execute_query::PostgresExecuteQueryNode;
use super::{cadence, changed, first_row, watched, PostgresWatchQueryNode};

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::basic("only_one_statement_can_be_watched", one_statement),
        NodeTest::basic("the_cadence_is_at_least_the_poll_floor", cadence_floor),
        NodeTest::basic("the_first_result_and_every_move_count_as_a_change", changes),
        NodeTest::fake("a_script_refuses_before_dialing", script_refused),
        NodeTest::fake("a_placeholder_without_a_port_refuses_before_dialing", missing_port),
        NodeTest::fake("a_bad_cadence_refuses_before_dialing", bad_cadence),
        NodeTest::basic("row_carries_the_first_row_or_null", first_row_or_null),
        NodeTest::basic("the_placeholder_pattern_is_postgres_query_s", same_pattern),
        NodeTest::fake("a_watcher_dials_the_database_s_door_for_watchers_when_it_has_one", watching_door),
    ]
}

/// `row` is the first row, and `null` once the query answers none: the
/// bus stays open for the next run, and a reader is told the row is
/// gone instead of being left on it.
fn first_row_or_null() -> WeftResult<()> {
    let rows = vec![json!({ "count": 7, "name": "ada" }), json!({ "count": 9, "name": "bo" })];
    assert_eq!(first_row(&rows), json!({ "count": 7, "name": "ada" }), "the FIRST row, not the last");
    assert_eq!(first_row(&[]), json!(null), "no rows, null");

    // `row` moves only when the first row does: a change further down
    // the list sends on `rows` alone.
    let more = vec![json!({ "count": 7, "name": "ada" }), json!({ "count": 10, "name": "bo" })];
    assert!(changed(Some(rows.as_slice()), more.as_slice()));
    assert!(!changed(Some(&first_row(&rows)), &first_row(&more)));
    assert!(changed(Some(&first_row(&rows)), &first_row(&[])), "the row going away is news");
    Ok(())
}

/// The compiler finds this node's placeholders with the same pattern
/// as Postgres: Query's, which its own test holds to `placeholders()`.
fn same_pattern() -> WeftResult<()> {
    assert_eq!(
        PostgresWatchQueryNode.manifest().placeholders_are_inputs,
        PostgresExecuteQueryNode.manifest().placeholders_are_inputs,
    );
    assert!(PostgresWatchQueryNode.manifest().placeholders_are_inputs.is_some());
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
    assert_eq!(cadence(7.5)?, std::time::Duration::from_millis(7500));
    assert!(cadence(4.9).is_err());
    assert!(cadence(0.2).is_err());
    assert!(cadence(f64::NAN).is_err());
    Ok(())
}

fn changes() -> WeftResult<()> {
    let rows = vec![json!({ "id": 1 })];
    let empty: Vec<serde_json::Value> = Vec::new();
    assert!(changed(None, rows.as_slice()), "the feed opens with what is there");
    assert!(changed(None, empty.as_slice()), "an empty first result is still the first message");
    assert!(!changed(Some(rows.as_slice()), rows.as_slice()));
    let grown = vec![json!({ "id": 1 }), json!({ "id": 2 })];
    assert!(changed(Some(rows.as_slice()), grown.as_slice()));
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
        .expect_err("a cadence under the poll floor refuses")
        .to_string();
    assert!(err.contains("intervalSecs must be at least 5"), "{err}");
    Ok(())
}

/// The database name a watching run dials on the wired connection, under
/// this node's own manifest: what `watch_config` makes of an opened
/// connection, with no socket behind it.
struct DialsInto(&'static str);

impl NodeManifest for DialsInto {
    fn manifest(&self) -> &'static NodeMetadata {
        PostgresWatchQueryNode.manifest()
    }
}

#[async_trait]
impl Node for DialsInto {
    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        let config = watch_config(&ctx.open(&account).await?)?;
        if config.get_dbname() != Some(self.0) {
            weft::node_bail!("a watcher dials {:?}, not {:?}", config.get_dbname(), self.0);
        }
        Ok(())
    }
}

/// A PostgresDatabase keeps a door apart for watchers, named on its
/// connection, so they never take the connections queries need; any
/// other connection (a PostgresAccess) is dialed as it is.
async fn watching_door(rig: FakeRig) -> WeftResult<()> {
    let inputs = |rig: &FakeRig| json!({ "account": rig.access("postgres"), "query": "SELECT 1", "intervalSecs": 5 });
    connection(&rig);
    rig.run(&DialsInto("app"), inputs(&rig)).await.ok()?;
    rig.connection_value("postgres", "watchDatabase", "app_watch");
    rig.run(&DialsInto("app_watch"), inputs(&rig)).await.ok()?;
    Ok(())
}
