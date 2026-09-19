//! PostgresWatchQuery: keep one query's result live on a bus.
//!
//! The query runs at start and again every `intervalSecs`; each time
//! the rows differ from the last run they go out as one `rows` message
//! on the bus. A `channel` adds a second wake: the node LISTENs on it
//! and a `NOTIFY` re-runs the query at once, so a trigger in the
//! database turns the cadence into a backstop. The node runs until
//! the run ends (a caller leaving ends a route's run), and the bus
//! closes with it, which is what the Stream behind it reads as the
//! end of the feed.
//!
//! Parameters bind exactly like PostgresExecuteQuery: every `$name`
//! is the port of that name. One statement only: a script has no one
//! result to watch.
//!
//! An output port named after a COLUMN is its own bus carrying that
//! column out of the first row, so the common live-counter shape
//! (`-> (count: Bus)` over `select count(*)`) sends `7` rather than
//! `[{"count": 7}]` for every consumer to unwrap. Same rule as
//! PostgresExecuteQuery's declared outputs, one result later: the
//! first row's columns by name. A tick whose query answered no rows
//! sends nothing on those ports and does not close them, because the
//! next tick may well answer one.

use async_trait::async_trait;
use serde_json::{json, Value};

use weft::bus::BusOptions;
use weft::{Access, ExecutionContext, Node, NodeErrExt, NodeManifest, WeftResult};

use super::postgres::{
    connect, connect_listening, plan, query_json, refuse_shadowed_columns,
    refuse_unanswered_ports, ListenEvent, Plan,
};

#[derive(NodeManifest)]
pub struct PostgresWatchQueryNode;

#[cfg(feature = "node-tests")]
mod tests;

/// The statement and the ports it reads, or why the query cannot be
/// watched. Pure: what a test proves without a database.
pub fn watched(query: &str) -> WeftResult<(String, Vec<String>)> {
    match plan(query)? {
        Plan::Query { sql, names } => Ok((sql, names)),
        Plan::Script { .. } | Plan::Steps(_) => weft::node_bail!(
            "a watched query is one statement (its result is what is kept live); this SQL \
             holds several. Keep the SELECT here and run the rest with Postgres: Query"
        ),
    }
}

/// The cadence as a duration, refusing what would hammer the database.
pub fn cadence(interval_secs: f64) -> WeftResult<std::time::Duration> {
    if !interval_secs.is_finite() || interval_secs < 1.0 {
        weft::node_bail!("intervalSecs must be at least 1, got {interval_secs}");
    }
    Ok(std::time::Duration::from_secs_f64(interval_secs))
}

#[async_trait]
impl Node for PostgresWatchQueryNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        let query: String = ctx.inputs.get("query")?;
        let interval = cadence(ctx.inputs.get("intervalSecs")?)?;
        let channel: Option<String> = ctx.inputs.opt::<String>("channel")?.filter(|c| !c.trim().is_empty());
        let (sql, names) = watched(&query)?;
        let values: Vec<Value> = ctx
            .inputs
            .for_holes(
                &names,
                "query",
                |name| format!("${name}"),
                |name| format!("PostgresWatchQuery({name}: String) {{ ... }}"),
            )?
            .into_iter()
            .cloned()
            .collect();

        let conn = ctx.open(&account).await?;
        // With a channel the one connection both queries and listens;
        // without one, a plain dial.
        let (client, mut notifications) = match &channel {
            Some(channel) => {
                let (client, rx) = connect_listening(&ctx, &conn, channel).await?;
                (client, Some(rx))
            }
            None => (connect(&ctx, &conn).await?, None),
        };

        let bus = ctx.open_bus("rows", BusOptions::default(), "watch").await?;
        // One bus per declared column port, opened before the first
        // query so a consumer parked on any of them is attached for the
        // opening result.
        let columns = declared_columns(&ctx);
        let mut column_buses = Vec::with_capacity(columns.len());
        for column in &columns {
            column_buses
                .push((column.clone(), ctx.open_bus(column, BusOptions::default(), "watch").await?));
        }
        let cancel = ctx.cancellation();
        let mut last: Option<Vec<Value>> = None;
        loop {
            let rows = query_json(&client, &sql, &names, &values).await?;
            // A column named `rows` asks for the port the whole result
            // already rides on, so it would never reach a reader.
            if let Some(first) = rows.first() {
                refuse_shadowed_columns(first, &["rows"])?;
                refuse_unanswered_ports(first, ctx.declared_outputs(), &["rows"])?;
            }
            if changed(last.as_deref(), &rows) {
                bus.send("rows", json!(rows)).node_err("sending the rows on the bus")?;
                for (column, value) in first_row_columns(&rows, &columns) {
                    let bus = column_buses
                        .iter()
                        .find(|(name, _)| name == &column)
                        .map(|(_, bus)| bus)
                        .expect("a bus per declared column, opened above");
                    bus.send(&column, value).node_err("sending a column on the bus")?;
                }
                last = Some(rows);
            }
            // The next wake: the cadence, a notification, or the run
            // ending. A notification arriving during the query is not
            // lost: the channel buffers it and the next select sees it.
            //
            // Biased, and the cancellation arm goes first on purpose:
            // cancelling drops the listening connection's sender, so
            // at the end of a run both that arm and the ended-stream
            // arm are ready at once. An unbiased select picks between
            // them at random, and half the time a perfectly normal
            // ending would be reported as a dead connection.
            tokio::select! {
                biased;
                _ = cancel.cancelled() => return Ok(()),
                notified = wait_notification(&mut notifications) => {
                    if let Err(reason) = notified {
                        weft::node_bail!("the LISTEN connection ended: {reason}");
                    }
                }
                _ = tokio::time::sleep(interval) => {}
            }
        }
    }
}

/// What each declared column port carries out of a change: that column
/// of the FIRST row, for every column the row actually has.
///
/// A tick whose query answered no rows sends nothing and closes
/// nothing: the next tick may well answer one, and closing would end
/// the feed for good.
fn first_row_columns(rows: &[Value], columns: &[String]) -> Vec<(String, Value)> {
    let Some(first) = rows.first() else { return Vec::new() };
    columns
        .iter()
        .map(|column| {
            let value = first
                .get(column)
                .expect("refuse_unanswered_ports bails on a declared column the first row lacks");
            (column.clone(), value.clone())
        })
        .collect()
}

/// The output ports the author declared beyond `rows`: each names a
/// column of the watched query, and each becomes its own bus.
fn declared_columns(ctx: &ExecutionContext) -> Vec<String> {
    let mut columns: Vec<String> =
        ctx.declared_outputs().keys().filter(|name| *name != "rows").cloned().collect();
    // The declared map has no order of its own; a stable one keeps the
    // bus markers going out the same way on every run, which the
    // journal compares across runs.
    columns.sort();
    columns
}

/// Whether the result moved since the last message. The first result
/// always counts: a feed opens with what is there.
pub fn changed(last: Option<&[Value]>, rows: &[Value]) -> bool {
    last != Some(rows)
}

/// The next notification: `Ok` when one arrived, and the reason the
/// listening connection is gone when it is. Pends forever with no
/// channel, so the cadence alone wakes the loop.
async fn wait_notification(
    notifications: &mut Option<tokio::sync::mpsc::UnboundedReceiver<ListenEvent>>,
) -> Result<(), String> {
    match notifications {
        Some(rx) => match rx.recv().await {
            Some(ListenEvent::Notified(_)) => Ok(()),
            Some(ListenEvent::Failed(reason)) => Err(reason),
            // The sender went without a word, which the driver only
            // does when the client dropped.
            None => Err("the database closed it or the network dropped".into()),
        },
        None => std::future::pending().await,
    }
}
