//! PostgresWatchQuery: keep one query's result live on a bus.
//!
//! The query runs at start and again every `intervalSecs`; each time
//! the rows differ from the last run they go out as one message on the
//! `rows` bus, and each time the FIRST row differs it goes out on the
//! `row` bus. A `channel` adds a second wake: the node LISTENs on it
//! and a `NOTIFY` re-runs the query at once, so a trigger in the
//! database turns the cadence into a backstop. The node runs until
//! the run ends (a caller leaving ends a route's run), and the buses
//! close with it, which is what the Stream behind one reads as the
//! end of the feed.
//!
//! Parameters bind exactly like PostgresExecuteQuery: every `$name`
//! is the port of that name. One statement only: a script has no one
//! result to watch.
//!
//! `row` is PostgresExecuteQuery's `row` one result later, with one
//! difference a feed forces: a result with no rows sends `null` rather
//! than closing the bus, because closing would end the feed for good
//! while the next run may well answer a row, and sending nothing would
//! leave a reader showing a row that is gone. Neither bus can be
//! declared narrower: a bus carries no type for its messages, so there
//! is nothing for a declaration to hold them to.

use async_trait::async_trait;
use serde_json::{json, Value};

use weft::bus::BusOptions;
use weft::context::PendingBus;
use weft::signal::poll_endpoint::MIN_POLL_INTERVAL_SECS;
use weft::{Access, RunContext, Node, NodeErrExt, NodeManifest, WeftResult};

use super::postgres::{connect_alone, connect_listening, mistake, plan, query_json, ListenEvent, Plan, WrittenBy};

#[derive(NodeManifest)]
pub struct PostgresWatchQueryNode;

#[cfg(feature = "node-tests")]
mod tests;

/// The statement and the ports it reads, or why the query cannot be
/// watched. Pure: what a test proves without a database.
pub fn watched(query: &str) -> WeftResult<(String, Vec<String>)> {
    match plan(query)? {
        Plan::Query { sql, names } => Ok((sql, names)),
        Plan::Script { .. } | Plan::Steps(_) => mistake!(
            "a watched query is one statement (its result is what is kept live); this SQL \
             holds several. Keep the SELECT here and run the rest with Postgres: Query"
        ),
    }
}

/// The cadence as a duration, refusing what would hammer the database:
/// the same floor every polling signal has.
pub fn cadence(interval_secs: f64) -> WeftResult<std::time::Duration> {
    if !interval_secs.is_finite() || interval_secs < MIN_POLL_INTERVAL_SECS as f64 {
        mistake!("intervalSecs must be at least {MIN_POLL_INTERVAL_SECS}, got {interval_secs}");
    }
    Ok(std::time::Duration::from_secs_f64(interval_secs))
}

#[async_trait]
impl Node for PostgresWatchQueryNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
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
        let (mut client, mut notifications) = match &channel {
            Some(channel) => {
                let (client, rx) = connect_listening(&ctx, &conn, channel).await?;
                (client, Some(rx))
            }
            None => (connect_alone(&ctx, &conn).await?, None),
        };

        // Both open with the first result, so a query that fails before
        // it answers is a failure `_error` can catch, and a consumer of
        // either is attached for the opening result.
        let mut rows_bus = read_bus(&ctx, "rows")?;
        let mut row_bus = read_bus(&ctx, "row")?;
        let cancel = ctx.cancellation();
        let mut last_rows: Option<Vec<Value>> = None;
        let mut last_row: Option<Value> = None;
        loop {
            let rows = query_json(&mut client, &sql, &names, &values, WrittenBy::Author).await?;
            let row = first_row(&rows);
            if changed(last_rows.as_deref(), rows.as_slice()) {
                if let Some(bus) = &mut rows_bus {
                    bus.open().await?.send("rows", json!(rows)).node_err("sending the rows on the bus")?;
                }
                last_rows = Some(rows);
            }
            if changed(last_row.as_ref(), &row) {
                if let Some(bus) = &mut row_bus {
                    bus.open().await?.send("row", row.clone()).node_err("sending the first row on the bus")?;
                }
                last_row = Some(row);
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

/// What the `row` bus carries for one result: its first row, or
/// `null` when it answered none.
pub fn first_row(rows: &[Value]) -> Value {
    rows.first().cloned().unwrap_or(Value::Null)
}

/// The bus on `port`, only when something reads it (a journaled bus
/// records every message, so one nobody reads would be written on every
/// change for nothing), opening with the first piece it carries.
fn read_bus<'c>(ctx: &'c RunContext, port: &str) -> WeftResult<Option<PendingBus<'c>>> {
    let journal = ctx.journal_policy()?;
    Ok(ctx.is_output_wired(port).then(|| ctx.pending_bus(port, BusOptions { journal, ..BusOptions::default() }, "watch")))
}

/// Whether a bus's value moved since its last message. The first one
/// always counts: a feed opens with what is there.
pub fn changed<T: PartialEq + ?Sized>(last: Option<&T>, now: &T) -> bool {
    last != Some(now)
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
