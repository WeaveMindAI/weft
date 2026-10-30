//! PostgresExecuteQuery: run SQL against the wired database and emit
//! the answered rows.
//!
//! The query's parameters are the node's own input ports, declared
//! inline and read by name in the SQL:
//!
//!     lookup = PostgresExecuteQuery(userId: String) -> (row: User) {
//!       account: db.access
//!       query: "SELECT * FROM users WHERE telegram_id = $userId"
//!     }
//!
//! Every `$name` binds the port of that name (typed the way the
//! module doc of `postgres.rs` says), so a value from the graph is a
//! wire into a port, never a list somebody has to assemble first.
//! Several statements in one query run as a script: one that names
//! no port goes to the server whole through the simple protocol; one
//! that does runs statement by statement through the extended
//! protocol, each with the ports it names, inside one transaction.
//! Either way the rows of the LAST statement come back.
//!
//! They come back twice from the one result: every row on `rows`, the
//! first on `row`. Both are narrowable, so a program that knows the
//! shape declares it (`-> (row: User)` with `type User = { id: String,
//! name: String, .. }`) and the emit check holds every row to it. With
//! no rows `row` closes, unless the program typed it to admit null
//! (`-> (row: User | Null)`), which reads as "no such row" instead.

use async_trait::async_trait;
use serde_json::Value;

use weft::{Access, RunContext, Node, NodeManifest, NodeOutput, WeftResult};

use super::postgres::{connect, plan, ports_read, Plan, Statement, WrittenBy};

#[derive(NodeManifest)]
pub struct PostgresExecuteQueryNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for PostgresExecuteQueryNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        let query: String = ctx.inputs.get("query")?;
        let plan = plan(&query)?;
        // The placeholders and the inputs the program added have to
        // match both ways. The compiler checks the query written in the
        // source (`placeholdersAreInputs`); one arriving on a wire is
        // checked here, by the ctx, which names whichever side is
        // short. A script reads no port, so it declares none.
        let names = ports_read(&plan);
        let values: Vec<&Value> = ctx.inputs.for_holes(
            &names,
            "query",
            |name| format!("${name}"),
            |name| format!("PostgresExecuteQuery({name}: String) {{ ... }}"),
        )?;
        // The value each port carries, so a statement can pick its
        // own in its own order.
        let by_name: Vec<(&str, Value)> =
            names.iter().map(String::as_str).zip(values.into_iter().cloned()).collect();
        let bind = |wanted: &[String]| -> Vec<Value> {
            wanted
                .iter()
                .map(|n| {
                    by_name
                        .iter()
                        .find(|(name, _)| *name == n.as_str())
                        .map(|(_, v)| v.clone())
                        .expect("for_holes answered every hole the plan names")
                })
                .collect()
        };

        let mut client = connect(&ctx, &account).await?;
        let rows = match plan {
            Plan::Query { sql, names } => client.query_json(&sql, &names, &bind(&names), WrittenBy::Author).await?,
            Plan::Script { head, last } => client.script_json(&head, &last).await?,
            Plan::Steps(steps) => {
                let bound: Vec<(Statement, Vec<Value>)> =
                    steps.into_iter().map(|s| { let v = bind(&s.names); (s, v) }).collect();
                client.steps_json(&bound).await?
            }
        };
        // A wire carries at most 100 KB, and the SQL is where the bound
        // goes: said here, before the emission, with the fix.
        if let Some(weight) = weft::storage::over_wire(&rows) {
            weft::node_bail!(
                "the query answered {} rows weighing {weight}. Add a `LIMIT` and read the rest a page \
                 at a time (`OFFSET`, or `WHERE id > $after` with the last id of the page before), \
                 or select only the columns you need. Anything the statement wrote stays written",
                rows.len()
            );
        }
        emit(&ctx, rows).await
    }
}

/// Send one answered result ([`answer`]), reading off the program's
/// declaration of `row` whether it takes `null`.
pub async fn emit(ctx: &RunContext, rows: Vec<Value>) -> WeftResult<()> {
    let row_admits_null = ctx.output_type("row")?.contains_null();
    ctx.pulse_downstream(answer(rows, row_admits_null)).await
}

/// What one answered result emits: `rows` (`[]` for none) and `count`
/// always, and `row` the first row. With no rows, `row` carries `null`
/// when the program typed it to admit null, and is left out otherwise,
/// which closes it.
pub fn answer(rows: Vec<Value>, row_admits_null: bool) -> NodeOutput {
    let mut output = NodeOutput::new().set("count", rows.len() as f64);
    match rows.first() {
        Some(first) => output = output.set("row", first.clone()),
        None if row_admits_null => output = output.set("row", Value::Null),
        None => {}
    }
    output.set("rows", Value::Array(rows))
}
