//! PostgresExecuteQuery: run SQL against the wired database and emit
//! the answered rows.
//!
//! The query's parameters are the node's own input ports, declared
//! inline and read by name in the SQL:
//!
//!     lookup = PostgresExecuteQuery(user_id: String) {
//!       account: db.access
//!       query: "SELECT * FROM users WHERE telegram_id = $user_id"
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
//! Custom output ports (`-> (sheet: String)`) read the first row's
//! columns by name, so a one-row query hands each value out as its
//! own port without a Python node in between.

use async_trait::async_trait;
use serde_json::{json, Value};

use weft::{Access, ExecutionContext, Node, NodeManifest, WeftResult};

use super::postgres::{
    connect, plan, ports_read, query_json, refuse_shadowed_columns, refuse_unanswered_ports,
    script_json, steps_json, Plan,
    Statement,
};

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

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        let query: String = ctx.inputs.get("query")?;
        let plan = plan(&query)?;
        // The placeholders and the node's own custom ports have to
        // match both ways; the ctx owns that matching (the Format node
        // has the same job with `{{name}}` holes) and names whichever
        // side is short. A script reads no port, so it declares none.
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

        let conn = ctx.open(&account).await?;
        let mut client = connect(&ctx, &conn).await?;
        let rows = match plan {
            Plan::Query { sql, names } => query_json(&client, &sql, &names, &bind(&names)).await?,
            Plan::Script { head, last } => script_json(&client, &head, &last).await?,
            Plan::Steps(steps) => {
                let bound: Vec<(Statement, Vec<Value>)> =
                    steps.into_iter().map(|s| { let v = bind(&s.names); (s, v) }).collect();
                steps_json(&mut client, &bound).await?
            }
        };
        let count = rows.len() as f64;
        // Custom outputs read the first row's columns by name; a
        // query that answered no rows leaves them unmentioned, which
        // closes them.
        let first = rows.first().cloned().unwrap_or(Value::Null);
        // A column named like one of this node's own ports would be
        // overwritten by the node a line below, silently.
        refuse_shadowed_columns(&first, &["rows", "count"])?;
        // ...and a port the author declared that no column answers.
        refuse_unanswered_ports(&first, ctx.declared_outputs(), &["rows", "count"])?;
        let output = ctx.fan_declared(&first).set("rows", json!(rows)).set("count", count);
        ctx.pulse_downstream(output).await
    }
}
