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

use super::postgres::{connect, placeholders, query_json, script_json, steps_json, Statement};

#[derive(NodeManifest)]
pub struct PostgresExecuteQueryNode;

#[cfg(feature = "node-tests")]
mod tests;

/// How one firing runs, decided from the query text alone, BEFORE
/// any connection is opened: a query that cannot run is refused
/// without a dial, and the refusal names the placeholder at fault.
#[derive(Debug, PartialEq)]
pub enum Plan {
    /// One statement: `names[i]` is the port that `$(i + 1)` in `sql`
    /// reads, so the values bind in that order and a refusal from the
    /// driver (which only ever says "parameter 3") can be repeated
    /// back to the author in their own words.
    Query { sql: String, names: Vec<String> },
    /// Several statements naming no port: sent whole, verbatim,
    /// through the simple protocol.
    Script { sql: String },
    /// Several statements, at least one naming a port: each runs on
    /// its own with the ports it names, in one transaction.
    Steps(Vec<Statement>),
}

/// A driver error with the port name spliced in wherever it names a
/// parameter by position. `$1` is weft's numbering, not the author's:
/// on its own, "error serializing parameter 1" points at a thing that
/// does not appear anywhere in their source.
fn name_the_ports(error: weft::error::WeftError, names: &[String]) -> weft::error::WeftError {
    let text = error.to_string();
    let Some(rest) = text.split("parameter ").nth(1) else {
        return error;
    };
    let digits: String = rest.chars().take_while(char::is_ascii_digit).collect();
    let Some(name) = digits.parse::<usize>().ok().and_then(|n| names.get(n.wrapping_sub(1))) else {
        return error;
    };
    weft::error::node_error(format!("{text} (parameter {digits} is the `{name}` port, `${name}`)"))
}

/// What one firing runs, decided from the query text alone. Pure, and
/// BEFORE any connection is opened, so a query that cannot run is
/// refused without a dial.
pub fn plan(query: &str) -> WeftResult<Plan> {
    let mut parsed = placeholders(query)?;
    if parsed.statements.len() == 1 {
        let Statement { sql, names } = parsed.statements.remove(0);
        return Ok(Plan::Query { sql, names });
    }
    if parsed.names().is_empty() {
        // The scanner's own copy of the text, so one function decides
        // what reaches the server on both paths.
        return Ok(Plan::Script { sql: parsed.verbatim });
    }
    Ok(Plan::Steps(parsed.statements))
}

/// The ports a plan reads, first appearance first, each once: what
/// the wired ports have to match both ways.
fn ports_read(plan: &Plan) -> Vec<String> {
    match plan {
        Plan::Query { names, .. } => names.clone(),
        Plan::Script { .. } => Vec::new(),
        Plan::Steps(steps) => {
            let mut out: Vec<String> = Vec::new();
            for s in steps {
                for n in &s.names {
                    if !out.contains(n) {
                        out.push(n.clone());
                    }
                }
            }
            out
        }
    }
}

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
                        .expect("for_holes proved every named port arrived")
                })
                .collect()
        };

        let conn = ctx.open(&account).await?;
        let mut client = connect(&ctx, &conn).await?;
        let rows = match plan {
            Plan::Query { sql, names } => query_json(&client, &sql, &bind(&names))
                .await
                .map_err(|e| name_the_ports(e, &names))?,
            Plan::Script { sql } => script_json(&client, &sql).await?,
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
        let output = ctx.fan_declared(&first).set("rows", json!(rows)).set("count", count);
        ctx.pulse_downstream(output).await
    }
}
