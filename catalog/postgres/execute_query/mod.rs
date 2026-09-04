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
//! Several statements in one query run as a script through the
//! simple protocol, which takes no parameters, so a script and
//! placeholders cannot mix.

use std::collections::BTreeMap;

use async_trait::async_trait;
use serde_json::{json, Value};

use weft::node::NodeOutput;
use weft::{Access, ExecutionContext, Node, NodeManifest, WeftResult};

use super::postgres::{connect, placeholders, query_json, script_json};

#[derive(NodeManifest)]
pub struct PostgresExecuteQueryNode;

#[cfg(feature = "node-tests")]
mod tests;

/// How one firing runs: the SQL to send and the values to bind, in
/// placeholder order, or a script to run whole. Pure, decided from
/// the query text and the ports that arrived, BEFORE any connection
/// is opened: a query that cannot run is refused without a dial, and
/// the refusal names the port or placeholder at fault.
#[derive(Debug, PartialEq)]
pub enum Plan {
    /// `names[i]` is the port that `$(i + 1)` in `sql` reads, so the
    /// values bind in that order and a refusal from the driver (which
    /// only ever says "parameter 3") can be repeated back to the
    /// author in their own words.
    Query { sql: String, names: Vec<String> },
    Script { sql: String },
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

/// What one firing runs, decided from the query text alone: a
/// parameterised query and the ports its placeholders name, or a
/// script. Pure, and BEFORE any connection is opened, so a query that
/// cannot run is refused without a dial.
pub fn plan(query: &str) -> WeftResult<Plan> {
    let parsed = placeholders(query)?;
    if parsed.statements > 1 {
        if let Some(first) = parsed.names.first() {
            weft::node_bail!(
                "the query holds {} statements, which run as a script, and a script cannot \
                 take parameters (it reads `${first}`); split the parameterised statement \
                 into its own PostgresExecuteQuery, or inline the value",
                parsed.statements
            );
        }
        // The scanner's own copy of the text, so one function decides
        // what reaches the server on both paths.
        return Ok(Plan::Script { sql: parsed.sql });
    }
    Ok(Plan::Query { sql: parsed.sql, names: parsed.names })
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
        let params: Vec<Value> = match &plan {
            Plan::Query { names, .. } => ctx
                .inputs
                .for_holes(
                    names,
                    "query",
                    |name| format!("${name}"),
                    |name| format!("PostgresExecuteQuery({name}: String) {{ ... }}"),
                )?
                .into_iter()
                .cloned()
                .collect(),
            Plan::Script { .. } => ctx
                .inputs
                .for_holes(
                    &[],
                    "query",
                    |name| format!("${name}"),
                    |name| format!("PostgresExecuteQuery({name}: String) {{ ... }}"),
                )
                .map_err(|e| {
                    weft::error::node_error(format!(
                        "{e} (this query holds several statements, so it runs as a script, and \
                         a script takes no parameters)"
                    ))
                })?
                .into_iter()
                .cloned()
                .collect(),
        };

        let conn = ctx.open(&account).await?;
        let client = connect(&ctx, &conn).await?;
        let rows = match plan {
            Plan::Query { sql, names } => query_json(&client, &sql, &params)
                .await
                .map_err(|e| name_the_ports(e, &names))?,
            Plan::Script { sql } => script_json(&client, &sql).await?,
        };
        let count = rows.len() as f64;
        ctx.pulse_downstream(NodeOutput::new().set("rows", json!(rows)).set("count", count))
            .await
    }
}
