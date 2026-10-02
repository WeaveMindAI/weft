//! PostgresUpdateRows: update matching rows, SET columns from a JSON
//! object, WHERE from a SQL condition whose `$name` placeholders read
//! the node's own input ports (as in PostgresExecuteQuery), and emit
//! the changed rows (RETURNING *).

use async_trait::async_trait;
use serde_json::{json, Value};

use weft::node::NodeOutput;
use weft::{Access, ExecutionContext, Node, NodeManifest, WeftResult};

use super::postgres::{connect, mistake, placeholders, query_json, quote_ident, Statement};

/// The UPDATE for a table, its SET columns, and a WHERE condition, plus
/// the ports the condition reads in the order its `$1..$k` bind them.
///
/// The condition's `$name` placeholders are numbered by the same
/// reader PostgresExecuteQuery uses (a `$` inside a string, a
/// dollar-quoted body or a comment is left alone), so they take
/// `$1..$k`; the SET placeholders continue after them.
pub fn update_sql(
    table: &str,
    columns: &[String],
    condition: &str,
) -> WeftResult<(String, Vec<String>)> {
    if columns.is_empty() {
        mistake!("set holds no columns; nothing to update");
    }
    if condition.trim().is_empty() {
        mistake!(
            "where is empty; an unconditional update is almost always a mistake, write \
             `true` explicitly to update every row"
        );
    }
    let mut parsed = placeholders(condition)?;
    if parsed.statements.len() != 1 {
        mistake!("where is one SQL condition; this one holds several statements");
    }
    let Statement { sql: condition, names } = parsed.statements.remove(0);
    let first_set_param = names.len() + 1;
    let sets: Vec<String> = columns
        .iter()
        .enumerate()
        .map(|(i, c)| Ok(format!("{} = ${}", quote_ident(c)?, first_set_param + i)))
        .collect::<WeftResult<_>>()?;
    // The condition is PARENTHESISED and put on its own line. Glued
    // inline, a condition ending in a line comment swallows the
    // `RETURNING *` that follows it, and the node then reports zero
    // changed rows having changed them. Wrapped, the same input is a
    // syntax error the database refuses outright.
    let sql = format!(
        "UPDATE {} SET {} WHERE (
{}
) RETURNING *",
        quote_ident(table)?,
        sets.join(", "),
        condition
    );
    Ok((sql, names))
}

#[derive(NodeManifest)]
pub struct PostgresUpdateRowsNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for PostgresUpdateRowsNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        let table: String = ctx.inputs.get("table")?;
        let set: Value = ctx.inputs.get("set")?;
        let condition: String = ctx.inputs.get("where")?;

        let Some(obj) = set.as_object() else {
            mistake!("set must be an object of column: value pairs");
        };
        let columns: Vec<String> = obj.keys().cloned().collect();
        let (sql, names) = update_sql(&table, &columns, &condition)?;
        // The condition's placeholders and the node's own custom ports
        // have to match both ways, as in PostgresExecuteQuery.
        let mut params: Vec<Value> = ctx
            .inputs
            .for_holes(
                &names,
                "where",
                |name| format!("${name}"),
                |name| format!("PostgresUpdateRows({name}: String) {{ ... }}"),
            )?
            .into_iter()
            .cloned()
            .collect();
        params.extend(obj.values().cloned());

        let conn = ctx.open(&account).await?;
        let client = connect(&ctx, &conn).await?;
        let rows = query_json(&client, &sql, &names, &params).await?;
        let count = rows.len() as f64;
        ctx.pulse_downstream(NodeOutput::new().set("rows", json!(rows)).set("count", count))
            .await
    }
}
