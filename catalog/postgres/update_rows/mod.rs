//! PostgresUpdateRows: update matching rows, SET columns from a JSON
//! object, WHERE from a SQL condition whose placeholders continue
//! after the SET's, and emit the changed rows (RETURNING *).

use async_trait::async_trait;
use serde_json::{json, Value};

use weft::node::NodeOutput;
use weft::{ExecutionContext, Node, NodeManifest, WeftResult};

use super::postgres::{connect, query_json, quote_ident};

/// The UPDATE for a table, its SET columns, and a WHERE condition.
/// SET placeholders are `$1..$n` in column order; the condition's own
/// placeholders start at `$n+1` (the node's whereParams).
pub fn update_sql(table: &str, columns: &[String], condition: &str) -> WeftResult<String> {
    if columns.is_empty() {
        weft::node_bail!("set holds no columns; nothing to update");
    }
    if condition.trim().is_empty() {
        weft::node_bail!(
            "where is empty; an unconditional update is almost always a mistake, write \
             `true` explicitly to update every row"
        );
    }
    let sets: Vec<String> = columns
        .iter()
        .enumerate()
        .map(|(i, c)| Ok(format!("{} = ${}", quote_ident(c)?, i + 1)))
        .collect::<WeftResult<_>>()?;
    Ok(format!(
        "UPDATE {} SET {} WHERE {} RETURNING *",
        quote_ident(table)?,
        sets.join(", "),
        condition
    ))
}

/// Renumber the condition's `$1..$k` placeholders to start after the
/// SET parameters, so the node author writes the condition standalone
/// (`email = $1`) and the offset stays an implementation detail.
pub fn offset_placeholders(condition: &str, by: usize) -> String {
    let mut out = String::with_capacity(condition.len());
    let mut chars = condition.chars().peekable();
    while let Some(c) = chars.next() {
        if c != '$' {
            out.push(c);
            continue;
        }
        let mut digits = String::new();
        while chars.peek().is_some_and(|d| d.is_ascii_digit()) {
            digits.push(chars.next().expect("peeked"));
        }
        match digits.parse::<usize>() {
            Ok(n) => out.push_str(&format!("${}", n + by)),
            Err(_) => out.push('$'),
        }
    }
    out
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
        let conn_str: String = ctx.inputs.get("connectionString")?;
        let table: String = ctx.inputs.get("table")?;
        let set: Value = ctx.inputs.get("set")?;
        let condition: String = ctx.inputs.get("where")?;
        let where_params: Vec<Value> = ctx.inputs.list("whereParams")?;

        let Some(obj) = set.as_object() else {
            weft::node_bail!("set must be an object of column: value pairs");
        };
        let columns: Vec<String> = obj.keys().cloned().collect();
        let mut params: Vec<Value> = obj.values().cloned().collect();
        let condition = offset_placeholders(&condition, params.len());
        let sql = update_sql(&table, &columns, &condition)?;
        params.extend(where_params);

        let client = connect(&ctx, &conn_str).await?;
        let rows = query_json(&client, &sql, &params).await?;
        let count = rows.len() as f64;
        ctx.pulse_downstream(NodeOutput::new().set("rows", json!(rows)).set("count", count))
            .await
    }
}
