//! PostgresUpdateRows: update matching rows, SET columns from a JSON
//! object, WHERE from a SQL condition whose `$name` placeholders read
//! the node's own input ports (as in PostgresExecuteQuery), and emit
//! the changed rows (RETURNING *): how many changed, and as many of
//! them as fit the 100 KB a wire carries, never failing once the update
//! has committed.

use async_trait::async_trait;
use serde_json::Value;

use weft::node::NodeOutput;
use weft::{Access, RunContext, Node, NodeManifest, WeftResult};

use super::postgres::{connect, mistake, placeholders, quote_ident, Statement, WrittenBy};

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

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
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

        let mut client = connect(&ctx, &account).await?;
        // The node writes this SQL, so a column it could not read back is
        // refused before any row is changed, with no cast to suggest, and
        // a changed value it could not read back undoes the update.
        let rows = client.query_json(&sql, &names, &params, WrittenBy::Node).await?;
        // The update has committed by now, so nothing past this point
        // may fail it: `count` is every changed row, and `rows` the
        // leading ones that fit the 100 KB a wire carries, with
        // `truncated` saying when some were left out.
        let count = rows.len() as f64;
        let (kept, truncated) = capped_rows(rows);
        ctx.pulse_downstream(
            NodeOutput::new()
                .set("rows", Value::Array(kept))
                .set("count", count)
                .set("truncated", truncated),
        )
        .await
    }
}

/// The leading `rows` that one wire carries, and whether any were left
/// out. A first row heavier than a wire on its own leaves none.
pub fn capped_rows(mut rows: Vec<Value>) -> (Vec<Value>, bool) {
    let end = match weft::storage::wire_page(&rows, 0, usize::MAX) {
        Ok(fits) => fits.end,
        Err(_) => 0,
    };
    let truncated = end < rows.len();
    rows.truncate(end);
    (rows, truncated)
}
