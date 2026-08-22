//! PostgresUpdateRows: update matching rows, SET columns from a JSON
//! object, WHERE from a SQL condition whose own `$1..$k` placeholders
//! read from whereParams, and emit the changed rows (RETURNING *).

use async_trait::async_trait;
use serde_json::{json, Value};

use weft::node::NodeOutput;
use weft::{Access, ExecutionContext, Node, NodeErrExt, NodeManifest, WeftResult};

use super::postgres::{connect, query_json, quote_ident};

/// The UPDATE for a table, its SET columns, and a WHERE condition.
///
/// The condition is passed through BYTE FOR BYTE: its `$1..$k` are
/// the whereParams, exactly as the node author wrote them. The SET
/// placeholders are the ones that move, continuing at
/// `first_set_param`, because those are generated here and nobody
/// else has an opinion about their numbers.
///
/// The other way round (renumbering the condition) would mean
/// rewriting the user's SQL, and telling a real placeholder from the
/// same characters inside a quoted string, a dollar-quoted block or a
/// comment is a job for a Postgres parser. Getting it wrong produces
/// valid SQL that compares the wrong thing, which nothing downstream
/// can catch.
pub fn update_sql(
    table: &str,
    columns: &[String],
    condition: &str,
    first_set_param: usize,
) -> WeftResult<String> {
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
        .map(|(i, c)| Ok(format!("{} = ${}", quote_ident(c)?, first_set_param + i)))
        .collect::<WeftResult<_>>()?;
    // The condition is PARENTHESISED and put on its own line. Glued
    // inline, a condition ending in a line comment swallows the
    // `RETURNING *` that follows it, and the node then reports zero
    // changed rows having changed them. Wrapped, the same input is a
    // syntax error the database refuses outright.
    Ok(format!(
        "UPDATE {} SET {} WHERE (
{}
) RETURNING *",
        quote_ident(table)?,
        sets.join(", "),
        condition
    ))
}

/// How many parameters the WHERE condition actually declares, counted
/// by the database.
///
/// Asked as a throwaway `SELECT ... LIMIT 0` that never runs: preparing
/// it is what makes Postgres parse the condition and report the
/// parameters it found. Nothing here scans the text, because the
/// characters a placeholder is made of also appear inside strings,
/// dollar-quoted blocks and comments, and only a real parser tells
/// those apart.
async fn condition_params(
    client: &tokio_postgres::Client,
    table: &str,
    condition: &str,
) -> WeftResult<usize> {
    let probe =
        format!("SELECT 1 FROM {} WHERE (\n{condition}\n) LIMIT 0", quote_ident(table)?);
    let stmt = client.prepare(&probe).await.node_err("postgres: read the where condition")?;
    Ok(stmt.params().len())
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
        let where_params: Vec<Value> = ctx.inputs.list("whereParams")?;

        let Some(obj) = set.as_object() else {
            weft::node_bail!("set must be an object of column: value pairs");
        };
        let columns: Vec<String> = obj.keys().cloned().collect();

        let conn = ctx.open(&account).await?;
        let client = connect(&ctx, &conn).await?;
        // The condition's parameters go FIRST, so its `$1..$k` mean
        // what the node author wrote and the SQL is never rewritten.
        // Which makes a miscount DANGEROUS rather than merely wrong: a
        // condition naming a `$3` it did not supply a value for would
        // otherwise land on one of the SET values and quietly compare
        // against that, updating the wrong rows and reporting success.
        //
        // So the database counts the condition's placeholders, using
        // the parser that will run the statement. Telling a real
        // placeholder from the same characters inside a quoted string
        // or a dollar-quoted block is its job, and only its answer is
        // worth trusting.
        let declared = condition_params(&client, &table, &condition).await?;
        if declared != where_params.len() {
            weft::node_bail!(
                "where names {declared} parameter(s) but whereParams carries {}; the \
                 condition's $1, $2, ... are positions in whereParams",
                where_params.len()
            );
        }
        let sql = update_sql(&table, &columns, &condition, where_params.len() + 1)?;
        let mut params: Vec<Value> = where_params;
        params.extend(obj.values().cloned());

        let rows = query_json(&client, &sql, &params).await?;
        let count = rows.len() as f64;
        ctx.pulse_downstream(NodeOutput::new().set("rows", json!(rows)).set("count", count))
            .await
    }
}
