//! PostgresInsertRow: insert one row, columns from a JSON object,
//! and emit the inserted row (RETURNING *).

use async_trait::async_trait;
use serde_json::Value;

use weft::node::NodeOutput;
use weft::{Access, ExecutionContext, Node, NodeManifest, WeftResult};

use super::postgres::{connect, query_json, quote_ident};

/// The INSERT for a table and its column names: quoted identifiers,
/// one placeholder per column, RETURNING *.
pub fn insert_sql(table: &str, columns: &[String]) -> WeftResult<String> {
    if columns.is_empty() {
        weft::node_bail!("values holds no columns; nothing to insert");
    }
    let cols: Vec<String> =
        columns.iter().map(|c| quote_ident(c)).collect::<WeftResult<_>>()?;
    let placeholders: Vec<String> = (1..=columns.len()).map(|i| format!("${i}")).collect();
    Ok(format!(
        "INSERT INTO {} ({}) VALUES ({}) RETURNING *",
        quote_ident(table)?,
        cols.join(", "),
        placeholders.join(", ")
    ))
}

#[derive(NodeManifest)]
pub struct PostgresInsertRowNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for PostgresInsertRowNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        let table: String = ctx.inputs.get("table")?;
        let values: Value = ctx.inputs.get("values")?;

        let Some(obj) = values.as_object() else {
            weft::node_bail!("values must be an object of column: value pairs");
        };
        let columns: Vec<String> = obj.keys().cloned().collect();
        let params: Vec<Value> = obj.values().cloned().collect();
        let sql = insert_sql(&table, &columns)?;

        let conn = ctx.open(&account).await?;
        let client = connect(&ctx, &conn).await?;
        let rows = query_json(&client, &sql, &params).await?;
        // `RETURNING *` answers exactly one row for one inserted row;
        // none means the insert was discarded (a rule or trigger on
        // the table), and inventing an empty row would send a
        // downstream node something that was never stored.
        let Some(row) = rows.into_iter().next() else {
            weft::node_bail!(
                "the insert stored no row: the table has a rule or trigger that discarded it"
            );
        };
        ctx.pulse_downstream(NodeOutput::new().set("row", row)).await
    }
}
