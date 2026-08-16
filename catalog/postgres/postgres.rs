//! Shared Postgres plumbing: connect over the user's connection
//! string (TLS-capable), map JSON parameters onto SQL placeholders,
//! and turn answered rows back into JSON.
//!
//! Parameter typing: a JSON string binds as text, a number as
//! float8, a bool as bool, null as SQL NULL, and an object or array
//! as jsonb. A query needing another type casts its placeholder
//! (`$1::int`, `$2::timestamptz`), the standard Postgres idiom.

use serde_json::{json, Map, Value};
use tokio_postgres::types::{ToSql, Type};
use tokio_postgres::Row;

use weft::{ExecutionContext, NodeErrExt, WeftResult};

/// Connect with the given connection string. TLS is available when
/// the server asks for it (public roots); `sslmode=disable` in the
/// string keeps a local database plain. The connection task is
/// spawned; dropping the client ends it.
pub async fn connect(ctx: &ExecutionContext, conn_str: &str) -> WeftResult<tokio_postgres::Client> {
    let roots = rustls::RootCertStore {
        roots: webpki_roots::TLS_SERVER_ROOTS.iter().cloned().collect(),
    };
    let tls_config = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let tls = tokio_postgres_rustls::MakeRustlsConnect::new(tls_config);
    let (client, connection) = tokio_postgres::connect(conn_str, tls)
        .await
        .node_err("postgres: connect")?;
    // The connection future must be driven for the client to work; it
    // ends when the client drops. A mid-run connection failure
    // surfaces on the next query as a loud error, so the task only
    // logs.
    let cancel = ctx.cancellation();
    tokio::spawn(async move {
        tokio::select! {
            r = connection => {
                if let Err(e) = r {
                    tracing::warn!("postgres connection ended: {e}");
                }
            }
            _ = cancel.cancelled() => {}
        }
    });
    Ok(client)
}

/// One JSON value as a boxed SQL parameter (see the module doc for
/// the mapping).
fn param_of(value: &Value) -> Box<dyn ToSql + Sync + Send> {
    match value {
        Value::Null => Box::new(Option::<String>::None),
        Value::Bool(b) => Box::new(*b),
        Value::Number(n) => Box::new(n.as_f64().unwrap_or(f64::NAN)),
        Value::String(s) => Box::new(s.clone()),
        other => Box::new(other.clone()),
    }
}

/// A JSON parameter list as SQL parameters, boxed for the query call.
pub fn params_of(values: &[Value]) -> Vec<Box<dyn ToSql + Sync + Send>> {
    values.iter().map(param_of).collect()
}

/// Quote an identifier (a table or column name) for SQL, refusing an
/// empty one. Doubles embedded quotes, the standard escape.
pub fn quote_ident(name: &str) -> WeftResult<String> {
    if name.trim().is_empty() {
        weft::node_bail!("an empty identifier cannot name a table or column");
    }
    Ok(format!("\"{}\"", name.replace('"', "\"\"")))
}

/// One answered cell as JSON. Every common Postgres type maps; a type
/// this shim does not carry is a loud error naming the fix (cast to
/// text in the query), never a silently wrong value.
fn cell_of(row: &Row, i: usize) -> WeftResult<Value> {
    let ty = row.columns()[i].type_();
    let name = row.columns()[i].name();
    let v = match *ty {
        Type::BOOL => row.try_get::<_, Option<bool>>(i).map(|v| json!(v)),
        Type::INT2 => row.try_get::<_, Option<i16>>(i).map(|v| json!(v)),
        Type::INT4 => row.try_get::<_, Option<i32>>(i).map(|v| json!(v)),
        Type::INT8 => row.try_get::<_, Option<i64>>(i).map(|v| json!(v)),
        Type::FLOAT4 => row.try_get::<_, Option<f32>>(i).map(|v| json!(v)),
        Type::FLOAT8 => row.try_get::<_, Option<f64>>(i).map(|v| json!(v)),
        Type::TEXT | Type::VARCHAR | Type::BPCHAR | Type::NAME => {
            row.try_get::<_, Option<String>>(i).map(|v| json!(v))
        }
        Type::JSON | Type::JSONB => {
            row.try_get::<_, Option<Value>>(i).map(|v| v.unwrap_or(Value::Null))
        }
        Type::UUID => row.try_get::<_, Option<uuid::Uuid>>(i).map(|v| json!(v)),
        Type::TIMESTAMPTZ => row
            .try_get::<_, Option<chrono::DateTime<chrono::Utc>>>(i)
            .map(|v| json!(v.map(|t| t.to_rfc3339()))),
        Type::TIMESTAMP => row
            .try_get::<_, Option<chrono::NaiveDateTime>>(i)
            .map(|v| json!(v.map(|t| t.to_string()))),
        Type::DATE => row
            .try_get::<_, Option<chrono::NaiveDate>>(i)
            .map(|v| json!(v.map(|d| d.to_string()))),
        _ => {
            weft::node_bail!(
                "column '{name}' has type {ty}, which this node does not carry; cast it in \
                 the query (e.g. {name}::text)"
            );
        }
    };
    v.node_err("postgres: read a cell")
}

/// Answered rows as JSON objects keyed by column name.
pub fn rows_to_json(rows: &[Row]) -> WeftResult<Vec<Value>> {
    rows.iter()
        .map(|row| {
            let mut obj = Map::new();
            for i in 0..row.columns().len() {
                obj.insert(row.columns()[i].name().to_string(), cell_of(row, i)?);
            }
            Ok(Value::Object(obj))
        })
        .collect()
}

/// Run `sql` with JSON `params` and answer the rows as JSON.
pub async fn query_json(
    client: &tokio_postgres::Client,
    sql: &str,
    params: &[Value],
) -> WeftResult<Vec<Value>> {
    let boxed = params_of(params);
    let refs: Vec<&(dyn ToSql + Sync)> =
        boxed.iter().map(|b| b.as_ref() as &(dyn ToSql + Sync)).collect();
    let rows = client.query(sql, &refs).await.node_err("postgres: run the query")?;
    rows_to_json(&rows)
}
