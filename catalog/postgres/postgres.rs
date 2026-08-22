//! Shared Postgres plumbing: connect as the wired database
//! connection, map JSON parameters onto SQL placeholders, and turn
//! answered rows back into JSON.
//!
//! The connection stores `host`, `database`, `user`, `password`, and
//! the optional `port` and `sslmode`; this module is the one place
//! that vocabulary is read, so every Postgres node dials the same way.
//!
//! Parameter typing: a JSON string binds as text, a bool as bool,
//! null as SQL NULL against any column, and an object or array as
//! jsonb. A number binds to whichever numeric column it is compared
//! with or written to (the integers, the floats, and `numeric` within
//! the range a fixed-point decimal holds), refusing a value the column
//! cannot hold rather than rounding it. A query needing another type (a
//! timestamp, a uuid) casts its placeholder (`$1::timestamptz`), the
//! standard Postgres idiom.

use serde_json::{json, Map, Value};
use tokio_postgres::config::SslMode;
use tokio_postgres::types::{ToSql, Type};
use tokio_postgres::{Config, Row};

use weft::access::OpenedConnection;
use weft::{ExecutionContext, NodeErrExt, WeftError, WeftResult};

/// Where and how to dial, read off the connection. Pure, so the
/// vocabulary and every refusal are testable without a database.
pub fn dial_config(conn: &OpenedConnection) -> WeftResult<Config> {
    let port: u16 = match conn.opt_value("port").map(str::trim).unwrap_or("") {
        "" => 5432,
        p => p.parse().map_err(|_| {
            WeftError::Input(
                "the connection's port is not a number; reconnect it with a numeric port".into(),
            )
        })?,
    };
    // Absent TLS means `require`, the safe default for a database
    // reached over a network; `disable` is the escape for a database
    // that serves no TLS at all. Whenever TLS IS used the chain and
    // the hostname are verified, so the three names a provider hands
    // out for that (`require`, `verify-ca`, `verify-full`) all mean
    // the same dial here and are all accepted, rather than making
    // someone edit what their provider gave them. `prefer` is the
    // exception worth knowing: it uses TLS only if the server offers
    // it, so a server that declines leaves nothing to verify.
    let ssl_mode = match conn.opt_value("sslmode").map(str::trim).unwrap_or("") {
        "" | "require" | "verify-ca" | "verify-full" => SslMode::Require,
        "prefer" => SslMode::Prefer,
        "disable" => SslMode::Disable,
        other => {
            return Err(WeftError::Input(format!(
                "the connection's TLS setting is '{other}'; reconnect it with require, \
                 verify-ca, verify-full, prefer or disable"
            )))
        }
    };
    let mut config = Config::new();
    config
        .host(conn.value("host")?)
        .port(port)
        .dbname(conn.value("database")?)
        .user(conn.value("user")?)
        .password(conn.value("password")?)
        .ssl_mode(ssl_mode);
    Ok(config)
}

/// Dial the wired database. The connection task is spawned; dropping
/// the client ends it.
pub async fn connect(
    ctx: &ExecutionContext,
    conn: &OpenedConnection,
) -> WeftResult<tokio_postgres::Client> {
    let config = dial_config(conn)?;
    // The runtime's own trust settings, not a second opinion: one
    // answer to "which certificates does weft trust", and it pins the
    // crypto provider, which building a config here would leave to
    // whichever one the host binary happened to install.
    let tls_config = weft::net::tls_config().map_err(WeftError::Config)?;
    let tls = tokio_postgres_rustls::MakeRustlsConnect::new((*tls_config).clone());
    let (client, connection) = config.connect(tls).await.map_err(|e| {
        // A handshake failure is the one whose fix is not in the
        // message the driver gives back, because weft is stricter
        // than what a provider means by `require`: it checks the
        // chain and the hostname every time. Said ONLY for that
        // failure, so a wrong password does not come back with four
        // lines about certificate authorities.
        let handshake = e.to_string().starts_with("error performing TLS handshake");
        let hint = if handshake {
            ". Weft checks the server's certificate and hostname on every TLS connection, \
             so a database presenting one that is not signed by a public authority is \
             refused even where other clients accept it. Such a database has to be \
             reached over a private network with the connection's TLS setting on \
             `disable`"
        } else {
            ""
        };
        WeftError::NodeExecution(format!("postgres: connect: {}{hint}", pg_detail(&e)))
    })?;
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

/// A driver error with the reason underneath it.
///
/// `tokio_postgres::Error`'s own text for a rejected parameter is the
/// fixed string "error serializing parameter 0"; WHY it was rejected
/// lives in the source underneath. Printing only the top of the chain
/// throws away every refusal this module takes care to word, so the
/// chain is walked and joined.
fn pg_detail(e: &tokio_postgres::Error) -> String {
    let mut out = e.to_string();
    let mut source = std::error::Error::source(e);
    while let Some(cause) = source {
        let text = cause.to_string();
        if !out.contains(&text) {
            out.push_str(": ");
            out.push_str(&text);
        }
        source = cause.source();
    }
    out
}

/// A JSON number bound so the COLUMN decides how it travels.
///
/// Postgres infers a parameter's type from where it sits in the
/// statement, and a Rust type answers for exactly one: `i64` accepts
/// int8 and nothing else, `f64` accepts float8 and nothing else.
/// Picking one here means picking which columns work, and a plain
/// `5` would be refused by `WHERE id = $1` against a `serial`
/// (int4) column, which is the commonest shape there is.
///
/// So this accepts the whole numeric family and encodes into
/// whichever member the server asked for. A value that does not fit
/// the column is refused rather than rounded or wrapped: a parameter
/// silently turned into a different number would be written to the
/// database as one.
#[derive(Debug)]
struct SqlNumber(serde_json::Number);

impl SqlNumber {
    /// The value as an integer, or the reason it is not one.
    fn integer(&self) -> Result<i64, Box<dyn std::error::Error + Sync + Send>> {
        if let Some(i) = self.0.as_i64() {
            return Ok(i);
        }
        // A whole number that arrived tagged as a double still IS a
        // whole number, and everything that has been through
        // JavaScript or back out of jsonb arrives that way. Only the
        // value matters, not how it was written.
        let wide = self.float();
        if wide.fract() == 0.0 && wide >= i64::MIN as f64 && wide <= i64::MAX as f64 {
            return Ok(wide as i64);
        }
        Err(format!("{} does not fit a whole-number column", self.0).into())
    }

    /// The value as a double. Always available: a JSON number is one
    /// of i64, u64 or f64, and every one of those has a double form.
    fn float(&self) -> f64 {
        self.0.as_f64().expect("a JSON number always has a double form")
    }

    /// The value narrowed to a single-precision float, refusing a
    /// magnitude that does not survive the trip. `as f32` turns a
    /// too-large number into infinity, which the database stores
    /// happily, and a stored infinity is not the number anyone wrote.
    fn single(&self) -> Result<f32, Box<dyn std::error::Error + Sync + Send>> {
        let wide = self.float();
        let narrow = wide as f32;
        // Too big becomes infinity and too small becomes zero, and a
        // stored infinity or a stored zero is not the number anyone
        // wrote. Both are refused.
        if wide.is_finite() && !narrow.is_finite() {
            return Err(format!("{wide} is too large for a real column").into());
        }
        if narrow == 0.0 && wide != 0.0 {
            return Err(format!("{wide} is too small for a real column").into());
        }
        Ok(narrow)
    }

    /// The value as a decimal, so a `numeric` column can be compared
    /// with or written to at all.
    ///
    /// Read from the number's own text rather than re-derived, which
    /// is the closest this can get: a JSON number is already a double
    /// by the time it arrives, so a literal with more significant
    /// digits than a double holds was rounded before anything here
    /// saw it. A value that has to survive exactly travels as a
    /// string and is cast in the SQL (`$1::numeric`).
    fn decimal(&self) -> Result<rust_decimal::Decimal, Box<dyn std::error::Error + Sync + Send>> {
        use std::str::FromStr;
        rust_decimal::Decimal::from_str(&self.0.to_string()).map_err(|e| {
            format!("{} cannot be bound to a numeric column exactly: {e}", self.0).into()
        })
    }
}

/// A JSON null, bound as SQL NULL against ANY column.
///
/// `Option::<String>::None` would not do: it inherits `String`'s idea
/// of which columns it answers for, so a null could only be written to
/// a text one. A null has no type of its own, which is exactly what
/// makes it bindable everywhere.
#[derive(Debug)]
struct SqlNull;

impl ToSql for SqlNull {
    fn to_sql(
        &self,
        _: &Type,
        _: &mut bytes::BytesMut,
    ) -> Result<tokio_postgres::types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
        Ok(tokio_postgres::types::IsNull::Yes)
    }

    fn accepts(_: &Type) -> bool {
        true
    }

    tokio_postgres::types::to_sql_checked!();
}

impl ToSql for SqlNumber {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut bytes::BytesMut,
    ) -> Result<tokio_postgres::types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
        // Narrowing is checked, never truncated, so a value the column
        // cannot hold says so instead of arriving as a different one.
        fn fit<T: TryFrom<i64>>(
            v: i64,
            ty: &Type,
        ) -> Result<T, Box<dyn std::error::Error + Sync + Send>> {
            T::try_from(v).map_err(|_| format!("{v} does not fit a {ty} column").into())
        }
        match *ty {
            Type::INT2 => fit::<i16>(self.integer()?, ty)?.to_sql(ty, out),
            Type::INT4 => fit::<i32>(self.integer()?, ty)?.to_sql(ty, out),
            Type::INT8 => self.integer()?.to_sql(ty, out),
            Type::FLOAT4 => self.single()?.to_sql(ty, out),
            Type::FLOAT8 => self.float().to_sql(ty, out),
            Type::NUMERIC => self.decimal()?.to_sql(ty, out),
            _ => Err(format!("a number cannot be bound to a {ty} column").into()),
        }
    }

    fn accepts(ty: &Type) -> bool {
        matches!(
            *ty,
            Type::INT2
                | Type::INT4
                | Type::INT8
                | Type::FLOAT4
                | Type::FLOAT8
                | Type::NUMERIC
        )
    }

    tokio_postgres::types::to_sql_checked!();
}

/// One JSON value as a boxed SQL parameter (see the module doc for
/// the mapping).
fn param_of(value: &Value) -> Box<dyn ToSql + Sync + Send> {
    match value {
        Value::Null => Box::new(SqlNull),
        Value::Bool(b) => Box::new(*b),
        Value::Number(n) => Box::new(SqlNumber(n.clone())),
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
        // Read back as the exact decimal it is stored as, then carried
        // as a JSON number. A node that can WRITE a numeric column has
        // to be able to read one: `insert_row` answers with the row it
        // just stored, so a missing read arm here means writing the
        // row and then failing on the way out.
        // A node that can WRITE a numeric column has to be able to
        // read one: `insert_row` answers with the row it just stored,
        // so a missing arm here means writing the row and then failing
        // on the way out.
        //
        // Carried as an ordinary JSON number, which is a double, so a
        // value with more significant digits than a double holds
        // arrives rounded. Anything that has to survive exactly is
        // cast to text in the query and comes back as a string.
        Type::NUMERIC => row.try_get::<_, Option<rust_decimal::Decimal>>(i).map(|v| {
            use rust_decimal::prelude::ToPrimitive;
            json!(v.and_then(|d| d.to_f64()))
        }),
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
    let rows = client
        .query(sql, &refs)
        .await
        .map_err(|e| weft::error::node_error(format!("postgres: run the query: {}", pg_detail(&e))))?;
    rows_to_json(&rows)
}
