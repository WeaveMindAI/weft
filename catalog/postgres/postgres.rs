//! Shared Postgres plumbing: connect as the wired database
//! connection, map JSON parameters onto SQL placeholders, and turn
//! answered rows back into JSON.
//!
//! The connection stores `host`, `database`, `user`, `password`, and
//! the optional `port` and `sslmode`; this module is the one place
//! that vocabulary is read, so every Postgres node dials the same way.
//!
//! Parameter typing: a JSON string binds as the TEXT form of whatever
//! type the query infers for its placeholder (a text column takes it
//! as it is; `$when::timestamptz`, a uuid column, a numeric column each
//! parse it the way a quoted literal is parsed), a bool as bool, null
//! as SQL NULL against any column, and an object or array as jsonb. A
//! number binds to whichever numeric column it is compared with or
//! written to (the integers, the floats, and `numeric` within the range
//! a fixed-point decimal holds), refusing a value the column cannot
//! hold rather than rounding it.

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

/// What a listening connection hands back: a notification the server
/// raised, or the driver's own reason the connection died. The reason
/// is known only inside the connection task, and carrying it out is
/// what lets the node name an auth failure or a server shutdown
/// instead of saying the connection ended and leaving the reader to
/// go and read the worker logs.
pub enum ListenEvent {
    Notified(tokio_postgres::Notification),
    Failed(String),
}

/// Dial the wired database and `LISTEN` on `channel`: the client, and
/// the stream of notifications the server raises on that channel (a
/// `NOTIFY channel` in a trigger, say). The connection task is driven
/// here so its asynchronous messages can be read off it; it ends when
/// the client drops or the run is cancelled, and the stream ends with
/// it.
pub async fn connect_listening(
    ctx: &ExecutionContext,
    conn: &OpenedConnection,
    channel: &str,
) -> WeftResult<(tokio_postgres::Client, tokio::sync::mpsc::UnboundedReceiver<ListenEvent>)> {
    use futures::StreamExt;
    let config = dial_config(conn)?;
    let tls_config = weft::net::tls_config().map_err(WeftError::Config)?;
    let tls = tokio_postgres_rustls::MakeRustlsConnect::new((*tls_config).clone());
    let (client, mut connection) = config
        .connect(tls)
        .await
        .map_err(|e| WeftError::NodeExecution(format!("postgres: connect: {}", pg_detail(&e))))?;
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let cancel = ctx.cancellation();
    tokio::spawn(async move {
        let mut messages = futures::stream::poll_fn(move |cx| connection.poll_message(cx));
        loop {
            tokio::select! {
                next = messages.next() => match next {
                    Some(Ok(tokio_postgres::AsyncMessage::Notification(n))) => {
                        if tx.send(ListenEvent::Notified(n)).is_err() {
                            break;
                        }
                    }
                    Some(Ok(_)) => {}
                    Some(Err(e)) => {
                        // The receiver turns this into the node's own
                        // message, so the log line is only worth
                        // writing when nobody is left to read it.
                        let detail = pg_detail(&e);
                        if tx.send(ListenEvent::Failed(detail)).is_err() {
                            tracing::warn!("postgres listening connection ended: {e}");
                        }
                        break;
                    }
                    None => break,
                },
                _ = cancel.cancelled() => break,
            }
        }
    });
    client
        .batch_execute(&format!("LISTEN {}", quote_ident(channel)?))
        .await
        .map_err(|e| WeftError::NodeExecution(format!("postgres: LISTEN {channel}: {}", pg_detail(&e))))?;
    Ok((client, rx))
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

/// A JSON string, bound in TEXT format against ANY column: Postgres
/// parses it as it parses a quoted literal, so `$when::timestamptz`,
/// `WHERE id = $id` on a uuid column and `$amount::numeric` all take a
/// string port. `String` itself would not do: it binds in binary and
/// answers only for the text types, so a placeholder the query casts
/// (or compares with a non-text column) refused the string before the
/// database ever saw it.
#[derive(Debug)]
struct SqlText(String);

impl ToSql for SqlText {
    fn to_sql(
        &self,
        _: &Type,
        out: &mut bytes::BytesMut,
    ) -> Result<tokio_postgres::types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
        out.extend_from_slice(self.0.as_bytes());
        Ok(tokio_postgres::types::IsNull::No)
    }

    fn accepts(_: &Type) -> bool {
        true
    }

    fn encode_format(&self, _: &Type) -> tokio_postgres::types::Format {
        tokio_postgres::types::Format::Text
    }

    tokio_postgres::types::to_sql_checked!();
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
        Value::String(s) => Box::new(SqlText(s.clone())),
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

/// Refuse a result whose columns collide with the ports the node
/// answers itself, naming which and what to do.
///
/// These nodes hand a row's columns out as ports of the same name, and
/// they also answer a few ports of their own (`rows`, and for a query
/// `count`). Those live in ONE namespace, so a column called `count`
/// asks for a port that is already spoken for, and the node's own
/// value is what a reader gets. It reached production once: `select
/// count(*) as count` over nine cards served `1`, the number of rows,
/// with nothing anywhere saying so, and the types agreed on both sides.
///
/// So it refuses instead. The column is never lost (every column is in
/// `rows` whatever happens here), and aliasing it in the SQL is the
/// whole fix.
pub fn refuse_shadowed_columns(first_row: &Value, own_ports: &[&str]) -> WeftResult<()> {
    let Some(row) = first_row.as_object() else { return Ok(()) };
    for port in own_ports {
        if !row.contains_key(*port) {
            continue;
        }
        weft::node_bail!(
            "this query answers a column called '{port}', which is also one of this node's own \
             output ports, so the column has nowhere to go: reading '{port}' gives you what the \
             node puts there, not your column. Alias it in the SQL (`... as {port}_value`), or \
             read it out of `rows`"
        );
    }
    Ok(())
}

/// The mirror of [`refuse_shadowed_columns`]: a port the author
/// declared that the query answers no column for.
///
/// These nodes fill a declared port from the first row's column of the
/// same name, and a name that is not there is simply not filled, so the
/// port closes and everything behind it skips. Nothing says why, and
/// the mistake is a one-character typo or a column left out of the
/// SELECT. It cost a real debugging cycle: `-> (card: JsonDict)` over
/// `select id, name, note ...` closed `card`, both branches skipped,
/// the caller got a 500, and the journal showed `rows` fully populated
/// right beside `card closed`.
///
/// The node holds both halves at this instant, so it says so. Only
/// when a row actually came back: a query that answered nothing closes
/// every declared port, and that is the correct reading of no rows.
pub fn refuse_unanswered_ports(
    first_row: &Value,
    declared: &std::collections::HashMap<String, weft::weft_type::WeftType>,
    own_ports: &[&str],
) -> WeftResult<()> {
    let Some(row) = first_row.as_object() else { return Ok(()) };
    let mut missing: Vec<&str> = declared
        .keys()
        .map(String::as_str)
        .filter(|name| !own_ports.contains(name) && !row.contains_key(*name))
        .collect();
    if missing.is_empty() {
        return Ok(());
    }
    missing.sort_unstable();
    let answered: Vec<&str> = row.keys().map(String::as_str).collect();
    weft::node_bail!(
        "this node declares the output port{} {}, and the query answers no column of that \
         name: the columns it answered are {}. A port filled from a column nobody selected \
         closes, and everything behind it skips. Select the column (or alias one to that \
         name, `... as {}`), or drop the port",
        if missing.len() == 1 { "" } else { "s" },
        missing.iter().map(|m| format!("'{m}'")).collect::<Vec<_>>().join(", "),
        answered.iter().map(|a| format!("'{a}'")).collect::<Vec<_>>().join(", "),
        missing[0]
    );
}

/// A driver refusal said back in the author's own words.
///
/// Postgres counts parameters and the author names them, so every
/// refusal about one arrives as `$4` for a port called `reason`, and
/// this node is the only thing that holds the mapping between the two.
///
/// One refusal gets more than a rename. "could not determine data type"
/// means the parameter sits somewhere the server cannot work out what
/// it should be (inside a `jsonb_build_object`, most often), and the
/// fix is a cast the author has no reason to expect. It is worth
/// spelling out, because the alternative is a database error arriving
/// from inside a loop with a number in it that appears nowhere in the
/// source.
pub fn spell_parameters(detail: &str, names: &[String]) -> String {
    /// Every `<lead><digits>` in `text` rewritten as `$<port>`, when
    /// that position reads a port. A number with no port behind it (the
    /// author's own `$$` body, a dollar amount, a count that is not an
    /// index) is left exactly as written.
    fn rewrite(text: &str, lead: &str, names: &[String]) -> String {
        let mut out = String::with_capacity(text.len());
        let mut rest = text;
        while let Some(at) = rest.find(lead) {
            let after = at + lead.len();
            let digits: String = rest[after..].chars().take_while(char::is_ascii_digit).collect();
            let port = digits
                .parse::<usize>()
                .ok()
                .and_then(|n| names.get(n.checked_sub(1)?));
            match port {
                Some(port) => {
                    out.push_str(&rest[..at]);
                    // `$4` becomes `$reason` and `parameter 4` becomes
                    // `parameter $reason`: the `$` the port is written
                    // with is supplied here, so a `$` lead is dropped
                    // rather than doubled.
                    if lead != "$" {
                        out.push_str(lead);
                    }
                    out.push_str(&format!("${port}"));
                    rest = &rest[after + digits.len()..];
                }
                None => {
                    out.push_str(&rest[..after]);
                    rest = &rest[after..];
                }
            }
        }
        out.push_str(rest);
        out
    }

    // The server spells a parameter two ways: `$4` in a message about
    // the SQL, and `parameter 4` in one about the value sent for it.
    // The `$` pass runs first, so a `parameter $4` is already spelled
    // out by the time the second pass looks, and the second pass finds
    // no digits there to touch again.
    let mut out = rewrite(&rewrite(detail, "$", names), "parameter ", names);
    if detail.contains("could not determine data type of parameter") {
        out.push_str(
            ". Postgres cannot tell what type that parameter should be from where it sits in \
             the SQL, so say it: write `::text` (or the type the column wants) after it",
        );
    }
    out
}

/// Run one statement with JSON `params` and answer the rows as JSON.
///
/// `names` is the PORT each position reads in SQL the author wrote, so
/// a refusal naming a position can be said back naming the port. Empty
/// for a node that writes its own SQL: there the numbering is the
/// node's, or the author's own `$1` in a `where`, and rewriting either
/// would point at something that is not there.
pub async fn query_json(
    client: &tokio_postgres::Client,
    sql: &str,
    names: &[String],
    params: &[Value],
) -> WeftResult<Vec<Value>> {
    let boxed = params_of(params);
    let refs: Vec<&(dyn ToSql + Sync)> =
        boxed.iter().map(|b| b.as_ref() as &(dyn ToSql + Sync)).collect();
    let rows = client.query(sql, &refs).await.map_err(|e| {
        weft::error::node_error(format!(
            "postgres: run the query: {}",
            spell_parameters(&pg_detail(&e), names)
        ))
    })?;
    rows_to_json(&rows)
}

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
    /// Several statements naming no port. Everything before the last
    /// goes whole through the simple protocol, which is the only one
    /// that runs transaction control and the utility statements the
    /// extended protocol refuses; the LAST one runs on its own,
    /// through the extended protocol, on the same session.
    ///
    /// The split is there so a cell means the same thing whatever
    /// shape the query had. The simple protocol types no column, so
    /// every value comes back as the text Postgres prints, and a
    /// `timestamptz` that reads `2026-09-16T22:14:48+00:00` from a
    /// one-statement query read `2026-09-16 22:14:48.601019+00` from a
    /// script. Same column, same table, two formats, decided by
    /// something the author never made a choice about; sorting the
    /// second as text is wrong, because a space sorts before `T`.
    /// Only the last statement's rows are ever read, so only the last
    /// statement has to be typed.
    Script { head: Vec<String>, last: String },
    /// Several statements, at least one naming a port: each runs on
    /// its own with the ports it names, in one transaction.
    Steps(Vec<Statement>),
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
        // No placeholder moved, so each statement's `sql` is its own
        // text as written.
        let last = parsed.statements.pop().expect("more than one statement").sql;
        let head = parsed.statements.into_iter().map(|s| s.sql).collect();
        return Ok(Plan::Script { head, last });
    }
    Ok(Plan::Steps(parsed.statements))
}

/// The ports a plan reads, first appearance first, each once: what
/// the wired ports have to match both ways.
pub fn ports_read(plan: &Plan) -> Vec<String> {
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

/// A query's named placeholders resolved to what the driver wants,
/// one entry per statement: the statement's SQL with every `$name`
/// rewritten to its positional `$N` (numbered within THAT statement,
/// because each statement is its own round trip) and the port names
/// in that order (so `names[N-1]` is what `$N` binds).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlaceholderSql {
    pub statements: Vec<Statement>,
}

/// One statement of a query, ready for the extended protocol.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Statement {
    pub sql: String,
    pub names: Vec<String>,
}

impl PlaceholderSql {
    /// Every port name any statement reads, first appearance first,
    /// each once.
    pub fn names(&self) -> Vec<String> {
        let mut out: Vec<String> = Vec::new();
        for s in &self.statements {
            for n in &s.names {
                if !out.contains(n) {
                    out.push(n.clone());
                }
            }
        }
        out
    }
}

/// Read a query's `$name` placeholders, the way a node's custom input
/// ports reach its SQL. Within each statement, each distinct name gets
/// the position of its first appearance (`$a ... $b ... $a` becomes
/// `$1 ... $2 ... $1`), so the same port can be read several times and
/// binds once; a name read by two statements binds once per statement.
///
/// Only SQL that Postgres itself would read as a parameter is
/// touched: a `$` inside a string literal, a quoted identifier, a
/// comment, or a dollar-quoted body (`$$ ... $$`, `$fn$ ... $fn$`, the
/// way a `DO` block or a function body is written) stays exactly as
/// written. Statements are split the same way (a `;` inside any of
/// those is not a separator) so the caller can run each on its own.
///
/// A positional `$1` is refused: there is no parameter list for it to
/// index any more, and naming the port is the fix. In a script that
/// names no port at all nothing is renumbered, so a `$1` there reaches
/// the server as written: it is the author's own (a `PREPARE`) and it
/// stays.
pub fn placeholders(sql: &str) -> WeftResult<PlaceholderSql> {
    let chars: Vec<char> = sql.chars().collect();
    // The whole text with placeholders numbered per statement, which
    // is what each statement sends.
    let mut out = String::with_capacity(sql.len());
    let mut names: Vec<String> = Vec::new();
    let mut statements: Vec<Statement> = Vec::new();
    // Where the current statement began in `out`.
    let mut statement_start = 0usize;
    // The first `$1`-shaped tag seen, if any: what it means depends on
    // whether this turns out to be one statement or a script.
    let mut positional: Option<String> = None;
    // Whether the current statement has any text besides whitespace
    // and comments, so a trailing `;` (or `;;`) does not count an
    // empty statement.
    let mut statement_has_text = false;
    // Close the statement being read: its numbered SQL and the ports
    // it names, then start a fresh numbering.
    fn close_statement(
        out: &mut String,
        names: &mut Vec<String>,
        statements: &mut Vec<Statement>,
        statement_start: &mut usize,
    ) {
        statements.push(Statement {
            sql: out[*statement_start..].trim().to_string(),
            names: std::mem::take(names),
        });
        *statement_start = out.len();
    }
    let ident_start = |c: char| c.is_ascii_alphabetic() || c == '_';
    let ident_char = |c: char| c.is_ascii_alphanumeric() || c == '_';
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        let next = chars.get(i + 1).copied();
        match c {
            // Line comment: copied through, never scanned.
            '-' if next == Some('-') => {
                while i < chars.len() && chars[i] != '\n' {
                    out.push(chars[i]);
                    i += 1;
                }
            }
            // Block comment, nested the way Postgres nests them.
            '/' if next == Some('*') => {
                let mut depth = 0usize;
                loop {
                    if i >= chars.len() {
                        weft::node_bail!("the SQL opens a /* comment it never closes");
                    }
                    if chars[i] == '/' && chars.get(i + 1) == Some(&'*') {
                        depth += 1;
                        out.push_str("/*");
                        i += 2;
                    } else if chars[i] == '*' && chars.get(i + 1) == Some(&'/') {
                        depth -= 1;
                        out.push_str("*/");
                        i += 2;
                        if depth == 0 {
                            break;
                        }
                    } else {
                        out.push(chars[i]);
                        i += 1;
                    }
                }
            }
            // String literal (`''` is the escape; an `E'...'` string also
            // escapes with a backslash) and quoted identifier.
            '\'' | '"' => {
                let quote = c;
                let escapes_with_backslash = quote == '\''
                    && i > 0
                    && matches!(chars[i - 1], 'e' | 'E')
                    && (i < 2 || !ident_char(chars[i - 2]));
                statement_has_text = true;
                out.push(quote);
                i += 1;
                loop {
                    let Some(&ch) = chars.get(i) else {
                        weft::node_bail!("the SQL opens a {quote} quote it never closes");
                    };
                    if ch == '\\' && escapes_with_backslash {
                        out.push(ch);
                        if let Some(&escaped) = chars.get(i + 1) {
                            out.push(escaped);
                        }
                        i += 2;
                        continue;
                    }
                    out.push(ch);
                    i += 1;
                    if ch == quote {
                        if chars.get(i) == Some(&quote) {
                            out.push(quote);
                            i += 1;
                            continue;
                        }
                        break;
                    }
                }
            }
            // `$` inside an identifier (`x$y` is a legal name) is the name.
            '$' if i > 0 && ident_char(chars[i - 1]) => {
                out.push('$');
                i += 1;
            }
            '$' => {
                // The tag between this `$` and the next: empty for `$$`,
                // an identifier for `$tag$`. Digits after `$` cannot be
                // a tag, so `$1` is a positional parameter.
                let mut j = i + 1;
                while j < chars.len() && ident_char(chars[j]) {
                    j += 1;
                }
                let tag: String = chars[i + 1..j].iter().collect();
                let is_tag = tag.is_empty() || ident_start(tag.chars().next().unwrap_or(' '));
                if is_tag && chars.get(j) == Some(&'$') {
                    // Dollar-quoted body: copy through to the closing tag.
                    let close: Vec<char> = format!("${tag}$").chars().collect();
                    let body_start = j + 1;
                    let mut k = body_start;
                    let mut found = None;
                    while k + close.len() <= chars.len() {
                        if chars[k..k + close.len()] == close[..] {
                            found = Some(k);
                            break;
                        }
                        k += 1;
                    }
                    let Some(end) = found else {
                        weft::node_bail!("the SQL opens a ${tag}$ quote it never closes");
                    };
                    out.extend(&chars[i..end + close.len()]);
                    i = end + close.len();
                    statement_has_text = true;
                    continue;
                }
                // A tag that cannot be a port name: all digits (`$1`,
                // the positional form) or digit-leading (`$1st`). In a
                // SCRIPT it is not a weft placeholder at all (`PREPARE
                // ... $1` is the author's own SQL), so it is copied
                // through and the refusal is left to the end, where the
                // statement count is known.
                if !tag.is_empty() && !ident_start(tag.chars().next().unwrap_or(' ')) {
                    positional.get_or_insert_with(|| tag.clone());
                    out.push('$');
                    out.push_str(&tag);
                    i = j;
                    statement_has_text = true;
                    continue;
                }
                if tag.is_empty() {
                    // A bare `$` (an operator such as `$>` in some
                    // extensions) is SQL, not a placeholder.
                    out.push('$');
                    i += 1;
                    statement_has_text = true;
                    continue;
                }
                let position = match names.iter().position(|n| n == &tag) {
                    Some(p) => p + 1,
                    None => {
                        names.push(tag.clone());
                        names.len()
                    }
                };
                out.push_str(&format!("${position}"));
                i = j;
                statement_has_text = true;
            }
            ';' => {
                if statement_has_text {
                    close_statement(&mut out, &mut names, &mut statements, &mut statement_start);
                    statement_has_text = false;
                }
                out.push(';');
                // The separator belongs to no statement.
                statement_start = out.len();
                i += 1;
            }
            _ => {
                if !c.is_whitespace() {
                    statement_has_text = true;
                }
                out.push(c);
                i += 1;
            }
        }
    }
    if statement_has_text {
        close_statement(&mut out, &mut names, &mut statements, &mut statement_start);
    }
    if statements.is_empty() {
        weft::node_bail!(
            "the query is empty (nothing but whitespace or comments); write the SQL to run"
        );
    }
    let parsed = PlaceholderSql { statements };
    // A `$1` is weft's own numbering and the author cannot write it,
    // wherever a statement binds a port. A script that binds nothing
    // is renumbered nowhere, so `$1` there is theirs and stays.
    if let Some(tag) = positional {
        let bound = parsed.names();
        if bound.is_empty() && parsed.statements.len() == 1 {
            weft::node_bail!(
                "the SQL uses `${tag}`, which is not a name this node can bind: parameters are \
                 the node's own input ports, read by name. Declare the port and name it: \
                 `PostgresExecuteQuery(user_id: String) {{ ... WHERE id = $user_id }}`"
            );
        }
        if !bound.is_empty() {
            weft::node_bail!(
                "the SQL mixes named parameters ({}) with `${tag}`, which is not a name this \
                 node can bind; name every parameter after the input port that carries it",
                bound.join(", ")
            );
        }
    }
    Ok(parsed)
}

/// Run several statements that name ports, one round trip each
/// through the extended protocol (so every value binds typed, the way
/// a single query's do), inside ONE transaction: a statement that
/// fails rolls the ones before it back, so a half-run script never
/// leaves the database between two states. Answers the rows of the
/// LAST statement, the way a script does.
pub async fn steps_json(
    client: &mut tokio_postgres::Client,
    steps: &[(Statement, Vec<Value>)],
) -> WeftResult<Vec<Value>> {
    let tx = client
        .transaction()
        .await
        .map_err(|e| weft::error::node_error(format!("postgres: begin the transaction: {}", pg_detail(&e))))?;
    let mut last: Vec<Value> = Vec::new();
    for (index, (statement, params)) in steps.iter().enumerate() {
        let boxed = params_of(params);
        let refs: Vec<&(dyn ToSql + Sync)> =
            boxed.iter().map(|b| b.as_ref() as &(dyn ToSql + Sync)).collect();
        let rows = tx.query(statement.sql.as_str(), &refs).await.map_err(|e| {
            weft::error::node_error(format!(
                "postgres: statement {} of the script: {} (the transaction was rolled back)",
                index + 1,
                spell_parameters(&pg_detail(&e), &statement.names)
            ))
        })?;
        last = rows_to_json(&rows)?;
    }
    tx.commit()
        .await
        .map_err(|e| weft::error::node_error(format!("postgres: commit the transaction: {}", pg_detail(&e))))?;
    Ok(last)
}

/// Run a script: everything before the last statement whole through
/// the simple-query protocol, then the last statement on its own
/// through the extended one, and answer the last statement's rows.
///
/// The simple protocol is the only one Postgres runs several
/// statements through, and the only one that runs transaction control
/// and the utility statements the extended protocol refuses, so the
/// leading statements keep it. It also types no column, so everything
/// it answers is the text Postgres prints. Only the last statement's
/// rows are ever read, so it is the only one that has to come back
/// typed, and running it separately on the SAME session leaves any
/// transaction the script opened open around it.
pub async fn script_json(
    client: &tokio_postgres::Client,
    head: &[String],
    last: &str,
) -> WeftResult<Vec<Value>> {
    if !head.is_empty() {
        run_script_head(client, &head.join(";\n")).await?;
    }
    let rows = client.query(last, &[]).await.map_err(|e| {
        weft::error::node_error(format!(
            "postgres: the last statement of the script: {}. Its rows are the ones this node \
             answers, so it runs on its own and has to be one Postgres can prepare; a \
             statement that cannot be (VACUUM, and the other utility commands) belongs \
             before the last one",
            pg_detail(&e)
        ))
    })?;
    rows_to_json(&rows)
}

/// The leading statements, whole, for their effects only: nothing
/// reads their rows.
async fn run_script_head(client: &tokio_postgres::Client, sql: &str) -> WeftResult<()> {
    client
        .simple_query(sql)
        .await
        .map_err(|e| weft::error::node_error(format!("postgres: run the script: {}", pg_detail(&e))))?;
    Ok(())
}
