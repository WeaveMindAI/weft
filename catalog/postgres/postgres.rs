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
//! as SQL NULL against any column, and an object or array as jsonb
//! against a json or jsonb placeholder and as its JSON text against any
//! other (so a list of numbers writes a pgvector column). A
//! number binds to whichever numeric column it is compared with or
//! written to (the integers, the floats, and `numeric` within the range
//! a fixed-point decimal holds), refusing a value the column cannot
//! hold rather than rounding it.

use serde_json::{json, Map, Value};

/// Fail the node now for a mistake in the program itself: the SQL it
/// wrote, a setting, a value of the wrong shape. Such a failure is an
/// input error, so it fails the run even when the node's `_error`
/// output is wired: the fix is editing the program, and nothing a
/// program does at run time can route around it. A refusal that
/// depends on the database's data or state stays `node_bail!`, which
/// `_error` catches.
macro_rules! mistake {
    ($($arg:tt)*) => {
        return Err(weft::WeftError::Input(format!($($arg)*)))
    };
}
pub(crate) use mistake;

/// A refusal from the server, as the error it is. A statement the
/// server cannot even plan (SQLSTATE class 42: a syntax error, a
/// table, column or function that does not exist, a parameter whose
/// type it cannot infer) is a mistake in the program's SQL, so it is
/// an input error that fails the run even with `_error` wired. Anything
/// else (a broken constraint, a `fail(..)` the SQL raised, a missing
/// privilege, a dropped connection) is the database's answer to this
/// run, which a program can handle.
pub fn refusal(message: String, e: &tokio_postgres::Error) -> WeftError {
    let in_the_sql = e.code().is_some_and(|state| {
        state.code().starts_with("42") && *state != tokio_postgres::error::SqlState::INSUFFICIENT_PRIVILEGE
    });
    if in_the_sql {
        WeftError::Input(message)
    } else {
        weft::error::node_error(message)
    }
}
use tokio_postgres::config::SslMode;
use tokio_postgres::types::{FromSql, Kind, ToSql, Type};
use tokio_postgres::{Config, GenericClient, Row};

use weft::access::OpenedConnection;
use weft::{RunContext, WeftError, WeftResult};

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
        .ssl_mode(ssl_mode)
        .connect_timeout(CONNECT_WITHIN)
        // A query waiting on a server that vanished gets no answer and no
        // error; these give it up about a minute after the server stopped
        // answering (`weft::net::KEEPALIVE_IDLE` says how), while a live
        // server's long query still waits as long as it needs.
        .keepalives(true)
        .keepalives_idle(weft::net::KEEPALIVE_IDLE)
        .keepalives_interval(weft::net::KEEPALIVE_INTERVAL)
        .keepalives_retries(weft::net::KEEPALIVE_RETRIES)
        .tcp_user_timeout(weft::net::UNANSWERED_FOR);
    Ok(config)
}

/// How long reaching the database's address may take before the dial
/// gives up. It bounds the TCP connect alone; the sign-in after it is
/// covered by the keepalive settings the config sets.
const CONNECT_WITHIN: std::time::Duration = std::time::Duration::from_secs(30);

/// The connection half of a dialed client: the future that must be
/// driven for the client to work.
type Connection = tokio_postgres::Connection<
    tokio_postgres::Socket,
    <tokio_postgres_rustls::MakeRustlsConnect as tokio_postgres::tls::MakeTlsConnect<tokio_postgres::Socket>>::Stream,
>;

/// Dial the wired database: the one handshake behind `connect` and
/// `connect_listening`, each of which drives the connection its own way.
async fn dial(ctx: &RunContext, conn: &OpenedConnection) -> WeftResult<(tokio_postgres::Client, Connection)> {
    dial_with(ctx, &dial_config(conn)?).await
}

/// The first wait before asking a full database again, doubled on every
/// refusal up to [`FULL_LONGEST_WAIT`].
const FULL_FIRST_WAIT: std::time::Duration = std::time::Duration::from_millis(500);
const FULL_LONGEST_WAIT: std::time::Duration = std::time::Duration::from_secs(10);

/// How often a worker says it is waiting on a full database: once a
/// minute at most, with how many runs wait, so a busy minute is one
/// line rather than one per run.
const SAY_FULL_EVERY: std::time::Duration = std::time::Duration::from_secs(60);

/// This worker's runs held back by a full database, counted per run
/// (from its first refusal until it gets a connection or stops), never
/// per refused attempt: how many runs were held back is what tells the
/// person how short the database is.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct FullWaits {
    /// When this worker last said so.
    pub(crate) said_at: Option<std::time::Instant>,
    /// Runs that began waiting since then.
    pub(crate) began: u64,
    /// Runs waiting right now.
    pub(crate) waiting: u64,
}

static FULL_WAITS: std::sync::Mutex<FullWaits> = std::sync::Mutex::new(FullWaits { said_at: None, began: 0, waiting: 0 });

/// One run waiting on a full database, counted in [`FULL_WAITS`] while
/// it lives.
struct HeldBack;

impl HeldBack {
    fn begin() -> Self {
        FULL_WAITS.lock().expect("FULL_WAITS poisoned").begin();
        Self
    }
}

impl Drop for HeldBack {
    fn drop(&mut self) {
        FULL_WAITS.lock().expect("FULL_WAITS poisoned").waiting -= 1;
    }
}

impl FullWaits {
    pub(crate) fn begin(&mut self) {
        self.began += 1;
        self.waiting += 1;
    }

    /// What to log at `now`, when a minute has passed since the last
    /// notice; the count of runs that began waiting starts again from
    /// there.
    pub(crate) fn notice(&mut self, now: std::time::Instant) -> Option<String> {
        if self.said_at.is_some_and(|at| now.duration_since(at) < SAY_FULL_EVERY) {
            return None;
        }
        let said = format!(
            "postgres: the database has no connection free (too many clients); {} run(s) wait for one now, \
             {} began waiting since the last notice. Runs slow down until one frees up. If this keeps happening, \
             raise the database's size: on a PostgresDatabase node, raise `memory` (its connection count follows it)",
            self.waiting, self.began
        );
        self.said_at = Some(now);
        self.began = 0;
        Some(said)
    }
}

/// [`dial`] with the place and the sign-in already read off the
/// connection. A database with no connection left to give
/// (`too_many_connections`) refused before anything ran, so the run
/// waits for one to free up instead of failing: the size of the database
/// is a limit to slow down at, and the warning names the lever.
async fn dial_with(ctx: &RunContext, config: &Config) -> WeftResult<(tokio_postgres::Client, Connection)> {
    let cancel = ctx.cancellation();
    let mut wait = FULL_FIRST_WAIT;
    // Counted once for the run, at its first refusal, and no longer once
    // it gets a connection or stops.
    let mut held_back: Option<HeldBack> = None;
    loop {
        match open(config).await? {
            Ok(opened) => return Ok(opened),
            Err(e) if e.code() == Some(&tokio_postgres::error::SqlState::TOO_MANY_CONNECTIONS) => {
                if held_back.is_none() {
                    held_back = Some(HeldBack::begin());
                }
                say_full(std::time::Instant::now());
                tokio::select! {
                    _ = tokio::time::sleep(wait) => {}
                    _ = cancel.cancelled() => return Err(WeftError::Cancelled),
                }
                wait = (wait * 2).min(FULL_LONGEST_WAIT);
            }
            Err(e) => return Err(connect_failed(e)),
        }
    }
}

/// Say that runs wait on a full database, when a minute has passed
/// since the last time.
fn say_full(now: std::time::Instant) {
    if let Some(said) = FULL_WAITS.lock().expect("FULL_WAITS poisoned").notice(now) {
        tracing::warn!("{said}");
    }
}

/// One handshake: the outer error is weft's own (the trust settings),
/// the inner one the database's answer.
async fn open(config: &Config) -> WeftResult<Result<(tokio_postgres::Client, Connection), tokio_postgres::Error>> {
    // The runtime's own trust settings, not a second opinion: one
    // answer to "which certificates does weft trust", and it pins the
    // crypto provider, which building a config here would leave to
    // whichever one the host binary happened to install.
    let tls_config = weft::net::tls_config().map_err(WeftError::Config)?;
    let tls = tokio_postgres_rustls::MakeRustlsConnect::new((*tls_config).clone());
    Ok(config.connect(tls).await)
}

/// A failed handshake as the node's error.
fn connect_failed(e: tokio_postgres::Error) -> WeftError {
    // A handshake failure is the one whose fix is not in the message the
    // driver gives back, because weft is stricter than what a provider
    // means by `require`: it checks the chain and the hostname every
    // time. Said ONLY for that failure, so a wrong password does not
    // come back with four lines about certificate authorities.
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
    WeftError::NodeFailed(format!("postgres: connect: {}{hint}", pg_detail(&e)))
}

/// The most runs of one worker that hold a connection to one database at
/// once. Kept low on purpose: Postgres takes 100 connections by default,
/// and several workers of a project share them, so 20 each lets a few
/// workers run side by side without the database refusing anyone. A
/// PostgresDatabase takes fewer on a small machine (24 at its default
/// 1Gi, see its `memory` input), so there a second worker can find it
/// full and waits for a connection to free up ([`dial_with`]).
/// A run past it waits for one to finish. A proper fix is a pooler in
/// front of the database (TODO.md, "Several workers of a project can open
/// more Postgres connections than the database takes").
pub(crate) const MOST_AT_ONCE: usize = 20;

/// How long a returning connection gets to clear its session before it
/// is let go instead. Clearing is a few short statements a healthy
/// server answers in milliseconds; a connection that sits longer is one
/// whose server is gone, and it must not keep its place under
/// [`MOST_AT_ONCE`] while the kernel works that out.
const CLEAR_WITHIN: std::time::Duration = std::time::Duration::from_secs(10);

/// Connections to one database, signed in once and held by the worker for
/// its runs to share (`ctx.shared`), instead of dialed and signed in again
/// by every run. Built from the connection's values, so a changed
/// password builds a fresh pool.
pub struct Pool {
    config: Config,
    idle: std::sync::Mutex<Vec<tokio_postgres::Client>>,
}

impl Pool {
    fn new(conn: &OpenedConnection) -> WeftResult<Self> {
        Ok(Self { config: dial_config(conn)?, idle: std::sync::Mutex::new(Vec::new()) })
    }
}

/// A client of the pool, for one run's use. It goes back to the pool when
/// dropped, once whatever the run left on its session is cleared
/// ([`clear_session`]), unless the connection broke or the run was
/// cancelled with it in hand (a statement may still be running on it;
/// closing the connection is what stops it).
///
/// SQL reaches it only through its own methods, so every statement a
/// run sends is seen on the way and the pool knows whether one of them
/// could have left a transaction open.
pub struct Pooled {
    client: Option<tokio_postgres::Client>,
    /// Held until the client is back in the pool or let go, so its place
    /// under the pool's limit stays taken while its session is cleared.
    pool: Option<weft::shared::SharedHandle<Pool>>,
    cancel: std::sync::Arc<weft::CancellationFlag>,
    /// Whether a statement this run sent opens a transaction
    /// ([`opens_transaction`]), so the one it opened may still be open.
    may_hold_transaction: bool,
}

impl Pooled {
    /// The client, for sending `statements`, noting whether any of them
    /// opens a transaction.
    fn sending<'a>(&mut self, statements: impl IntoIterator<Item = &'a str>) -> &mut tokio_postgres::Client {
        if statements.into_iter().any(opens_transaction) {
            self.may_hold_transaction = true;
        }
        self.client.as_mut().expect("a pooled client is held until it is dropped")
    }

    /// [`query_json`] on this client. Only `sql` is noted: the
    /// transaction `query_json` opens around a node's own write ends on
    /// every path (committed, or rolled back by the driver when dropped).
    pub async fn query_json(&mut self, sql: &str, names: &[String], params: &[Value], by: WrittenBy) -> WeftResult<Vec<Value>> {
        query_json(self.sending([sql]), sql, names, params, by).await
    }

    /// [`script_json`] on this client.
    pub async fn script_json(&mut self, head: &[String], last: &str) -> WeftResult<Vec<Value>> {
        let client = self.sending(head.iter().map(String::as_str).chain([last]));
        script_json(client, head, last).await
    }

    /// [`steps_json`] on this client.
    pub async fn steps_json(&mut self, steps: &[(Statement, Vec<Value>)]) -> WeftResult<Vec<Value>> {
        let client = self.sending(steps.iter().map(|(s, _)| s.sql.as_str()));
        steps_json(client, steps).await
    }
}

impl Drop for Pooled {
    fn drop(&mut self) {
        let (Some(client), Some(pool)) = (self.client.take(), self.pool.take()) else { return };
        if client.is_closed() || self.cancel.is_cancelled() {
            return;
        }
        let may_hold_transaction = self.may_hold_transaction;
        tokio::spawn(async move {
            match tokio::time::timeout(CLEAR_WITHIN, clear_session(&client, may_hold_transaction)).await {
                Ok(Ok(())) => pool.idle.lock().expect("pool poisoned").push(client),
                Ok(Err(e)) => tracing::warn!("postgres: a pooled connection is let go, its session could not be cleared: {e}"),
                Err(_) => tracing::warn!(
                    "postgres: a pooled connection is let go, its database did not answer within {}s while its session was cleared",
                    CLEAR_WITHIN.as_secs()
                ),
            }
        });
    }
}

/// Whether `statement` opens a transaction that outlives it: its first
/// word, past any comments, is `BEGIN` or `START` (`START TRANSACTION`).
/// Nothing else can: `COMMIT AND CHAIN` and its kin chain from a
/// transaction a `BEGIN` already opened, a function or procedure cannot
/// begin one, and a `BEGIN` inside a `DO` block or a function body is
/// past the statement's first word. Pure, so the decision is testable.
///
/// The statements are split by [`placeholders`], which reads quotes,
/// comments and dollar bodies the way the server does. Where the two
/// could still disagree (a database that turned off standard string
/// quoting), [`clear_session`] asks the server itself whether a
/// transaction was left open.
pub fn opens_transaction(statement: &str) -> bool {
    let mut rest = statement;
    loop {
        rest = rest.trim_start();
        if let Some(after) = rest.strip_prefix("--") {
            rest = after.find('\n').map_or("", |at| &after[at..]);
        } else if rest.starts_with("/*") {
            // Nested, the way Postgres nests them.
            let mut depth = 0usize;
            let mut at = 0;
            let bytes = rest.as_bytes();
            while at < bytes.len() {
                if bytes[at..].starts_with(b"/*") {
                    depth += 1;
                    at += 2;
                } else if bytes[at..].starts_with(b"*/") {
                    depth -= 1;
                    at += 2;
                    if depth == 0 {
                        break;
                    }
                } else {
                    at += 1;
                }
            }
            rest = &rest[at..];
        } else {
            break;
        }
    }
    let word: String = rest.chars().take_while(|c| c.is_ascii_alphanumeric() || *c == '_' || *c == '$').collect();
    word.eq_ignore_ascii_case("begin") || word.eq_ignore_ascii_case("start")
}

/// The column [`clear_session`] reads to find a transaction the run left
/// open that [`opens_transaction`] did not see coming.
const LEFT_OPEN: &str = "transaction_left_open";

/// Clear what a run left on a connection's session, so the next run gets
/// it as a fresh one would be: an open transaction, settings, temporary
/// tables, cursors, listens, advisory locks, and statements it prepared
/// in SQL. Everything `DISCARD ALL` does but drop the statements the
/// driver itself prepared, which it would then find gone.
///
/// The transaction is rolled back only when the run sent a statement
/// that opens one: a `ROLLBACK` with nothing open makes the server log a
/// warning on every run. The same round trip asks the server whether a
/// transaction is still open after that, by comparing when it began
/// with when this statement arrived (equal unless one began earlier),
/// and a connection where one is gets let go: closing it rolls the
/// transaction back, and the warning names the miss.
async fn clear_session(client: &tokio_postgres::Client, may_hold_transaction: bool) -> Result<(), String> {
    let rollback = if may_hold_transaction { "ROLLBACK; " } else { "" };
    let answer = client
        .simple_query(&format!(
            "{rollback}SELECT statement_timestamp() <> transaction_timestamp() AS {LEFT_OPEN}; \
             SELECT name FROM pg_prepared_statements WHERE from_sql"
        ))
        .await
        .map_err(|e| pg_detail(&e))?;
    let mut reset = String::from(
        "CLOSE ALL; SET SESSION AUTHORIZATION DEFAULT; RESET ALL; UNLISTEN *; \
         SELECT pg_advisory_unlock_all(); DISCARD PLANS; DISCARD TEMP; DISCARD SEQUENCES;",
    );
    for message in answer {
        let tokio_postgres::SimpleQueryMessage::Row(row) = message else { continue };
        if row.columns()[0].name() == LEFT_OPEN {
            if row.get(0) == Some("t") {
                return Err("the run left a transaction open through a statement not recognised as opening one".into());
            }
        } else if let Some(name) = row.get(0) {
            reset.push_str(&format!(" DEALLOCATE \"{}\";", name.replace('"', "\"\"")));
        }
    }
    client.batch_execute(&reset).await.map_err(|e| pg_detail(&e))
}

/// A client of the database `access` connects to, from the pool this
/// worker holds for it: an idle one, or a new one dialed when none is
/// idle. The connection task of a new one lives as long as the client
/// does.
pub async fn connect(ctx: &RunContext, access: &weft::access::Access) -> WeftResult<Pooled> {
    let pool = ctx.shared(access, |opened| async move { Pool::new(&opened) }).with_limit(MOST_AT_ONCE).await?;
    loop {
        let idle = pool.idle.lock().expect("pool poisoned").pop();
        match idle {
            Some(client) if !client.is_closed() => {
                return Ok(Pooled { client: Some(client), pool: Some(pool), cancel: ctx.cancellation(), may_hold_transaction: false });
            }
            // A connection the server or the network closed since.
            Some(_) => continue,
            None => break,
        }
    }
    let (client, connection) = dial_with(ctx, &pool.config).await?;
    // The connection future must be driven for the client to work; it
    // ends when the client drops. A connection failure surfaces on the
    // next query as a loud error, so the task only logs.
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            tracing::warn!("postgres connection ended: {e}");
        }
    });
    Ok(Pooled { client: Some(client), pool: Some(pool), cancel: ctx.cancellation(), may_hold_transaction: false })
}

/// A connection of its own to the wired database, for a run that holds
/// one for as long as it watches (it would hold a pooled one away from
/// every other run). Its task ends when the client drops or the run is
/// cancelled.
pub async fn connect_alone(ctx: &RunContext, conn: &OpenedConnection) -> WeftResult<tokio_postgres::Client> {
    let (client, connection) = dial(ctx, conn).await?;
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
    ctx: &RunContext,
    conn: &OpenedConnection,
    channel: &str,
) -> WeftResult<(tokio_postgres::Client, tokio::sync::mpsc::UnboundedReceiver<ListenEvent>)> {
    use futures::StreamExt;
    let (client, mut connection) = dial(ctx, conn).await?;
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
        .map_err(|e| WeftError::NodeFailed(format!("postgres: LISTEN {channel}: {}", pg_detail(&e))))?;
    Ok((client, rx))
}

/// A driver error with the reason underneath it.
///
/// `tokio_postgres::Error`'s own text for a rejected parameter is the
/// fixed string "error serializing parameter 0"; WHY it was rejected
/// lives in the source underneath. Printing only the top of the chain
/// throws away every refusal this module takes care to word, so the
/// chain is walked and joined.
///
/// A connection that closed under a statement says only "connection
/// closed", so that one gets what happened said plainly.
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
    if e.is_closed() {
        out.push_str(
            ". The connection to the database was lost before it answered: the database \
             server went away or stopped answering (it restarted, or the network to it broke)",
        );
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
        // value matters, not how it was written. The top bound is
        // strict: `i64::MAX as f64` rounds up to 2^63, one past the
        // largest i64, while `i64::MIN as f64` is exactly -2^63.
        let wide = self.float();
        if wide.fract() == 0.0 && wide >= i64::MIN as f64 && wide < i64::MAX as f64 {
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

/// A JSON object or list, bound as jsonb against a json or jsonb
/// placeholder and as its JSON text, in TEXT format, against any other.
///
/// The text form is what lets a list of numbers write a pgvector column:
/// `[0.1,0.2]` is exactly the text a `vector` or `halfvec` parses, and
/// any other type that cannot parse it refuses it in the database, before
/// anything is written. A Postgres array placeholder (`int4[]`) is the
/// exception: its text is `{1,2}`, never `[1,2]`, so a list there is
/// refused here, naming the string form that does bind.
#[derive(Debug)]
struct SqlJson(Value);

impl ToSql for SqlJson {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut bytes::BytesMut,
    ) -> Result<tokio_postgres::types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
        match *ty {
            Type::JSON | Type::JSONB => self.0.to_sql(ty, out),
            _ if matches!(ty.kind(), Kind::Array(_)) => Err(format!(
                "a JSON list or object cannot be bound to a {ty} column; pass the array as a \
                 string in Postgres's own form, such as \"{{1,2,3}}\""
            )
            .into()),
            _ => {
                out.extend_from_slice(self.0.to_string().as_bytes());
                Ok(tokio_postgres::types::IsNull::No)
            }
        }
    }

    fn accepts(_: &Type) -> bool {
        true
    }

    fn encode_format(&self, ty: &Type) -> tokio_postgres::types::Format {
        match *ty {
            Type::JSON | Type::JSONB => tokio_postgres::types::Format::Binary,
            _ => tokio_postgres::types::Format::Text,
        }
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
        other => Box::new(SqlJson(other.clone())),
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
        mistake!("an empty identifier cannot name a table or column");
    }
    Ok(format!("\"{}\"", name.replace('"', "\"\"")))
}

/// A scalar type a column can come back as, and so how its cells are
/// read into JSON.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scalar {
    Bool,
    Int2,
    Int4,
    Int8,
    Float4,
    Float8,
    Numeric,
    Text,
    Json,
    Uuid,
    Timestamptz,
    Timestamp,
    Date,
}

impl Scalar {
    /// An enum travels as its label's text, and `citext` and ltree's
    /// types come from extensions, so they have no fixed type id and are
    /// known by name: all of them read as text ([`PgText`]).
    fn of(ty: &Type) -> Option<Self> {
        Some(match *ty {
            Type::BOOL => Self::Bool,
            Type::INT2 => Self::Int2,
            Type::INT4 => Self::Int4,
            Type::INT8 => Self::Int8,
            Type::FLOAT4 => Self::Float4,
            Type::FLOAT8 => Self::Float8,
            Type::NUMERIC => Self::Numeric,
            Type::TEXT | Type::VARCHAR | Type::BPCHAR | Type::NAME => Self::Text,
            _ if matches!(ty.kind(), Kind::Enum(_)) => Self::Text,
            _ if matches!(ty.kind(), Kind::Simple)
                && matches!(ty.name(), "citext" | "ltree" | "lquery" | "ltxtquery") =>
            {
                Self::Text
            }
            Type::JSON | Type::JSONB => Self::Json,
            Type::UUID => Self::Uuid,
            Type::TIMESTAMPTZ => Self::Timestamptz,
            Type::TIMESTAMP => Self::Timestamp,
            Type::DATE => Self::Date,
            _ => return None,
        })
    }
}

/// How the cells of a column of some type are read into JSON. [`carry`]
/// is the one answer to "can a node read this column": [`readable`]
/// refuses a statement before it runs when a column has none, and
/// [`cell_of`] reads every cell by it, so the two cannot disagree.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Carry {
    /// One value of that scalar type.
    One(Scalar),
    /// An array of it (`array_agg`, a `text[]` column), as a JSON list,
    /// each element read the way its scalar column would be. One
    /// dimension: that is what `Vec` reads. A deeper array is a value of
    /// the same type, so it passes [`readable`] and the driver refuses it
    /// with its own message while reading; a node's own write undoes
    /// itself then ([`query_json`]).
    List(Scalar),
    /// A pgvector `vector`, as a JSON list of numbers.
    Vector,
    /// A pgvector `halfvec`, as a JSON list of numbers.
    HalfVector,
}

/// How a column of type `ty` is read, or `None` when no node can read it.
///
/// pgvector's types come from an extension, so they have no fixed type
/// id the way the built-in ones do, and are known by name.
pub fn carry(ty: &Type) -> Option<Carry> {
    match ty.kind() {
        Kind::Array(element) => Scalar::of(element).map(Carry::List),
        Kind::Simple if ty.name() == "vector" => Some(Carry::Vector),
        Kind::Simple if ty.name() == "halfvec" => Some(Carry::HalfVector),
        _ => Scalar::of(ty).map(Carry::One),
    }
}

/// Who wrote the SQL a statement runs, which decides what a refusal
/// over a column no node can read tells the author to do, and whether
/// the statement runs in a transaction of its own ([`query_json`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WrittenBy {
    /// The author, in the node's query: a cast in that query fixes it.
    /// Run as written, since a statement of the author's may be one
    /// Postgres refuses inside a transaction (`VACUUM`, `CREATE INDEX
    /// CONCURRENTLY`).
    Author,
    /// The node itself (an insert or update it builds, answering
    /// `RETURNING *`): there is no query of the author's to cast in, and
    /// the write commits only once its answer is read.
    Node,
}

/// A statement's columns, each with how it is read. The only way to turn
/// rows into JSON, and only [`readable`] makes one, so no row is decoded
/// past a column nobody checked.
#[derive(Debug)]
pub struct Readable(Vec<Carry>);

/// A column of a type no node reads, found when its statement was
/// prepared, before it ran. Worded by whoever knows where the statement
/// sits; every wording is an input error, since the fix is the program
/// or the table, never something a run can route around.
#[derive(Debug)]
pub struct Unreadable {
    pub column: String,
    pub ty: Type,
}

impl Unreadable {
    /// What is wrong, for a statement the author wrote.
    fn what(&self) -> String {
        format!("column '{}' has type {}, which this node does not carry", self.column, self.ty)
    }

    /// The column as SQL names it, always quoted: a bare name may fold to
    /// another, split, or be a word Postgres reads as something else
    /// (`user` is the signed-in role), so the fix a refusal suggests runs
    /// as it is copied.
    fn spelled(&self) -> String {
        format!("\"{}\"", self.column.replace('"', "\"\""))
    }

    /// The fix, for a statement the author wrote.
    fn cast(&self) -> String {
        format!("cast it in the query (e.g. {}::text)", self.spelled())
    }

    /// The refusal of a statement that runs on its own: nothing ran.
    pub fn refusal(&self, by: WrittenBy) -> WeftError {
        WeftError::Input(match by {
            WrittenBy::Author => {
                format!("{}, so the statement was refused before it ran; {}", self.what(), self.cast())
            }
            WrittenBy::Node => format!(
                "the table's column '{name}' has type {ty}, which this node cannot read back, \
                 so nothing was written. Do the write with Postgres: Query instead, returning \
                 that column cast to a type it reads (`RETURNING {spelled}::text`)",
                name = self.column,
                spelled = self.spelled(),
                ty = self.ty
            ),
        })
    }

    /// The refusal of a script's last statement ([`script_json`]): the
    /// statements before it have already run by the time it is prepared.
    pub fn in_script(&self) -> WeftError {
        WeftError::Input(format!(
            "postgres: the statements before the script's last one already ran (a transaction \
             the script opened is rolled back), and the last one was refused before it ran: \
             its {}; {}",
            self.what(),
            self.cast()
        ))
    }

    /// The refusal of statement `number` (from 1) of a script run in one
    /// transaction ([`steps_json`]).
    pub fn in_step(&self, number: usize) -> WeftError {
        WeftError::Input(format!(
            "postgres: statement {number} of the script: {}, so it was refused before it ran; {} \
             (the transaction was rolled back)",
            self.what(),
            self.cast()
        ))
    }
}

/// How each column of a statement is read, refusing the statement when
/// one of them cannot be. Postgres says what columns a statement answers
/// when it is prepared, before it runs, so this is checked before anything
/// is written. It checks TYPES; a value of a readable type that still
/// cannot be read (a two-dimensional array) is caught while reading, and
/// a node's own write rolls back then ([`query_json`]).
pub fn readable<'a>(columns: impl IntoIterator<Item = (&'a str, &'a Type)>) -> Result<Readable, Unreadable> {
    let mut carried = Vec::new();
    for (name, ty) in columns {
        let Some(how) = carry(ty) else {
            return Err(Unreadable { column: name.to_string(), ty: ty.clone() });
        };
        carried.push(how);
    }
    Ok(Readable(carried))
}

impl Readable {
    /// Answered rows as JSON objects keyed by column name, or what could
    /// not be read.
    fn rows(&self, rows: &[Row]) -> Result<Vec<Value>, String> {
        rows.iter()
            .map(|row| {
                let mut obj = Map::new();
                for (i, how) in self.0.iter().enumerate() {
                    let name = row.columns()[i].name();
                    let cell = cell_of(row, i, *how).map_err(|e| format!("column '{name}': {}", pg_detail(&e)))?;
                    obj.insert(name.to_string(), cell);
                }
                Ok(Value::Object(obj))
            })
            .collect()
    }
}

/// One answered cell as JSON, read the way its column's [`Carry`] says.
fn cell_of(row: &Row, i: usize, how: Carry) -> Result<Value, tokio_postgres::Error> {
    match how {
        Carry::One(scalar) => one(row, i, scalar),
        Carry::List(scalar) => list(row, i, scalar),
        Carry::Vector => row.try_get::<_, Option<PgVector<false>>>(i).map(|v| json!(v.map(|v| v.0))),
        Carry::HalfVector => row.try_get::<_, Option<PgVector<true>>>(i).map(|v| json!(v.map(|v| v.0))),
    }
}

/// One scalar cell as JSON (`null` for NULL).
fn one(row: &Row, i: usize, scalar: Scalar) -> Result<Value, tokio_postgres::Error> {
    match scalar {
        Scalar::Bool => row.try_get::<_, Option<bool>>(i).map(|v| json!(v)),
        Scalar::Int2 => row.try_get::<_, Option<i16>>(i).map(|v| json!(v)),
        Scalar::Int4 => row.try_get::<_, Option<i32>>(i).map(|v| json!(v)),
        Scalar::Int8 => row.try_get::<_, Option<i64>>(i).map(|v| json!(v)),
        Scalar::Float4 => row.try_get::<_, Option<f32>>(i).map(|v| v.map_or(Value::Null, single)),
        Scalar::Float8 => row.try_get::<_, Option<f64>>(i).map(|v| json!(v)),
        // A node that can WRITE a numeric column has to be able to
        // read one: `insert_row` answers with the row it just stored.
        Scalar::Numeric => row.try_get::<_, Option<PgNumeric>>(i).map(|v| v.map_or(Value::Null, |n| n.0)),
        Scalar::Text => row.try_get::<_, Option<PgText>>(i).map(|v| json!(v.map(|t| t.0))),
        Scalar::Json => row.try_get::<_, Option<Value>>(i).map(|v| v.unwrap_or(Value::Null)),
        Scalar::Uuid => row.try_get::<_, Option<uuid::Uuid>>(i).map(|v| json!(v)),
        Scalar::Timestamptz | Scalar::Timestamp | Scalar::Date => {
            row.try_get::<_, Option<PgMoment>>(i).map(|v| json!(v.map(|m| m.0)))
        }
    }
}

/// One array cell as a JSON list, each element read the way [`one`]
/// reads its scalar.
fn list(row: &Row, i: usize, scalar: Scalar) -> Result<Value, tokio_postgres::Error> {
    match scalar {
        Scalar::Bool => array(row, i, |v: bool| json!(v)),
        Scalar::Int2 => array(row, i, |v: i16| json!(v)),
        Scalar::Int4 => array(row, i, |v: i32| json!(v)),
        Scalar::Int8 => array(row, i, |v: i64| json!(v)),
        Scalar::Float4 => array(row, i, single),
        Scalar::Float8 => array(row, i, |v: f64| json!(v)),
        Scalar::Numeric => array(row, i, |v: PgNumeric| v.0),
        Scalar::Text => array(row, i, |v: PgText| json!(v.0)),
        Scalar::Json => array(row, i, |v: Value| v),
        Scalar::Uuid => array(row, i, |v: uuid::Uuid| json!(v)),
        Scalar::Timestamptz | Scalar::Timestamp | Scalar::Date => array(row, i, |v: PgMoment| json!(v.0)),
    }
}

/// A cell [`carry`] reads as text: the text types, an enum's label, and
/// the extension types `citext`, `ltree`, `lquery` and `ltxtquery`, each
/// read the way the driver reads it as a string (ltree's types open with
/// a version byte it strips). `String` itself would not do: it accepts no
/// enum.
struct PgText(String);

impl<'a> FromSql<'a> for PgText {
    fn from_sql(ty: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        <&str as FromSql>::from_sql(ty, raw).map(|s| Self(s.to_string()))
    }

    fn accepts(ty: &Type) -> bool {
        carry(ty) == Some(Carry::One(Scalar::Text))
    }
}

/// A `numeric` cell as JSON ([`numeric_from_bytes`]).
struct PgNumeric(Value);

impl<'a> FromSql<'a> for PgNumeric {
    fn from_sql(_: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        Ok(Self(numeric_from_bytes(raw)?))
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::NUMERIC
    }
}

/// A `numeric` value in binary form, as Postgres's `numeric_send` writes
/// it, as a JSON number: eight bytes of header, each a big-endian 16-bit
/// word (how many digits follow, the weight of the first one, the sign,
/// the display scale), then the digits, each 0 to 9999, base 10000. The
/// value is the sum of digit `i` times 10000 to the power `weight - i`.
///
/// Every legal value maps, which a fixed-point decimal type cannot do (it
/// refuses NaN, the infinities and anything past about 28 significant
/// digits). A finite value arrives as the nearest double, so one with more
/// significant digits than a double holds arrives rounded: anything that
/// has to survive exactly is cast to text in the query and comes back as a
/// string. NaN, `Infinity` and `-Infinity` (sign words 0xC000, 0xD000 and
/// 0xF000), which JSON has no number for, are `null`, as a double's are,
/// and so is a value past a double's range (about 1.8e308).
pub fn numeric_from_bytes(raw: &[u8]) -> Result<Value, String> {
    let word = |at: usize| u16::from_be_bytes([raw[at], raw[at + 1]]);
    if raw.len() < 8 {
        return Err(format!("a numeric value of {} bytes is shorter than its header", raw.len()));
    }
    let count = word(0) as i16;
    let weight = i32::from(word(2) as i16);
    let negative = match word(4) {
        0x0000 => false,
        0x4000 => true,
        0xC000 | 0xD000 | 0xF000 => return Ok(Value::Null),
        sign => return Err(format!("a numeric value has the unknown sign word {sign:#06x}")),
    };
    if count < 0 || raw.len() != 8 + count as usize * 2 {
        return Err(format!("a numeric value declares {count} digits but carries {} bytes of them", raw.len() - 8));
    }
    // The digits written out in decimal, four characters each, and the
    // power of ten the last of them stands for: the digits `12 3400` with
    // weight 0 are `000123400e-4`, which is 12.34. A double's parser
    // rounds that text to the nearest double, however many digits it
    // holds.
    let mut text = String::from(if negative { "-0" } else { "0" });
    for at in (8..raw.len()).step_by(2) {
        let digit = word(at);
        if digit > 9999 {
            return Err(format!("a numeric value carries the digit {digit}, past 9999"));
        }
        text.push_str(&format!("{digit:04}"));
    }
    let exponent = 4 * (weight - (i32::from(count) - 1));
    let value: f64 = format!("{text}e{exponent}").parse().map_err(|e| format!("a numeric value read as {text}: {e}"))?;
    Ok(serde_json::Number::from_f64(value).map_or(Value::Null, Value::Number))
}

/// A `timestamptz`, `timestamp` or `date` cell as its text
/// ([`moment_from_bytes`]).
struct PgMoment(String);

impl<'a> FromSql<'a> for PgMoment {
    fn from_sql(ty: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        moment_from_bytes(ty, raw).map(Self)
    }

    fn accepts(ty: &Type) -> bool {
        matches!(*ty, Type::TIMESTAMPTZ | Type::TIMESTAMP | Type::DATE)
    }
}

/// A `timestamptz` (`2026-09-16T22:14:48.601019+00:00`), `timestamp`
/// (`2026-09-16 22:14:48.601019`) or `date` (`2026-09-16`) in binary form
/// as text. Postgres also stores `infinity` and `-infinity` in each, as
/// the largest and smallest value of its integer (microseconds since 2000
/// for the timestamps, days since 2000 for a date), which no calendar type
/// reads; they come back as those two words, Postgres's own spelling. A
/// moment past year 262143, which Postgres holds and chrono does not, is
/// refused as unreadable.
pub fn moment_from_bytes(ty: &Type, raw: &[u8]) -> Result<String, Box<dyn std::error::Error + Sync + Send>> {
    let infinite = match raw.len() {
        8 if raw == i64::MAX.to_be_bytes() => Some("infinity"),
        8 if raw == i64::MIN.to_be_bytes() => Some("-infinity"),
        4 if raw == i32::MAX.to_be_bytes() => Some("infinity"),
        4 if raw == i32::MIN.to_be_bytes() => Some("-infinity"),
        _ => None,
    };
    if let Some(word) = infinite {
        return Ok(word.to_string());
    }
    Ok(match *ty {
        Type::TIMESTAMPTZ => chrono::DateTime::<chrono::Utc>::from_sql(ty, raw)?.to_rfc3339(),
        Type::TIMESTAMP => chrono::NaiveDateTime::from_sql(ty, raw)?.to_string(),
        Type::DATE => chrono::NaiveDate::from_sql(ty, raw)?.to_string(),
        _ => return Err(format!("{ty} is not a date or a timestamp").into()),
    })
}

/// One array cell as a JSON list (`null` for a NULL array, `null` for a
/// NULL element), each element turned into JSON by `each`.
fn array<'a, T: tokio_postgres::types::FromSql<'a>>(
    row: &'a Row,
    i: usize,
    each: impl Fn(T) -> Value,
) -> Result<Value, tokio_postgres::Error> {
    row.try_get::<_, Option<Vec<Option<T>>>>(i).map(|v| match v {
        None => Value::Null,
        Some(items) => Value::Array(items.into_iter().map(|e| e.map_or(Value::Null, &each)).collect()),
    })
}

/// A single-precision float as the JSON number written the shortest way
/// that reads back as the same float: `0.1`, where widening it to a
/// double would print `0.10000000149011612`. A NaN or an infinity, which
/// JSON has no number for, is `null`, as a double's is.
pub fn single(v: f32) -> Value {
    json!(v.to_string().parse::<f64>().expect("a float's own text parses back"))
}

/// A pgvector value: a `vector` when `HALF` is false, a `halfvec` when
/// it is true, read from the binary form the extension sends.
struct PgVector<const HALF: bool>(Vec<Value>);

impl<'a, const HALF: bool> FromSql<'a> for PgVector<HALF> {
    fn from_sql(_: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        Ok(Self(vector_from_bytes(raw, HALF)?.into_iter().map(single).collect()))
    }

    fn accepts(ty: &Type) -> bool {
        carry(ty) == Some(if HALF { Carry::HalfVector } else { Carry::Vector })
    }
}

/// The numbers of a pgvector value in binary form, as pgvector's
/// `vector_send` and `halfvec_send` write it: the dimension count as a
/// big-endian 16-bit integer, 16 unused bits, then each element
/// big-endian, a 32-bit float for a `vector` and a 16-bit IEEE half for a
/// `halfvec` (`half`). Anything whose length does not match the dimension
/// count it declares is refused, never read partly.
pub fn vector_from_bytes(raw: &[u8], half: bool) -> Result<Vec<f32>, String> {
    let kind = if half { "halfvec" } else { "vector" };
    let Some((head, body)) = raw.split_first_chunk::<4>() else {
        return Err(format!("a {kind} value of {} bytes is shorter than its header", raw.len()));
    };
    let dim = i16::from_be_bytes([head[0], head[1]]);
    let width = if half { 2 } else { 4 };
    if dim < 0 || body.len() != dim as usize * width {
        return Err(format!(
            "a {kind} value declares {dim} dimensions but carries {} bytes of them",
            body.len()
        ));
    }
    Ok(if half {
        body.chunks_exact(2).map(|b| half_to_f32(u16::from_be_bytes([b[0], b[1]]))).collect()
    } else {
        body.chunks_exact(4).map(|b| f32::from_be_bytes([b[0], b[1], b[2], b[3]])).collect()
    })
}

/// An IEEE 754 half-precision float (1 sign bit, 5 exponent bits, 10
/// fraction bits) widened to a single. Every half is exactly a single,
/// so nothing is rounded.
fn half_to_f32(bits: u16) -> f32 {
    let sign = if bits & 0x8000 == 0 { 1.0 } else { -1.0 };
    let exponent = i32::from((bits >> 10) & 0x1f);
    let fraction = f32::from(bits & 0x3ff);
    match exponent {
        // Subnormal: no implicit leading 1.
        0 => sign * fraction * 2f32.powi(-24),
        31 if fraction == 0.0 => sign * f32::INFINITY,
        31 => f32::NAN,
        e => sign * (1.0 + fraction / 1024.0) * 2f32.powi(e - 15),
    }
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
/// would point at something that is not there. `by` says who wrote the
/// SQL, which decides how a column no node can read is refused.
///
/// A statement the node wrote runs in a transaction of its own and
/// commits only once every row it answered is read, so a write whose
/// answer cannot be read back is never left stored for a retry to store
/// twice. The transaction ends on every path (the commit, or the
/// `ROLLBACK` the driver queues when it is dropped unfinished, which
/// reaches the server before anything sent after it), so it is not one
/// [`Pooled`] has to roll back for the next run.
pub async fn query_json(
    client: &mut tokio_postgres::Client,
    sql: &str,
    names: &[String],
    params: &[Value],
    by: WrittenBy,
) -> WeftResult<Vec<Value>> {
    let failed = |failed: Failed| match failed {
        Failed::Server(e) => refusal(
            format!("postgres: run the query: {}", spell_parameters(&pg_detail(&e), names)),
            &e,
        ),
        Failed::Unreadable(column) => column.refusal(by),
        Failed::Cell(e) => match by {
            WrittenBy::Author => weft::error::node_error(format!("postgres: read a cell: {e}")),
            WrittenBy::Node => weft::error::node_error(format!(
                "postgres: read the row back: {e}. The write was rolled back, so nothing was written"
            )),
        },
    };
    match by {
        WrittenBy::Author => query_checked(&*client, sql, params).await.map_err(failed),
        WrittenBy::Node => {
            let tx = client
                .transaction()
                .await
                .map_err(|e| weft::error::node_error(format!("postgres: begin the write: {}", pg_detail(&e))))?;
            let rows = query_checked(&tx, sql, params).await.map_err(failed)?;
            // A deferred constraint is checked here, so a refusal stays
            // the database's answer the program can handle.
            tx.commit().await.map_err(|e| refusal(format!("postgres: commit the write: {}", pg_detail(&e)), &e))?;
            Ok(rows)
        }
    }
}

/// Why [`query_checked`] answered no rows. Each caller words it, since
/// only the caller knows what had already run.
enum Failed {
    /// The server refused the statement, preparing or running it.
    Server(tokio_postgres::Error),
    /// A column it answers has a type no node reads; it never ran.
    Unreadable(Unreadable),
    /// It ran, and a value it answered could not be read.
    Cell(String),
}

/// Prepare `sql`, refuse it when a column it answers cannot be read
/// ([`readable`]), and only then run it and read its rows, so a
/// statement that writes never writes and then fails on the way out
/// over a column's type. Preparing is a step running the SQL text takes
/// anyway, so the check costs no extra round trip.
async fn query_checked(client: &(impl GenericClient + Sync), sql: &str, params: &[Value]) -> Result<Vec<Value>, Failed> {
    let statement = client.prepare(sql).await.map_err(Failed::Server)?;
    let readable =
        readable(statement.columns().iter().map(|c| (c.name(), c.type_()))).map_err(Failed::Unreadable)?;
    let boxed = params_of(params);
    let refs: Vec<&(dyn ToSql + Sync)> =
        boxed.iter().map(|b| b.as_ref() as &(dyn ToSql + Sync)).collect();
    let rows = client.query(&statement, &refs).await.map_err(Failed::Server)?;
    readable.rows(&rows).map_err(Failed::Cell)
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
        Plan::Steps(steps) => distinct_names(steps),
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
        distinct_names(&self.statements)
    }
}

/// Every port the statements read, each once, in order of first use.
fn distinct_names(statements: &[Statement]) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for s in statements {
        for n in &s.names {
            if !out.contains(n) {
                out.push(n.clone());
            }
        }
    }
    out
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
///
/// The compiler finds the same names before any run, with the regular
/// expression in the nodes' `placeholdersAreInputs` (JSON holds no
/// comment, so this is the only marker); a node test of
/// PostgresExecuteQuery holds the two to each other.
// SYNC: placeholders <-> catalog/postgres/execute_query/metadata.json placeholdersAreInputs.pattern, catalog/postgres/watch_query/metadata.json placeholdersAreInputs.pattern
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
                        mistake!("the SQL opens a /* comment it never closes");
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
                // The `e` has to start a token of its own: after a word
                // character or a `$` (part of an identifier like `a$e`,
                // or of a placeholder `$E`) it ends that token instead.
                let escapes_with_backslash = quote == '\''
                    && i > 0
                    && matches!(chars[i - 1], 'e' | 'E')
                    && (i < 2 || !(ident_char(chars[i - 2]) || chars[i - 2] == '$'));
                statement_has_text = true;
                out.push(quote);
                i += 1;
                loop {
                    let Some(&ch) = chars.get(i) else {
                        mistake!("the SQL opens a {quote} quote it never closes");
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
                        mistake!("the SQL opens a ${tag}$ quote it never closes");
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
        mistake!(
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
            mistake!(
                "the SQL uses `${tag}`, which is not a name this node can bind: parameters are \
                 the node's own input ports, read by name. Declare the port on the node, \
                 `(userId: String)`, and write `$userId` where its value goes"
            );
        }
        if !bound.is_empty() {
            mistake!(
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
async fn steps_json(
    client: &mut tokio_postgres::Client,
    steps: &[(Statement, Vec<Value>)],
) -> WeftResult<Vec<Value>> {
    let tx = client
        .transaction()
        .await
        .map_err(|e| weft::error::node_error(format!("postgres: begin the transaction: {}", pg_detail(&e))))?;
    let mut last: Vec<Value> = Vec::new();
    for (index, (statement, params)) in steps.iter().enumerate() {
        let failed = |e: tokio_postgres::Error| {
            refusal(
                format!(
                    "postgres: statement {} of the script: {} (the transaction was rolled back)",
                    index + 1,
                    spell_parameters(&pg_detail(&e), &statement.names)
                ),
                &e,
            )
        };
        // Only the last statement's rows are answered, so only its
        // columns have to be ones a node can read.
        if index + 1 == steps.len() {
            last = query_checked(&tx, &statement.sql, params).await.map_err(|f| match f {
                Failed::Server(e) => failed(e),
                Failed::Unreadable(column) => column.in_step(index + 1),
                Failed::Cell(e) => weft::error::node_error(format!(
                    "postgres: statement {} of the script: read a cell: {e} (the transaction was rolled back)",
                    index + 1
                )),
            })?;
        } else {
            let boxed = params_of(params);
            let refs: Vec<&(dyn ToSql + Sync)> =
                boxed.iter().map(|b| b.as_ref() as &(dyn ToSql + Sync)).collect();
            tx.execute(statement.sql.as_str(), &refs).await.map_err(failed)?;
        }
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
async fn script_json(
    client: &tokio_postgres::Client,
    head: &[String],
    last: &str,
) -> WeftResult<Vec<Value>> {
    if !head.is_empty() {
        run_script_head(client, &head.join(";\n")).await?;
    }
    query_checked(client, last, &[]).await.map_err(|f| match f {
        Failed::Server(e) => refusal(
            format!(
                "postgres: the last statement of the script: {}. Its rows are the ones this \
                 node answers, so it runs on its own and has to be one Postgres can prepare; \
                 a statement that cannot be (VACUUM, and the other utility commands) belongs \
                 before the last one",
                pg_detail(&e)
            ),
            &e,
        ),
        Failed::Unreadable(column) => column.in_script(),
        Failed::Cell(e) => weft::error::node_error(format!(
            "postgres: every statement of the script ran, and what the last one answered could not be read: {e}"
        )),
    })
}

/// The leading statements, whole, for their effects only: nothing
/// reads their rows.
async fn run_script_head(client: &tokio_postgres::Client, sql: &str) -> WeftResult<()> {
    client
        .simple_query(sql)
        .await
        .map_err(|e| refusal(format!("postgres: run the script: {}", pg_detail(&e)), &e))?;
    Ok(())
}
