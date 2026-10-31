//! Connecting to the install's database. Every pool weft opens comes from
//! here, and keeps its connections for as long as the process runs. A
//! database that sleeps (a serverless Postgres) sleeps once no query has
//! run for a while, whatever connections stay open, and closes them as it
//! goes; a pool finds each one closed when it next hands it out, drops it
//! and connects again. Closing idle connections earlier would only make the
//! next call open new ones (an encrypted handshake and a sign-in). How
//! many connections each pool takes is `crate::budget`'s to say.
//!
//! Every connection notices a database that vanished without a word (its
//! machine gone, the network to it broken): the system checks a quiet
//! connection for a live peer, and gives up on one whose statement goes
//! unanswered ([`config`]). A statement in flight on it then fails as
//! [`unreachable`], and a pool hands the connection out no more.
//!
//! Statements run through [`Client`], on a [`Pool`], a [`Conn`] taken from
//! one, a [`Transaction`] or a [`Session`]. Every text value a statement
//! binds goes without its NUL characters, which Postgres refuses, a
//! value that must read back exactly is kept as [`JsonText`], and only a
//! value that can hold no NUL goes into `JSONB` ([`Json`]) (all
//! three in `text`'s module doc).

mod client;
mod session;
#[cfg(any(test, feature = "test-helpers"))]
pub mod testing;
mod text;
mod tls;

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::Context as _;
use deadpool_postgres::{Manager, ManagerConfig, PoolError, RecyclingMethod};

pub use client::{Client, FromRow, Transaction};
pub use session::Session;
pub use text::{Json, JsonText, JsonbSafe, Names, Plain, WeftMade};
pub use tokio_postgres::types::{FromSql, ToSql};
pub use tokio_postgres::error::DbError;
pub use tokio_postgres::{Config, Notification, Row};

/// A value a statement takes for one of its `$n`.
pub type Param<'a> = &'a (dyn ToSql + Sync);

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// How long a boot keeps trying to reach a database that is not
/// answering yet (a local one still starting, a serverless one waking).
const REACH_FOR: Duration = Duration::from_secs(60);

/// The first pause before trying again to open a connection the database
/// refused; it doubles up to [`MOST_PAUSE`].
const FIRST_PAUSE: Duration = Duration::from_millis(10);
const MOST_PAUSE: Duration = Duration::from_secs(1);

/// The name a broker's record pool connections carry
/// (`application_name`), ahead of the broker's replica: one name per
/// broker, so its batches are told apart from other brokers' in the
/// database's own view of its sessions.
pub const RECORD_POOL_APPLICATION: &str = "weft-records";

/// What went wrong talking to the database.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The call got no connection, so not one of its statements ran.
    #[error("{why}")]
    NoConnection {
        why: NoConnection,
        /// The database's refusal, when it refused a new connection.
        #[source]
        refused: Option<tokio_postgres::Error>,
    },
    /// A statement read for its one row ([`Client::one`],
    /// [`Client::scalar`]) returned none.
    #[error("the database returned no row")]
    NoRow,
    /// The statement failed, or the connection it ran on did.
    #[error("{}", said(.0))]
    Postgres(tokio_postgres::Error),
    /// The pool itself failed (a hook, its runtime): nothing weft's pools
    /// are built to do.
    #[error("the connection pool failed: {0}")]
    Pool(PoolError),
}

impl From<tokio_postgres::Error> for Error {
    fn from(e: tokio_postgres::Error) -> Self {
        Self::Postgres(e)
    }
}

/// `e` in full: tokio-postgres says only "db error" or "error serializing
/// parameter 0" itself and keeps what went wrong underneath, which is the
/// part a person reading the error needs.
fn said(e: &tokio_postgres::Error) -> String {
    match e.as_db_error() {
        Some(db) => format!("error returned from database: {db}"),
        None => match std::error::Error::source(e) {
            Some(cause) => format!("{e}: {cause}"),
            None => e.to_string(),
        },
    }
}

impl Error {
    /// What the database answered, when the statement failed there: its
    /// code (SQLSTATE), message, and the constraint it broke.
    pub fn as_db_error(&self) -> Option<&DbError> {
        match self {
            Self::Postgres(e) => e.as_db_error(),
            _ => None,
        }
    }

    /// The database's own error code (SQLSTATE), when it answered with one.
    pub fn code(&self) -> Option<&str> {
        self.as_db_error().map(|e| e.code().code())
    }
}

/// Why a call got no connection, so not one of its statements ran.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum NoConnection {
    /// The database was too busy to give the call one: every connection
    /// this copy may hold was in use for the whole wait, or the database
    /// refused a new one because it was full (SQLSTATE 53300). Waiting
    /// works once the load passes, and more capacity makes it pass sooner.
    #[error("the database was too busy to give this a connection")]
    Busy,
    /// No new connection could be opened within the wait while this copy
    /// had room for one: the database is down, restarting, still starting
    /// (57P03), or out of reach of the network. Waiting works once it
    /// answers again; capacity has nothing to do with it.
    #[error("the database could not be reached")]
    Unreachable,
    /// The pool was closed: this process is shutting down.
    #[error("the database's connection pool is closed")]
    Closed,
}

/// Whether a database error means the database could not be reached or
/// is restarting, rather than that the statement itself failed: no
/// connection to be had (whatever kept it, [`no_connection`] says which),
/// a connection that broke, or the server refusing because it is shutting
/// down or starting up. Only that is worth asking again on; anything else
/// fails the same way next time. The one reading, for every service that
/// answers a caller who can wait.
pub fn unreachable(e: &Error) -> bool {
    match e {
        Error::NoConnection { .. } => true,
        Error::Postgres(e) => connection_lost(e),
        Error::NoRow | Error::Pool(_) => false,
    }
}

/// Whether `e` is the connection failing rather than the statement: it
/// closed or broke (the system gave up on a silent peer, the server
/// went away), or the server ended the session because it is shutting
/// down or restarting (SQLSTATE class 08, and 57P01 to 57P03).
fn connection_lost(e: &tokio_postgres::Error) -> bool {
    e.is_closed()
        || std::error::Error::source(e).is_some_and(|cause| cause.is::<std::io::Error>())
        || e.code().is_some_and(|code| {
            let code = code.code();
            code.starts_with("08") || matches!(code, "57P01" | "57P02" | "57P03")
        })
}

/// Why `e` means the call got no connection ([`NoConnection`]), or `None`
/// when it got one.
pub fn no_connection(e: &Error) -> Option<NoConnection> {
    match e {
        Error::NoConnection { why, .. } => Some(*why),
        _ => None,
    }
}

/// Whether a database error means the database was too busy to give the
/// call a connection ([`NoConnection::Busy`]).
pub fn busy(e: &Error) -> bool {
    no_connection(e) == Some(NoConnection::Busy)
}

/// [`busy`], anywhere in `e`'s chain.
pub fn database_busy(e: &anyhow::Error) -> bool {
    e.chain().filter_map(|cause| cause.downcast_ref::<Error>()).any(busy)
}

/// [`no_connection`], of the first cause in `e`'s chain that has one.
pub fn database_no_connection(e: &anyhow::Error) -> Option<NoConnection> {
    e.chain().filter_map(|cause| cause.downcast_ref::<Error>()).find_map(no_connection)
}

/// [`unreachable`], anywhere in `e`'s chain.
pub fn database_unreachable(e: &anyhow::Error) -> bool {
    e.chain().any(|cause| match cause.downcast_ref::<Error>() {
        Some(e) => unreachable(e),
        None => cause.downcast_ref::<tokio_postgres::Error>().is_some_and(connection_lost),
    })
}

/// Why the database refused to open a connection, when waiting may help:
/// it was full (53300), starting, stopping, or out of reach. `None` for a
/// refusal that comes back the same every time (a wrong password, a
/// database that does not exist).
fn refused_connection(e: &tokio_postgres::Error) -> Option<NoConnection> {
    if e.code().is_some_and(|code| code.code() == "53300") {
        return Some(NoConnection::Busy);
    }
    connection_lost(e).then_some(NoConnection::Unreachable)
}

/// How every connection to the install's database at `url` is made: the
/// address's own settings, and a check for a peer that vanished without a
/// word (`weft_core::net::KEEPALIVE_IDLE`): a quiet connection is probed,
/// and one whose statement goes unanswered for
/// `weft_core::net::UNANSWERED_FOR` is given up on, so a connection to a
/// database gone silent fails within about a minute instead of waiting
/// for the system's own limits (two hours idle, about fifteen minutes with
/// a statement in flight).
pub fn config(url: &str) -> anyhow::Result<Config> {
    let mut config: Config = url.parse().context("not a Postgres address")?;
    config
        .keepalives(true)
        .keepalives_idle(weft_core::net::KEEPALIVE_IDLE)
        .keepalives_interval(weft_core::net::KEEPALIVE_INTERVAL)
        .keepalives_retries(weft_core::net::KEEPALIVE_RETRIES)
        .tcp_user_timeout(weft_core::net::UNANSWERED_FOR);
    Ok(config)
}

/// A pool on `url` of at most `max_connections`, each request for a
/// connection waiting at most `acquire_timeout`, its waits counted as
/// `waits` says. The database may still be starting: it is tried again
/// for a minute before the error is answered.
pub async fn connect(url: &str, max_connections: u32, acquire_timeout: Duration, waits: Waits) -> anyhow::Result<Pool> {
    reach(config(url)?, max_connections, acquire_timeout, waits).await
}

/// The record pool of the broker `replica`, of `max_connections` (named
/// `RECORD_POOL_APPLICATION/<replica>`), on `url`, as [`connect`].
pub async fn connect_record_pool(url: &str, replica: &str, max_connections: u32, acquire_timeout: Duration) -> anyhow::Result<Pool> {
    let mut named = config(url)?;
    named.application_name(format!("{RECORD_POOL_APPLICATION}/{replica}"));
    reach(named, max_connections, acquire_timeout, Waits::Said).await
}

/// [`Pool::open`], then a first connection made, so a database that does
/// not answer fails the boot instead of its first request.
async fn reach(config: Config, max_connections: u32, acquire_timeout: Duration, waits: Waits) -> anyhow::Result<Pool> {
    let pool = Pool::open(config, max_connections, acquire_timeout, waits);
    reaching(|| async { pool.get().await.map(drop) }).await?;
    Ok(pool)
}

/// Run `attempt` until the database answers it, for [`REACH_FOR`] at most:
/// a database still starting, or waking, is waited for.
pub(crate) async fn reaching<T, F, Fut>(mut attempt: F) -> anyhow::Result<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    let deadline = Instant::now() + REACH_FOR;
    loop {
        match attempt().await {
            Ok(done) => return Ok(done),
            Err(e) if Instant::now() < deadline => {
                tracing::warn!(target: "weft_task_store::db", error = %e, "the database is not answering yet; trying again");
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
            Err(e) => return Err(e.into()),
        }
    }
}

/// Whether a pool's waits are the install being held back by its
/// database (`crate::throttle`): the work and record pools, which every
/// request and every batch go through. A lock pool is not: its holders
/// wait for each other by design (`crate::locks`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Waits {
    /// A request that waited longer than [`SLOW_WAIT`] for one of these
    /// pools' connections while none of them opened, or whose wait ran
    /// out with every connection handed out, waited for capacity, said by
    /// `crate::throttle`.
    Said,
    /// Part of how the pool is used.
    Expected,
}

/// How long a request may wait for a connection of a pool whose waits
/// are said ([`Waits::Said`]) before its wait counts as the database
/// holding the install back. A second: a request through these pools
/// runs a few short statements, so a connection comes free within
/// milliseconds while the pool keeps up; opening a new one takes
/// milliseconds on a machine and well under a second to a cloud database;
/// and a second is where a person waiting on an editor action or a CLI
/// command starts to feel it.
pub const SLOW_WAIT: Duration = Duration::from_secs(1);

/// A pool of connections to the install's database. Clones share it.
#[derive(Clone)]
pub struct Pool {
    inner: deadpool_postgres::Pool,
    shared: Arc<Shared>,
}

/// What a pool's clones and the connections it handed out share.
struct Shared {
    config: Config,
    in_use: InUse,
    waits: Waits,
    acquire_timeout: Duration,
    /// When the pool last opened a connection; `None` before the first.
    last_opened: Arc<Mutex<Option<Instant>>>,
}

impl Pool {
    /// A pool on `config` with no connection yet, of at most
    /// `max_connections`, each [`Self::get`] waiting at most
    /// `acquire_timeout`, its waits counted as `waits` says.
    pub fn open(config: Config, max_connections: u32, acquire_timeout: Duration, waits: Waits) -> Pool {
        let manager = Manager::from_config(
            config.clone(),
            tls::connector(),
            // A connection the database closed (it restarted, it went to
            // sleep, the system gave up on a silent peer) is closed here
            // too, and is dropped instead of handed out.
            ManagerConfig { recycling_method: RecyclingMethod::Fast },
        );
        let last_opened = Arc::new(Mutex::new(None));
        let opened = last_opened.clone();
        let inner = deadpool_postgres::Pool::builder(manager)
            .max_size(max_connections as usize)
            .post_create(deadpool_postgres::Hook::sync_fn(move |_, _| {
                *opened.lock().expect("last opened") = Some(Instant::now());
                Ok(())
            }))
            .build()
            .expect("a pool with no timeouts of its own always builds");
        let shared = Arc::new(Shared { config, in_use: InUse::new(max_connections), waits, acquire_timeout, last_opened });
        Pool { inner, shared }
    }

    /// How this pool's connections are made: a test opens a narrower pool,
    /// or a [`Session`], on the same database.
    pub fn config(&self) -> &Config {
        &self.shared.config
    }

    /// How long one [`Self::get`] waits for a connection, a new one being
    /// opened included.
    pub fn acquire_timeout(&self) -> Duration {
        self.shared.acquire_timeout
    }

    /// A connection of this pool, for several statements or a
    /// [`Transaction`]; it goes back to the pool when dropped.
    ///
    /// The whole wait is bounded by the pool's `acquire_timeout`, a new
    /// connection being opened included. A new connection the database
    /// refused for now (it is full, starting, stopping, or out of reach) is
    /// tried again within the wait, as a database still starting answers
    /// soon.
    pub async fn get(&self) -> Result<Conn> {
        let began = Instant::now();
        let deadline = began + self.shared.acquire_timeout;
        let mut pause = FIRST_PAUSE;
        let object = loop {
            // The wait ran out: busy if every connection of the pool was
            // handed out, unreachable if a place was still opening one
            // that never came.
            let ran_out = || {
                let why = if self.shared.in_use.full() { NoConnection::Busy } else { NoConnection::Unreachable };
                Error::NoConnection { why, refused: None }
            };
            let Ok(got) = tokio::time::timeout_at(deadline.into(), self.inner.get()).await else { return Err(ran_out()) };
            let refused = match got {
                Ok(object) => break object,
                Err(PoolError::Closed) => return Err(Error::NoConnection { why: NoConnection::Closed, refused: None }),
                Err(PoolError::Backend(e)) => e,
                Err(e) => return Err(Error::Pool(e)),
            };
            let Some(why) = refused_connection(&refused) else { return Err(refused.into()) };
            if Instant::now() + pause >= deadline {
                return Err(Error::NoConnection { why, refused: Some(refused) });
            }
            tokio::time::sleep(pause).await;
            pause = (pause * 2).min(MOST_PAUSE);
        };
        self.shared.in_use.handed_out();
        let conn = Conn { object, pool: self.shared.clone() };
        let waited = began.elapsed();
        if self.shared.waits == Waits::Said && waited > SLOW_WAIT {
            let last_opened = *self.shared.last_opened.lock().expect("last opened");
            if waited_for_capacity(waited, Instant::now(), last_opened) {
                crate::throttle::waited();
            }
        }
        Ok(conn)
    }

    /// Whether every connection this pool may hold is handed out to a
    /// request ([`InUse`]): a wait on it that ran out was a wait for one
    /// of them to come free, never a database that does not answer.
    pub fn every_connection_handed_out(&self) -> bool {
        self.shared.in_use.full()
    }

    /// Close the pool: every connection it holds is closed, and every
    /// [`Self::get`] after this fails as [`NoConnection::Closed`].
    pub fn close(&self) {
        self.inner.close();
    }

    /// Whether [`Self::close`] was called: a loop working through the pool
    /// stops instead of trying again.
    pub fn is_closed(&self) -> bool {
        self.inner.is_closed()
    }
}

/// A connection taken from a [`Pool`], back in it once dropped.
pub struct Conn {
    object: deadpool_postgres::Object,
    pool: Arc<Shared>,
}

impl Conn {
    /// Begin a transaction on this connection; it rolls back unless
    /// committed.
    pub async fn transaction(&mut self) -> Result<Transaction<'_>> {
        Ok(Transaction::new(self.object.transaction().await?))
    }
}

impl Drop for Conn {
    fn drop(&mut self) {
        self.pool.in_use.given_back();
    }
}

/// How many of one pool's connections are handed out right now: one more
/// for every [`Conn`] [`Pool::get`] returns, one less when it is dropped.
/// A place whose connection is still being opened is never counted: a
/// pool trying to open a connection to a database that does not answer
/// has its places taken, and nothing in use.
#[derive(Debug)]
struct InUse {
    connections: AtomicU32,
    max: u32,
}

impl InUse {
    fn new(max: u32) -> Self {
        Self { connections: AtomicU32::new(0), max }
    }

    fn handed_out(&self) {
        self.connections.fetch_add(1, Ordering::AcqRel);
    }

    fn given_back(&self) {
        let before = self.connections.try_update(Ordering::AcqRel, Ordering::Acquire, |n| Some(n.saturating_sub(1)));
        debug_assert_ne!(before, Ok(0), "a connection given back that was never counted as handed out");
    }

    /// Every connection this pool may hold is handed out to a request.
    fn full(&self) -> bool {
        self.connections.load(Ordering::Acquire) >= self.max
    }
}

/// Whether an acquire that took `waited` and ended at `now` waited for
/// capacity: its pool opened no connection inside it (the last at
/// `last_opened`). With one opened inside the wait, the time went to
/// connecting (a serverless database waking makes the first requests
/// after it open connections for a second or more, every time), and that
/// is the install working as it should.
fn waited_for_capacity(waited: Duration, now: Instant, last_opened: Option<Instant>) -> bool {
    match (last_opened, now.checked_sub(waited)) {
        (None, _) => true,
        (Some(opened), Some(began)) => opened < began,
        // The wait began before this process's clock can say: a
        // connection it opened since is inside it.
        (Some(_), None) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A pool is full only with every connection it may hold handed out;
    /// one given back frees its place.
    #[test]
    fn a_pool_is_full_only_with_every_connection_handed_out() {
        let in_use = InUse::new(2);
        assert!(!in_use.full(), "nothing handed out");
        in_use.handed_out();
        assert!(!in_use.full(), "room for one more");
        in_use.handed_out();
        assert!(in_use.full());
        in_use.given_back();
        assert!(!in_use.full(), "one given back");
    }

    /// A pool whose one place keeps trying a database that refuses it
    /// has nothing in use, and the wait that runs out reads unreachable,
    /// never busy.
    #[tokio::test]
    async fn a_database_that_refuses_reads_unreachable() {
        // Port 1 on this machine refuses at once, and the pool tries again
        // until the wait ends, as it does for a database still starting.
        let refused = config("postgres://weft:secret@127.0.0.1:1/weft").unwrap();
        let pool = Pool::open(refused, 1, Duration::from_millis(300), Waits::Said);
        let e = pool.get().await.err().expect("nothing answers on port 1");
        assert!(!pool.every_connection_handed_out(), "nothing is in use");
        assert_eq!(no_connection(&e), Some(NoConnection::Unreachable), "{e}");
        assert!(unreachable(&e));
        let chained = anyhow::Error::from(e).context("read the run");
        assert_eq!(database_no_connection(&chained), Some(NoConnection::Unreachable));
        assert!(database_unreachable(&chained));
        assert!(!database_busy(&chained));
    }

    /// A slow acquire is a wait for capacity only when no connection
    /// opened while it waited: one that opened inside the wait means the
    /// time went to connecting (a database waking), and is not counted.
    #[test]
    fn a_slow_acquire_counts_only_when_no_connection_opened_inside_it() {
        let now = Instant::now() + Duration::from_secs(60);
        let waited = Duration::from_secs(3);
        assert!(waited_for_capacity(waited, now, None), "no connection ever opened");
        assert!(waited_for_capacity(waited, now, Some(now - Duration::from_secs(10))), "the last opened before the wait");
        assert!(!waited_for_capacity(waited, now, Some(now - Duration::from_secs(1))), "one opened inside the wait");
        assert!(!waited_for_capacity(waited, now, Some(now)), "the request's own new connection");
    }

    /// Every connection is made to notice a database that went silent.
    #[test]
    fn every_connection_checks_for_a_silent_database() {
        let config = config("postgres://weft@db.example/weft?sslmode=require").unwrap();
        assert!(config.get_keepalives());
        assert_eq!(config.get_keepalives_idle(), weft_core::net::KEEPALIVE_IDLE);
        assert_eq!(config.get_keepalives_interval(), Some(weft_core::net::KEEPALIVE_INTERVAL));
        assert_eq!(config.get_keepalives_retries(), Some(weft_core::net::KEEPALIVE_RETRIES));
        assert_eq!(config.get_tcp_user_timeout(), Some(&weft_core::net::UNANSWERED_FOR));
    }
}
