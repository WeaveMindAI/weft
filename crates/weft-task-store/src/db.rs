//! Connecting to the install's database. Every pool weft opens comes from
//! here, and keeps its connections for as long as the process runs. A
//! database that sleeps (a serverless Postgres) sleeps once no query has
//! run for a while, whatever connections stay open, and closes them as it
//! goes; a pool finds that on its next call (see `PING_AFTER_IDLE`) and
//! connects again. Closing idle connections earlier would only make the
//! next call open new ones (an encrypted handshake and a sign-in).

use std::time::Duration;

use sqlx::postgres::{PgConnectOptions, PgPool, PgPoolOptions};
use sqlx::Connection;

/// How long a connection may have sat idle before it is pinged on its way
/// out of the pool. A connection a request just handed back goes straight
/// out again: the ping is one more round trip to the database, and a
/// request that runs thirty queries would pay it thirty times. One that
/// sat longer may have been cut under the pool (the database restarted,
/// the network dropped it), so it is checked and, if dead, replaced.
pub const PING_AFTER_IDLE: Duration = Duration::from_secs(5);

/// How long a boot keeps trying to reach a database that is not
/// answering yet (a local one still starting, a serverless one waking).
const REACH_FOR: Duration = Duration::from_secs(60);

/// How many connections the broker writes workers' batches of records on
/// (its record pool): as many batches go at once per broker, and the
/// search index waits while one broker has that many going, which is its
/// pool full.
pub const RECORD_POOL_CONNECTIONS: u32 = 8;

/// The name a broker's record pool connections carry
/// (`application_name`), ahead of the broker's replica: one name per
/// broker, so its batches are counted apart from other brokers'.
pub const RECORD_POOL_APPLICATION: &str = "weft-records";

/// Whether a database error means the database could not be reached or
/// is restarting, rather than that the statement itself failed: no
/// connection to be had, a connection that broke, or the server refusing
/// because it is shutting down or starting up (SQLSTATE class 08, and
/// 57P01 to 57P03). Only that is worth asking again on; anything else
/// fails the same way next time. The one reading, for every service that
/// answers a caller who can wait.
pub fn unreachable(e: &sqlx::Error) -> bool {
    match e {
        sqlx::Error::PoolTimedOut | sqlx::Error::PoolClosed | sqlx::Error::Io(_) | sqlx::Error::Protocol(_) => true,
        sqlx::Error::Database(db) => db
            .code()
            .is_some_and(|code| code.starts_with("08") || matches!(&*code, "57P01" | "57P02" | "57P03")),
        _ => false,
    }
}

/// A pool on `url` of at most `max_connections`, each request for a
/// connection waiting at most `acquire_timeout`. The database may still be
/// starting: it is tried again for a minute before the error is answered.
pub async fn connect(url: &str, max_connections: u32, acquire_timeout: Duration) -> anyhow::Result<PgPool> {
    reach(url.parse()?, max_connections, acquire_timeout).await
}

/// The record pool of the broker `replica` (`RECORD_POOL_CONNECTIONS`,
/// named `RECORD_POOL_APPLICATION/<replica>`), on `url`, as [`connect`].
pub async fn connect_record_pool(url: &str, replica: &str, acquire_timeout: Duration) -> anyhow::Result<PgPool> {
    let named: PgConnectOptions = url.parse()?;
    reach(named.application_name(&format!("{RECORD_POOL_APPLICATION}/{replica}")), RECORD_POOL_CONNECTIONS, acquire_timeout).await
}

async fn reach(connect: PgConnectOptions, max_connections: u32, acquire_timeout: Duration) -> anyhow::Result<PgPool> {
    let deadline = std::time::Instant::now() + REACH_FOR;
    loop {
        match options(max_connections, acquire_timeout).connect_with(connect.clone()).await {
            Ok(pool) => return Ok(pool),
            Err(e) if std::time::Instant::now() < deadline => {
                tracing::warn!(target: "weft_task_store::db", error = %e, "the database is not answering yet; trying again");
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
            Err(e) => return Err(e.into()),
        }
    }
}

/// The options every pool is built with.
pub fn options(max_connections: u32, acquire_timeout: Duration) -> PgPoolOptions {
    PgPoolOptions::new()
        .max_connections(max_connections)
        .min_connections(0)
        .idle_timeout(None)
        .acquire_timeout(acquire_timeout)
        .test_before_acquire(false)
        .before_acquire(|conn, meta| {
            Box::pin(async move {
                if meta.idle_for >= PING_AFTER_IDLE {
                    conn.ping().await?;
                }
                Ok(true)
            })
        })
}
