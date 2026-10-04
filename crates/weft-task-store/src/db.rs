//! Connecting to the install's database. Every pool weft opens comes from
//! here, so every one lets go of a connection it has not used for
//! [`IDLE_CLOSE`]: a database that scales to zero (a serverless Postgres)
//! sleeps only once no connection to it is open, and a pool that kept its
//! idle connections would keep it up long after the last request.

use std::time::Duration;

use sqlx::postgres::{PgPool, PgPoolOptions};
use sqlx::Connection;

/// How long a pool keeps a connection nothing uses.
pub const IDLE_CLOSE: Duration = Duration::from_secs(30);

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

/// A pool on `url` of at most `max_connections`, each request for a
/// connection waiting at most `acquire_timeout`. The database may still be
/// starting: it is tried again for a minute before the error is answered.
pub async fn connect(url: &str, max_connections: u32, acquire_timeout: Duration) -> anyhow::Result<PgPool> {
    let deadline = std::time::Instant::now() + REACH_FOR;
    loop {
        match options(max_connections, acquire_timeout).connect(url).await {
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
        .idle_timeout(IDLE_CLOSE)
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
