//! Connecting to the install's database. Every pool weft opens comes from
//! here, so every one lets go of a connection it has not used for
//! [`IDLE_CLOSE`]: a database that scales to zero (a serverless Postgres)
//! sleeps only once no connection to it is open, and a pool that kept its
//! idle connections would keep it up long after the last request.

use std::time::Duration;

use sqlx::postgres::{PgPool, PgPoolOptions};

/// How long a pool keeps a connection nothing uses.
pub const IDLE_CLOSE: Duration = Duration::from_secs(30);

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
}
