//! Advisory locks in Postgres, shared by every role that coordinates
//! through the database: a key derived from a `(domain, scope)` pair, a
//! try-lock that skips when somebody holds it, and a lock that waits for
//! its holder without holding a connection while it waits. Each is held
//! by a transaction, so it is released however the holder ends.

/// Derive a stable i64 advisory-lock key from a `(domain, scope)`
/// pair. The domain string namespaces unrelated coordination
/// regimes so a collision in one doesn't bleed into another.
///
/// Uses FNV-1a, which is spec-stable: the same `(domain, scope)`
/// pair always derives to the same i64, regardless of rustc
/// version, std implementation, or build target. `DefaultHasher`
/// is NOT spec-stable across toolchains and using it here means
/// a rolling deploy across a rustc upgrade would split the lock
/// space mid-migration. FNV-1a is plenty for our needs (a few
/// thousand distinct keys at most) and the implementation is one
/// inlined function.
///
/// Collisions across (domain, scope) pairs in a 64-bit space are
/// vanishingly unlikely (birthday at ~4 billion distinct keys)
/// and harmless: two unrelated operations would serialize against
/// each other, adding latency, not breaking correctness.
pub fn advisory_key(domain: &str, scope: &str) -> i64 {
    // FNV-1a 64-bit: pure function of input bytes. Spec at
    // http://www.isthe.com/chongo/tech/comp/fnv/. We mix domain +
    // separator + scope so `advisory_key("foo", "barbaz") !=
    // advisory_key("foobar", "baz")`.
    const FNV_OFFSET: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x00000100000001b3;
    let mut hash: u64 = FNV_OFFSET;
    for byte in domain.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    // Mix a null separator byte between domain and scope so
    // (domain="foo", scope="barbaz") and (domain="foobar",
    // scope="baz") hash differently. `^= 0` is the FNV-1a XOR step
    // for a `\0` byte (a no-op visually, but the paired multiply
    // below is the round that actually separates the two segments).
    // Do NOT drop these two lines without re-pinning advisory_key's
    // test vectors; they change the hash output.
    hash ^= 0;
    hash = hash.wrapping_mul(FNV_PRIME);
    for byte in scope.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash as i64
}

/// Run `body` while holding the TRANSACTION-SCOPED advisory lock for
/// `key`, TRY-locking. Returns `Ok(None)` immediately if another holder
/// has the lock (caller decides what skipping means), else runs `body`
/// and returns `Ok(Some(result))`.
///
/// Panic-safety is the reason this is transaction-scoped, not session-
/// scoped. `pg_advisory_lock` (session-scoped) is NOT released when a
/// pooled connection is returned to the pool, so a panic mid-`body`
/// would orphan the lock on a recycled connection and wedge every future
/// acquisition until that physical connection ages out. A
/// `pg_try_advisory_xact_lock` is held by the transaction and released
/// the instant the transaction ends, including the ROLLBACK that sqlx's
/// `Transaction::drop` issues on a panic unwind. So we hold the lock via
/// a live `Transaction` (its connection is the lock holder) while `body`
/// runs its own work on SEPARATE pool connections, then drop the
/// transaction to release. No `catch_unwind`, no orphaned lock.
///
/// The lock lives in Postgres, so it serializes across N dispatcher
/// replicas.
pub async fn with_advisory_lock<T, F, Fut>(
    pg_pool: &sqlx::postgres::PgPool,
    key: i64,
    body: F,
) -> anyhow::Result<Option<T>>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<T>>,
{
    // The transaction's connection holds the xact lock for as long as the
    // transaction is alive. We never write through `tx`; `body` uses the
    // pool. Dropping `tx` (normal end OR panic unwind) rolls back and
    // releases the lock.
    let mut tx = pg_pool.begin().await?;
    let got: bool = sqlx::query_scalar("SELECT pg_try_advisory_xact_lock($1)")
        .bind(key)
        .fetch_one(&mut *tx)
        .await?;
    if !got {
        return Ok(None);
    }
    let result = body().await;
    // Explicit rollback releases the xact lock now (rather than waiting
    // for the implicit drop-rollback); errors here are non-fatal because
    // the drop would release it anyway.
    let _ = tx.rollback().await;
    result.map(Some)
}

/// How often a caller waiting in [`with_lock_waiting`] says so.
///
/// There is no deadline on the wait: a holder whose process dies drops
/// the lock with its connection, so a wait ends on its own, and a
/// deadline could only fail work that was merely queued. What a long wait
/// gets instead is a breadcrumb at this interval, so it is visible.
const WAITING_BREADCRUMB: std::time::Duration = std::time::Duration::from_secs(20);

/// Hold the lock for `key`, waiting behind its current holder, while
/// `body` runs. `waiting_for` names what is waited on, for the breadcrumb
/// a long wait leaves.
///
/// `lock_pool` is a pool of its own, never the one `body` works through.
/// An advisory lock lives in a transaction, so holding one holds a
/// connection that then does nothing, while every query the locked body
/// makes takes another. From one shared pool that is a deadlock waiting
/// for a busy day: as many concurrent holders as the pool is wide, none
/// able to get a connection to do its work, and so none able to finish
/// and release.
///
/// The wait POLLS a try-lock instead of blocking inside Postgres. A
/// blocking `pg_advisory_xact_lock` waits in the server, so every waiter
/// would pin a connection of the lock pool for as long as it waits, and a
/// burst on one key would exhaust the lock pool for every other key. A
/// waiter holds nothing between attempts, and a lock pool all of whose
/// connections are held by holders is waited out the same way. Polling
/// has no queue, so a waiter is not served in its turn; the backoff stays
/// short so one that has waited long is never far behind a newcomer.
pub async fn with_lock_waiting<T, F, Fut>(
    lock_pool: &sqlx::postgres::PgPool,
    key: i64,
    waiting_for: &str,
    body: F,
) -> anyhow::Result<T>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<T>>,
{
    let waiting_since = std::time::Instant::now();
    let mut said_at = std::time::Duration::ZERO;
    let mut backoff = std::time::Duration::from_millis(5);
    // Acquire first, then run the body once: the transaction holding the
    // lock has to outlive the loop, and the body is `FnOnce`.
    let tx = loop {
        match lock_pool.begin().await {
            Ok(mut tx) => {
                let got: bool = sqlx::query_scalar("SELECT pg_try_advisory_xact_lock($1)")
                    .bind(key)
                    .fetch_one(&mut *tx)
                    .await
                    .map_err(|e| anyhow::Error::from(e).context(format!("wait for {waiting_for}")))?;
                if got {
                    break tx;
                }
                // Rolled back BEFORE the sleep, so the connection goes
                // back to the pool while this caller waits.
                let _ = tx.rollback().await;
            }
            // A full pool: every lock connection is (or just was) held by a
            // holder, each of which ends, so wait for one like for the
            // lock. A pool with room that still timed out could not
            // connect at all, which is no wait.
            Err(sqlx::Error::PoolTimedOut) if lock_pool.size() >= lock_pool.options().get_max_connections() => {}
            Err(e) => return Err(anyhow::Error::from(e).context(format!("wait for {waiting_for}"))),
        }
        let waited = waiting_since.elapsed();
        if waited.saturating_sub(said_at) >= WAITING_BREADCRUMB {
            said_at = waited;
            tracing::info!(target: "weft_task_store::locks", waited_secs = waited.as_secs(), "still waiting for {waiting_for}");
        }
        // Jittered, so a burst of waiters on one key does not retry in
        // lockstep on the same tick.
        let jitter = std::time::Duration::from_micros(
            (uuid::Uuid::new_v4().as_u128() as u64) % (backoff.as_micros() as u64).max(1),
        );
        tokio::time::sleep(backoff + jitter).await;
        backoff = (backoff * 2).min(std::time::Duration::from_millis(50));
    };
    let result = body().await;
    // The xact lock dies with the transaction, panic or not.
    let _ = tx.rollback().await;
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Pin the FNV-1a output values. FNV-1a is spec-stable so
    /// these values are fixed across toolchains, OSes, and build
    /// targets. If any of these assertions fail, the
    /// implementation of `advisory_key` has been changed in a
    /// way that breaks lock-space compatibility with deployed
    /// processes. Roll forward only after confirming no two processes
    /// running different implementations coexist.
    #[test]
    fn advisory_key_pinned_values() {
        assert_eq!(advisory_key("weft_supervisor_coord", "tenant-a"), 5099131965359238650);
        assert_eq!(advisory_key("weft_project_transition", "00000000-0000-0000-0000-000000000042"), 8667617249734746809);
    }

    /// Domain separation: same scope, different domain, different key.
    #[test]
    fn advisory_key_domains_separate() {
        assert_ne!(advisory_key("weft_reaper", "tenant-a"), advisory_key("some_other_domain", "tenant-a"));
    }
}
