//! Coordination primitives shared across the dispatcher; it owns no table
//! of its own.
//!
//! - **`now_unix`**, the wall clock every lease and timestamp column is
//!   written in.
//! - **Advisory-lock key derivation** (`advisory_key` + the per-regime
//!   domain constants). Serializes cross-process state transitions
//!   without a dedicated lock table.

pub fn now_unix() -> i64 {
    // System clock past UNIX_EPOCH is a hard invariant; the fallback
    // to 0 in the old shape made threshold checks like
    // `now_unix - 90` go negative and silently mask broken clocks.
    // Fail loud instead.
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock is past UNIX_EPOCH")
        .as_secs() as i64
}

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

// Domain strings for advisory-key derivation. Use
// `advisory_key(domain, scope)` at the call site.

/// Serializes the read-state-then-flip entry into a PROJECT lifecycle
/// transition (activate, deactivate, the build marker, the has-infra
/// worker relocation), keyed by project id. Held only for the
/// microseconds of the read-and-CAS (or the short platform-call-bounded
/// worker relocation): the TRANSITIONAL STATE written into the project
/// row is the durable mutual exclusion that makes competing verbs
/// REJECT instantly; this lock only stops two verbs from both winning
/// the flip. Never held across a build, a drain, or user code.
pub const PROJECT_TRANSITION_DOMAIN: &str = "weft_project_transition";
/// Serializes a tenant's signal registrations from the route overlap
/// check to the signal row's insert, install-wide, keyed by tenant id.
/// The check reads the tenant's mounted routes and the insert writes
/// one; two routes of one activation register concurrently, and
/// without this each checked before the other had written, so two
/// routes that claim the same call both armed (the e2e `api_overlap`
/// shape). Held on the lock pool, so a waiter pins no work connection.
pub const SIGNAL_MOUNT_DOMAIN: &str = "weft_signal_mount";
/// Serializes each background reaper install-wide, keyed by the
/// reaper's name, so one dispatcher replica runs a given sweep at a
/// time and the others skip that turn (see `reaper`).
pub const REAPER_DOMAIN: &str = "weft_reaper";
/// Serializes issuing an upgrade of one owner's copies, keyed by
/// `<project>/<instance>` (empty instance: the shared copies), so the
/// in-flight check and the insert are one step install-wide.
pub const UPGRADE_ISSUE_DOMAIN: &str = "weft_upgrade_issue";

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

/// Like `with_advisory_lock` but BLOCKS until the lock is acquired
/// (`pg_advisory_xact_lock`, no try), then runs `body`. Use when the
/// caller must serialize behind the current holder rather than skip
/// (e.g. re-placing a single signal: the second re-placement waits for
/// the first, then re-checks state under the lock). Same panic-safety:
/// the xact lock dies with the transaction. While blocked it pins one
/// pool connection, which is fine for brief, low-contention per-key
/// serialization (one signal token / one project at a time).
pub async fn with_advisory_lock_blocking<T, F, Fut>(
    pg_pool: &sqlx::postgres::PgPool,
    key: i64,
    body: F,
) -> anyhow::Result<T>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<T>>,
{
    let mut tx = pg_pool.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(key)
        .execute(&mut *tx)
        .await?;
    let result = body().await;
    let _ = tx.rollback().await;
    result
}

/// How often a caller waiting for a project's transition lock says so.
///
/// There is NO deadline on the wait. Every holder does a few database
/// writes and returns (the version tree's writes in `api::versions`,
/// the start of an infra sync in `api::infra`, which awaits the sync
/// itself outside the lock), and a holder whose process dies drops the
/// lock with its connection. So a wait ends on its own, and the only
/// thing a deadline could catch is a slow database, where it would fail
/// a `weft run` or `weft checkpoint` that was merely queued. What a long
/// wait gets instead is a breadcrumb at this interval, so a slow one is
/// visible rather than silent.
const PROJECT_LOCK_BREADCRUMB: std::time::Duration = std::time::Duration::from_secs(20);

/// Turn a failure of [`with_project_transition_lock`] into the answer an
/// HTTP caller should get.
///
/// Every caller of that lock goes through here so they cannot disagree
/// about the same failure. One kind is left: the lock's own database
/// work failed, which is a server error. WAITING is not a
/// failure and never arrives here, because the wait has no deadline (see
/// [`PROJECT_LOCK_BREADCRUMB`]).
pub fn lock_answer(what: &str, e: anyhow::Error) -> (axum::http::StatusCode, String) {
    (axum::http::StatusCode::INTERNAL_SERVER_ERROR, format!("{what}: {e}"))
}

/// Hold the per-project transition lock for `project_id`, waiting behind
/// the current holder. The state-level rejection happens inside `body`,
/// which re-reads the row under the lock.
///
/// `lock_pool` is the dispatcher's LOCK pool, never the work pool. An
/// advisory lock lives in a transaction, so holding one holds a
/// connection that then does nothing, while every query the locked body
/// makes takes another. From one shared pool that is a deadlock waiting
/// for a busy day: as many concurrent operations as the pool is wide,
/// each holding a lock, none able to get a connection to do its work,
/// and so none able to finish and release. That is reachable with no
/// contention at all, one operation per project on as many projects as
/// the pool is wide. See `DispatcherState::lock_pool`.
///
/// The wait POLLS a try-lock instead of blocking inside Postgres. A
/// blocking `pg_advisory_xact_lock` waits in the server, so every waiter
/// would pin a connection of the lock pool for as long as it waits, and
/// a burst on one project would exhaust the lock pool for every project
/// on the process. A waiter holds nothing between attempts.
pub async fn with_project_transition_lock<T, F, Fut>(
    lock_pool: &sqlx::postgres::PgPool,
    project_id: uuid::Uuid,
    body: F,
) -> anyhow::Result<T>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<T>>,
{
    let key = advisory_key(PROJECT_TRANSITION_DOMAIN, &project_id.to_string());
    let waiting_since = std::time::Instant::now();
    let mut said_at = std::time::Duration::ZERO;
    let mut backoff = std::time::Duration::from_millis(5);
    // Acquire first, then run the body once: the transaction holding the
    // lock has to outlive the loop, and the body is `FnOnce`.
    let tx = loop {
        let mut tx = lock_pool.begin().await?;
        let got: bool = sqlx::query_scalar("SELECT pg_try_advisory_xact_lock($1)")
            .bind(key)
            .fetch_one(&mut *tx)
            .await?;
        if got {
            break tx;
        }
        // Rolled back BEFORE the sleep, so the connection goes back to
        // the pool while this caller waits.
        let _ = tx.rollback().await;
        // A breadcrumb instead of a deadline (see
        // `PROJECT_LOCK_BREADCRUMB`): a long wait must not be silent.
        let waited = waiting_since.elapsed();
        if waited.saturating_sub(said_at) >= PROJECT_LOCK_BREADCRUMB {
            said_at = waited;
            tracing::info!(
                target: "weft_dispatcher::lease",
                %project_id,
                waited_secs = waited.as_secs(),
                "still waiting for this project's transition lock; a version-tree write or \
                 the start of an infra sync holds it"
            );
        }
        // Jittered, because a try-lock loop has no queue: without it a
        // burst of waiters on one project converges on the same tick and
        // retries in lockstep for ever, so the one that has waited
        // longest is no likelier to win than the one that just arrived.
        let jitter = std::time::Duration::from_micros(
            (uuid::Uuid::new_v4().as_u128() as u64)
                % (backoff.as_micros() as u64).max(1),
        );
        tokio::time::sleep(backoff + jitter).await;
        backoff = (backoff * 2).min(std::time::Duration::from_millis(200));
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
        assert_eq!(
            advisory_key("weft_supervisor_coord", "tenant-a"),
            5099131965359238650,
        );
        assert_eq!(
            advisory_key(
                PROJECT_TRANSITION_DOMAIN,
                "00000000-0000-0000-0000-000000000042",
            ),
            8667617249734746809,
        );
    }

    /// Domain separation: same scope, different domain, different key.
    #[test]
    fn advisory_key_domains_separate() {
        let a = advisory_key(REAPER_DOMAIN, "tenant-a");
        let b = advisory_key("some_other_domain", "tenant-a");
        assert_ne!(a, b);
    }
}
