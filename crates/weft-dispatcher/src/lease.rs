//! Lease + coordination primitives shared across the dispatcher.
//!
//! This is the generic toolkit the pooled placement machinery is built
//! on; it owns no table of its own. Two things live here:
//!
//! - **Row-ownership lease helpers** (`LEASE_DURATION_SECS`,
//!   `LEASE_RENEW_INTERVAL_SECS`, `is_lease_live`, `now_unix`). A
//!   registry row (e.g. a `listener_pod` or `supervisor_pod` entry)
//!   carries a `leased_until_unix`; its owning dispatcher Pod renews it
//!   every `LEASE_RENEW_INTERVAL_SECS`, and on hard death the lease
//!   expires after `LEASE_DURATION_SECS` so a sibling Pod can adopt it.
//! - **Advisory-lock key derivation** (`advisory_key` + the per-regime
//!   domain constants). Serializes cross-Pod state transitions (listener
//!   + supervisor pick-or-spawn) without a dedicated lock table.

/// How long a row-ownership lease is valid before it must be renewed.
pub const LEASE_DURATION_SECS: i64 = 30;

/// Soft renewal interval. Owners renew this often to stay ahead of
/// expiry. Set well below `LEASE_DURATION_SECS`.
pub const LEASE_RENEW_INTERVAL_SECS: u64 = 10;

/// Spawn grace for a freshly-placed pool pod (listener / supervisor).
///
/// A new pod has a window between "spawned + registry row inserted" and
/// "its first work is recorded in the DB" (a listener's
/// `signal.listener_pod`, a supervisor's `infra_owner` row). During that
/// window the idle reaper would see ZERO work on the pod and tear it
/// down mid-setup (the "object has been deleted" race). The pod's
/// registry row carries a `grace_until_unix`; the idle reaper skips any
/// pod still inside its grace. Sized to comfortably cover spawn ->
/// health-wait -> register -> first work-row write under cluster load,
/// while still letting a pod whose placement genuinely failed get reaped
/// shortly after. Distinct from the ownership lease: the lease says
/// "which dispatcher drives this pod," the grace says "this pod is too
/// young to be judged idle yet."
pub const SPAWN_GRACE_SECS: i64 = 30;

pub fn is_lease_live(leased_until_unix: i64) -> bool {
    leased_until_unix >= now_unix()
}

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
/// regimes (supervisor-vs-sync, listener-op-vs-row, future
/// additions) so a collision in one doesn't bleed into another.
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

/// Domain strings for advisory-key derivation. Use
/// `advisory_key(domain, scope)` at the call site.
pub const SUPERVISOR_COORD_DOMAIN: &str = "weft_supervisor_coord";
/// Serializes listener pick-or-spawn so a cold-start burst of
/// concurrent placements funnels to ONE new listener instead of each
/// spawning its own (the thundering-herd race). One scope value
/// (`"placement"`): the pool is global.
pub const LISTENER_POOL_DOMAIN: &str = "weft_listener_pool";
/// Serializes pool scale-DOWN (drain + reap) cluster-wide so two
/// dispatcher replicas never consolidate the same pool at once (which
/// would drain one pod twice, or drain two pods onto each other). One
/// scope value per pool (`"listener"` / `"supervisor"`). A dispatcher
/// that fails the try-lock simply skips this cycle; the next sweep
/// retries. Mirrors the pick-or-spawn lock shape.
pub const POOL_SCALEDOWN_DOMAIN: &str = "weft_pool_scaledown";
/// Serializes the RE-PLACEMENT of a single signal (reserve generation ->
/// register on the new pod -> write the holder) cluster-wide, keyed by
/// signal token. Without it, two concurrent re-placements of one token
/// (a drain racing a fire-path re-place, or two idle-signal fires on two
/// dispatchers) can leave the holder column pointing at a pod registered
/// under a LOWER generation than another still-live pod, defeating the
/// broker's stale-fire fence (which assumes the row's generation is the
/// highest any live holder carries). Holding this across the whole
/// sequence makes "the highest reserved generation is the final holder"
/// an invariant instead of a race outcome. The scope is the token.
pub const SIGNAL_PLACEMENT_DOMAIN: &str = "weft_signal_placement";

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

/// Convenience wrapper: hold the cluster-wide scale-down lock for
/// `pool_scope` (`"listener"` / `"supervisor"`). `Ok(None)` means a
/// sibling is consolidating this pool right now; skip this cycle.
pub async fn with_scaledown_lock<T, F, Fut>(
    pg_pool: &sqlx::postgres::PgPool,
    pool_scope: &str,
    body: F,
) -> anyhow::Result<Option<T>>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<T>>,
{
    with_advisory_lock(pg_pool, advisory_key(POOL_SCALEDOWN_DOMAIN, pool_scope), body).await
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Pin the FNV-1a output values. FNV-1a is spec-stable so
    /// these values are fixed across toolchains, OSes, and build
    /// targets. If any of these assertions fail, the
    /// implementation of `advisory_key` has been changed in a
    /// way that breaks lock-space compatibility with deployed
    /// pods. Roll forward only after confirming no two pods
    /// running different implementations coexist.
    #[test]
    fn advisory_key_pinned_values() {
        assert_eq!(
            advisory_key(SUPERVISOR_COORD_DOMAIN, "tenant-a"),
            5099131965359238650,
        );
    }

    /// Domain separation: same scope, different domain, different key.
    #[test]
    fn advisory_key_domains_separate() {
        let a = advisory_key(SUPERVISOR_COORD_DOMAIN, "tenant-a");
        let b = advisory_key("some_other_domain", "tenant-a");
        assert_ne!(a, b);
    }
}
