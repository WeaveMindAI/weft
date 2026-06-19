//! Tenant listener lease management.
//!
//! `tenant_listener` records the per-tenant listener Deployment +
//! its admin URL/token. Multi-Pod dispatchers re-attach to a
//! sibling Pod's listener via this row; restarts adopt orphan rows
//! whose owner has gone away.
//!
//! Lifecycle: `ListenerPool::with_listener` upserts the row inside
//! a transactional advisory-lock for state transitions. Every Pod
//! periodically calls `renew_tenant_listener` to keep the row's
//! ownership lease alive. On hard death the lease expires after
//! `LEASE_DURATION_SECS` and another Pod can adopt the row.
//!
//! Note: this row-ownership lease is distinct from the per-operation
//! advisory locks in `ListenerPool::with_listener`. The row lease
//! says "Pod X currently drives state transitions for this tenant";
//! the operation locks say "someone is currently using the listener,
//! reaper back off."

use anyhow::Result;
use sqlx::postgres::PgPool;

/// How long a row-ownership lease is valid before it must be renewed.
pub const LEASE_DURATION_SECS: i64 = 30;

/// Soft renewal interval. Owners renew this often to stay ahead of
/// expiry. Set well below `LEASE_DURATION_SECS`.
pub const LEASE_RENEW_INTERVAL_SECS: u64 = 10;

pub async fn migrate(pool: &PgPool) -> Result<()> {
    let stmts = [
        r#"CREATE TABLE IF NOT EXISTS tenant_listener (
            tenant_id TEXT PRIMARY KEY,
            owner_pod_id TEXT NOT NULL,
            leased_until_unix BIGINT NOT NULL,
            namespace TEXT NOT NULL,
            admin_url TEXT NOT NULL,
            state TEXT NOT NULL DEFAULT 'starting',
            -- Sentinel "an op is mid-flight on this tenant's listener,"
            -- mirroring the supervisor coord pattern. Listener ops
            -- (register_signal / display_signal / etc.) arm this with
            -- a short TTL under a per-tenant xact-scoped advisory
            -- lock; the reaper takes the same lock and reads the
            -- sentinel before deciding to delete the row + scale down
            -- the listener pod. No session-scoped locks anywhere.
            op_in_flight_until_unix BIGINT
        )"#,
        r#"CREATE INDEX IF NOT EXISTS idx_tenant_listener_owner ON tenant_listener(owner_pod_id)"#,
    ];
    for sql in stmts {
        sqlx::query(sql).execute(pool).await?;
    }
    Ok(())
}

pub fn is_lease_live(leased_until_unix: i64) -> bool {
    leased_until_unix >= now_unix()
}

pub async fn renew_tenant_listener(
    pool: &PgPool,
    tenant_id: &str,
    pod_id: &str,
) -> Result<bool> {
    let leased_until = now_unix() + LEASE_DURATION_SECS;
    // Only renew rows that are still LIVE (starting / alive). A row left in
    // `stopping` means a reap began and did not finish (the owner crashed
    // between `claim_stopping` and `delete_row`, or `backend.stop` hung).
    // Renewing such a row keeps its lease alive forever, which defeats the
    // crash-recovery in `decide_under_lock` (it only adopts a `stopping` row
    // once the lease lapses) and wedges every `with_listener` into waiting on a
    // stop that never completes. Letting a stuck `stopping` lease lapse IS the
    // recovery: the next ensure adopts it, re-runs the idempotent stop, and
    // respawns.
    let res = sqlx::query(
        "UPDATE tenant_listener \
         SET leased_until_unix = $1 \
         WHERE tenant_id = $2 AND owner_pod_id = $3 AND state <> 'stopping'",
    )
    .bind(leased_until)
    .bind(tenant_id)
    .bind(pod_id)
    .execute(pool)
    .await?;
    Ok(res.rows_affected() > 0)
}

/// One row-state snapshot for the listener reaper. The reaper walks
/// every row, takes an EXCLUSIVE per-tenant operation lock to fence
/// off in-flight `with_listener` calls, then decides whether to kill.
#[derive(Debug, Clone)]
pub struct ListenerRowSnapshot {
    pub tenant_id: String,
    pub namespace: String,
    pub state: String,
}

pub async fn list_tenant_listener_rows(pool: &PgPool) -> Result<Vec<ListenerRowSnapshot>> {
    let rows: Vec<(String, String, String)> = sqlx::query_as(
        "SELECT tenant_id, namespace, state FROM tenant_listener",
    )
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|(tenant_id, namespace, state)| ListenerRowSnapshot {
            tenant_id,
            namespace,
            state,
        })
        .collect())
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
pub const LISTENER_OP_DOMAIN: &str = "tenant_listener_op";
pub const LISTENER_ROW_DOMAIN: &str = "tenant_listener_row";

/// TTL on the `sync_in_flight_until_unix` sentinel and on the
/// `tenant_listener.op_in_flight_until_unix` sentinel. Both are
/// dispatcher-side heartbeats: a short TTL bounds "how long after
/// the dispatcher dies before the reaper can clean up." The
/// `TtlHeartbeat` machinery (below) re-arms the sentinel every
/// `TTL / 3` while work runs, so user-code runtime doesn't push
/// against this bound; only dispatcher liveness does.
pub const SENTINEL_TTL_SECS: i64 = 30;

/// Background heartbeat that re-arms a TTL sentinel column on a
/// fixed interval. Owns the spawned task; Drop aborts it. The
/// sentinel naturally expires `TTL` seconds after the last
/// heartbeat (i.e. after the holder dies, panics, or releases).
///
/// One abstraction, two callers:
///   - `ActivateKeepAlive` (listener.rs): wraps this for the
///     per-tenant listener op-sentinel.
///   - the `sync` handler (api/infra.rs): spawns one directly for
///     the per-project sync-in-flight sentinel.
///
/// Both pass a refresh closure; the only per-call-site difference
/// is which sentinel column gets re-armed.
pub struct TtlHeartbeat {
    handle: tokio::task::JoinHandle<()>,
}

impl TtlHeartbeat {
    /// `refresh` is invoked every `interval` until Drop. Failures
    /// are logged; the sentinel falls back to TTL expiry. The
    /// label is for tracing only.
    pub fn spawn<F, Fut>(label: &'static str, interval: std::time::Duration, refresh: F) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = anyhow::Result<()>> + Send,
    {
        let handle = tokio::spawn(async move {
            loop {
                tokio::time::sleep(interval).await;
                if let Err(e) = refresh().await {
                    tracing::warn!(
                        target: "weft_dispatcher::lease",
                        heartbeat = label,
                        error = %e,
                        "TtlHeartbeat refresh failed; sentinel will expire"
                    );
                }
            }
        });
        Self { handle }
    }
}

impl Drop for TtlHeartbeat {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

/// Default heartbeat interval: TTL / 3 (with a 5s floor so we
/// don't hammer Postgres for tiny TTLs).
pub fn heartbeat_interval() -> std::time::Duration {
    std::time::Duration::from_secs((SENTINEL_TTL_SECS / 3).max(5) as u64)
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
        assert_eq!(
            advisory_key(LISTENER_OP_DOMAIN, "tenant-a"),
            -2454865506030971390,
        );
        assert_eq!(
            advisory_key(LISTENER_ROW_DOMAIN, "tenant-a"),
            8045791053396251109,
        );
    }

    /// Domain separation: same scope, different domain → different key.
    #[test]
    fn advisory_key_domains_separate() {
        let a = advisory_key(SUPERVISOR_COORD_DOMAIN, "tenant-a");
        let b = advisory_key(LISTENER_OP_DOMAIN, "tenant-a");
        let c = advisory_key(LISTENER_ROW_DOMAIN, "tenant-a");
        assert_ne!(a, b);
        assert_ne!(b, c);
        assert_ne!(a, c);
    }
}
