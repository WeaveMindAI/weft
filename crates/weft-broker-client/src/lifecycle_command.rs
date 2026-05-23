//! Shared SQL contract for claiming `infra_lifecycle_command` rows.
//!
//! Two consumers race for rows from this table:
//!   - the broker's `supervisor_claim_command` handler (supervisor
//!     verbs: apply / stop / terminate);
//!   - the dispatcher's `lifecycle_claimer` loop (dispatcher verbs:
//!     deactivate / reactivate).
//!
//! Both use the same UPDATE shape: FOR UPDATE SKIP LOCKED to race
//! across pods, plus a lease predicate so a pod that died mid-
//! execution (k8s rolling update, OOM kill, crash between claim
//! and complete) doesn't pin the row forever.
//!
//! The SQL itself can't be 100% shared because each caller's
//! RETURNING column list differs (the broker wants
//! `running_policy`; the dispatcher doesn't). The CLAIM PREDICATE
//! and the LEASE TTL constant ARE shared, which is what mattered.

use std::time::Duration;

/// How long a claim stays exclusive before being reclaimable.
///
/// Tuned for k8s rolling updates: a graceful-shutdown deletes a pod
/// in ~30s. A worker that gets `SIGTERM` mid-lifecycle drops its
/// claim implicitly (the row sits with `claimed_by_pod = <old>`
/// until the lease expires). 5 minutes is comfortably longer than
/// the longest expected single-command execution (`wait_for_drain`
/// in supervisor is ~10 minutes, BUT that one stays heartbeating
/// in-process so the claim is renewed; the lease applies to truly
/// abandoned rows).
///
/// If a future command needs a longer wall-clock, the pattern is
/// to renew the claim mid-execution (a periodic UPDATE of
/// `claimed_at_unix`) rather than to grow this constant.
pub const CLAIM_LEASE_TTL: Duration = Duration::from_secs(300);

/// SQL predicate that identifies "claimable" rows. Use inside the
/// inner SELECT of an UPDATE / FOR UPDATE SKIP LOCKED claim:
///
/// ```ignore
/// "UPDATE infra_lifecycle_command \
///  SET claimed_by_pod = $1, claimed_at_unix = EXTRACT(EPOCH FROM NOW())::BIGINT \
///  WHERE id = ( \
///     SELECT id FROM infra_lifecycle_command \
///     WHERE <caller's tenant/verb filter> AND <CLAIMABLE_PREDICATE> \
///     ORDER BY id ASC FOR UPDATE SKIP LOCKED LIMIT 1 \
///  ) \
///  RETURNING <caller's columns>"
/// ```
///
/// The predicate covers two cases:
///   - "not yet claimed" (the original happy path);
///   - "claimed by a dead pod whose lease expired" (the recovery
///     path). The lease bound is the SQL fragment
///     `NOW() - INTERVAL 'CLAIM_LEASE_TTL_SECS seconds'` rendered
///     by `claimable_predicate()`.
pub fn claimable_predicate() -> String {
    format!(
        "(claimed_by_pod IS NULL \
          OR claimed_at_unix < EXTRACT(EPOCH FROM NOW() - INTERVAL '{secs} seconds')::BIGINT) \
         AND completed_at_unix IS NULL",
        secs = CLAIM_LEASE_TTL.as_secs()
    )
}
