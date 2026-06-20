//! Shared SQL contract for claiming `infra_lifecycle_command` rows.
//!
//! Two consumers claim rows from this table, by DIFFERENT authorities:
//!   - the dispatcher's `lifecycle_claimer` loop (dispatcher verbs:
//!     deactivate / reactivate) serializes via the per-command
//!     `claimed_by_pod` claim lease (`claimable_predicate`), because the
//!     dispatcher has no per-project ownership lease of its own;
//!   - the broker's `supervisor_claim_command` handler (supervisor
//!     verbs: apply / stop / terminate) serializes via the EXCLUSIVE
//!     `infra_owner` lease (`owns_project_predicate`): a supervisor runs
//!     a project's command, and writes its `infra_node` state, only
//!     while it owns the project. The supervisor uses NO per-command
//!     claim lease (it would be redundant with exclusive ownership and
//!     its 300s expiry would wrongly let a sibling re-run a long command
//!     mid-flight). This is the supervisor's single-actor authority.
//!
//! The claim SQL is not 100% shared: each caller's RETURNING column list
//! and its serialization predicate differ. The CLAIM_LEASE_TTL constant
//! and `claimable_predicate` belong to the dispatcher path;
//! `owns_project_predicate` belongs to the supervisor path.

use std::time::Duration;

/// How long a DISPATCHER command claim stays exclusive before being
/// reclaimable. (Supervisor verbs do not use this lease; their authority
/// is `owns_project_predicate`.)
///
/// Tuned for k8s rolling updates: a graceful-shutdown deletes a pod in
/// ~30s. A dispatcher pod that gets `SIGTERM` mid-verb drops its claim
/// implicitly (the row sits with `claimed_by_pod = <old>` until the lease
/// expires). 5 minutes is comfortably longer than a dispatcher verb's
/// execution (deactivate/reactivate are signal-table transactions, not
/// long kubectl drains), so a live claimer never has its row reclaimed.
pub const CLAIM_LEASE_TTL: Duration = Duration::from_secs(300);

/// TTL on a supervisor's EXCLUSIVE `infra_owner` lease over a project.
/// The supervisor renews every owned project's lease on each ownership
/// tick (well inside this window), so a live supervisor holds its
/// projects indefinitely; the TTL only bounds how long a DEAD
/// supervisor's projects stay un-adopted before a live one claims them.
/// Kept short relative to `CLAIM_LEASE_TTL` so a crashed supervisor's
/// infra is re-owned quickly (health monitoring resumes), but well above
/// the supervisor's ownership-tick interval so a slow tick never drops a
/// lease it still wants.
pub const INFRA_OWNER_LEASE_SECS: i64 = 45;

/// Max projects a supervisor claims in ONE ownership tick. Claiming is
/// memory-gated (a pod stops claiming once its memory pressure reaches
/// the shared saturation threshold), so this just bounds how fast a pod
/// fills between ticks: it claims a batch, the next tick re-reads its
/// (now higher) pressure, and stops when saturated. Small enough that
/// pressure feedback throttles before a pod overshoots, large enough
/// that a cold pool fills in a few ticks.
pub const SUPERVISOR_CLAIM_BATCH: i64 = 16;

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

/// SQL `EXISTS (...)` fragment that is true iff `$pod_param` currently
/// holds a LIVE `infra_owner` lease over the project named by
/// `project_col`. This is the supervisor's ONE single-actor authority:
/// a supervisor may run a project's lifecycle command, and write its
/// `infra_node` state, only while it owns the project. The moment a
/// drain / lease-takeover moves ownership to another pod, every write
/// from the old pod is rejected and the command flows to the new owner.
///
/// Unlike the dispatcher's `claimed_by_pod` claim lease (which serializes
/// the dispatcher's own verbs and is the right tool there), the
/// supervisor needs no per-command claim lease at all: `infra_owner` is
/// exclusive (one pod per project) and continuously renewed on each
/// ownership tick, and a single owner's work loop is sequential, so two
/// supervisors can never run kubectl for one project. The supervisor's
/// kubectl ops are declarative (apply manifests, scale-to-N, delete-by-
/// label), so even the bounded window of one in-flight call from a
/// just-displaced owner converges rather than corrupts: it is the SAME
/// command's desired state, re-applied.
///
/// `$pod_param` is the 1-based bind index of the pod name (e.g. `"$1"`);
/// `project_col` is the SQL expression yielding the project id to check
/// (a column reference like `"c.project_id"` or a bind like `"$2"`). All
/// time comes from the DB clock so a skewed app host can't mis-judge the
/// lease.
///
/// The pod bound at `$pod_param` MUST be the supervisor's `WEFT_POD_NAME`
/// (the Deployment name stored in `infra_owner.supervisor_pod`), NOT the
/// auth token's (suffixed) pod name.
// SYNC: supervisor pod_name (the infra_owner lease key compared here) <-> crates/weft-broker-client/src/protocol.rs (Supervisor*Request.pod_name), crates/weft-infra-supervisor/src/lib.rs (SupervisorState.pod_name), crates/weft-dispatcher/src/supervisor_pool.rs (render_supervisor_manifest WEFT_POD_NAME env)
pub fn owns_project_predicate(pod_param: &str, project_col: &str) -> String {
    format!(
        "EXISTS ( \
            SELECT 1 FROM infra_owner io \
            WHERE io.project_id = {project_col} \
              AND io.supervisor_pod = {pod_param} \
              AND io.leased_until_unix >= EXTRACT(EPOCH FROM NOW())::BIGINT \
         )"
    )
}
