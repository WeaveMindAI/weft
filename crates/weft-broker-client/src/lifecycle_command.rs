//! Shared SQL contract for claiming `infra_lifecycle_command` rows.
//!
//! Two consumers claim rows from this table, by DIFFERENT authorities:
//!   - the dispatcher's `lifecycle_claimer` loop (dispatcher verbs:
//!     deactivate / reactivate) serializes via the per-command
//!     `claimed_by_pod` claim lease (`claimable_predicate`), because the
//!     dispatcher has no per-project ownership lease of its own;
//!   - the broker's `supervisor_claim_command` handler
//!     (`lifecycle_writes::next_command`; supervisor
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
/// long cluster drains), so a live claimer never has its row reclaimed.
pub const CLAIM_LEASE_TTL: Duration = Duration::from_secs(300);

/// TTL on a supervisor's EXCLUSIVE `infra_owner` lease over a project.
/// The supervisor renews every owned project's lease on each ownership
/// tick (well inside this window), so a live supervisor holds its
/// projects indefinitely; the TTL only bounds how long a DEAD
/// supervisor's projects stay un-adopted before a live one claims them.
/// Kept short relative to `CLAIM_LEASE_TTL` so a crashed supervisor's
/// infra is re-owned quickly (health monitoring resumes), but well above
/// the supervisor's ownership-tick interval so a slow tick never drops a
/// lease it still wants. 45 seconds in real time, at this install's
/// pace (`weft_core::time_scale`).
pub fn infra_owner_lease_secs() -> i64 {
    weft_core::time_scale::scaled_secs(45)
}

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
/// ownership tick, and the owner runs one project's commands in order,
/// so two supervisors can never change one project's cluster objects. The
/// supervisor's cluster calls are declarative (apply manifests, scale-to-N, delete-by-
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
    live_lease_exists(Some(pod_param), project_col)
}

/// SQL `EXISTS (...)` fragment that is true iff a LIVE `infra_owner`
/// lease covers the project named by `project_col`: held by the pod
/// bound at `pod_param` when one is given (that is
/// [`owns_project_predicate`]), by any supervisor otherwise. Time comes
/// from the DB clock.
pub fn live_lease_exists(pod_param: Option<&str>, project_col: &str) -> String {
    let pod = pod_param
        .map(|p| format!("AND io.supervisor_pod = {p} "))
        .unwrap_or_default();
    format!(
        "EXISTS ( \
            SELECT 1 FROM infra_owner io \
            WHERE io.project_id = {project_col} \
              {pod}AND io.leased_until_unix >= EXTRACT(EPOCH FROM NOW())::BIGINT \
         )"
    )
}

/// SQL condition that is true iff the `project` row aliased
/// `project_alias` is one a supervisor may own: only a namespaced
/// (paid-tier) project has infra. The ownership tick claims only these,
/// so every `infra_owner` row names one.
pub fn claimable_project(project_alias: &str) -> String {
    format!("{project_alias}.project_namespace <> ''")
}

/// The verbs a supervisor claims, as the SQL list inside `verb IN (...)`.
/// A macro so a partial index's DDL (a `&'static str` that cannot call a
/// function) splices the same text in with `concat!`; every other query
/// uses [`SUPERVISOR_VERBS_SQL`].
// SYNC: the supervisor verbs <-> crates/weft-broker-client/src/protocol.rs
//       (InfraLifecycleVerb Apply / Stop / Terminate)
#[macro_export]
macro_rules! supervisor_verbs_sql {
    () => {
        "'apply', 'stop', 'terminate'"
    };
}

/// [`supervisor_verbs_sql!`] as a constant.
pub const SUPERVISOR_VERBS_SQL: &str = supervisor_verbs_sql!();

/// The verbs the dispatcher's `lifecycle_claimer` claims, as the SQL
/// list inside `verb IN (...)`. A macro for the same reason as
/// [`supervisor_verbs_sql!`]: its partial index splices it in.
// SYNC: the dispatcher verbs <-> crates/weft-broker-client/src/protocol.rs
//       (InfraLifecycleVerb Deactivate / Reactivate)
#[macro_export]
macro_rules! dispatcher_verbs_sql {
    () => {
        "'deactivate', 'reactivate'"
    };
}

/// [`dispatcher_verbs_sql!`] as a constant.
pub const DISPATCHER_VERBS_SQL: &str = dispatcher_verbs_sql!();

/// SQL condition that is true iff the `infra_lifecycle_command` row
/// aliased `command_alias` still waits on a supervisor: not completed,
/// of a supervisor verb (apply / stop / terminate; deactivate and
/// reactivate are the dispatcher's), on a [`claimable_project`]. A
/// cancel-flagged row still counts: only a supervisor running it (and
/// hitting the cancel check) completes it.
pub fn pending_supervisor_command(command_alias: &str) -> String {
    format!(
        "{c}.verb IN ({verbs}) \
         AND {c}.completed_at_unix IS NULL \
         AND EXISTS ( \
             SELECT 1 FROM project cp \
             WHERE cp.id = {c}.project_id AND {claimable} \
         )",
        c = command_alias,
        verbs = SUPERVISOR_VERBS_SQL,
        claimable = claimable_project("cp"),
    )
}

/// The channel an `infra_lifecycle_command` row notifies on, from the
/// `infra_command_notify` trigger in the dispatcher's
/// `infra_lifecycle_command::GROUP`: once when the command is issued,
/// and once when it completes. The claimers wake on the first, and
/// whoever waits on a command's outcome wakes on the second.
pub const INFRA_COMMAND_CHANNEL: &str = "weft_infra_command";

/// What an [`INFRA_COMMAND_CHANNEL`] notification says.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InfraCommandSignal<'a> {
    /// A command was issued for this project (`issued:<project_id>`).
    Issued { project_id: &'a str },
    /// This command completed (`done:<id>`).
    Done { id: i64 },
}

impl<'a> InfraCommandSignal<'a> {
    /// Read a notification's payload; `None` for one this build does
    /// not know, which concerns nobody here.
    // SYNC: the payload shapes <-> the `infra_command_notify` function in
    //       crates/weft-dispatcher/src/infra_lifecycle_command.rs (GROUP)
    pub fn parse(payload: &'a str) -> Option<Self> {
        if let Some(project_id) = payload.strip_prefix("issued:") {
            return Some(Self::Issued { project_id });
        }
        payload.strip_prefix("done:")?.parse().ok().map(|id| Self::Done { id })
    }
}

#[cfg(test)]
mod verb_tests {
    use crate::protocol::InfraLifecycleVerb;

    /// The literal list names exactly the supervisor's verbs.
    #[test]
    fn the_supervisor_verb_list_names_the_supervisor_verbs() {
        let want = [InfraLifecycleVerb::Apply, InfraLifecycleVerb::Stop, InfraLifecycleVerb::Terminate]
            .map(|v| format!("'{}'", v.as_str()))
            .join(", ");
        assert_eq!(super::SUPERVISOR_VERBS_SQL, want);
    }

    /// The literal list names exactly the dispatcher's verbs.
    #[test]
    fn the_dispatcher_verb_list_names_the_dispatcher_verbs() {
        let want = [InfraLifecycleVerb::Deactivate, InfraLifecycleVerb::Reactivate]
            .map(|v| format!("'{}'", v.as_str()))
            .join(", ");
        assert_eq!(super::DISPATCHER_VERBS_SQL, want);
    }
}

#[cfg(test)]
mod signal_tests {
    use super::InfraCommandSignal;

    #[test]
    fn both_payloads_read_back() {
        assert_eq!(
            InfraCommandSignal::parse("issued:00000000-0000-0000-0000-000000000001"),
            Some(InfraCommandSignal::Issued { project_id: "00000000-0000-0000-0000-000000000001" }),
        );
        assert_eq!(InfraCommandSignal::parse("done:42"), Some(InfraCommandSignal::Done { id: 42 }));
    }

    #[test]
    fn an_unknown_payload_concerns_nobody() {
        assert_eq!(InfraCommandSignal::parse("done:x"), None);
        assert_eq!(InfraCommandSignal::parse("started:1"), None);
    }
}
