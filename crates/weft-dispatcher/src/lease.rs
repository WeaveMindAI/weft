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

pub use weft_task_store::locks::{advisory_key, with_advisory_lock};

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
/// Serializes changing the install's domains with putting them on the
/// platform's door, install-wide, so two changes at once never leave the
/// door serving a list the database does not hold.
pub const DOMAINS_DOMAIN: &str = "weft_domains";



/// Turn a failure of [`with_project_transition_lock`] into the answer an
/// HTTP caller should get.
///
/// Every caller of that lock goes through here so they cannot disagree
/// about the same failure. One kind is left: the lock's own database
/// work failed, which is a server error. WAITING is not a failure and
/// never arrives here, because the wait has no deadline
/// (`weft_task_store::locks::with_lock_waiting`).
pub fn lock_answer(what: &str, e: anyhow::Error) -> (axum::http::StatusCode, String) {
    (axum::http::StatusCode::INTERNAL_SERVER_ERROR, format!("{what}: {e}"))
}

/// Hold the per-project transition lock for `project_id`, waiting behind
/// the current holder, on the dispatcher's lock pool
/// (`DispatcherState::lock_pool`). The state-level rejection happens
/// inside `body`, which re-reads the row under the lock. Every holder
/// does a few database writes and returns (the version tree's writes in
/// `api::versions`, the start of an infra sync in `api::infra`, which
/// awaits the sync itself outside the lock).
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
    weft_task_store::locks::with_lock_waiting(
        lock_pool,
        key,
        &format!("project {project_id}'s transition lock, which a version-tree write or the start of an infra sync holds"),
        body,
    )
    .await
}
