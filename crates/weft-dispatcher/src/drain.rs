//! Every "let the running work finish, then go on" goes through here.
//!
//! A take-down that waits (`--running-policy wait`), a resync, a new image
//! going live, an infra stop: each one first stops new work coming in (a
//! trigger that is not Live refuses its callers and parks its events),
//! then waits for the work already going in what it touches, up to a
//! deadline, and cancels what is left there.
//!
//! "Work going" is two things added up. What the project's live workers
//! say they drive right now, per trigger, at their door tick
//! (`worker_lease.in_flight`, from the door's own count of the runs it
//! admitted): a run a worker bore at its door is counted from the moment
//! the door takes it, before anything of it is on record. And the runs on
//! record that are queued for a worker or being driven by one
//! (`weft_task_store::in_flight_sql!`). A run is often counted twice, once
//! by its worker and once by its row, which is harmless: the count is only
//! ever compared to zero. A run parked on a wait is not going: it survives
//! a take-down and resumes when its trigger is back.

use std::time::Duration;

use tokio::time::Instant;

use weft_core::activation::ActivationKey;
use weft_core::exec::CancelCause;
use weft_core::instance::Copies;
use weft_core::running_policy::RunningPolicy;
use weft_core::ExecutionId;
use weft_task_store::drain::{DrainLoop, DrainStep, WakeOn};

use crate::state::DispatcherState;

/// What a drain waits on: the work of one project that reaches something.
#[derive(Debug, Clone, Copy)]
pub struct DrainScope<'a> {
    pub project_id: uuid::Uuid,
    pub reaching: Reaching<'a>,
    /// The run that asked for the drain (a program taking its own infra
    /// down): it is waited for like any other, but never cancelled by the
    /// drain; its own `StopSelf` decides its fate.
    pub except: Option<ExecutionId>,
}

/// What the work a drain waits on reaches.
#[derive(Debug, Clone, Copy)]
pub enum Reaching<'a> {
    /// The runs these activations' triggers fired (each for its owner).
    Triggers(&'a [ActivationKey]),
    /// The runs that may use these infra copies: every run of the project
    /// for the shared copies, an instance's runs for its own (which run
    /// uses which copy is not recorded).
    Copies(&'a Copies),
    /// The runs on images of the program other than this one.
    OtherImages(&'a str),
}

/// How a drain ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Drained {
    /// Nothing was left going.
    Empty,
    /// The policy was `cancel`, or the deadline came with this much still
    /// going: what was on record was cancelled.
    Cancelled { left: i64 },
}

/// How often a waiting drain counts again: the workers state what they
/// drive once a second.
const COUNT_EVERY: Duration = Duration::from_secs(1);

/// How often a drain still waiting says so in the log.
const BREADCRUMB_EVERY: Duration = Duration::from_secs(30);

/// Wait for the work `scope` reaches, as `policy` says: `cancel` cancels
/// what is going now; `wait` waits until nothing is going, or until
/// `deadline` (none: for as long as it takes, logged every half minute),
/// where it cancels what is left.
pub async fn drain(state: &DispatcherState, scope: &DrainScope<'_>, deadline: Option<Instant>, policy: RunningPolicy) -> anyhow::Result<Drained> {
    if policy == RunningPolicy::Cancel {
        let left = going(&state.pg_pool, scope).await?;
        cancel_left(state, scope, &CancelCause::User).await?;
        return Ok(if left == 0 { Drained::Empty } else { Drained::Cancelled { left } });
    }
    let started = Instant::now();
    let mut breadcrumb = started + BREADCRUMB_EVERY;
    loop {
        let left = going(&state.pg_pool, scope).await?;
        if left == 0 {
            return Ok(Drained::Empty);
        }
        let now = Instant::now();
        if deadline.is_some_and(|deadline| now >= deadline) {
            tracing::warn!(
                target: "weft_dispatcher::drain",
                project_id = %scope.project_id, ?scope.reaching, left,
                "the wait for running work reached its cap; cancelling what is left"
            );
            cancel_left(state, scope, &scope.reaching.cap_cause()).await?;
            return Ok(Drained::Cancelled { left });
        }
        if now >= breadcrumb {
            tracing::info!(
                target: "weft_dispatcher::drain",
                project_id = %scope.project_id, ?scope.reaching, left,
                waited_secs = now.duration_since(started).as_secs(),
                "still waiting for running work to finish"
            );
            breadcrumb = now + BREADCRUMB_EVERY;
        }
        tokio::time::sleep(COUNT_EVERY).await;
    }
}

impl Reaching<'_> {
    /// What a run still going at a drain's deadline is cancelled for.
    fn cap_cause(&self) -> CancelCause {
        let detail = match self {
            Self::Triggers(_) => "its trigger was taken down, and the wait for running work to finish reached its cap",
            Self::Copies(_) => "the infra it may use is going down, and the wait for running work to finish reached its cap",
            Self::OtherImages(_) => {
                "this execution ran on an older image of the program, and the wait for it reached its cap after a new one went live"
            }
        };
        CancelCause::Runtime { detail: detail.into() }
    }
}

/// How much work `scope` reaches is going right now (see the module doc).
pub async fn going(pool: &sqlx::PgPool, scope: &DrainScope<'_>) -> anyhow::Result<i64> {
    Ok(driven_by_workers(pool, scope).await? + on_record(pool, scope).await?.len() as i64)
}

/// Cancel every run on record `scope` reaches that is queued or being
/// driven, but the one that asked, for `cause`.
pub(crate) async fn cancel_left(state: &DispatcherState, scope: &DrainScope<'_>, cause: &CancelCause) -> anyhow::Result<()> {
    let runs = on_record(&state.pg_pool, scope).await?;
    let targets: Vec<(ExecutionId, &CancelCause)> =
        runs.iter().filter(|id| Some(**id) != scope.except).map(|id| (*id, cause)).collect();
    crate::api::execution::cancel_execution_ids(state, &targets).await?;
    Ok(())
}

/// The keys as the two arrays the queries join on: triggers, and owners
/// (`NULL` for the shared one).
fn key_arrays(keys: &[ActivationKey]) -> (Vec<String>, Vec<Option<String>>) {
    keys.iter().map(|key| (key.trigger.clone(), key.instance().map(|m| m.as_str().to_string()))).unzip()
}

/// What the project's live workers say they drive of what `scope` reaches.
async fn driven_by_workers(pool: &sqlx::PgPool, scope: &DrainScope<'_>) -> anyhow::Result<i64> {
    const ALIVE: &str = weft_task_store::worker_alive!("l");
    let sum = |filter: &str| {
        format!(
            "SELECT COALESCE(SUM(f.n::bigint), 0)::bigint FROM worker_lease l \
             CROSS JOIN LATERAL jsonb_each_text(l.in_flight) AS f(token, n) \
             WHERE l.project_id = $1 AND {ALIVE} AND {filter}"
        )
    };
    let project_id = scope.project_id;
    Ok(match scope.reaching {
        Reaching::Triggers(keys) => {
            let (triggers, instances) = key_arrays(keys);
            sqlx::query_scalar(&sum(
                "f.token IN (SELECT s.token FROM signal s \
                 JOIN unnest($2::text[], $3::text[]) AS k(trigger, instance) \
                   ON s.activation_trigger = k.trigger AND s.instance_id IS NOT DISTINCT FROM k.instance \
                 WHERE s.project_id = $1 AND NOT s.is_resume)",
            ))
            .bind(project_id)
            .bind(&triggers)
            .bind(&instances)
            .fetch_one(pool)
            .await?
        }
        Reaching::Copies(Copies::Shared | Copies::Every) => sqlx::query_scalar(&sum("TRUE")).bind(project_id).fetch_one(pool).await?,
        Reaching::Copies(Copies::Instance(instance)) => {
            sqlx::query_scalar(&sum(
                "f.token IN (SELECT s.token FROM signal s WHERE s.project_id = $1 AND NOT s.is_resume AND s.instance_id = $2)",
            ))
            .bind(project_id)
            .bind(instance.as_str())
            .fetch_one(pool)
            .await?
        }
        Reaching::OtherImages(keep) => {
            sqlx::query_scalar(&sum("l.binary_hash <> $2")).bind(project_id).bind(keep).fetch_one(pool).await?
        }
    })
}

/// The runs on record `scope` reaches that are queued for a worker or
/// being driven by one.
async fn on_record(pool: &sqlx::PgPool, scope: &DrainScope<'_>) -> anyhow::Result<Vec<ExecutionId>> {
    const GOING: &str = concat!("(r.state = 'queued' OR ", weft_task_store::in_flight_sql!("r"), ")");
    let select = |filter: &str| format!("SELECT r.execution_id FROM run r WHERE r.project_id = $1 AND {GOING} AND {filter}");
    let project_id = scope.project_id;
    Ok(match scope.reaching {
        Reaching::Triggers(keys) => {
            let (triggers, instances) = key_arrays(keys);
            sqlx::query_scalar(&select(
                "EXISTS (SELECT 1 FROM unnest($2::text[], $3::text[]) AS k(trigger, instance) \
                 WHERE r.fired_by = k.trigger AND r.instance_id IS NOT DISTINCT FROM k.instance)",
            ))
            .bind(project_id)
            .bind(&triggers)
            .bind(&instances)
            .fetch_all(pool)
            .await?
        }
        Reaching::Copies(Copies::Shared | Copies::Every) => sqlx::query_scalar(&select("TRUE")).bind(project_id).fetch_all(pool).await?,
        Reaching::Copies(Copies::Instance(instance)) => {
            sqlx::query_scalar(&select("r.instance_id = $2")).bind(project_id).bind(instance.as_str()).fetch_all(pool).await?
        }
        Reaching::OtherImages(keep) => {
            sqlx::query_scalar(&select("r.binary_hash <> $2")).bind(project_id).bind(keep).fetch_all(pool).await?
        }
    })
}

/// A trigger's activation changing status: one starting to deactivate has
/// a drain to land.
pub(crate) static WAKE_ON: &[WakeOn] = &[WakeOn::any(crate::holders::HELD_SIGNALS_CHANNEL)];

/// The take-downs that wait: every activation left `Deactivating` lands
/// `Inactive` once nothing it waits on is going, and at its deadline what
/// is left is cancelled first. Looks every second while any waits.
pub fn drain_loop(state: DispatcherState) -> DrainLoop {
    DrainLoop::new("drains", WAKE_ON, weft_task_store::drain::SAFETY_POLL_INTERVAL, move || {
        let state = state.clone();
        async move {
            let deactivating = state.activations.list_deactivating().await?;
            if deactivating.is_empty() {
                return Ok(DrainStep::Done);
            }
            let now = crate::lease::now_unix();
            for (project_id, key) in deactivating {
                let deadline = state
                    .activations
                    .list(project_id)
                    .await?
                    .into_iter()
                    .find(|a| a.key == key)
                    .and_then(|a| a.lifecycle.drain_deadline_unix);
                let keys = std::slice::from_ref(&key);
                let scope = DrainScope { project_id, reaching: Reaching::Triggers(keys), except: None };
                if deadline.is_some_and(|deadline| now >= deadline) {
                    tracing::warn!(
                        target: "weft_dispatcher::drain",
                        %project_id, trigger = %key,
                        "a deactivation's wait reached its cap; cancelling what is left"
                    );
                    cancel_left(&state, &scope, &scope.reaching.cap_cause()).await?;
                }
                land_if_drained(&state, project_id, &key).await?;
            }
            Ok(DrainStep::RetryIn(COUNT_EVERY))
        }
    })
}

/// Land the activation `key` `Inactive` when it is `Deactivating` and
/// nothing it waits on is going. Idempotent: a stale view loses the
/// compare-and-set and the next look checks again, and an activate that
/// flipped it back to `Active` meanwhile wins it, so the deactivation
/// rolls back cleanly.
pub(crate) async fn land_if_drained(state: &DispatcherState, project_id: uuid::Uuid, key: &ActivationKey) -> anyhow::Result<()> {
    use crate::activation_store::ProjectStatus;
    let draining = state
        .activations
        .list(project_id)
        .await?
        .iter()
        .any(|a| &a.key == key && a.lifecycle.status == ProjectStatus::Deactivating);
    if !draining {
        return Ok(());
    }
    let scope = DrainScope { project_id, reaching: Reaching::Triggers(std::slice::from_ref(key)), except: None };
    if going(&state.pg_pool, &scope).await? > 0 {
        return Ok(());
    }
    if state.activations.cas_status(project_id, key, ProjectStatus::Deactivating, ProjectStatus::Inactive).await? {
        tracing::info!(target: "weft_dispatcher::drain", %project_id, trigger = %key, "drain finished: deactivating -> inactive");
        // Both frontends see the transitional state end without asking.
        crate::transition::publish_transition_changed(state, project_id).await;
        // A trigger that landed taking no work may leave nothing of the
        // project taking any: its front goes.
        crate::front::let_go_if_idle(state, project_id).await?;
    }
    Ok(())
}
