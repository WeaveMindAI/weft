//! The supervisor role: the one owner of each project's infrastructure.
//!
//! A supervisor is tenant-agnostic: it owns the infrastructure of a SET
//! of projects (the exclusive `infra_owner` lease, claimed and renewed by
//! the ownership loop) and reconciles only those, through the platform's
//! [`weft_platform_traits::InfraHost`]: containers on the local Docker
//! daemon, or machines of their own on a cloud. It never talks to
//! Postgres; the broker is its door.
//!
//! Three loops: ownership (claim and renew), lifecycle (run the
//! `infra_lifecycle_command` rows: apply, stop, terminate) and health
//! (flaky / recovered, and the health rules of each copy). On the
//! machine they run for as long as the process does ([`run_loops`]); a
//! supervisor that scales to zero runs one pass of each per tick
//! ([`tick`]). Everything here is library code, so integration tests wire
//! the same loops against fakes from `weft-platform-traits` and this
//! crate's own `FakeBroker`.

use std::sync::Arc;
use std::time::Duration;

use weft_platform_traits::clock::Clock;
use weft_platform_traits::InfraHost;

pub mod broker_ops;
pub mod health;
pub mod health_engine;
pub mod inside;
pub mod lifecycle;
pub mod ownership;

#[cfg(any(test, feature = "test-helpers"))]
pub mod testing;

/// Cloneable state threaded through the loops.
///
/// All external dependencies are behind trait objects so tests can swap
/// them for fakes.
#[derive(Clone)]
pub struct SupervisorState {
    pub broker: Arc<dyn broker_ops::BrokerSupervisorOps>,
    /// This supervisor replica's id: it identifies its command claims
    /// AND keys its `infra_owner` leases, sent on every broker write so
    /// the broker's ownership gate compares the lease against THIS.
    pub replica: String,
    /// Where the infrastructure runs.
    pub host: Arc<dyn InfraHost>,
    /// How a terminate asks a node to remove another node living inside
    /// its endpoint ([`inside`]).
    pub inside: Arc<dyn inside::InsideRemover>,
    pub clock: Arc<dyn Clock>,
    /// How often the ownership loop renews this supervisor's project
    /// leases and claims more: a third of `infra_owner_lease_secs`, so one
    /// slow tick never lets a lease it still wants lapse.
    pub ownership_interval: Duration,
    /// How often the health loop looks at every owned project.
    pub health_interval: Duration,
    /// The health loop's actions in flight, and what its looks came to
    /// since the last ownership sync reported it.
    pub health: Arc<tokio::sync::Mutex<health::HealthRegistry>>,
    /// Raised by the lifecycle loop when the broker says a command waits
    /// on a project nobody owns: the ownership loop ticks at once rather
    /// than at the end of its interval.
    pub ownership_wanted: Arc<tokio::sync::Notify>,
    /// Who is changing a project's copies on the host right now: a
    /// lifecycle command shares its project's lock for its whole run, and
    /// the ownership loop's sweep deletes a gone copy only while holding
    /// it alone. See [`ProjectLocks`].
    pub project_locks: Arc<ProjectLocks>,
    /// Held for the whole of one [`tick`], so the passes of a supervisor
    /// that scales to zero run one after another in this process: a pass
    /// gives every lease back as it ends, which must never take a lease
    /// from under a command another pass of this process runs.
    pub pass: Arc<tokio::sync::Mutex<()>>,
}

/// One lock per project, so this process's sweep deletion and its
/// commands on the same project never interleave on the host.
///
/// A copy's id is derived from (project, node, instance), so a node removed
/// and then added back names the very same copy. Without this, the sweep
/// could judge the copy gone, an apply of the re-added node could adopt
/// its kept disks, and the sweep's delete would then take the disks and
/// the fresh containers with it. Commands share the lock: they run side by
/// side whenever they touch different copies (the broker hands out a
/// command only once every older one touching any of its copies has ended,
/// `lifecycle_writes::next_command`), and the sweep takes it alone. The
/// lock covers this process only: other supervisors are kept off by the
/// project's `infra_owner` lease, which the sweep holds and re-checks
/// through its judgment and deletion (`ownership::sweep_gone_copies`)
/// exactly as an apply does.
#[derive(Default)]
pub struct ProjectLocks(std::sync::Mutex<std::collections::HashMap<uuid::Uuid, Arc<tokio::sync::RwLock<()>>>>);

impl ProjectLocks {
    fn entry(&self, project: uuid::Uuid) -> Arc<tokio::sync::RwLock<()>> {
        let mut locks = self.0.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        // A lock nobody holds or waits on is dropped, so the map stays
        // the size of what is in flight. Safe under the map's own lock:
        // every holder or waiter has a clone, so its count is above one.
        locks.retain(|_, l| Arc::strong_count(l) > 1);
        locks.entry(project).or_default().clone()
    }

    /// Share the project's lock (a lifecycle command), waiting while the
    /// sweep holds it.
    pub async fn share(&self, project: uuid::Uuid) -> tokio::sync::OwnedRwLockReadGuard<()> {
        self.entry(project).read_owned().await
    }

    /// The project's lock alone, if nobody holds it (the sweep, which
    /// never waits: an apply can hold a project for minutes, and the next
    /// tick sweeps the project again).
    pub fn try_alone(&self, project: uuid::Uuid) -> Option<tokio::sync::OwnedRwLockWriteGuard<()>> {
        self.entry(project).try_write_owned().ok()
    }
}

/// Run the three loops for as long as the process lives. Returns only
/// when one of them ends, which is abnormal (each is a `loop`), with what
/// ended it.
pub async fn run_loops(state: SupervisorState) -> anyhow::Result<()> {
    let (lifecycle_changes, ownership_changed) = tokio::sync::mpsc::unbounded_channel();
    let (health_changes, health_ownership_changed) = tokio::sync::mpsc::unbounded_channel();
    tokio::select! {
        r = ownership::run_loop(state.clone(), vec![lifecycle_changes, health_changes]) => {
            r.and(Err(anyhow::anyhow!("the supervisor's ownership loop ended")))
        }
        r = lifecycle::run_loop(state.clone(), ownership_changed) => {
            r.and(Err(anyhow::anyhow!("the supervisor's lifecycle loop ended")))
        }
        r = health::run_loop(state.clone(), health_ownership_changed) => {
            r.and(Err(anyhow::anyhow!("the supervisor's health loop ended")))
        }
    }
}

/// What one [`tick`] came to.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Pass {
    /// The projects it looked at, and ran the commands of, with nothing
    /// given up: what was written down as given up on them is cleared.
    pub settled: Vec<uuid::Uuid>,
}

/// One pass of every loop, for a supervisor that scales to zero: renew and
/// claim, sweep the gone copies, run every waiting command of what it
/// owns, look at their health, and give its leases back. Each command runs
/// to its end inside the pass, side by side with the others the way
/// [`run_loops`] runs them. The leases are renewed for as long as the pass
/// acts on the host, the sweep and the health look included: deleting a
/// copy's disks, a start or a stop takes the cloud a minute or more, and a
/// lease left to lapse meanwhile hands the project to a sibling, which
/// would act on it at the same time. The pass runs inside
/// the request that woke it, which is what keeps its CPU on.
///
/// Nothing here books the next look: the broker books it each time the
/// pass syncs its ownership (`SupervisorSyncOwnershipResponse::next_due_unix`),
/// from what is in the database: a health window ending, a protocol's
/// retry, a failed pass's retry, a lease of a sibling lapsing. What the
/// pass could not get through goes with its last sync ([`failed`],
/// [`give_up`], and a step over every project in [`act`]), and a pass
/// always ends.
pub async fn tick(state: &SupervisorState) -> Pass {
    // A wake that lands during another pass waits for it and then runs
    // its own, which finds whatever that one left waiting.
    let _pass = state.pass.lock().await;
    let mut ownership = ownership::Ownership::default();
    match ownership::claim(state, &mut ownership).await {
        Err(e) => tracing::error!(target: "weft_infra_supervisor", error = %format!("{e:#}"), "the supervisor could not sync its ownership; it looks again at the next wake"),
        Ok((_, held)) => {
            let (changes, ownership_changed) = tokio::sync::mpsc::unbounded_channel();
            tokio::select! {
                emptied = act(state, held, ownership_changed) => ownership.emptied(&emptied),
                renewing = ownership::keep_renewing(state.clone(), ownership.clone(), vec![changes]) => {
                    tracing::error!(target: "weft_infra_supervisor", result = ?renewing, "the supervisor's ownership renewal ended mid-pass");
                }
            }
        }
    }
    match ownership::end(state, ownership.held_projects()).await {
        Ok(pass) => pass,
        Err(e) => {
            tracing::error!(target: "weft_infra_supervisor", error = %format!("{e:#}"), "the supervisor could not give its leases back; they lapse on their own");
            Pass::default()
        }
    }
}

/// What a pass does under the leases [`tick`] renews beside it: sweep the
/// gone copies of what the host `held` (when it could be listed), run the
/// commands, then look at the health of what it owns. `ownership_changed`
/// carries what the renewal changed, and lives as long as this does.
///
/// A step over every project that fails (listing what the host holds,
/// reading what it owns for the health look) fails the pass over each
/// project it holds as it ends, the ones a renewal claimed mid-pass
/// included, and, with no listing, over each project whose failed pass's
/// retry was due and that only the listing could make ownable: the end of
/// the pass reports it (`SupervisorSyncOwnershipRequest::all_failed`), so
/// each is tried again on the failed pass's clock until it gives up,
/// written down for `weft status`.
///
/// Hands back the projects whose every copy the sweep deleted, which the
/// pass's last sync no longer carries in its listing.
async fn act(
    state: &SupervisorState,
    held: anyhow::Result<Vec<weft_core::infra::NodeRef>>,
    mut ownership_changed: tokio::sync::mpsc::UnboundedReceiver<ownership::OwnershipChange>,
) -> Vec<uuid::Uuid> {
    let emptied = match held {
        Ok(copies) => ownership::sweep_gone_copies(state, copies).await,
        Err(e) => {
            let error = format!("could not list what the host holds to delete what its gone copies left: {e:#}");
            tracing::error!(target: "weft_infra_supervisor", error, "the supervisor's pass sweeps nothing; a later pass tries again");
            state.health.lock().await.all_troubled(health::Step::Sweep, &error);
            Vec::new()
        }
    };
    if let Err(e) = lifecycle::drain(state.clone(), &mut ownership_changed).await {
        tracing::error!(target: "weft_infra_supervisor", error = %format!("{e:#}"), "the supervisor's commands stopped mid-pass");
    }
    if let Err(e) = health::tick(state).await {
        let error = format!("could not read what it owns to look at its health: {e:#}");
        tracing::error!(target: "weft_infra_supervisor", error, "the supervisor's pass looks at nothing; a later pass tries again");
        state.health.lock().await.all_troubled(health::Step::Look, &error);
    }
    emptied
}

/// How long a step of a pass asks the broker again while it says it
/// cannot answer right now ([`weft_broker_client::client::broker_unavailable`]).
const PATIENCE: Duration = Duration::from_secs(20);

/// Run `step`, again while the broker says it cannot answer right now,
/// for at most [`PATIENCE`]: a broker starting up or its database waking
/// passes in moments. Any other failure, or one that outlasts it, is the
/// step's answer.
pub(crate) async fn patiently<T, F, Fut>(state: &SupervisorState, mut step: F) -> anyhow::Result<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<T>>,
{
    let started = state.clock.now();
    let mut wait = Duration::from_millis(250);
    loop {
        match step().await {
            Err(e) if weft_broker_client::client::broker_unavailable(&e) && state.clock.now() - started < PATIENCE => {
                tracing::warn!(target: "weft_infra_supervisor", error = %format!("{e:#}"), retry_in_ms = wait.as_millis() as u64, "the broker could not answer; asking again");
                state.clock.sleep(wait).await;
                wait = (wait * 2).min(Duration::from_secs(4));
            }
            answer => return answer,
        }
    }
}

/// A `step` of a pass over `project` failed (a health look, a sweep of
/// its gone copies, a command's ending that could not be written): log
/// `error`, and the next ownership sync reports it, which writes it down
/// for `weft status` and has a later pass try the project again until it
/// gives up (`SupervisorSyncOwnershipRequest::failed`). A project that
/// failed is not reported settled by its pass, unless the step comes
/// right within it (`HealthRegistry::untroubled`).
pub(crate) async fn failed(state: &SupervisorState, project: uuid::Uuid, step: health::Step, error: &str) {
    tracing::error!(target: "weft_infra_supervisor", project_id = %project, error, "the supervisor could not get through the project's infra; a later pass tries again");
    state.health.lock().await.troubled(project, step, error);
}

/// A health rule of `project` gave up: log `error`, and the next
/// ownership sync writes it down for `weft status` until a pass reports
/// the project settled. Nothing tries it again on a clock: a person's
/// look or command, or what the machines report changing, does
/// (`SupervisorSyncOwnershipRequest::gave_up`).
pub(crate) async fn give_up(state: &SupervisorState, project: uuid::Uuid, error: &str) {
    tracing::error!(target: "weft_infra_supervisor", project_id = %project, error, "a health rule gave up on the project's infra; a look asked for it, or its next change, tries again");
    state.health.lock().await.troubled(project, health::Step::Rule, error);
}
