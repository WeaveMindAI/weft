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
//! (flaky / recovered, and the project's health protocols). On the
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
pub mod lifecycle;
pub mod ownership;
pub mod protocol;

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
    pub clock: Arc<dyn Clock>,
    /// How often the ownership loop renews this supervisor's project
    /// leases and claims more: a third of `infra_owner_lease_secs`, so one
    /// slow tick never lets a lease it still wants lapse.
    pub ownership_interval: Duration,
    /// How often the health loop looks at every owned project.
    pub health_interval: Duration,
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
    /// that scales to zero run one after another in this process. Two
    /// wakes landing together otherwise ran two passes side by side, and
    /// both claimed and ran the same command: claiming marks nothing on
    /// the broker, and only the commands a running pass names as its own
    /// stop a second claim of one.
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

    /// The project's lock alone, if nobody holds it (the sweep, which must
    /// never wait: an apply can hold a project for minutes, and the
    /// ownership tick that sweeps also renews every lease).
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

/// One pass of every loop, for a supervisor that scales to zero: renew
/// and claim, run every waiting command of what it owns, look at their
/// health. Each command runs to its end inside the pass, side by side with
/// the others the way [`run_loops`] runs them, and the leases are renewed
/// all along as they are there: a start or a stop takes the cloud a minute
/// or two, and a lease left to lapse meanwhile hands the project to a
/// sibling, which runs the command again from the start. Answers when to
/// look again: at the health interval while a project it owns has a
/// command waiting, while the host listing or the gone-copy sweep left
/// something, or while the health it saw is not settled (`health::tick`);
/// otherwise when the soonest lease a sibling holds over a project with a
/// command waiting, or over one the host holds copies of, lapses (the
/// sibling may be gone, and only a lapsed lease is taken over), and the
/// moment after. `None` when there is nothing to look at anywhere: infra
/// that runs fine needs no look, and a command being issued, a machine
/// saying how its unit stands changed, or the cloud saying a machine went
/// away wakes it.
pub async fn tick(state: &SupervisorState) -> anyhow::Result<Option<Duration>> {
    // A wake that lands during another pass waits for it and then runs
    // its own, which finds whatever that one left waiting.
    let _pass = state.pass.lock().await;
    let mut owned = std::collections::HashSet::new();
    let synced = ownership::tick(state, &mut owned).await?;
    let (changes, ownership_changed) = tokio::sync::mpsc::unbounded_channel();
    tokio::select! {
        drained = lifecycle::drain(state.clone(), ownership_changed) => drained?,
        renewing = ownership::keep_renewing(state.clone(), owned, vec![changes]) => {
            renewing?;
            anyhow::bail!("the supervisor's ownership renewal ended mid-pass");
        }
    }
    let unsettled = health::tick(state).await?;
    Ok(next_look(&synced, unsettled, state.health_interval))
}

/// When a supervisor that scales to zero looks again after a pass that
/// found `synced`, and health `unsettled` or not.
fn next_look(synced: &ownership::Synced, unsettled: bool, health_interval: Duration) -> Option<Duration> {
    if synced.owns_work || unsettled {
        return Some(health_interval);
    }
    synced.others_lapse_in.map(|lapse| lapse + Duration::from_secs(1))
}
