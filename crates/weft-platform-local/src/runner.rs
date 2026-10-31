//! Workers as processes on the machine.
//!
//! A worker is the project's compiled program (`<workers dir>/<binary
//! hash>/worker`, built on this machine by `images`), run as a plain
//! process in the project's folder, so its nodes reach the person's files
//! and programs the way anything else they run does. One process per
//! project, program and worker settings, started the first time weft
//! calls it and stopped once it has had nothing to do for the idle window
//! (`workerIdleStopSeconds`), unless the project keeps copies warm
//! (`workers.min_instances`). A change of settings or of program starts a
//! new one for the calls after it; the old one finishes what it drives and
//! goes once idle, the project's front included, which gives the port to
//! the new front once it has nothing left running.
//!
//! The project's front (`Runner::front`: the program its callers reach)
//! listens on the project's own port, so a caller talks to the worker
//! with nothing of weft's in between, and weft reaches it there too. It is
//! kept running for as long as it is the front. It is a worker of its own,
//! its name carrying the port (`LocalRunner::name_of`): a program that
//! served calls on a loopback port before it became the front keeps that
//! worker, which finishes what it runs and goes once idle, while the front
//! starts beside it. Only one process can hold a port, so a new front on
//! the same port starts once the old one stopped: a caller arriving in
//! that moment is refused and tries again. A front given another port
//! leaves the old one on its own, no front any more, until it goes idle.
//!
//! Every other worker listens on a loopback port of its own. Weft's own
//! endpoints answer only the project's worker key
//! (`weft_core::caller_token::worker_door_key`), which the worker derives
//! from the project secret it holds. The worker calls the broker with a
//! token the install signed for exactly its project.
//!
//! A worker's environment is exactly what the runner gives it: nothing of
//! the runtime's own passes on (its secrets above all) except the
//! machine's `PATH` and `HOME`, so a node finds the programs the person
//! has, the same ones the needs check found (`needs`). The worker still
//! runs as the person, so it can read whatever they can, the install's
//! `secrets.env` included. What it prints goes to
//! `<install dir>/workers/logs/<name>.log`.
//!
//! A restart of the runtime leaves its workers running to finish their
//! runs. The workers each life starts are written down
//! (`registry`, `<install dir>/workers/running.json`), so the next life
//! tells the ones an earlier life left to stop, and forgets them once they
//! are gone.
//!
//! One copy per project, program and settings serves every run: a laptop
//! has one machine, so `max_instances` changes nothing here, and
//! `concurrency` is the worker's own business.

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use sha2::{Digest, Sha256};
use weft_platform_traits::identity::Principal;
use weft_platform_traits::{
    Clock, Patience, PortTaken, ProjectFolders, Runner, WorkerEndpoint, WorkerSettings, WorkerTarget, WorkerWontStart, WORKER_AUTH_HEADER,
    WORKER_PORT_TAKEN_EXIT, WORKER_PORT_TAKEN_LINE,
};

use crate::identity::LocalIdentity;
use crate::layout::InstallLayout;
use crate::processes::{Exit, Processes, Signal, Spawn};
use crate::registry::{self, Entry, Registry};

/// How long a started worker gets to answer its health check. The
/// worker's own start is weft's code (the user's nodes run later, per
/// run), so a worker that does not answer by then is broken.
const START_DEADLINE: Duration = Duration::from_secs(120);

/// How long a worker being stopped gets to exit before it is killed: one
/// idle for its window, one that stopped answering, one an earlier life of
/// the runtime left, and a front giving way to the next once it has
/// nothing left running (`weft_engine::worker` lets a run go within five
/// seconds of being asked, its record written).
const WORKER_STOP_GRACE: Duration = Duration::from_secs(10);

/// How many of a worker's last lines an error about its start carries.
const LOG_TAIL_LINES: usize = 50;

/// A local worker's broker token never expires on its own: the process is
/// the credential's life, and rotating `WEFT_IDENTITY_KEY` revokes every
/// one at once.
const WORKER_TOKEN_LIFE_SECS: i64 = 100 * 365 * 24 * 3600;

/// What the runner needs besides the machine's processes.
#[derive(Clone)]
pub struct LocalRunnerConfig {
    /// The broker's address as a process on this machine reaches it
    /// (`InstallConfig::role_addresses(Vantage::Machine)`).
    pub broker_url: String,
    /// The install's caller-ticket secret, each project's own secret is
    /// derived from (`weft_core::caller_token::ProjectSecret`); a worker is
    /// given only its project's.
    pub install_secret: Vec<u8>,
    /// How long a worker with nothing to do stays up.
    pub idle_stop: Duration,
    /// The install's folders: its workers' logs, scratch folders and
    /// registry go in `InstallLayout::workers`.
    pub install: InstallLayout,
    /// Where the machine keeps weft's programs (`images::Program`): the
    /// workers, one folder per binary hash, and the node test servers,
    /// shared by every install.
    pub workers_dir: PathBuf,
    /// What a worker gets of the machine's own environment: its `PATH` and
    /// `HOME`, as the runtime has them.
    pub machine_env: BTreeMap<String, String>,
    /// The install's clock factor (`weft_core::time_scale`). Its workers
    /// run at it too: a worker's claim heartbeat and the broker's claim
    /// length are one protocol, and must agree.
    pub time_scale: f64,
    /// What the install allows at its public edge, which a worker's door
    /// holds its callers to.
    pub edge: weft_platform_traits::config::EdgeConfig,
    /// The address a project's own port opens on: the install's public
    /// port's.
    pub project_ip: std::net::IpAddr,
}

pub struct LocalRunner {
    processes: Arc<dyn Processes>,
    folders: Arc<dyn ProjectFolders>,
    identity: Arc<LocalIdentity>,
    clock: Arc<dyn Clock>,
    http: reqwest::Client,
    cfg: LocalRunnerConfig,
    /// The workers this life started, by name.
    running: parking_lot::Mutex<HashMap<String, Arc<Running>>>,
    /// One start at a time per worker name.
    starting: tokio::sync::Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
    /// This life of the runtime, on every worker it starts (its registry
    /// entry and its name), so a worker an earlier life left running is
    /// never taken for one of this life's (nor replaced by name while it
    /// finishes its runs).
    life: String,
    /// The workers of an earlier life already told to stop, so the
    /// runtime's log says so once per worker.
    told: parking_lot::Mutex<std::collections::HashSet<String>>,
    /// Each project's front: what runs on its port.
    fronts: parking_lot::Mutex<HashMap<uuid::Uuid, Front>>,
    /// The name of the worker each project's calls go to now
    /// ([`Self::keep_only_current_warm`]): only that one starts warm. A
    /// project not named here since this life began starts its workers
    /// as their settings say.
    current: parking_lot::Mutex<HashMap<uuid::Uuid, String>>,
    registry: Registry,
    /// What the registry holds: every worker this life and earlier ones
    /// started that is not known to be gone.
    entries: parking_lot::Mutex<Vec<Entry>>,
}

/// What a project's callers reach: the program and settings, and on which
/// port (so a front that went down is started again). Its worker is the
/// one named for both (`LocalRunner::name_of`), kept up however idle.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Front {
    port: u16,
    target: WorkerTarget,
}

/// A worker process this life started.
struct Running {
    entry: Entry,
    base_url: String,
    /// The project's port, when this worker is its front and holds it.
    port: Option<u16>,
    /// Calls handed out and not yet finished.
    holds: AtomicUsize,
    last_used: parking_lot::Mutex<Instant>,
    /// Kept up however idle (the project's `min_instances` is above 0),
    /// for as long as it is the worker the project's calls go to
    /// (`LocalRunner::keep_only_current_warm`): until its settings or
    /// program change, or, for one without the port, until its program
    /// becomes the front, which takes its calls from then on. A front is
    /// kept up besides, for as long as it is the front.
    keep_warm: AtomicBool,
}

/// Held for as long as a caller talks to the worker.
struct Hold {
    running: Arc<Running>,
    clock: Arc<dyn Clock>,
}

impl Drop for Hold {
    fn drop(&mut self) {
        *self.running.last_used.lock() = self.clock.now();
        self.running.holds.fetch_sub(1, Ordering::SeqCst);
    }
}

impl LocalRunner {
    /// A runner that knows the workers earlier lives left running (its
    /// registry, read here).
    pub fn new(
        processes: Arc<dyn Processes>,
        folders: Arc<dyn ProjectFolders>,
        identity: Arc<LocalIdentity>,
        clock: Arc<dyn Clock>,
        cfg: LocalRunnerConfig,
    ) -> anyhow::Result<Self> {
        let registry = Registry::in_dir(&cfg.install.workers());
        let entries = registry.load()?;
        Ok(Self {
            processes,
            folders,
            identity,
            clock,
            http: weft_core::net::http_client().timeout(Duration::from_secs(5)).build().expect("reqwest client"),
            cfg,
            running: parking_lot::Mutex::new(HashMap::new()),
            starting: tokio::sync::Mutex::new(HashMap::new()),
            life: random_hex(4),
            told: parking_lot::Mutex::new(std::collections::HashSet::new()),
            fronts: parking_lot::Mutex::new(HashMap::new()),
            current: parking_lot::Mutex::new(HashMap::new()),
            registry,
            entries: parking_lot::Mutex::new(entries),
        })
    }

    /// The name of this life's worker for `target` ([`worker_name`], then
    /// the life), and the project's `port` when it is the front: a worker
    /// that holds the port is never the one that does not, so a program
    /// becoming the front starts its front beside the worker it had, never
    /// in its place. Logs and scratch folders go by name, so two processes
    /// never share one.
    fn name_of(&self, target: &WorkerTarget, port: Option<u16>) -> String {
        let base = format!("{}-{}", worker_name(target.project, &target.image, &target.settings), self.life);
        match port {
            Some(port) => format!("{base}-front{port}"),
            None => base,
        }
    }

    /// The name of `front`'s worker.
    fn front_name(&self, front: &Front) -> String {
        self.name_of(&front.target, Some(front.port))
    }

    /// The project's port, when `target` is what its front runs.
    fn front_port(&self, target: &WorkerTarget) -> Option<u16> {
        self.fronts.lock().get(&target.project).filter(|front| front.target == *target).map(|front| front.port)
    }

    /// Keep warm only the worker `target`'s calls go to (its front, when
    /// `target` is what the front runs), and only when its settings ask
    /// for it: every other worker of the project, another program's,
    /// other settings', or the loopback copy of a program that became the
    /// front, finishes what it drives and goes once idle.
    fn keep_only_current_warm(&self, target: &WorkerTarget) {
        let name = self.name_of(target, self.front_port(target));
        // Held while every worker is decided, the way `ensure` decides one
        // that just started.
        let mut current = self.current.lock();
        current.insert(target.project, name.clone());
        let warm = target.settings.min_instances > 0;
        let project = worker_name_prefix(target.project);
        for running in self.running.lock().values() {
            if running.entry.name.starts_with(&project) {
                running.keep_warm.store(warm && running.entry.name == name, Ordering::SeqCst);
            }
        }
    }

    fn hold(&self, running: &Arc<Running>) -> WorkerEndpoint {
        running.holds.fetch_add(1, Ordering::SeqCst);
        *running.last_used.lock() = self.clock.now();
        WorkerEndpoint {
            base_url: running.base_url.clone(),
            bearer: running.entry.key.clone(),
            hold: Some(Box::new(Hold { running: running.clone(), clock: self.clock.clone() })),
        }
    }

    async fn healthy(&self, base_url: &str, key: &str) -> bool {
        self.http
            .get(format!("{base_url}/_weft/healthz"))
            .header(WORKER_AUTH_HEADER, weft_platform_traits::worker_auth_value(key))
            .send()
            .await
            .is_ok_and(|r| r.status().is_success())
    }

    /// `project`'s own secret, which its workers hold.
    fn secret_of(&self, project: uuid::Uuid) -> weft_core::caller_token::ProjectSecret {
        weft_core::caller_token::ProjectSecret::of(&self.cfg.install_secret, project)
    }

    /// Change the registry and write it down.
    fn record(&self, change: impl FnOnce(&mut Vec<Entry>)) -> anyhow::Result<()> {
        let mut entries = self.entries.lock();
        change(&mut entries);
        self.registry.save(&entries)
    }

    /// Whether `entry`'s process still runs ([`registry::still_runs`]).
    async fn still_there(&self, entry: &Entry) -> anyhow::Result<bool> {
        registry::still_runs(self.processes.as_ref(), entry).await
    }

    /// `entry` out of the registry.
    fn drop_entry(&self, entry: &Entry) -> anyhow::Result<()> {
        self.record(|entries| entries.retain(|e| !(e.name == entry.name && e.pid == entry.pid)))
    }

    /// `entry`'s worker is gone: out of the registry, its log and scratch
    /// folder removed.
    fn forget(&self, entry: &Entry) -> anyhow::Result<()> {
        self.drop_entry(entry)?;
        self.registry.remove_leftovers(entry);
        Ok(())
    }

    /// Stop `entry`'s worker the way a platform replaces a process: told to
    /// stop, given [`WORKER_STOP_GRACE`] to exit on its own, killed after
    /// that, and whatever it started killed with it; then forgotten. One
    /// already gone ended on its own (it crashed, or was killed from
    /// outside): its log's last lines go to the runtime's log before the
    /// log goes with it.
    async fn stop(&self, entry: &Entry) -> anyhow::Result<()> {
        if !self.still_there(entry).await? {
            tracing::warn!(
                target: "weft_platform_local::runner",
                worker = %entry.name, last_lines = %log_tail(&self.registry.log_of(&entry.name)),
                "a worker had ended on its own before weft stopped it"
            );
        } else {
            self.processes.signal(entry.pid, Signal::Term)?;
            let asked = Instant::now();
            while self.still_there(entry).await? && asked.elapsed() < WORKER_STOP_GRACE {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            // Its process group, whether it exited within the grace or
            // not: a program a node started can outlive the worker that
            // started it. Sent the moment the worker was seen gone (or
            // still there), so its group's number is still its own: one
            // found long dead above is never sent this.
            self.processes.signal(entry.pid, Signal::Kill)?;
        }
        self.forget(entry)
    }

    /// Kill `entry`'s worker outright (the runs on it die with it), and
    /// forget it.
    async fn kill(&self, entry: &Entry) -> anyhow::Result<()> {
        self.registry.kill_and_clear(self.processes.as_ref(), entry).await?;
        self.drop_entry(entry)
    }

    /// Start the worker for `target`, on the project's `port` when it is
    /// the front, and wait until it answers. A port another program holds
    /// is [`PortTaken`]; a worker that exits as it starts, whose program is
    /// gone, or whose project has no folder here, is [`WorkerWontStart`].
    ///
    /// A project's worker runs in the project's folder; a node test server
    /// runs in its own scratch folder, since the package it tests may
    /// belong to no project with a folder here (the standard library's).
    async fn start(&self, target: &WorkerTarget, name: &str, port: Option<u16>) -> anyhow::Result<Arc<Running>> {
        let program = crate::images::Program::of(&target.image).ok_or_else(|| {
            anyhow::anyhow!("{} names none of weft's programs (`weft-worker:<binary hash>`, `weft-node-tests:<test hash>`)", target.image)
        })?;
        let (log, scratch) = (self.registry.log_of(name), self.registry.scratch_of(name));
        let cwd = match program {
            crate::images::Program::Worker(_) => self.folders.folder(target.project).await?.ok_or_else(|| {
                anyhow::Error::new(WorkerWontStart {
                    why: format!(
                        "project {} has no folder on this machine, and a worker on a local install runs in its project's folder; \
                         open the project from its folder on this machine",
                        target.project
                    ),
                })
            })?,
            crate::images::Program::NodeTestServer(_) => scratch.clone(),
        };
        let file = program.file(&self.cfg.workers_dir);
        if !file.is_file() {
            return Err(anyhow::Error::new(WorkerWontStart {
                why: format!("its program {} ({}) is no longer on this machine", target.image, file.display()),
            }));
        }
        let listen = std::net::SocketAddr::new(
            match port {
                Some(_) => self.cfg.project_ip,
                None => std::net::IpAddr::from([127, 0, 0, 1]),
            },
            match port {
                Some(port) => port,
                None => self.processes.free_port()?,
            },
        );
        for dir in [log.parent().expect("a worker's log is in a folder").to_path_buf(), scratch.clone()] {
            std::fs::create_dir_all(&dir).map_err(|e| anyhow::anyhow!("create {}: {e}", dir.display()))?;
        }
        let secret = self.secret_of(target.project);
        let key = secret.worker_door_key();
        let token = self.identity.mint(
            Principal::Worker { tenant: target.tenant.clone(), project: target.project },
            now_unix() + WORKER_TOKEN_LIFE_SECS,
        );
        let env = worker_env(target, &self.cfg, &secret, &token, listen, &scratch);
        let spawned = self
            .processes
            .spawn(&Spawn { program: file, args: program.args().iter().map(|a| a.to_string()).collect(), cwd, env, log: log.clone() })
            .map_err(|e| e.context(format!("start the worker of project {}", target.project)))?;
        let tracked = self.processes.started(spawned.pid).await.and_then(|started| {
            let entry = Entry {
                name: name.to_string(),
                pid: spawned.pid,
                life: self.life.clone(),
                key,
                project: target.project,
                image: target.image.clone(),
                // A process gone already has no start time: the wait below
                // finds it ended.
                started: started.unwrap_or_default(),
            };
            if !entry.started.is_empty() {
                self.record(|entries| entries.push(entry.clone()))?;
            }
            Ok(entry)
        });
        let entry = match tracked {
            Ok(entry) => entry,
            // Started but written down nowhere, so nothing would ever stop
            // it: killed now, with whatever it started.
            Err(e) => {
                let lost = format!("could not keep track of the worker of project {} (pid {}): {e:#}", target.project, spawned.pid);
                return Err(match self.processes.signal(spawned.pid, Signal::Kill) {
                    Ok(()) => anyhow::anyhow!("{lost}; it was killed"),
                    Err(kill) => anyhow::anyhow!("{lost}; killing it failed too ({kill:#}): stop it by hand with `kill -9 -- -{}`", spawned.pid),
                });
            }
        };
        let base_url = format!("http://{}", std::net::SocketAddr::new(loopback_if_any(listen.ip()), listen.port()));

        let started_at = Instant::now();
        loop {
            if self.healthy(&base_url, &entry.key).await {
                break;
            }
            let ended = *spawned.exit.borrow();
            if let Some(exit) = ended {
                let tail = log_tail(&log);
                self.forget(&entry)?;
                return Err(start_failure(target.project, exit, port, &tail));
            }
            if started_at.elapsed() > START_DEADLINE {
                let tail = log_tail(&log);
                self.kill(&entry).await?;
                anyhow::bail!(
                    "the worker of project {} did not answer its health check within {}s; its last lines:\n{tail}",
                    target.project,
                    START_DEADLINE.as_secs()
                );
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        Ok(Arc::new(Running {
            entry,
            base_url,
            port,
            holds: AtomicUsize::new(0),
            last_used: parking_lot::Mutex::new(self.clock.now()),
            // Decided by `ensure` as it joins the running ones.
            keep_warm: AtomicBool::new(false),
        }))
    }

    /// The worker for `target`, started when it is not running: the
    /// project's front, on its port, when `target` is what the front runs,
    /// and one on a loopback port of its own otherwise.
    async fn ensure(&self, target: &WorkerTarget) -> anyhow::Result<Arc<Running>> {
        // Which worker this is depends on whether `target` is the front,
        // which a `front` holding the gate may change meanwhile: asked
        // again once the gate is held, so a call that waited behind a
        // front moving off this program never starts it on the port again.
        let (port, name, _one_start) = loop {
            let port = self.front_port(target);
            let name = self.name_of(target, port);
            let gate = self.starting.lock().await.entry(name.clone()).or_default().clone();
            let held = gate.lock_owned().await;
            if self.front_port(target) == port {
                break (port, name, held);
            }
        };
        let known = self.running.lock().get(&name).cloned();
        if let Some(running) = known {
            if self.healthy(&running.base_url, &running.entry.key).await {
                return Ok(running);
            }
            // One that ended on its own is said by `stop`, with its log.
            if self.still_there(&running.entry).await? {
                tracing::warn!(
                    target: "weft_platform_local::runner",
                    worker = %name, last_lines = %log_tail(&self.registry.log_of(&name)),
                    "a worker stopped answering; starting it again"
                );
            }
            self.running.lock().remove(&name);
            self.stop(&running.entry).await?;
        }
        let running = match self.start(target, &name, port).await {
            // Another program holds the port: this is no front any more.
            // Its calls go to a loopback worker of its program from now
            // on, which is the one kept warm.
            Err(e) if PortTaken::of(&e).is_some() => {
                let mut fronts = self.fronts.lock();
                if fronts.get(&target.project).is_some_and(|front| front.target == *target && Some(front.port) == port) {
                    fronts.remove(&target.project);
                    // Decided with the front still held, so a `front` naming
                    // it again meanwhile is never undone here.
                    let mut current = self.current.lock();
                    if current.get(&target.project) == Some(&name) {
                        current.insert(target.project, self.name_of(target, None));
                    }
                }
                return Err(e);
            }
            started => started?,
        };
        // Warm only when it is the worker the project's calls go to now
        // (one an older program's run resumes on finishes it and goes once
        // idle), decided as it joins the others under the same lock
        // `keep_only_current_warm` decides them under, so a `prepare`
        // during its start is never missed.
        let current = self.current.lock();
        let warm = target.settings.min_instances > 0 && current.get(&target.project).is_none_or(|now| *now == name);
        running.keep_warm.store(warm, Ordering::SeqCst);
        self.running.lock().insert(name, running.clone());
        drop(current);
        Ok(running)
    }

    /// How much `worker` still runs, when anything: calls handed to it and
    /// not finished, or runs it says it drives. One that does not answer
    /// runs nothing anyone can reach.
    async fn still_running(&self, worker: &Running) -> Option<u64> {
        let holds = worker.holds.load(Ordering::SeqCst) as u64;
        let driving = self.driving(&worker.base_url, &worker.entry.key).await.unwrap_or(0);
        (holds + driving > 0).then_some(holds + driving)
    }

    /// How many runs the worker at `base_url` drives, as its health check
    /// says; `None` when it does not answer.
    async fn driving(&self, base_url: &str, key: &str) -> Option<u64> {
        let answer = self
            .http
            .get(format!("{base_url}/_weft/healthz"))
            .header(WORKER_AUTH_HEADER, weft_platform_traits::worker_auth_value(key))
            .send()
            .await
            .ok()
            .filter(|r| r.status().is_success())?;
        // SYNC: the health answer <-> crates/weft-engine/src/worker.rs (healthz)
        let health: serde_json::Value = answer.json().await.ok()?;
        health.get("driving").and_then(serde_json::Value::as_u64)
    }

    /// Stop every worker that has had nothing to do for the idle window,
    /// and every worker an earlier life of the runtime left behind. A
    /// worker left behind may still be driving runs (the runtime restarted
    /// under it, an upgrade say), so it is told to stop the way a cloud
    /// platform replaces one (its durable runs go back for another worker,
    /// its fast runs end first) and forgotten on a later sweep once it
    /// exited; this life's own worker for the same project has another
    /// name, so nothing replaces it by name meanwhile. Called on an
    /// interval by the process keeper. A worker still driving a run nobody
    /// holds a call for (one whose caller went away and that goes on
    /// without them) is doing something: it stays up. Each worker is tried
    /// whatever the others did, and the ones that would not go are named
    /// together at the end.
    pub async fn sweep_idle(&self) -> anyhow::Result<()> {
        let fronts: std::collections::HashSet<String> = self.fronts.lock().values().map(|front| self.front_name(front)).collect();
        let now = self.clock.now();
        let quiet: Vec<Arc<Running>> = self
            .running
            .lock()
            .values()
            .filter(|r| {
                !r.keep_warm.load(Ordering::SeqCst)
                    && !fronts.contains(&r.entry.name)
                    && r.holds.load(Ordering::SeqCst) == 0
                    && now.duration_since(*r.last_used.lock()) >= self.cfg.idle_stop
            })
            .cloned()
            .collect();
        let mut left: Vec<(u32, String)> = Vec::new();
        for r in quiet {
            if self.driving(&r.base_url, &r.entry.key).await.is_some_and(|driving| driving > 0) {
                *r.last_used.lock() = self.clock.now();
                continue;
            }
            // Stopped under its start gate, so no call starts it again in
            // between, and only if nothing took it while it was asked: a
            // call that came since holds it or used it.
            let gate = self.starting.lock().await.entry(r.entry.name.clone()).or_default().clone();
            let _no_start = gate.lock().await;
            let still_idle = {
                let mut running = self.running.lock();
                let idle = running.get(&r.entry.name).is_some_and(|now_running| {
                    Arc::ptr_eq(now_running, &r)
                        && r.holds.load(Ordering::SeqCst) == 0
                        && self.clock.now().duration_since(*r.last_used.lock()) >= self.cfg.idle_stop
                });
                if idle {
                    running.remove(&r.entry.name);
                }
                idle
            };
            // Told to stop first, as every planned stop is: a run that
            // reached it on its own port since the look above is handed
            // back if it can pause, and runs on until the kill if it
            // cannot.
            if still_idle {
                if let Err(e) = self.stop(&r.entry).await {
                    left.push((r.entry.pid, format!("{}: {e:#}", r.entry.name)));
                }
            }
        }
        // A gate nothing is starting under and no worker answers to.
        let known: std::collections::HashSet<String> = self.running.lock().keys().cloned().collect();
        self.starting.lock().await.retain(|name, gate| known.contains(name) || Arc::strong_count(gate) > 1);
        let earlier: Vec<Entry> = self.entries.lock().iter().filter(|e| e.life != self.life).cloned().collect();
        for entry in earlier {
            if let Err(e) = self.let_earlier_go(&entry).await {
                left.push((entry.pid, format!("{}: {e:#}", entry.name)));
            }
        }
        if left.is_empty() {
            return Ok(());
        }
        let pids: Vec<String> = left.iter().map(|(pid, _)| pid.to_string()).collect();
        let why: Vec<String> = left.iter().map(|(_, e)| e.clone()).collect();
        anyhow::bail!("the idle sweep could not stop every worker it should have ({}); stop them by hand with `kill -9 {}`", why.join("; "), pids.join(" "))
    }

    /// Tell a worker an earlier life of the runtime left running to stop,
    /// or forget it once it is gone.
    async fn let_earlier_go(&self, entry: &Entry) -> anyhow::Result<()> {
        if !self.still_there(entry).await? {
            return self.forget(entry);
        }
        if self.told.lock().insert(entry.name.clone()) {
            tracing::info!(
                target: "weft_platform_local::runner",
                worker = %entry.name, pid = entry.pid,
                "a worker an earlier run of the runtime started is finishing its runs before it goes; `kill -9 {}` stops it now",
                entry.pid
            );
        }
        // Told again on every sweep until it exits: a worker heeds the
        // first and ignores the rest.
        self.processes.signal(entry.pid, Signal::Term)
    }
}

#[async_trait]
impl Runner for LocalRunner {
    async fn prepare(&self, target: &WorkerTarget) -> anyhow::Result<()> {
        target.settings.validate().map_err(|e| anyhow::anyhow!("project {}: {e}", target.project))?;
        // A worker started with other settings or on another program keeps
        // what it drives and stops once idle, like any other: calls go to
        // one started with these from now on (its name follows them). The
        // one they go to is kept warm again when it is asked for, a worker
        // these settings had before another prepare included.
        self.keep_only_current_warm(target);
        if target.settings.min_instances > 0 {
            self.ensure(target).await?;
        }
        Ok(())
    }

    // A local process is up in seconds, so every caller waits it out:
    // there is no bring-up long enough to answer `WorkerStarting` to.
    async fn endpoint(&self, target: &WorkerTarget, _patience: Patience) -> anyhow::Result<WorkerEndpoint> {
        let running = self.ensure(target).await?;
        Ok(self.hold(&running))
    }

    async fn front(&self, target: &WorkerTarget, port: Option<u16>) -> anyhow::Result<weft_core::projects::ProjectAddress> {
        let port = port.ok_or_else(|| anyhow::anyhow!("a local project's front needs the project's port"))?;
        let name = self.name_of(target, Some(port));
        // A worker of this project's that holds the port (the front before,
        // or a former one serving on): it gives the port up only once it
        // has nothing left running, so nothing on it is cut off. Until then
        // it goes on answering at the port, and the next look (the
        // reaper's, every half minute: `weft_dispatcher::front`) moves the
        // port once it is idle. Stopped under its start gate, so a call for
        // its program meanwhile waits and then starts a worker of its own
        // rather than one this stop would take down.
        let holding: Vec<Arc<Running>> =
            self.running.lock().values().filter(|r| r.port == Some(port) && r.entry.name != name).cloned().collect();
        for held in holding {
            let gate = self.starting.lock().await.entry(held.entry.name.clone()).or_default().clone();
            let _no_start = gate.lock().await;
            if let Some(busy) = self.still_running(&held).await {
                tracing::info!(
                    target: "weft_platform_local::runner",
                    project = %target.project, worker = %held.entry.name, running = busy,
                    "the project's port moves to its new version once the worker holding it has nothing left running"
                );
                return Ok(weft_core::projects::ProjectAddress::Serving { url: port_url(self.cfg.project_ip, port) });
            }
            // No front while the port moves, taken before the gate is let
            // go: a call for the old program waiting on it then finds it no
            // front any more and starts a loopback worker of its own
            // instead of taking the port back, and a call for the new one
            // meanwhile does the same rather than racing this stop for the
            // port. The new front is named once the port is free, below.
            self.fronts.lock().remove(&target.project);
            if self.running.lock().remove(&held.entry.name).is_some() {
                self.stop(&held.entry).await?;
            }
        }
        // The port is free of this life's workers: the new front is the
        // one wanting it from now on. A worker its program had on a
        // loopback port is left to finish what it runs, and is no longer
        // kept warm: the front takes its calls.
        self.fronts.lock().insert(target.project, Front { port, target: target.clone() });
        self.keep_only_current_warm(target);
        // This project's workers an earlier life of the runtime left
        // running (`sweep_idle` tells them to stop, but only on its next
        // pass): the last front among them still holds the project's port,
        // and the project would otherwise move to another port on every
        // restart of the runtime. They stop the same way, letting their
        // runs go first.
        let earlier: Vec<Entry> =
            self.entries.lock().iter().filter(|e| e.project == target.project && e.life != self.life).cloned().collect();
        for entry in earlier {
            self.stop(&entry).await?;
        }
        let running = self.ensure(target).await?;
        let port = running.port.ok_or_else(|| anyhow::anyhow!("project {}'s front started without its port", target.project))?;
        Ok(weft_core::projects::ProjectAddress::Serving { url: port_url(self.cfg.project_ip, port) })
    }

    // A worker with other settings already finishes what it drives and
    // stops once idle, since calls go to one started with the current
    // ones (`prepare`, `front`). Settling only says again which one is
    // kept warm: a `prepare` from a caller holding settings from before a
    // change may have made the stale one the warm one.
    async fn settle(&self, target: &WorkerTarget) -> anyhow::Result<()> {
        self.keep_only_current_warm(target);
        Ok(())
    }

    async fn let_front_go(&self, project: uuid::Uuid) -> anyhow::Result<()> {
        // No longer a front, it stops once idle like any worker: a project
        // that takes no calls keeps no instance warm.
        let front = self.fronts.lock().remove(&project);
        if let Some(front) = front {
            if let Some(running) = self.running.lock().get(&self.front_name(&front)) {
                running.keep_warm.store(false, Ordering::SeqCst);
            }
        }
        Ok(())
    }

    async fn kill_workers(&self, _tenant: &str, project: uuid::Uuid) -> anyhow::Result<()> {
        let prefix = worker_name_prefix(project);
        // Every start of the project's workers under way is waited out, so
        // its worker is killed with the rest. The dispatcher asks for no
        // new worker of a project it is removing (its runs are taken down
        // before this). Taken in name order, so two kills of one project
        // never hold each other's gates.
        let mut gates: Vec<(String, Arc<tokio::sync::Mutex<()>>)> =
            self.starting.lock().await.iter().filter(|(name, _)| name.starts_with(&prefix)).map(|(name, gate)| (name.clone(), gate.clone())).collect();
        gates.sort_by(|a, b| a.0.cmp(&b.0));
        let gates: Vec<Arc<tokio::sync::Mutex<()>>> = gates.into_iter().map(|(_, gate)| gate).collect();
        let mut no_starts = Vec::with_capacity(gates.len());
        for gate in gates {
            no_starts.push(gate.lock_owned().await);
        }
        self.fronts.lock().remove(&project);
        self.current.lock().remove(&project);
        self.running.lock().retain(|name, _| !name.starts_with(&prefix));
        let workers: Vec<Entry> = self.entries.lock().iter().filter(|e| e.project == project).cloned().collect();
        let killed = self.kill_all(&workers, &format!("project {project}")).await;
        drop(no_starts);
        killed
    }

    /// Nothing is kept here past the workers' processes.
    async fn retire(&self, _tenant: &str, _project: uuid::Uuid) -> anyhow::Result<()> {
        Ok(())
    }

    async fn forget_image(&self, image: &str) -> anyhow::Result<()> {
        let workers: Vec<Entry> = self.entries.lock().iter().filter(|e| e.image == image).cloned().collect();
        let names: std::collections::HashSet<&str> = workers.iter().map(|e| e.name.as_str()).collect();
        self.running.lock().retain(|name, _| !names.contains(name.as_str()));
        self.kill_all(&workers, image).await
    }
}

impl LocalRunner {
    /// Kill every one of `workers` (of `whose`), each tried whatever the
    /// others did; the ones that would not go are named with the command
    /// that stops them by hand.
    async fn kill_all(&self, workers: &[Entry], whose: &str) -> anyhow::Result<()> {
        let mut left = Vec::new();
        for entry in workers {
            if let Err(e) = self.kill(entry).await {
                left.push((entry.pid, format!("{e:#}")));
            }
        }
        if left.is_empty() {
            return Ok(());
        }
        let pids: Vec<String> = left.iter().map(|(pid, _)| pid.to_string()).collect();
        let why: Vec<String> = left.iter().map(|(pid, e)| format!("{pid}: {e}")).collect();
        anyhow::bail!(
            "could not kill the workers of {whose} ({}); stop them by hand with `kill -9 {}`",
            why.join("; "),
            pids.join(" ")
        )
    }
}

/// Why a worker that ended while it started cannot serve: its port held
/// by another program ([`PortTaken`], when it was asked for the project's
/// `front_port`), the program itself failing ([`WorkerWontStart`]: no
/// retry changes it), or a signal from outside, which says nothing of the
/// program and is tried again.
fn start_failure(project: uuid::Uuid, exit: Exit, front_port: Option<u16>, tail: &str) -> anyhow::Error {
    let said = |how: String| format!("the worker of project {project} did not come up ({how}); its last lines:\n{tail}");
    match exit {
        Exit::Code(WORKER_PORT_TAKEN_EXIT) if tail.contains(WORKER_PORT_TAKEN_LINE) => match front_port {
            Some(port) => anyhow::Error::new(PortTaken { port }),
            // A free port another program took between the pick and the
            // start: the next start picks another.
            None => anyhow::anyhow!(said("the free port it was given was taken meanwhile".into())),
        },
        Exit::Code(code) => anyhow::Error::new(WorkerWontStart { why: said(format!("exited with code {code}")) }),
        Exit::Signal(signal) => anyhow::anyhow!(said(format!("killed by signal {signal}"))),
    }
}

/// The last lines of the log at `path`; what went wrong reading it, when
/// it could not be read.
fn log_tail(path: &Path) -> String {
    match std::fs::read(path) {
        Ok(bytes) => {
            let text = String::from_utf8_lossy(&bytes);
            let all: Vec<&str> = text.lines().collect();
            all[all.len().saturating_sub(LOG_TAIL_LINES)..].join("\n")
        }
        Err(e) => format!("(its log {} could not be read: {e})", path.display()),
    }
}

/// Where a caller on this machine reaches the project port `port` opened
/// on `ip`.
fn port_url(ip: std::net::IpAddr, port: u16) -> String {
    format!("http://{}", std::net::SocketAddr::new(loopback_if_any(ip), port))
}

/// The address a caller on this machine reaches a port opened on `ip` at.
fn loopback_if_any(ip: std::net::IpAddr) -> std::net::IpAddr {
    if ip.is_unspecified() {
        std::net::IpAddr::from([127, 0, 0, 1])
    } else {
        ip
    }
}

fn now_unix() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock past UNIX_EPOCH")
        .as_secs() as i64
}

fn random_hex(bytes: usize) -> String {
    let mut buf = vec![0u8; bytes];
    getrandom::fill(&mut buf).expect("the OS random source");
    hex::encode(buf)
}

fn worker_name_prefix(project: uuid::Uuid) -> String {
    format!("weft-w-{}-", project.simple())
}

/// The start of the names of `project`'s workers on `image`.
fn worker_image_prefix(project: uuid::Uuid, image: &str) -> String {
    let digest = Sha256::digest(image.as_bytes());
    let hex: String = digest.iter().take(6).map(|b| format!("{b:02x}")).collect();
    format!("{}{hex}-", worker_name_prefix(project))
}

/// The name of `project`'s worker on `image` with `settings`, which the
/// runner suffixes with its life (`LocalRunner::name_of`): the same for
/// the same three, so a change of settings starts a worker that has them
/// (a process's environment is fixed when it starts). A project id
/// belongs to one install, so the name is that install's.
pub fn worker_name(project: uuid::Uuid, image: &str, settings: &WorkerSettings) -> String {
    let encoded = serde_json::to_vec(settings).expect("worker settings serialize");
    let digest = Sha256::digest(&encoded);
    let hex: String = digest.iter().take(4).map(|b| format!("{b:02x}")).collect();
    format!("{}{hex}", worker_image_prefix(project, image))
}

/// The worker's whole environment: what of the machine's own it gets,
/// where it listens (`listen`), its scratch folder, and what it is.
// SYNC: the worker's environment <-> crates/weft-compiler/src/codegen.rs (write_main_rs Args, PORT),
//       crates/weft-engine/src/test_runner.rs (Args::Serve, PORT),
//       crates/weft-engine/src/listen.rs (LISTEN_ENV),
//       crates/weft-engine/src/scratch.rs (SCRATCH_DIR_ENV),
//       crates/weft-platform-gcp/src/runner.rs (container),
//       crates/weft-core/src/caller_token.rs (ProjectSecret::from_env),
//       crates/weft-engine/src/worker.rs (identity_from_env),
//       crates/weft-core/src/needs.rs (NEEDS_FROM_ENV)
fn worker_env(
    target: &WorkerTarget,
    cfg: &LocalRunnerConfig,
    secret: &weft_core::caller_token::ProjectSecret,
    token: &str,
    listen: std::net::SocketAddr,
    scratch: &Path,
) -> BTreeMap<String, String> {
    let mut env: BTreeMap<String, String> = cfg.machine_env.clone();
    let mut set = |k: &str, v: String| {
        env.insert(k.to_string(), v);
    };
    set("PORT", listen.port().to_string());
    set("WEFT_LISTEN", listen.ip().to_string());
    set("WEFT_SCRATCH_DIR", scratch.display().to_string());
    set("WEFT_PROJECT_ID", target.project.to_string());
    set("WEFT_TENANT_ID", target.tenant.clone());
    set("WEFT_BROKER_URL", cfg.broker_url.clone());
    set("WEFT_WORKER_IDENTITY", format!("token:{token}"));
    set("WEFT_PROJECT_SECRET", secret.to_hex());
    if let Some(binary_hash) = &target.binary_hash {
        set("WEFT_BINARY_HASH", binary_hash.clone());
    }
    // A local worker's callers reach it straight on the machine (its
    // front's port), or through the install's relay, which says who they
    // are itself.
    set("WEFT_TRUSTED_HOPS", "0".into());
    set("WEFT_INVALID_TOKENS_PER_MINUTE", weft_platform_traits::config::invalid_tokens_env(cfg.edge.invalid_tokens_per_minute));
    set(weft_core::needs::NEEDS_FROM_ENV, weft_core::needs::NeedsFrom::Machine.env_value().into());
    for (name, value) in target.settings.worker_env() {
        set(name, value);
    }
    if cfg.time_scale != 1.0 {
        set(weft_core::time_scale::TIME_SCALE_ENV, cfg.time_scale.to_string());
    }
    env
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::processes::fake::{Behaviour, FakeProcesses};
    use weft_platform_traits::{FakeClock, FakeProjectFolders};

    /// Where a worker built by hand for an environment test listens.
    const LISTEN: std::net::SocketAddr = std::net::SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST), 14200);

    fn target(min_instances: u32) -> WorkerTarget {
        WorkerTarget {
            tenant: "local".into(),
            project: uuid::Uuid::from_u128(7),
            image: "weft-worker:abc".into(),
            binary_hash: Some("ab".into()),
            settings: WorkerSettings { min_instances, ..Default::default() },
        }
    }

    /// A machine for one test: an install folder, the worker programs
    /// `abc` and `def`, and project 7's folder.
    struct Machine {
        install: tempfile::TempDir,
        workers: tempfile::TempDir,
        folder: tempfile::TempDir,
        processes: Arc<FakeProcesses>,
        folders: Arc<FakeProjectFolders>,
        clock: Arc<FakeClock>,
    }

    impl Machine {
        fn new() -> Self {
            let workers = tempfile::tempdir().unwrap();
            for hash in ["abc", "def"] {
                std::fs::create_dir_all(workers.path().join(hash)).unwrap();
                std::fs::write(workers.path().join(hash).join("worker"), "").unwrap();
            }
            let folder = tempfile::tempdir().unwrap();
            let folders = Arc::new(FakeProjectFolders::new());
            folders.set(target(0).project, folder.path());
            Self { install: tempfile::tempdir().unwrap(), workers, folder, processes: Arc::new(FakeProcesses::new()), folders, clock: FakeClock::new() }
        }

        fn cfg(&self) -> LocalRunnerConfig {
            LocalRunnerConfig {
                broker_url: "http://127.0.0.1:14113/broker".into(),
                install_secret: vec![0xcd; 32],
                idle_stop: Duration::from_secs(300),
                install: InstallLayout::new(self.install.path()),
                workers_dir: self.workers.path().to_path_buf(),
                machine_env: [("PATH".to_string(), "/usr/bin:/opt/bin".to_string()), ("HOME".to_string(), "/home/me".to_string())].into(),
                time_scale: 1.0,
                edge: weft_platform_traits::config::EdgeConfig {
                    trusted_proxy_hops: weft_platform_traits::config::ProxyHops { public: 0, outside: 0, domains: 0 },
                    invalid_tokens_per_minute: Some(30),
                },
                project_ip: std::net::IpAddr::from([127, 0, 0, 1]),
            }
        }

        fn runner(&self) -> LocalRunner {
            LocalRunner::new(
                self.processes.clone(),
                self.folders.clone(),
                Arc::new(LocalIdentity::from_hex(&"ab".repeat(32)).unwrap()),
                self.clock.clone(),
                self.cfg(),
            )
            .unwrap()
        }

        fn registry(&self) -> Vec<Entry> {
            Registry::in_dir(&InstallLayout::new(self.install.path()).workers()).load().unwrap()
        }

        /// Write down a worker an earlier life started, and make it run.
        fn earlier(&self, name: &str, pid: u32, project: uuid::Uuid) -> Entry {
            let entry = Entry {
                name: name.into(),
                pid,
                life: "0000".into(),
                key: "k".into(),
                project,
                image: "weft-worker:old".into(),
                started: format!("t{pid}"),
            };
            let mut all = self.registry();
            all.push(entry.clone());
            Registry::in_dir(&InstallLayout::new(self.install.path()).workers()).save(&all).unwrap();
            self.processes.running(pid, &entry.started);
            entry
        }

        /// The pids sent `signal`.
        fn signalled(&self, signal: Signal) -> Vec<u32> {
            self.processes.signals().into_iter().filter(|(_, s)| *s == signal).map(|(pid, _)| pid).collect()
        }
    }

    #[tokio::test]
    async fn the_first_call_starts_the_worker_in_its_folder_and_later_calls_reuse_it() {
        let m = Machine::new();
        let r = m.runner();
        let first = r.endpoint(&target(0), Patience::Brief).await.unwrap();
        let second = r.endpoint(&target(0), Patience::Brief).await.unwrap();
        assert_eq!(first.bearer, second.bearer, "one worker, one key");
        let spawns = m.processes.spawns();
        assert_eq!(spawns.len(), 1, "started once");
        assert_eq!(m.processes.seen_key.lock().clone(), Some(first.auth_value()), "the health check presents the worker's key");

        let spawn = &spawns[0];
        let name = r.name_of(&target(0), None);
        assert_eq!(spawn.program, m.workers.path().join("abc").join("worker"));
        assert!(spawn.args.is_empty(), "a worker serves as it is");
        assert_eq!(spawn.cwd, m.folder.path(), "a worker runs in its project's folder");
        assert_eq!(spawn.log, m.install.path().join(format!("workers/logs/{name}.log")));
        assert_eq!(first.base_url, format!("http://127.0.0.1:{}", spawn.env["PORT"]));
        assert_eq!(spawn.env["WEFT_LISTEN"], "127.0.0.1");
        assert_eq!(spawn.env["WEFT_BROKER_URL"], "http://127.0.0.1:14113/broker");
        let scratch = m.install.path().join(format!("workers/scratch/{name}"));
        assert_eq!(spawn.env["WEFT_SCRATCH_DIR"], scratch.display().to_string());
        assert!(scratch.is_dir());
        assert_eq!((spawn.env["PATH"].as_str(), spawn.env["HOME"].as_str()), ("/usr/bin:/opt/bin", "/home/me"), "the machine's programs");
        let unexpected: Vec<&String> = spawn
            .env
            .keys()
            .filter(|k| !k.starts_with("WEFT_") && !["PORT", "PATH", "HOME"].contains(&k.as_str()))
            .collect();
        assert!(unexpected.is_empty(), "nothing else of the runtime's environment: {unexpected:?}");

        let written = m.registry();
        assert_eq!(written.len(), 1);
        assert_eq!((written[0].name.as_str(), written[0].life.as_str(), written[0].key.as_str()), (name.as_str(), r.life.as_str(), first.bearer.as_str()));
    }

    /// A worker runs in its project's folder, and one with none here is
    /// refused naming the project, never started somewhere else.
    #[tokio::test]
    async fn a_project_with_no_folder_here_is_refused() {
        let m = Machine::new();
        let mut elsewhere = target(0);
        elsewhere.project = uuid::Uuid::from_u128(8);
        let e = m.runner().endpoint(&elsewhere, Patience::Brief).await.expect_err("no folder");
        assert!(WorkerWontStart::is(&e) && format!("{e:#}").contains(&elsewhere.project.to_string()), "{e:#}");
        assert!(m.processes.spawns().is_empty());
    }

    /// A node test server starts with `serve`, in its own scratch folder,
    /// whether or not its project has a folder here.
    #[tokio::test]
    async fn a_node_test_server_serves_from_its_scratch_folder() {
        let m = Machine::new();
        let dir = m.workers.path().join("node-tests/cd");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("worker"), "").unwrap();
        let mut server = target(0);
        server.image = "weft-node-tests:cd".into();
        server.binary_hash = None;
        server.project = uuid::Uuid::from_u128(8);
        let r = m.runner();
        r.endpoint(&server, Patience::Brief).await.unwrap();
        let spawn = &m.processes.spawns()[0];
        assert_eq!(spawn.program, dir.join("worker"));
        assert_eq!(spawn.args, ["serve"]);
        let scratch = m.install.path().join(format!("workers/scratch/{}", r.name_of(&server, None)));
        assert_eq!(spawn.cwd, scratch);
        assert!(!spawn.env.contains_key("WEFT_BINARY_HASH"));
        assert_eq!(spawn.env["WEFT_PROJECT_ID"], server.project.to_string());
    }

    /// A project's front listens on the project's port itself, and a new
    /// front takes it only once the old one stopped.
    #[tokio::test]
    async fn the_front_listens_on_the_projects_port_and_a_new_one_takes_it_over() {
        let m = Machine::new();
        let port = m.processes.free_port().unwrap();
        let r = m.runner();
        let address = r.front(&target(0), Some(port)).await.unwrap();
        assert_eq!(address, weft_core::projects::ProjectAddress::Serving { url: format!("http://127.0.0.1:{port}") });
        assert_eq!(m.processes.spawns()[0].env["PORT"], port.to_string(), "one listener, on the project's port");
        let old = m.processes.pids()[0];

        let mut next = target(0);
        next.image = "weft-worker:def".into();
        r.front(&next, Some(port)).await.unwrap();
        assert_eq!(m.signalled(Signal::Term), vec![old], "the old front was told to stop");
        assert_eq!(m.signalled(Signal::Kill), vec![old], "what it started goes with it once it exited");
        assert!(!m.processes.is_alive(old));
        let spawns = m.processes.spawns();
        assert_eq!(spawns.len(), 2);
        assert_eq!(spawns[1].env["PORT"], port.to_string());
        assert_eq!(spawns[1].program, m.workers.path().join("def").join("worker"));
        assert_eq!(m.registry().len(), 1, "the old front is forgotten once gone");
    }

    /// A front still running something keeps the project's port: the new
    /// version takes it over at the first look once the old one has
    /// nothing left running, and nothing on it is cut off meanwhile.
    #[tokio::test]
    async fn a_front_still_running_keeps_the_port_until_it_is_done() {
        let m = Machine::new();
        let port = m.processes.free_port().unwrap();
        let r = m.runner();
        r.front(&target(0), Some(port)).await.unwrap();
        let call = r.endpoint(&target(0), Patience::Brief).await.unwrap();

        let mut next = target(0);
        next.image = "weft-worker:def".into();
        let address = r.front(&next, Some(port)).await.unwrap();
        assert_eq!(address, weft_core::projects::ProjectAddress::Serving { url: format!("http://127.0.0.1:{port}") });
        assert!(m.processes.signals().is_empty(), "nothing is stopped while a call is on it");
        assert_eq!(m.processes.spawns().len(), 1, "the new version waits for the port");
        let unported = r.endpoint(&next, Patience::Brief).await.unwrap();
        assert_ne!(m.processes.spawns()[1].env["PORT"], port.to_string(), "its runs go to a worker without the port");
        drop(unported);

        drop(call);
        r.front(&next, Some(port)).await.unwrap();
        assert!(m.signalled(Signal::Term).contains(&m.processes.pids()[0]), "idle now, the old front stops");
        assert_eq!(m.processes.spawns().last().unwrap().env["PORT"], port.to_string());
        assert_eq!(m.processes.spawns().len(), 3, "the front starts beside the worker its program had");
    }

    /// The worker a program had on a loopback port is never stopped when
    /// the program becomes the front, whatever it is running: the front
    /// starts beside it and takes the calls after, and the old worker goes
    /// once idle like any other.
    #[tokio::test]
    async fn a_busy_worker_is_never_stopped_when_its_program_becomes_the_front() {
        let m = Machine::new();
        let port = m.processes.free_port().unwrap();
        let r = m.runner();
        r.front(&target(0), Some(port)).await.unwrap();
        let on_old_front = r.endpoint(&target(0), Patience::Brief).await.unwrap();
        let mut next = target(0);
        next.image = "weft-worker:def".into();
        r.front(&next, Some(port)).await.unwrap();
        let on_loopback = r.endpoint(&next, Patience::Brief).await.unwrap();
        let loopback = m.processes.pids()[1];

        drop(on_old_front);
        r.front(&next, Some(port)).await.unwrap();
        assert!(!m.signalled(Signal::Term).contains(&loopback) && m.processes.is_alive(loopback), "its call goes on");
        let on_front = r.endpoint(&next, Patience::Brief).await.unwrap();
        assert_eq!(on_front.base_url, format!("http://127.0.0.1:{port}"), "the calls after go to the front");
        drop(on_front);

        m.clock.advance(Duration::from_secs(600));
        r.sweep_idle().await.unwrap();
        assert!(m.processes.is_alive(loopback), "still held");
        drop(on_loopback);
        m.clock.advance(Duration::from_secs(301));
        r.sweep_idle().await.unwrap();
        assert!(m.signalled(Signal::Term).contains(&loopback), "idle now, it goes");
        assert!(m.processes.is_alive(m.processes.pids()[2]), "the front stays");
    }

    /// A program kept warm on a loopback port is no longer kept warm once
    /// it is the front, so the project never keeps two warm copies.
    #[tokio::test]
    async fn a_warm_worker_is_let_go_once_its_program_is_the_front() {
        let m = Machine::new();
        let port = m.processes.free_port().unwrap();
        let r = m.runner();
        r.prepare(&target(1)).await.unwrap();
        let loopback = m.processes.pids()[0];
        r.front(&target(1), Some(port)).await.unwrap();
        r.prepare(&target(1)).await.unwrap();
        assert_eq!(m.processes.spawns().len(), 2, "the front, beside it, and no third copy");
        m.clock.advance(Duration::from_secs(3600));
        r.sweep_idle().await.unwrap();
        assert!(m.signalled(Signal::Term).contains(&loopback));
        assert!(m.processes.is_alive(m.processes.pids()[1]), "the front stays");
    }

    /// A worker kept warm, then not (a prepare with other settings), is
    /// kept warm again by a prepare with its own settings, and the other
    /// one goes.
    #[tokio::test]
    async fn a_prepare_keeps_warm_the_worker_its_settings_name() {
        let m = Machine::new();
        let r = m.runner();
        r.prepare(&target(1)).await.unwrap();
        let mut other = target(1);
        other.settings.min_instances = 2;
        r.prepare(&other).await.unwrap();
        r.prepare(&target(1)).await.unwrap();
        let (first, second) = (m.processes.pids()[0], m.processes.pids()[1]);
        assert_eq!(m.processes.spawns().len(), 2, "the first is taken again");
        m.clock.advance(Duration::from_secs(3600));
        r.sweep_idle().await.unwrap();
        assert_eq!(m.signalled(Signal::Term), vec![second], "only the one the last prepare did not name");
        assert!(m.processes.is_alive(first));
    }

    /// A worker started for settings the project no longer has (a run of
    /// an older program resuming) is not kept warm by them: it finishes
    /// and goes once idle.
    #[tokio::test]
    async fn a_worker_for_settings_no_longer_current_starts_cold() {
        let m = Machine::new();
        let r = m.runner();
        r.prepare(&target(1)).await.unwrap();
        let mut older = target(1);
        older.settings.min_instances = 2;
        let held = r.endpoint(&older, Patience::Brief).await.unwrap();
        drop(held);
        let (current, resumed) = (m.processes.pids()[0], m.processes.pids()[1]);
        m.clock.advance(Duration::from_secs(3600));
        r.sweep_idle().await.unwrap();
        assert_eq!(m.signalled(Signal::Term), vec![resumed], "only the one the project's calls no longer go to");
        assert!(m.processes.is_alive(current));
    }

    /// A worker that cannot be stopped is named, and the sweep still tries
    /// every other one.
    #[tokio::test]
    async fn a_sweep_tries_every_worker_whatever_one_did() {
        let m = Machine::new();
        let stuck = m.earlier("weft-w-stuck", 1, target(0).project);
        let next = m.earlier("weft-w-next", 2, target(0).project);
        m.processes.refusing(stuck.pid);
        let r = m.runner();
        let e = format!("{:#}", r.sweep_idle().await.unwrap_err());
        assert!(e.contains(&stuck.name) && e.contains("kill -9 1"), "{e}");
        assert_eq!(m.signalled(Signal::Term), vec![next.pid], "the next one is told all the same");
    }

    /// A worker started but written down nowhere is killed rather than
    /// left running out of weft's sight.
    #[tokio::test]
    async fn a_worker_that_cannot_be_written_down_is_killed() {
        let m = Machine::new();
        let r = m.runner();
        std::fs::create_dir_all(InstallLayout::new(m.install.path()).workers().join("running.json")).unwrap();
        let e = format!("{:#}", r.endpoint(&target(0), Patience::Brief).await.unwrap_err());
        let pid = m.processes.pids()[0];
        assert!(e.contains("keep track") && e.contains("it was killed"), "{e}");
        assert_eq!(m.signalled(Signal::Kill), vec![pid]);
        assert!(!m.processes.is_alive(pid));
    }

    /// Killing a project's workers waits for a start of one in progress,
    /// so the worker it starts is killed too, never left serving after.
    #[tokio::test]
    async fn killing_a_projects_workers_waits_for_a_start_in_progress() {
        let m = Machine::new();
        let r = m.runner();
        r.endpoint(&target(0), Patience::Brief).await.unwrap();
        let gate = r.starting.lock().await.get(&r.name_of(&target(0), None)).cloned().unwrap();
        let starting = gate.lock_owned().await;
        let mut kill = Box::pin(r.kill_workers("local", target(0).project));
        assert!(tokio::time::timeout(Duration::from_millis(100), &mut kill).await.is_err(), "it waits for the start");
        assert!(m.signalled(Signal::Kill).is_empty());
        drop(starting);
        kill.await.unwrap();
        assert_eq!(m.signalled(Signal::Kill), vec![m.processes.pids()[0]]);
    }

    /// The runtime restarted: the project's front from its earlier life
    /// still holds the port, so it stops before the new front starts, and
    /// the project keeps its address. A pid another program took since is
    /// never signalled.
    #[tokio::test]
    async fn a_front_an_earlier_life_left_stops_so_the_project_keeps_its_port() {
        let m = Machine::new();
        let port = m.processes.free_port().unwrap();
        let old = m.earlier("weft-w-old", 1, target(0).project);
        let other = m.earlier("weft-w-other-project", 2, uuid::Uuid::from_u128(9));
        let reused = m.earlier("weft-w-reused", 3, target(0).project);
        m.processes.running(3, "another program");
        let r = m.runner();
        r.front(&target(0), Some(port)).await.unwrap();
        assert_eq!(m.signalled(Signal::Term), vec![old.pid], "only this project's, and never a pid another program holds");
        assert!(!m.processes.is_alive(old.pid) && m.processes.spawns().len() == 1);
        let names: Vec<String> = m.registry().into_iter().map(|e| e.name).collect();
        assert!(!names.contains(&old.name) && !names.contains(&reused.name), "both gone from weft's sight: {names:?}");
        assert!(names.contains(&other.name), "another project's is left to the sweep");
    }

    /// Killing a project's workers kills every one it has, this life's and
    /// an earlier one's, and forgets them.
    #[tokio::test]
    async fn killing_a_projects_workers_kills_them_outright() {
        let m = Machine::new();
        let earlier = m.earlier("weft-w-old", 1, target(0).project);
        let r = m.runner();
        r.endpoint(&target(0), Patience::Brief).await.unwrap();
        let mine = m.processes.pids()[0];
        r.kill_workers("local", target(0).project).await.unwrap();
        let mut killed = m.signalled(Signal::Kill);
        killed.sort();
        assert_eq!(killed, vec![earlier.pid, mine]);
        assert!(m.registry().is_empty());
        assert!(m.signalled(Signal::Term).is_empty(), "killed, not asked to stop");
        r.endpoint(&target(0), Patience::Brief).await.unwrap();
        assert_eq!(m.processes.spawns().len(), 2, "a call after it starts a new one");
    }

    /// A front whose port another program holds is told apart from any
    /// other failure to start, so the install can give the project
    /// another; the same exit for a worker on a port weft picked is a
    /// start tried again.
    #[test]
    fn a_port_another_program_holds_is_told_apart() {
        let project = uuid::Uuid::from_u128(7);
        let said = format!("{WORKER_PORT_TAKEN_LINE} 127.0.0.1:14200\n");
        let e = start_failure(project, Exit::Code(WORKER_PORT_TAKEN_EXIT), Some(14200), &said);
        assert_eq!(PortTaken::of(&e), Some(14200), "{e:#}");
        let e = start_failure(project, Exit::Code(WORKER_PORT_TAKEN_EXIT), None, &said);
        assert!(PortTaken::of(&e).is_none() && !WorkerWontStart::is(&e), "{e:#}");
        let e = start_failure(project, Exit::Code(WORKER_PORT_TAKEN_EXIT), Some(14200), "something else\n");
        assert!(PortTaken::of(&e).is_none() && WorkerWontStart::is(&e), "the code alone is not the line: {e:#}");
    }

    /// A worker killed from outside says nothing of the build; one that
    /// exits of its own cannot run it.
    #[test]
    fn a_killed_worker_is_no_build_that_cannot_start() {
        let project = uuid::Uuid::from_u128(7);
        assert!(!WorkerWontStart::is(&start_failure(project, Exit::Signal(9), None, "")));
        let e = start_failure(project, Exit::Code(101), None, "thread 'main' panicked");
        assert!(WorkerWontStart::is(&e) && format!("{e:#}").contains("code 101") && format!("{e:#}").contains("panicked"), "{e:#}");
    }

    /// A worker that exits as it starts, and one whose program is gone,
    /// are told apart as workers that cannot start: what runs on that
    /// build ends rather than waits. Nothing of the one that exited is
    /// left behind, and a front's port taken is said as such.
    #[tokio::test]
    async fn a_worker_that_cannot_start_is_told_apart() {
        let m = Machine::new();
        m.processes.behave(Behaviour::Ends { printed: "thread 'main' panicked\n".into(), exit: Exit::Code(101) });
        let r = m.runner();
        let e = r.endpoint(&target(0), Patience::Brief).await.expect_err("exited");
        assert!(WorkerWontStart::is(&e) && format!("{e:#}").contains("panicked"), "{e:#}");
        assert!(m.registry().is_empty());
        assert!(!m.install.path().join(format!("workers/logs/{}.log", r.name_of(&target(0), None))).exists(), "its last lines are in the error");

        // A port nothing here answers on, so only the exit can say it.
        let port = m.processes.free_port().unwrap();
        m.processes.behave(Behaviour::Ends { printed: format!("{WORKER_PORT_TAKEN_LINE} 127.0.0.1:{port}\n"), exit: Exit::Code(WORKER_PORT_TAKEN_EXIT) });
        let e = r.front(&target(0), Some(port)).await.expect_err("taken");
        assert_eq!(PortTaken::of(&e), Some(port), "{e:#}");
        assert!(r.fronts.lock().is_empty(), "no longer the front");

        let mut gone = target(0);
        gone.image = "weft-worker:fff".into();
        let e = r.endpoint(&gone, Patience::Brief).await.expect_err("no program");
        assert!(WorkerWontStart::is(&e) && format!("{e:#}").contains("weft-worker:fff"), "{e:#}");
    }

    #[tokio::test]
    async fn an_idle_worker_is_stopped_but_never_one_in_use_or_kept_warm() {
        let m = Machine::new();
        let r = m.runner();
        let held = r.endpoint(&target(0), Patience::Brief).await.unwrap();
        let pid = m.processes.pids()[0];
        let stopped = || m.signalled(Signal::Term).contains(&pid);
        m.clock.advance(Duration::from_secs(600));
        r.sweep_idle().await.unwrap();
        assert!(!stopped(), "a worker a caller holds stays up");

        drop(held);
        m.clock.advance(Duration::from_secs(299));
        r.sweep_idle().await.unwrap();
        assert!(!stopped(), "inside the idle window");
        m.clock.advance(Duration::from_secs(2));
        m.processes.driving.store(1, Ordering::SeqCst);
        r.sweep_idle().await.unwrap();
        assert!(!stopped(), "a run nobody holds a call for still goes on");
        m.processes.driving.store(0, Ordering::SeqCst);
        m.clock.advance(Duration::from_secs(301));
        r.sweep_idle().await.unwrap();
        assert!(stopped(), "past the idle window");
        assert!(m.registry().is_empty());
        assert!(!m.install.path().join(format!("workers/scratch/{}", r.name_of(&target(0), None))).exists(), "its scratch folder goes with it");

        let warm = m.runner();
        warm.prepare(&target(1)).await.unwrap();
        let warm_pid = *m.processes.pids().last().unwrap();
        m.clock.advance(Duration::from_secs(3600));
        warm.sweep_idle().await.unwrap();
        assert!(!m.signalled(Signal::Term).contains(&warm_pid), "min_instances keeps it up");

        // The settings change: the warm worker is no longer kept up, and
        // stops once idle like any other.
        warm.prepare(&target(0)).await.unwrap();
        warm.sweep_idle().await.unwrap();
        assert!(m.signalled(Signal::Term).contains(&warm_pid), "a worker started with other settings goes once idle");
    }

    #[tokio::test]
    async fn a_worker_an_earlier_life_left_running_is_told_to_stop_and_forgotten_once_gone() {
        let m = Machine::new();
        let old = m.earlier("weft-w-old", 1, target(0).project);
        let done = m.earlier("weft-w-done", 2, target(0).project);
        m.processes.deaf(old.pid);
        m.processes.signal(done.pid, Signal::Kill).unwrap();
        let r = m.runner();
        r.endpoint(&target(0), Patience::Brief).await.unwrap();
        r.sweep_idle().await.unwrap();
        r.sweep_idle().await.unwrap();
        let told = m.signalled(Signal::Term);
        assert_eq!(told, vec![old.pid, old.pid], "told on every sweep until it exits, and only it");
        let names: Vec<String> = m.registry().into_iter().map(|e| e.name).collect();
        assert!(names.contains(&old.name), "kept while it runs");
        assert!(!names.contains(&done.name), "one gone is forgotten");
        assert!(names.contains(&r.name_of(&target(0), None)), "this life's own is never touched");
    }

    /// Forgetting an image takes every worker running the program from it,
    /// and leaves anything else.
    #[tokio::test]
    async fn forgetting_an_image_kills_the_workers_on_it() {
        let m = Machine::new();
        let old = m.earlier("weft-w-old", 1, target(0).project);
        let r = m.runner();
        r.endpoint(&target(0), Patience::Brief).await.unwrap();
        r.forget_image("weft-worker:old").await.unwrap();
        assert_eq!(m.signalled(Signal::Kill), vec![old.pid]);
        assert_eq!(m.registry().len(), 1, "the worker on another image stays");
    }

    /// A worker's environment is fixed when it starts, so other settings
    /// name another worker.
    #[test]
    fn other_settings_name_another_worker_on_the_same_image() {
        let project = uuid::Uuid::from_u128(7);
        let one = worker_name(project, "weft-worker:abc", &target(0).settings);
        let other = worker_name(project, "weft-worker:abc", &target(1).settings);
        assert_ne!(one, other);
        assert_eq!(one, worker_name(project, "weft-worker:abc", &target(0).settings), "the same three, the same name");
        let same_image = worker_image_prefix(project, "weft-worker:abc");
        assert!(one.starts_with(&same_image) && other.starts_with(&same_image));
        assert!(!worker_name(project, "weft-worker:def", &target(0).settings).starts_with(&same_image));
    }

    /// A worker runs at its install's clock factor, so its claim heartbeat
    /// keeps pace with the claim length the broker sets.
    #[test]
    fn a_worker_runs_at_its_installs_pace() {
        let mut cfg = Machine::new().cfg();
        let secret = weft_core::caller_token::ProjectSecret::of(b"s", uuid::Uuid::nil());
        assert!(!worker_env(&target(0), &cfg, &secret, "t", LISTEN, Path::new("/s")).contains_key(weft_core::time_scale::TIME_SCALE_ENV), "real time says nothing");
        cfg.time_scale = 0.05;
        assert_eq!(worker_env(&target(0), &cfg, &secret, "t", LISTEN, Path::new("/s"))[weft_core::time_scale::TIME_SCALE_ENV], "0.05");
    }

    /// A worker runs the program's own code, so it is given its project's
    /// secret and never the install's.
    #[test]
    fn a_worker_holds_its_projects_secret_and_never_the_installs() {
        let cfg = Machine::new().cfg();
        let secret = weft_core::caller_token::ProjectSecret::of(&cfg.install_secret, target(0).project);
        let env = worker_env(&target(0), &cfg, &secret, "t", LISTEN, Path::new("/s"));
        assert_eq!(env["WEFT_PROJECT_SECRET"], secret.to_hex());
        assert!(!env.contains_key("WEFT_CALLER_TOKEN_SECRET"));
        let install = hex::encode(&cfg.install_secret);
        assert!(env.values().all(|v| !v.contains(&install)), "{env:?}");
    }
}
