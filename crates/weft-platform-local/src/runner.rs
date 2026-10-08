//! Workers on the local Docker daemon.
//!
//! One container per project, image and worker settings, started the first
//! time weft calls it and stopped once it has had nothing to do for the
//! idle window (`workerIdleStopSeconds`), unless the project keeps copies
//! warm (`workers.min_instances`). A change of settings starts a new one
//! for the calls after it; the old one finishes what it drives and goes
//! once idle, except the project's front, which gives the port to the new
//! one at once (its runs leave the way they do when a platform stops a
//! worker).
//!
//! The project's front (`Runner::front`: the program its callers reach)
//! publishes the project's own port itself, so a caller talks to the
//! worker with nothing of weft's in between, and it is kept running for
//! as long as it is the front. Only one container can hold a published
//! port, so a new front starts once the old one stopped: a caller
//! arriving in that moment is refused and tries again.
//!
//! Every worker also publishes a loopback port of its own, where this
//! process reaches it. Weft's own endpoints answer only the project's
//! worker key (`weft_core::caller_token::worker_door_key`), which the
//! worker derives from the project secret it holds.
//! The worker calls the broker with a token the install signed for
//! exactly its project.
//!
//! One copy per project, image and settings serves every execution: a laptop has
//! one machine, so `max_instances` changes nothing here, and
//! `concurrency` is the worker's own business.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use sha2::{Digest, Sha256};
use weft_platform_traits::identity::Principal;
use weft_platform_traits::{Clock, Patience, PortTaken, Runner, WorkerEndpoint, WorkerSettings, WorkerTarget, WORKER_AUTH_HEADER};

use crate::docker::{self, labels, roles, Docker};
use crate::identity::LocalIdentity;

/// The port a worker serves on inside its container.
const WORKER_PORT: u16 = 8080;

/// How long a started worker gets to answer its health check. The
/// worker's own start is weft's code (the user's nodes run later, per
/// execution), so a worker that does not answer by then is broken.
const START_DEADLINE: Duration = Duration::from_secs(120);

/// How long a worker being replaced (a front giving way to the next, one
/// that stopped answering) gets to let its runs go (`weft_engine::worker`:
/// five seconds for a step to end, then its record written) before it is
/// killed.
const WORKER_STOP_GRACE_SECS: u64 = 10;

/// A local worker's broker token never expires on its own: the container
/// is the credential's life, and rotating `WEFT_IDENTITY_KEY` revokes
/// every one at once.
const WORKER_TOKEN_LIFE_SECS: i64 = 100 * 365 * 24 * 3600;

/// What the runner needs besides Docker.
#[derive(Clone)]
pub struct LocalRunnerConfig {
    /// The broker's address as a container reaches it.
    pub broker_url: String,
    /// The install's caller-ticket secret, each project's own secret is
    /// derived from (`weft_core::caller_token::ProjectSecret`); a worker is
    /// given only its project's.
    pub install_secret: Vec<u8>,
    /// How long a worker with nothing to do stays up.
    pub idle_stop: Duration,
    /// Where the runner writes a worker's environment file for the moment
    /// it takes to start it (the secrets stay off the command line).
    pub scratch_dir: std::path::PathBuf,
    /// The install these workers belong to.
    pub install: weft_core::infra::Install,
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
    docker: Arc<dyn Docker>,
    identity: Arc<LocalIdentity>,
    clock: Arc<dyn Clock>,
    http: reqwest::Client,
    cfg: LocalRunnerConfig,
    /// The workers this process started, by container name.
    running: parking_lot::Mutex<HashMap<String, Arc<Running>>>,
    /// One start at a time per container name.
    starting: tokio::sync::Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
    /// This life of the process, on every worker it starts
    /// ([`labels::LIFE`]) and in its name, so a worker an earlier life
    /// left running is never taken for one of this life's (nor replaced
    /// by name while it finishes its runs).
    life: String,
    /// The workers of an earlier life already told to stop, so the
    /// daemon's log says so once per worker.
    told: parking_lot::Mutex<std::collections::HashSet<String>>,
    /// Each project's front: the container that publishes its port.
    fronts: parking_lot::Mutex<HashMap<uuid::Uuid, Front>>,
}

/// The container a project's callers reach, on which port, and what it
/// runs (so a front that went down is started again). A front is kept up
/// however idle.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Front {
    name: String,
    port: u16,
    target: WorkerTarget,
}

/// A worker container this process started.
struct Running {
    name: String,
    base_url: String,
    key: String,
    /// The project's port, when this worker is its front and holds it.
    port: Option<u16>,
    /// Calls handed out and not yet finished.
    holds: AtomicUsize,
    last_used: parking_lot::Mutex<Instant>,
    /// Kept up however idle (the project's `min_instances` is above 0),
    /// until the project's settings change and a worker started with the
    /// new ones takes its calls. A front is kept up besides, for as long
    /// as it is the front.
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
    pub fn new(docker: Arc<dyn Docker>, identity: Arc<LocalIdentity>, clock: Arc<dyn Clock>, cfg: LocalRunnerConfig) -> Self {
        Self {
            docker,
            identity,
            clock,
            http: reqwest::Client::builder().timeout(Duration::from_secs(5)).build().expect("reqwest client"),
            cfg,
            running: parking_lot::Mutex::new(HashMap::new()),
            starting: tokio::sync::Mutex::new(HashMap::new()),
            life: random_hex(4),
            told: parking_lot::Mutex::new(std::collections::HashSet::new()),
            fronts: parking_lot::Mutex::new(HashMap::new()),
        }
    }

    /// The container name of this life's worker for `target`
    /// ([`worker_name`], then the life).
    fn name_of(&self, target: &WorkerTarget) -> String {
        format!("{}-{}", worker_name(target.project, &target.image, &target.settings), self.life)
    }

    fn hold(&self, running: &Arc<Running>) -> WorkerEndpoint {
        running.holds.fetch_add(1, Ordering::SeqCst);
        *running.last_used.lock() = self.clock.now();
        WorkerEndpoint {
            base_url: running.base_url.clone(),
            bearer: running.key.clone(),
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

    /// Start the worker for `target`, publishing the project's `port` when
    /// it is the front, and wait until it answers. A port another program
    /// holds is [`PortTaken`].
    async fn start(&self, target: &WorkerTarget, name: &str, keep_warm: bool, port: Option<u16>) -> anyhow::Result<Arc<Running>> {
        docker::ensure_network(self.docker.as_ref()).await?;
        docker::remove_containers(self.docker.as_ref(), &[name.to_string()]).await?;
        let secret = self.secret_of(target.project);
        let key = secret.worker_door_key();
        let token = self.identity.mint(
            Principal::Worker { tenant: target.tenant.clone(), project: target.project },
            now_unix() + WORKER_TOKEN_LIFE_SECS,
        );
        let env = worker_env(target, &self.cfg, &secret, &token);
        let env_file = self.write_env_file(name, &env)?;
        let published = port.map(|port| std::net::SocketAddr::new(self.cfg.project_ip, port));
        let args = run_args(&self.cfg.install, target, name, &self.life, &env_file, published);
        let started = docker::run(self.docker.as_ref(), args).await;
        // The file holds the worker's secrets: one left behind is said.
        if let Err(e) = std::fs::remove_file(&env_file) {
            tracing::error!(target: "weft_platform_local::runner", file = %env_file.display(), error = %e, "a worker's environment file could not be removed; delete it by hand, it holds the worker's secrets");
        }
        if let Err(e) = started {
            let said = format!("{e:#}");
            if let Some(port) = port.filter(|_| said.contains("port is already allocated") || said.contains("address already in use")) {
                // Docker made the container before it failed to publish.
                docker::remove_containers(self.docker.as_ref(), &[name.to_string()]).await?;
                return Err(anyhow::Error::new(PortTaken { port }));
            }
            return Err(e.context(format!("start the worker of project {}", target.project)));
        }

        let printed = docker::run(self.docker.as_ref(), vec!["port".into(), name.into(), format!("{WORKER_PORT}/tcp")]).await?;
        let base_url = published_url(&printed, published)
            .ok_or_else(|| anyhow::anyhow!("`docker port {name}` printed no loopback address: {printed:?}"))?;

        let started_at = Instant::now();
        loop {
            if self.healthy(&base_url, &key).await {
                break;
            }
            let state = docker::run(
                self.docker.as_ref(),
                vec!["inspect".into(), "--format".into(), "{{.State.Status}}".into(), name.into()],
            )
            .await?;
            if state.trim() != "running" || started_at.elapsed() > START_DEADLINE {
                let logs = self.docker.exec(&["logs".into(), "--tail".into(), "50".into(), name.into()]).await?;
                anyhow::bail!(
                    "the worker of project {} did not come up ({}); its last lines:\n{}{}",
                    target.project,
                    if state.trim() == "running" { "no answer to its health check" } else { state.trim() },
                    logs.stdout,
                    logs.stderr
                );
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        Ok(Arc::new(Running {
            name: name.to_string(),
            base_url,
            key,
            holds: AtomicUsize::new(0),
            last_used: parking_lot::Mutex::new(self.clock.now()),
            keep_warm: AtomicBool::new(keep_warm),
            port,
        }))
    }

    fn write_env_file(&self, name: &str, env: &BTreeMap<&str, String>) -> anyhow::Result<std::path::PathBuf> {
        use std::io::Write as _;
        std::fs::create_dir_all(&self.cfg.scratch_dir)?;
        let path = self.cfg.scratch_dir.join(format!("{name}.env"));
        let mut opts = std::fs::OpenOptions::new();
        opts.write(true).create(true).truncate(true);
        #[cfg(unix)]
        std::os::unix::fs::OpenOptionsExt::mode(&mut opts, 0o600);
        let mut file = opts.open(&path).map_err(|e| anyhow::anyhow!("write {}: {e}", path.display()))?;
        for (k, v) in env {
            writeln!(file, "{k}={v}")?;
        }
        Ok(path)
    }

    /// The worker for `target`, started when it is not running: with the
    /// project's port and kept warm when it is the project's front.
    async fn ensure(&self, target: &WorkerTarget) -> anyhow::Result<Arc<Running>> {
        let name = self.name_of(target);
        let gate = self.starting.lock().await.entry(name.clone()).or_default().clone();
        let _one_start = gate.lock().await;
        let wants_port = self.fronts.lock().get(&target.project).filter(|front| front.name == name).map(|front| front.port);
        let known = self.running.lock().get(&name).cloned();
        if let Some(running) = known {
            // One started before it became the front holds no port: it
            // starts again with it. One that was a front and is no more
            // keeps its port and serves on until it goes idle.
            let as_wanted = wants_port.is_none() || running.port == wants_port;
            if as_wanted && self.healthy(&running.base_url, &running.key).await {
                return Ok(running);
            }
            if as_wanted {
                tracing::warn!(target: "weft_platform_local::runner", worker = %name, "a worker stopped answering; starting it again");
            }
            self.running.lock().remove(&name);
            docker::stop(self.docker.as_ref(), std::slice::from_ref(&name), WORKER_STOP_GRACE_SECS).await?;
        }
        let running = match self.start(target, &name, target.settings.min_instances > 0, wants_port).await {
            // Another program holds the port: this is no front any more.
            Err(e) if PortTaken::of(&e).is_some() => {
                let mut fronts = self.fronts.lock();
                if fronts.get(&target.project).is_some_and(|front| front.name == name) {
                    fronts.remove(&target.project);
                }
                return Err(e);
            }
            started => started?,
        };
        self.running.lock().insert(name, running.clone());
        Ok(running)
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
    /// and every worker container an earlier life of this process left
    /// behind ([`labels::LIFE`]). A worker left behind may still be
    /// driving runs (the daemon restarted under it, an upgrade say), so it
    /// is told to stop the way a cloud platform replaces one (its durable
    /// runs go back for another worker, its fast runs end first) and
    /// removed on a later sweep once it exited; this life's own worker for
    /// the same project has another name, so nothing replaces it by name
    /// meanwhile. Called on an interval by the process keeper. A worker still
    /// driving a run nobody holds a call for (one whose caller went away
    /// and that goes on without them) is doing something: it stays up.
    pub async fn sweep_idle(&self) -> anyhow::Result<()> {
        let fronts: std::collections::HashSet<String> = self.fronts.lock().values().map(|front| front.name.clone()).collect();
        let now = self.clock.now();
        let quiet: Vec<Arc<Running>> = self
            .running
            .lock()
            .values()
            .filter(|r| {
                !r.keep_warm.load(Ordering::SeqCst)
                    && !fronts.contains(&r.name)
                    && r.holds.load(Ordering::SeqCst) == 0
                    && now.duration_since(*r.last_used.lock()) >= self.cfg.idle_stop
            })
            .cloned()
            .collect();
        for r in quiet {
            if self.driving(&r.base_url, &r.key).await.is_some_and(|driving| driving > 0) {
                *r.last_used.lock() = self.clock.now();
                continue;
            }
            // Stopped under its start gate, so no call starts it again in
            // between, and only if nothing took it while it was asked: a
            // call that came since holds it or used it.
            let gate = self.starting.lock().await.entry(r.name.clone()).or_default().clone();
            let _no_start = gate.lock().await;
            let still_idle = {
                let mut running = self.running.lock();
                let idle = running.get(&r.name).is_some_and(|now_running| {
                    Arc::ptr_eq(now_running, &r)
                        && r.holds.load(Ordering::SeqCst) == 0
                        && self.clock.now().duration_since(*r.last_used.lock()) >= self.cfg.idle_stop
                });
                if idle {
                    running.remove(&r.name);
                }
                idle
            };
            // Told to stop first, as every planned stop is: a run that
            // reached it on its own port since the look above is handed
            // back if it can pause, and runs on until the kill if it
            // cannot.
            if still_idle {
                docker::stop(self.docker.as_ref(), std::slice::from_ref(&r.name), WORKER_STOP_GRACE_SECS).await?;
                docker::remove_containers(self.docker.as_ref(), std::slice::from_ref(&r.name)).await?;
            }
        }
        // A gate nothing is starting under and no worker answers to.
        let known: std::collections::HashSet<String> = self.running.lock().keys().cloned().collect();
        self.starting.lock().await.retain(|name, gate| known.contains(name) || Arc::strong_count(gate) > 1);
        let left: Vec<docker::ContainerRow> =
            docker::containers(self.docker.as_ref(), self.cfg.install.label_value(), &[(labels::ROLE, roles::WORKER)])
                .await?
                .into_iter()
                .filter(|c| c.label(labels::LIFE) != Some(self.life.as_str()))
                .collect();
        let named = |states: &[&str]| -> Vec<String> {
            left.iter().filter(|c| states.contains(&c.state.as_str())).map(|c| c.name.clone()).collect()
        };
        // A paused one (by hand) is neither: left as it is.
        let (going, gone) = (named(&["running"]), named(&["exited", "created", "dead"]));
        {
            let mut told = self.told.lock();
            for name in going.iter().filter(|name| told.insert((*name).clone())) {
                tracing::info!(
                    target: "weft_platform_local::runner",
                    worker = %name,
                    "a worker an earlier run of this daemon started is finishing its runs before it goes; `docker rm --force {name}` stops it now"
                );
            }
        }
        // Told again on every sweep until it exits: a worker heeds the
        // first and ignores the rest.
        docker::ask_to_stop(self.docker.as_ref(), &going).await?;
        docker::remove_containers(self.docker.as_ref(), &gone).await
    }

    fn forget_where(&self, keep: impl Fn(&Running) -> bool) {
        self.running.lock().retain(|_, r| keep(r));
    }
}

#[async_trait]
impl Runner for LocalRunner {
    async fn prepare(&self, target: &WorkerTarget) -> anyhow::Result<()> {
        target.settings.validate().map_err(|e| anyhow::anyhow!("project {}: {e}", target.project))?;
        // A worker started with other settings keeps what it drives and
        // stops once idle, like any other: calls go to one started with
        // these from now on (its name follows them).
        let current = self.name_of(target);
        let same_image = worker_image_prefix(target.project, &target.image);
        for running in self.running.lock().values() {
            if running.name.starts_with(&same_image) && running.name != current {
                running.keep_warm.store(false, Ordering::SeqCst);
            }
        }
        if target.settings.min_instances > 0 {
            self.ensure(target).await?;
        }
        Ok(())
    }

    // A local container is up in seconds, so every caller waits it out:
    // there is no bring-up long enough to answer `WorkerStarting` to.
    async fn endpoint(&self, target: &WorkerTarget, _patience: Patience) -> anyhow::Result<WorkerEndpoint> {
        let running = self.ensure(target).await?;
        Ok(self.hold(&running))
    }

    async fn front(&self, target: &WorkerTarget, port: Option<u16>) -> anyhow::Result<weft_core::projects::ProjectAddress> {
        let port = port.ok_or_else(|| anyhow::anyhow!("a local project's front needs the project's port"))?;
        let name = self.name_of(target);
        let wanted = Front { name: name.clone(), port, target: target.clone() };
        self.fronts.lock().insert(target.project, wanted);
        // A container of this project's that holds the port (the front
        // before, or a former one serving on until idle): it lets its runs
        // go and stops, which frees the port. Under its start gate, so a
        // call for its program meanwhile waits and then starts a worker of
        // its own rather than one this stop would take down.
        let holding: Vec<String> = self
            .running
            .lock()
            .values()
            .filter(|r| r.port == Some(port) && r.name != name)
            .map(|r| r.name.clone())
            .collect();
        for held in holding {
            let gate = self.starting.lock().await.entry(held.clone()).or_default().clone();
            let _no_start = gate.lock().await;
            if self.running.lock().remove(&held).is_some() {
                docker::stop(self.docker.as_ref(), std::slice::from_ref(&held), WORKER_STOP_GRACE_SECS).await?;
                docker::remove_containers(self.docker.as_ref(), std::slice::from_ref(&held)).await?;
            }
        }
        // This project's workers an earlier life of the daemon left running
        // (`sweep_idle` tells them to stop, but only on its next pass): the
        // last front among them still publishes the project's port, and the
        // project would otherwise move to another port on every restart of
        // the daemon. They stop the same way, letting their runs go first.
        let project_label = target.project.to_string();
        let earlier: Vec<String> =
            docker::containers(self.docker.as_ref(), self.cfg.install.label_value(), &[(labels::PROJECT, &project_label), (labels::ROLE, roles::WORKER)])
                .await?
                .into_iter()
                .filter(|c| c.label(labels::LIFE) != Some(self.life.as_str()) && c.state == "running")
                .map(|c| c.name)
                .collect();
        docker::stop(self.docker.as_ref(), &earlier, WORKER_STOP_GRACE_SECS).await?;
        docker::remove_containers(self.docker.as_ref(), &earlier).await?;
        let running = self.ensure(target).await?;
        let port = running.port.ok_or_else(|| anyhow::anyhow!("project {}'s front started without its port", target.project))?;
        Ok(weft_core::projects::ProjectAddress::Serving { url: format!("http://{}", std::net::SocketAddr::new(loopback_if_any(self.cfg.project_ip), port)) })
    }

    async fn let_front_go(&self, project: uuid::Uuid) -> anyhow::Result<()> {
        // No longer a front, it stops once idle like any worker: a project
        // that takes no calls keeps no instance warm.
        if let Some(front) = self.fronts.lock().remove(&project) {
            if let Some(running) = self.running.lock().get(&front.name) {
                running.keep_warm.store(false, Ordering::SeqCst);
            }
        }
        Ok(())
    }

    async fn retire(&self, _tenant: &str, project: uuid::Uuid) -> anyhow::Result<()> {
        self.fronts.lock().remove(&project);
        let project_label = project.to_string();
        let names: Vec<String> = docker::containers(self.docker.as_ref(), self.cfg.install.label_value(), &[(labels::PROJECT, &project_label)])
            .await?
            .into_iter()
            .filter(is_program_container)
            .map(|c| c.name)
            .collect();
        docker::remove_containers(self.docker.as_ref(), &names).await?;
        let prefix = worker_name_prefix(project);
        self.forget_where(|r| !r.name.starts_with(&prefix));
        Ok(())
    }

    async fn forget_image(&self, image: &str) -> anyhow::Result<()> {
        let names: Vec<String> = docker::containers(self.docker.as_ref(), self.cfg.install.label_value(), &[(labels::IMAGE, image)])
            .await?
            .into_iter()
            .filter(is_program_container)
            .map(|c| c.name)
            .collect();
        docker::remove_containers(self.docker.as_ref(), &names).await?;
        self.forget_where(|r| !names.contains(&r.name));
        Ok(())
    }
}

/// Whether a container runs a project's program: what retiring a project
/// or forgetting an image removes.
fn is_program_container(c: &docker::ContainerRow) -> bool {
    c.label(labels::ROLE) == Some(roles::WORKER)
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
    getrandom::getrandom(&mut buf).expect("the OS random source");
    hex::encode(buf)
}

fn worker_name_prefix(project: uuid::Uuid) -> String {
    format!("weft-w-{}-", project.simple())
}

/// The start of the container names of `project`'s workers on `image`.
fn worker_image_prefix(project: uuid::Uuid, image: &str) -> String {
    let digest = Sha256::digest(image.as_bytes());
    let hex: String = digest.iter().take(6).map(|b| format!("{b:02x}")).collect();
    format!("{}{hex}-", worker_name_prefix(project))
}

/// The name of `project`'s worker on `image` with `settings`, which the
/// runner suffixes with its life (`LocalRunner::name_of`): the same for
/// the same three, so a change of settings starts a worker that has them
/// (a container's CPU, memory and environment are fixed when it starts).
/// A project id belongs to one install, so the name is that install's.
pub fn worker_name(project: uuid::Uuid, image: &str, settings: &WorkerSettings) -> String {
    let encoded = serde_json::to_vec(settings).expect("worker settings serialize");
    let digest = Sha256::digest(&encoded);
    let hex: String = digest.iter().take(4).map(|b| format!("{b:02x}")).collect();
    format!("{}{hex}", worker_image_prefix(project, image))
}

/// The worker's environment.
// SYNC: the worker's environment <-> crates/weft-compiler/src/codegen.rs (write_main_rs Args),
//       crates/weft-platform-gcp/src/runner.rs (container),
//       crates/weft-core/src/caller_token.rs (ProjectSecret::from_env),
//       crates/weft-engine/src/worker.rs (identity_from_env)
fn worker_env(target: &WorkerTarget, cfg: &LocalRunnerConfig, secret: &weft_core::caller_token::ProjectSecret, token: &str) -> BTreeMap<&'static str, String> {
    let mut env = BTreeMap::new();
    env.insert("WEFT_PROJECT_ID", target.project.to_string());
    env.insert("WEFT_TENANT_ID", target.tenant.clone());
    env.insert("WEFT_BROKER_URL", cfg.broker_url.clone());
    env.insert("PORT", WORKER_PORT.to_string());
    env.insert("WEFT_WORKER_IDENTITY", format!("token:{token}"));
    env.insert("WEFT_PROJECT_SECRET", secret.to_hex());
    if let Some(binary_hash) = &target.binary_hash {
        env.insert("WEFT_BINARY_HASH", binary_hash.clone());
    }
    // A local worker's callers reach it straight on the machine (its
    // front's port), or through the install's relay, which says who they
    // are itself.
    env.insert("WEFT_TRUSTED_HOPS", "0".into());
    env.insert("WEFT_INVALID_TOKENS_PER_MINUTE", weft_platform_traits::config::invalid_tokens_env(cfg.edge.invalid_tokens_per_minute));
    for (name, value) in target.settings.worker_env() {
        env.insert(name, value);
    }
    if cfg.time_scale != 1.0 {
        env.insert(weft_core::time_scale::TIME_SCALE_ENV, cfg.time_scale.to_string());
    }
    env
}

/// `docker run` for a worker container the runner's `life` starts, which
/// publishes the project's port at `project` when it is the front.
fn run_args(
    install: &weft_core::infra::Install,
    target: &WorkerTarget,
    name: &str,
    life: &str,
    env_file: &std::path::Path,
    project: Option<std::net::SocketAddr>,
) -> Vec<String> {
    let mut l = BTreeMap::new();
    l.insert(labels::INSTALL, install.label_value().to_string());
    l.insert(labels::PROJECT, target.project.to_string());
    l.insert(labels::TENANT, target.tenant.clone());
    l.insert(labels::IMAGE, target.image.clone());
    l.insert(labels::ROLE, roles::WORKER.to_string());
    l.insert(labels::LIFE, life.to_string());
    let mut args = vec!["run".to_string(), "--detach".into(), "--name".into(), name.into()];
    args.extend(["--network".into(), docker::NETWORK.into()]);
    // Where a container reaches the machine's internal port.
    args.extend(docker::host_gateway_args());
    // No CPU or memory cap, whatever the settings say: they size the
    // machines a cloud pays for, and a local worker takes as much of this
    // machine as its runs ask for, like any program run on it.
    args.extend(["--env-file".into(), env_file.display().to_string()]);
    args.extend(["--publish".into(), format!("127.0.0.1::{WORKER_PORT}")]);
    if let Some(project) = project {
        args.extend(["--publish".into(), format!("{project}:{WORKER_PORT}")]);
    }
    args.extend(docker::label_args(&l));
    args.push(target.image.clone());
    args
}

/// `http://127.0.0.1:<port>` from what `docker port` printed (one line
/// per binding, `127.0.0.1:49153`): the worker's own loopback port, never
/// the project's port it also publishes when it is the front (`project`).
fn published_url(printed: &str, project: Option<std::net::SocketAddr>) -> Option<String> {
    let project = project.map(|at| at.to_string());
    printed
        .lines()
        .map(str::trim)
        .filter(|l| Some(*l) != project.as_deref())
        .find(|l| l.starts_with("127.0.0.1:"))
        .map(|addr| format!("http://{addr}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::docker::fake::FakeDocker;
    use weft_platform_traits::FakeClock;

    fn target(min_instances: u32) -> WorkerTarget {
        WorkerTarget {
            tenant: "local".into(),
            project: uuid::Uuid::from_u128(7),
            image: "weft-worker:abc".into(),
            binary_hash: Some("ab".into()),
            settings: WorkerSettings { min_instances, ..Default::default() },
        }
    }

    /// What the test workers say they drive.
    static DRIVING: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

    /// A worker that answers its health check only to `key`.
    async fn worker_answering() -> (String, Arc<parking_lot::Mutex<Option<String>>>) {
        use axum::http::HeaderMap;
        let expected: Arc<parking_lot::Mutex<Option<String>>> = Arc::default();
        let seen = expected.clone();
        let app = axum::Router::new().route(
            "/_weft/healthz",
            axum::routing::get(move |headers: HeaderMap| {
                let seen = seen.clone();
                async move {
                    let got = headers.get(WORKER_AUTH_HEADER).and_then(|v| v.to_str().ok()).map(str::to_string);
                    *seen.lock() = got;
                    let driving = DRIVING.load(Ordering::SeqCst);
                    axum::Json(serde_json::json!({ "driving": driving }))
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (format!("127.0.0.1:{}", addr.port()), expected)
    }

    fn runner(docker: Arc<FakeDocker>, clock: Arc<FakeClock>, dir: &std::path::Path) -> LocalRunner {
        LocalRunner::new(
            docker,
            Arc::new(LocalIdentity::from_hex(&"ab".repeat(32)).unwrap()),
            clock,
            LocalRunnerConfig {
                broker_url: "http://host.docker.internal:14113/broker".into(),
                install_secret: vec![0xcd; 32],
                idle_stop: Duration::from_secs(300),
                scratch_dir: dir.to_path_buf(),
                install: weft_core::infra::Install::default_install(),
                time_scale: 1.0,
                edge: weft_platform_traits::config::EdgeConfig {
                    trusted_proxy_hops: weft_platform_traits::config::ProxyHops { public: 0, outside: 0, domains: 0 },
                    invalid_tokens_per_minute: Some(30),
                },
                project_ip: std::net::IpAddr::from([127, 0, 0, 1]),
            },
        )
    }

    fn scratch() -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("weft-runner-test-{}", uuid::Uuid::new_v4().simple()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[tokio::test]
    async fn the_first_call_starts_the_worker_and_later_calls_reuse_it() {
        let (addr, seen_auth) = worker_answering().await;
        let docker = Arc::new(FakeDocker::new());
        docker.answer(&["port"], &format!("{addr}\n"));
        let dir = scratch();
        let r = runner(docker.clone(), FakeClock::new(), &dir);

        let first = r.endpoint(&target(0), Patience::Brief).await.unwrap();
        let second = r.endpoint(&target(0), Patience::Brief).await.unwrap();
        assert_eq!(first.base_url, format!("http://{addr}"));
        assert_eq!(first.bearer, second.bearer, "one worker, one key");
        assert_eq!(docker.calls_to("run").len(), 1, "started once");
        assert_eq!(seen_auth.lock().clone(), Some(first.auth_value()), "the health check presents the worker's key");

        let run = &docker.calls_to("run")[0];
        let name = r.name_of(&target(0));
        assert!(run.windows(2).any(|w| w == ["--name", name.as_str()]));
        assert!(run.windows(2).any(|w| w == ["--label".to_string(), format!("{}={}", labels::LIFE, r.life)]), "{run:?}");
        assert!(run.windows(2).any(|w| w == ["--publish", "127.0.0.1::8080"]));
        assert!(!run.iter().any(|a| a == "--memory" || a == "--cpus"), "a local worker is not capped: {run:?}");
        assert_eq!(run.last().unwrap(), "weft-worker:abc");
        assert!(!run.iter().any(|a| a.contains("WEFT_WORKER_IDENTITY")), "secrets stay off the command line");
        assert!(std::fs::read_dir(&dir).unwrap().next().is_none(), "the environment file is gone once started");
    }

    /// A project's front publishes the project's port itself, and a new
    /// front takes it only once the old one stopped.
    #[tokio::test]
    async fn the_front_publishes_the_projects_port_and_a_new_one_takes_it_over() {
        let (addr, _) = worker_answering().await;
        let docker = Arc::new(FakeDocker::new());
        docker.answer(&["port"], &format!("{addr}\n"));
        let r = runner(docker.clone(), FakeClock::new(), &scratch());
        let address = r.front(&target(0), Some(14200)).await.unwrap();
        assert_eq!(address, weft_core::projects::ProjectAddress::Serving { url: "http://127.0.0.1:14200".into() });
        let run = &docker.calls_to("run")[0];
        assert!(run.windows(2).any(|w| w == ["--publish", "127.0.0.1:14200:8080"]), "{run:?}");

        let mut next = target(0);
        next.image = "weft-worker:def".into();
        r.front(&next, Some(14200)).await.unwrap();
        let old = r.name_of(&target(0));
        let calls = docker.calls();
        let stopped = calls.iter().position(|c| c.first().map(String::as_str) == Some("stop") && c.contains(&old)).expect("the old front stopped");
        let started = calls.iter().rposition(|c| c.first().map(String::as_str) == Some("run")).unwrap();
        assert!(stopped < started, "the port is freed before the new front takes it");
        assert!(docker.calls_to("run")[1].windows(2).any(|w| w == ["--publish", "127.0.0.1:14200:8080"]));
    }

    /// The daemon restarted: the project's front from its earlier life
    /// still publishes the port, so it stops before the new front starts,
    /// and the project keeps its address.
    #[tokio::test]
    async fn a_front_an_earlier_life_left_stops_so_the_project_keeps_its_port() {
        let (addr, _) = worker_answering().await;
        let docker = Arc::new(FakeDocker::new());
        docker.answer(&["port"], &format!("{addr}\n"));
        let r = runner(docker.clone(), FakeClock::new(), &scratch());
        docker.answer(
            &["ps"],
            &format!(
                "{{\"Names\":\"weft-w-old\",\"State\":\"running\",\"Labels\":\"weft.role=worker,weft.life=0000\"}}\n\
                 {{\"Names\":\"weft-w-mine\",\"State\":\"running\",\"Labels\":\"weft.role=worker,weft.life={}\"}}\n",
                r.life
            ),
        );
        r.front(&target(0), Some(14200)).await.unwrap();
        let calls = docker.calls();
        let ps = calls.iter().find(|c| c.first().map(String::as_str) == Some("ps")).expect("listed");
        assert!(ps.contains(&format!("label=weft.project={}", target(0).project)), "only this project's: {ps:?}");
        let stopped = calls.iter().position(|c| c.first().map(String::as_str) == Some("stop") && c.contains(&"weft-w-old".to_string())).expect("the earlier front stopped");
        let started = calls.iter().position(|c| c.first().map(String::as_str) == Some("run")).unwrap();
        assert!(stopped < started, "the port is freed before the new front takes it");
        assert!(!calls.iter().any(|c| c.first().map(String::as_str) == Some("stop") && c.contains(&"weft-w-mine".to_string())), "one of this life's is never stopped here");
    }

    /// A port another program holds is told apart from any other failure
    /// to start, so the install can give the project another.
    #[tokio::test]
    async fn a_port_another_program_holds_is_told_apart() {
        let docker = Arc::new(FakeDocker::new());
        docker.fail(&["run"], "Bind for 127.0.0.1:14200 failed: port is already allocated");
        let r = runner(docker.clone(), FakeClock::new(), &scratch());
        let name = r.name_of(&target(0));
        let e = r.start(&target(0), &name, true, Some(14200)).await.err().expect("refused");
        assert_eq!(PortTaken::of(&e), Some(14200), "{e:#}");
        let e = r.start(&target(0), &name, true, None).await.err().expect("refused");
        assert_eq!(PortTaken::of(&e), None, "without a port, it is another failure");
    }

    #[tokio::test]
    async fn an_idle_worker_is_stopped_but_never_one_in_use_or_kept_warm() {
        let (addr, _) = worker_answering().await;
        let docker = Arc::new(FakeDocker::new());
        docker.answer(&["port"], &format!("{addr}\n"));
        let clock = FakeClock::new();
        let r = runner(docker.clone(), clock.clone(), &scratch());

        let name = r.name_of(&target(0));
        let removals = || docker.calls_to("rm").iter().filter(|c| c.contains(&name)).count();
        let held = r.endpoint(&target(0), Patience::Brief).await.unwrap();
        assert_eq!(removals(), 1, "a start clears any leftover of the same name first");
        clock.advance(Duration::from_secs(600));
        r.sweep_idle().await.unwrap();
        assert_eq!(removals(), 1, "a worker a caller holds stays up");

        drop(held);
        clock.advance(Duration::from_secs(299));
        r.sweep_idle().await.unwrap();
        assert_eq!(removals(), 1, "inside the idle window");
        clock.advance(Duration::from_secs(2));
        DRIVING.store(1, Ordering::SeqCst);
        r.sweep_idle().await.unwrap();
        assert_eq!(removals(), 1, "a run nobody holds a call for still goes on");
        DRIVING.store(0, Ordering::SeqCst);
        clock.advance(Duration::from_secs(301));
        r.sweep_idle().await.unwrap();
        assert_eq!(removals(), 2, "past the idle window");

        let warm = runner(docker.clone(), clock.clone(), &scratch());
        let warm_name = warm.name_of(&target(1));
        let warm_removals = || docker.calls_to("rm").iter().filter(|c| c.contains(&warm_name)).count();
        warm.prepare(&target(1)).await.unwrap();
        let before = warm_removals();
        clock.advance(Duration::from_secs(3600));
        warm.sweep_idle().await.unwrap();
        assert_eq!(warm_removals(), before, "min_instances keeps it up");

        // The settings change: the warm worker is no longer kept up, and
        // stops once idle like any other.
        warm.prepare(&target(0)).await.unwrap();
        warm.sweep_idle().await.unwrap();
        assert_eq!(warm_removals(), before + 1, "a worker started with other settings goes once idle");
    }

    #[tokio::test]
    async fn a_worker_a_previous_life_left_running_is_told_to_stop_and_removed_once_it_exited() {
        let docker = Arc::new(FakeDocker::new());
        let r = runner(docker.clone(), FakeClock::new(), &scratch());
        docker.answer(
            &["ps"],
            &format!(
                "{{\"Names\":\"weft-w-old\",\"State\":\"running\",\"Labels\":\"weft.role=worker,weft.life=0000\"}}\n\
                 {{\"Names\":\"weft-w-done\",\"State\":\"exited\",\"Labels\":\"weft.role=worker,weft.life=0000\"}}\n\
                 {{\"Names\":\"weft-w-mine\",\"State\":\"running\",\"Labels\":\"weft.role=worker,weft.life={}\"}}\n",
                r.life
            ),
        );
        r.sweep_idle().await.unwrap();
        r.sweep_idle().await.unwrap();
        let told: Vec<Vec<String>> = docker.calls_to("kill");
        assert_eq!(told.len(), 2, "told on every sweep until it exits");
        assert!(told[0].ends_with(&["--signal".to_string(), "TERM".into(), "weft-w-old".into()]), "{told:?}");
        assert!(!told.iter().flatten().any(|a| a == "weft-w-mine"), "one of this life's, even one still starting, is never told");
        let removed = docker.calls_to("rm");
        assert!(removed.iter().any(|c| c.contains(&"weft-w-done".to_string())), "an exited one is removed");
        assert!(!removed.iter().any(|c| c.contains(&"weft-w-old".to_string())), "a running one is never forced");
    }

    /// A worker's CPU, memory and environment are fixed when it starts, so
    /// other settings name another worker.
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
        let dir = scratch();
        let mut cfg = runner(Arc::new(FakeDocker::new()), FakeClock::new(), &dir).cfg;
        assert!(!worker_env(&target(0), &cfg, &weft_core::caller_token::ProjectSecret::of(b"s", uuid::Uuid::nil()), "t").contains_key(weft_core::time_scale::TIME_SCALE_ENV), "real time says nothing");
        cfg.time_scale = 0.05;
        assert_eq!(worker_env(&target(0), &cfg, &weft_core::caller_token::ProjectSecret::of(b"s", uuid::Uuid::nil()), "t")[weft_core::time_scale::TIME_SCALE_ENV], "0.05");
    }

    /// A worker runs the program's own code, so it is given its project's
    /// secret and never the install's.
    #[test]
    fn a_worker_holds_its_projects_secret_and_never_the_installs() {
        let dir = scratch();
        let cfg = runner(Arc::new(FakeDocker::new()), FakeClock::new(), &dir).cfg;
        let secret = weft_core::caller_token::ProjectSecret::of(&cfg.install_secret, target(0).project);
        let env = worker_env(&target(0), &cfg, &secret, "t");
        assert_eq!(env["WEFT_PROJECT_SECRET"], secret.to_hex());
        assert!(!env.contains_key("WEFT_CALLER_TOKEN_SECRET"));
        let install = hex::encode(&cfg.install_secret);
        assert!(env.values().all(|v| !v.contains(&install)), "{env:?}");
    }

    #[test]
    fn the_workers_own_published_port_is_its_url() {
        assert_eq!(published_url("0.0.0.0:1\n127.0.0.1:49153\n", None).as_deref(), Some("http://127.0.0.1:49153"));
        let project = Some("127.0.0.1:14200".parse().unwrap());
        assert_eq!(published_url("127.0.0.1:14200\n127.0.0.1:49153\n", project).as_deref(), Some("http://127.0.0.1:49153"), "never the project's port");
    }

    /// Forgetting an image takes every worker running the program from it,
    /// and leaves anything else.
    #[tokio::test]
    async fn forgetting_an_image_removes_the_workers_on_it() {
        let docker = Arc::new(FakeDocker::new());
        docker.answer(
            &["ps"],
            concat!(
                r#"{"Names":"weft-w-1","State":"exited","Labels":"weft.role=worker,weft.image=weft-worker:old"}"#, "\n",
                r#"{"Names":"weft-other","State":"running","Labels":"weft.image=weft-worker:old"}"#, "\n",
            ),
        );
        let r = runner(docker.clone(), FakeClock::new(), &scratch());
        r.forget_image("weft-worker:old").await.unwrap();
        let removed: Vec<Vec<String>> = docker.calls_to("rm");
        assert!(removed.iter().any(|c| c.contains(&"weft-w-1".to_string())));
        assert!(!removed.iter().any(|c| c.contains(&"weft-other".to_string())));
    }
}
