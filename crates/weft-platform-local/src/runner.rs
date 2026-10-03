//! Workers on the local Docker daemon.
//!
//! One container per project and image, started the first time weft calls
//! it and stopped once it has had nothing to do for the idle window
//! (`workerIdleStopSeconds`), unless the project keeps copies warm
//! (`workers.min_instances`). A long run is a container of its own running
//! `--run <execution_id>`, removed when it exits.
//!
//! Only this process calls a local worker: its port is published on
//! loopback, and it answers weft's own endpoints only to the key the
//! runner handed it at start (`WEFT_WORKER_DOOR`). The worker calls the
//! broker with a token the install signed for exactly its project.
//!
//! One copy per project and image serves every execution: a laptop has
//! one machine, so `max_instances` changes nothing here, and
//! `concurrency` is the worker's own business.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use sha2::{Digest, Sha256};
use weft_platform_traits::identity::Principal;
use weft_platform_traits::{Clock, Patience, Runner, WorkerEndpoint, WorkerTarget, WORKER_AUTH_HEADER};

use crate::docker::{self, labels, roles, Docker};
use crate::identity::LocalIdentity;

/// The port a worker serves on inside its container.
const WORKER_PORT: u16 = 8080;

/// How long a started worker gets to answer its health check. The
/// worker's own start is weft's code (the user's nodes run later, per
/// execution), so a worker that does not answer by then is broken.
const START_DEADLINE: Duration = Duration::from_secs(120);

/// A local worker's broker token never expires on its own: the container
/// is the credential's life, and rotating `WEFT_IDENTITY_KEY` revokes
/// every one at once.
const WORKER_TOKEN_LIFE_SECS: i64 = 100 * 365 * 24 * 3600;

/// What the runner needs besides Docker.
#[derive(Clone)]
pub struct LocalRunnerConfig {
    /// The broker's address as a container reaches it.
    pub broker_url: String,
    /// The secret live-caller tickets are signed with (hex).
    pub caller_token_secret: String,
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
}

/// A worker container this process started.
struct Running {
    name: String,
    base_url: String,
    key: String,
    /// Calls handed out and not yet finished.
    holds: AtomicUsize,
    last_used: parking_lot::Mutex<Instant>,
    /// Kept up however idle (the project's `min_instances` is above 0).
    keep_warm: bool,
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
        }
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
            .header(WORKER_AUTH_HEADER, format!("Bearer {key}"))
            .send()
            .await
            .is_ok_and(|r| r.status().is_success())
    }

    /// Start the worker for `target` and wait until it answers.
    async fn start(&self, target: &WorkerTarget, name: &str, keep_warm: bool) -> anyhow::Result<Arc<Running>> {
        docker::ensure_network(self.docker.as_ref()).await?;
        docker::remove_containers(self.docker.as_ref(), &[name.to_string()]).await?;
        let key = random_hex(32);
        let token = self.identity.mint(
            Principal::Worker { tenant: target.tenant.clone(), project: target.project },
            now_unix() + WORKER_TOKEN_LIFE_SECS,
        );
        let env = worker_env(target, &self.cfg, &format!("key:{key}"), &token);
        let env_file = self.write_env_file(name, &env)?;
        let args = run_args(&self.cfg.install, target, name, WorkerRun::Serve, &env_file);
        let started = docker::run(self.docker.as_ref(), args).await;
        let _ = std::fs::remove_file(&env_file);
        started.map_err(|e| e.context(format!("start the worker of project {}", target.project)))?;

        let port = docker::run(self.docker.as_ref(), vec!["port".into(), name.into(), format!("{WORKER_PORT}/tcp")]).await?;
        let base_url = published_url(&port)
            .ok_or_else(|| anyhow::anyhow!("`docker port {name}` printed no loopback address: {port:?}"))?;

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
            keep_warm,
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

    /// The worker for `target`, started when it is not running.
    async fn ensure(&self, target: &WorkerTarget) -> anyhow::Result<Arc<Running>> {
        let name = worker_name(target.project, &target.image);
        let gate = self.starting.lock().await.entry(name.clone()).or_default().clone();
        let _one_start = gate.lock().await;
        let known = self.running.lock().get(&name).cloned();
        if let Some(running) = known {
            if self.healthy(&running.base_url, &running.key).await {
                return Ok(running);
            }
            tracing::warn!(target: "weft_platform_local::runner", worker = %name, "a worker stopped answering; starting it again");
            self.running.lock().remove(&name);
        }
        let running = self.start(target, &name, target.settings.min_instances > 0).await?;
        self.running.lock().insert(name, running.clone());
        Ok(running)
    }

    /// Stop every worker that has had nothing to do for the idle window,
    /// and every worker container a previous life of this process left
    /// behind. Called on an interval by the process keeper.
    pub async fn sweep_idle(&self) -> anyhow::Result<()> {
        let now = self.clock.now();
        let idle: Vec<String> = {
            let mut running = self.running.lock();
            let idle: Vec<String> = running
                .values()
                .filter(|r| {
                    !r.keep_warm
                        && r.holds.load(Ordering::SeqCst) == 0
                        && now.duration_since(*r.last_used.lock()) >= self.cfg.idle_stop
                })
                .map(|r| r.name.clone())
                .collect();
            for name in &idle {
                running.remove(name);
            }
            idle
        };
        let known: std::collections::HashSet<String> = self.running.lock().keys().cloned().collect();
        let strays: Vec<String> = docker::containers(self.docker.as_ref(), self.cfg.install.label_value(), &[(labels::ROLE, roles::WORKER)])
            .await?
            .into_iter()
            .map(|c| c.name)
            .filter(|n| !known.contains(n) && !idle.contains(n))
            .collect();
        let mut gone = idle;
        gone.extend(strays);
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

    async fn start_long(&self, target: &WorkerTarget, execution_id: uuid::Uuid, _patience: Patience) -> anyhow::Result<()> {
        docker::ensure_network(self.docker.as_ref()).await?;
        let name = format!("weft-long-{}", execution_id.simple());
        let token = self.identity.mint(
            Principal::Worker { tenant: target.tenant.clone(), project: target.project },
            now_unix() + WORKER_TOKEN_LIFE_SECS,
        );
        // A long run serves no calls, so its door key opens nothing anyone
        // holds.
        let env = worker_env(target, &self.cfg, &format!("key:{}", random_hex(32)), &token);
        let env_file = self.write_env_file(&name, &env)?;
        let started = docker::run(self.docker.as_ref(), run_args(&self.cfg.install, target, &name, WorkerRun::Long(execution_id), &env_file)).await;
        let _ = std::fs::remove_file(&env_file);
        started.map(|_| ()).map_err(|e| e.context(format!("start the long run {execution_id}")))
    }

    async fn retire(&self, _tenant: &str, project: uuid::Uuid) -> anyhow::Result<()> {
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

    fn short_run_cap(&self) -> Option<Duration> {
        None
    }
}

/// Whether a container runs a project's program (a worker or a long
/// run): what retiring a project or forgetting an image removes.
fn is_program_container(c: &docker::ContainerRow) -> bool {
    matches!(c.label(labels::ROLE), Some(roles::WORKER | roles::LONG))
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

/// The container name of `project`'s worker on `image`: the same for the
/// same pair, so a restarted process finds (and replaces) its own. A
/// project id belongs to one install, so the name is that install's.
pub fn worker_name(project: uuid::Uuid, image: &str) -> String {
    let digest = Sha256::digest(image.as_bytes());
    let hex: String = digest.iter().take(6).map(|b| format!("{b:02x}")).collect();
    format!("{}{hex}", worker_name_prefix(project))
}

/// What a worker container runs.
enum WorkerRun {
    /// Serve HTTP (a short run per call, live callers).
    Serve,
    /// One execution as a job of its own.
    Long(uuid::Uuid),
}

/// The worker's environment.
// SYNC: the worker's environment <-> crates/weft-compiler/src/codegen.rs (write_main_rs Args),
//       crates/weft-engine/src/worker.rs (WorkerDoor::from_env, identity_from_env)
fn worker_env(target: &WorkerTarget, cfg: &LocalRunnerConfig, door: &str, token: &str) -> BTreeMap<&'static str, String> {
    let mut env = BTreeMap::new();
    env.insert("WEFT_PROJECT_ID", target.project.to_string());
    env.insert("WEFT_TENANT_ID", target.tenant.clone());
    env.insert("WEFT_BROKER_URL", cfg.broker_url.clone());
    env.insert("PORT", WORKER_PORT.to_string());
    env.insert("WEFT_WORKER_DOOR", door.to_string());
    env.insert("WEFT_WORKER_IDENTITY", format!("token:{token}"));
    env.insert("WEFT_CALLER_TOKEN_SECRET", cfg.caller_token_secret.clone());
    if cfg.time_scale != 1.0 {
        env.insert(weft_core::time_scale::TIME_SCALE_ENV, cfg.time_scale.to_string());
    }
    env
}

/// `docker run` for a worker container.
fn run_args(install: &weft_core::infra::Install, target: &WorkerTarget, name: &str, run: WorkerRun, env_file: &std::path::Path) -> Vec<String> {
    let mut l = BTreeMap::new();
    l.insert(labels::INSTALL, install.label_value().to_string());
    l.insert(labels::PROJECT, target.project.to_string());
    l.insert(labels::TENANT, target.tenant.clone());
    l.insert(labels::IMAGE, target.image.clone());
    l.insert(labels::ROLE, match run { WorkerRun::Serve => roles::WORKER, WorkerRun::Long(_) => roles::LONG }.to_string());
    let mut args = vec!["run".to_string(), "--detach".into(), "--name".into(), name.into()];
    if matches!(run, WorkerRun::Long(_)) {
        args.push("--rm".into());
    }
    args.extend([
        "--network".into(),
        docker::NETWORK.into(),
        // Where a container reaches the machine's internal port.
        "--add-host".into(),
        "host.docker.internal:host-gateway".into(),
        "--env-file".into(),
        env_file.display().to_string(),
        "--cpus".into(),
        docker_cpus(&target.settings.cpu),
        "--memory".into(),
        docker_memory(&target.settings.memory),
    ]);
    if matches!(run, WorkerRun::Serve) {
        args.extend(["--publish".into(), format!("127.0.0.1::{WORKER_PORT}")]);
    }
    args.extend(docker::label_args(&l));
    args.push(target.image.clone());
    if let WorkerRun::Long(execution_id) = run {
        args.extend(["--run".into(), execution_id.to_string()]);
    }
    args
}

/// A CPU count as `docker --cpus` takes it: `"1"`, `"0.5"`, or a
/// millicore count (`"500m"`) turned into a fraction.
pub fn docker_cpus(cpu: &str) -> String {
    match cpu.trim().strip_suffix('m').and_then(|m| m.parse::<f64>().ok()) {
        Some(milli) => format!("{}", milli / 1000.0),
        None => cpu.trim().to_string(),
    }
}

/// A memory size as `docker --memory` takes it: `Gi` becomes `g`, `Mi`
/// becomes `m`, `Ki` becomes `k`.
pub fn docker_memory(memory: &str) -> String {
    let m = memory.trim();
    for (suffix, unit) in [("Gi", "g"), ("Mi", "m"), ("Ki", "k"), ("G", "g"), ("M", "m"), ("K", "k")] {
        if let Some(n) = m.strip_suffix(suffix) {
            return format!("{n}{unit}");
        }
    }
    m.to_string()
}

/// `http://127.0.0.1:<port>` from what `docker port` printed (one line
/// per binding, `127.0.0.1:49153`).
fn published_url(printed: &str) -> Option<String> {
    printed
        .lines()
        .map(str::trim)
        .find(|l| l.starts_with("127.0.0.1:"))
        .map(|addr| format!("http://{addr}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::docker::fake::FakeDocker;
    use weft_platform_traits::{FakeClock, WorkerSettings};

    fn target(min_instances: u32) -> WorkerTarget {
        WorkerTarget {
            tenant: "local".into(),
            project: uuid::Uuid::from_u128(7),
            image: "weft-worker:abc".into(),
            settings: WorkerSettings { min_instances, ..Default::default() },
        }
    }

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
                    axum::http::StatusCode::OK
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
                caller_token_secret: "cd".repeat(32),
                idle_stop: Duration::from_secs(300),
                scratch_dir: dir.to_path_buf(),
                install: weft_core::infra::Install::default_install(),
                time_scale: 1.0,
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
        let name = worker_name(uuid::Uuid::from_u128(7), "weft-worker:abc");
        assert!(run.windows(2).any(|w| w == ["--name", name.as_str()]));
        assert!(run.windows(2).any(|w| w == ["--publish", "127.0.0.1::8080"]));
        assert!(run.windows(2).any(|w| w == ["--memory", "1g"]));
        assert_eq!(run.last().unwrap(), "weft-worker:abc");
        assert!(!run.iter().any(|a| a.contains("WEFT_WORKER_IDENTITY")), "secrets stay off the command line");
        assert!(std::fs::read_dir(&dir).unwrap().next().is_none(), "the environment file is gone once started");
    }

    #[tokio::test]
    async fn an_idle_worker_is_stopped_but_never_one_in_use_or_kept_warm() {
        let (addr, _) = worker_answering().await;
        let docker = Arc::new(FakeDocker::new());
        docker.answer(&["port"], &format!("{addr}\n"));
        let clock = FakeClock::new();
        let r = runner(docker.clone(), clock.clone(), &scratch());

        let name = worker_name(uuid::Uuid::from_u128(7), "weft-worker:abc");
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
        r.sweep_idle().await.unwrap();
        assert_eq!(removals(), 2, "past the idle window");

        let warm = runner(docker.clone(), clock.clone(), &scratch());
        warm.prepare(&target(1)).await.unwrap();
        let before = removals();
        clock.advance(Duration::from_secs(3600));
        warm.sweep_idle().await.unwrap();
        assert_eq!(removals(), before, "min_instances keeps it up");
    }

    /// A worker runs at its install's clock factor, so its claim heartbeat
    /// keeps pace with the claim length the broker sets.
    #[test]
    fn a_worker_runs_at_its_installs_pace() {
        let dir = scratch();
        let mut cfg = runner(Arc::new(FakeDocker::new()), FakeClock::new(), &dir).cfg;
        assert!(!worker_env(&target(0), &cfg, "key:k", "t").contains_key(weft_core::time_scale::TIME_SCALE_ENV), "real time says nothing");
        cfg.time_scale = 0.05;
        assert_eq!(worker_env(&target(0), &cfg, "key:k", "t")[weft_core::time_scale::TIME_SCALE_ENV], "0.05");
    }

    #[test]
    fn sizes_are_spelled_as_docker_takes_them() {
        assert_eq!(docker_memory("512Mi"), "512m");
        assert_eq!(docker_memory("2Gi"), "2g");
        assert_eq!(docker_cpus("500m"), "0.5");
        assert_eq!(docker_cpus("2"), "2");
        assert_eq!(published_url("0.0.0.0:1\n127.0.0.1:49153\n").as_deref(), Some("http://127.0.0.1:49153"));
    }

    #[test]
    fn a_long_run_names_its_execution_id_and_removes_itself() {
        let args = run_args(&weft_core::infra::Install::default_install(), &target(0), "weft-long-x", WorkerRun::Long(uuid::Uuid::from_u128(9)), std::path::Path::new("/e"));
        assert!(args.contains(&"--rm".to_string()));
        assert!(!args.contains(&"--publish".to_string()));
        assert_eq!(args[args.len() - 2..], ["--run".to_string(), uuid::Uuid::from_u128(9).to_string()]);
        assert!(args.windows(2).any(|w| w == ["--label", "weft.role=long"]));
    }

    /// Forgetting an image takes every container running the program from
    /// it, a long run's as well as a worker's, and leaves anything else.
    #[tokio::test]
    async fn forgetting_an_image_removes_the_workers_and_long_runs_on_it() {
        let docker = Arc::new(FakeDocker::new());
        docker.answer(
            &["ps"],
            concat!(
                r#"{"Names":"weft-w-1","State":"exited","Labels":"weft.role=worker,weft.image=weft-worker:old"}"#, "\n",
                r#"{"Names":"weft-long-1","State":"running","Labels":"weft.role=long,weft.image=weft-worker:old"}"#, "\n",
                r#"{"Names":"weft-other","State":"running","Labels":"weft.image=weft-worker:old"}"#, "\n",
            ),
        );
        let r = runner(docker.clone(), FakeClock::new(), &scratch());
        r.forget_image("weft-worker:old").await.unwrap();
        let removed: Vec<Vec<String>> = docker.calls_to("rm");
        assert!(removed.iter().any(|c| c.contains(&"weft-w-1".to_string())));
        assert!(removed.iter().any(|c| c.contains(&"weft-long-1".to_string())));
        assert!(!removed.iter().any(|c| c.contains(&"weft-other".to_string())));
    }
}
