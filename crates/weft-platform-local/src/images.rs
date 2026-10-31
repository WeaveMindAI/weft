//! Workers on this machine as programs, and infra images.
//!
//! A worker is no image here: it is a program the machine runs as a
//! process (`runner`), compiled on the machine from the host recipe the
//! compiler writes (`host_build`) into its folder in the machine's workers
//! folder (`layout::MachineLayout::workers`), shared by every install on
//! the machine, each holding it under a name of its own (`held-by/`). A
//! node package's test server is a program the same way ([`Program`]).
//! An infra image is built with `docker build` and kept in the Docker
//! daemon's own image store under the install's own names (no registry;
//! the infra host's containers use it straight from there).
//!
//! A build runs in this process. After a restart, a build it was running
//! is gone ([`BuildStatus::Gone`]), and the ledger lets the next caller
//! start it again.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use weft_core::builds::{BuildRecipe, WorkerPackaging, NODE_TEST_REPO, WORKER_REPO};
use weft_platform_traits::{BuildHandle, BuildRequest, BuildStatus, ImageBuilder, ImageDeleted};

use crate::docker::{self, Docker};
use crate::host_build::{self, HostBuilt, Toolchain};
use crate::layout::{InstallLayout, MachineLayout};

/// How many of a failed build's last output lines its failure keeps.
const FAILURE_LOG_LINES: usize = 60;

/// One of weft's own programs, which a local install runs as a process
/// where the cloud runs an image: what its ref names. A program's ref is
/// its bare tag (`weft-worker:<hash>`, `weft-node-tests:<hash>`), the same
/// for every install: each holds it through a file of its own in the
/// program's folder, never through a name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Program<'a> {
    /// A project's worker, by its binary hash.
    Worker(&'a str),
    /// A node package's test server (`weft_engine::test_runner`), by its
    /// test hash.
    NodeTestServer(&'a str),
}

impl<'a> Program<'a> {
    /// The program `image_ref` names; `None` for an image (an infra
    /// unit's) or a ref no program is spelled as.
    pub fn of(image_ref: &'a str) -> Option<Self> {
        let (repo, hash) = image_ref.split_once(':')?;
        // Never a path out of the folder.
        if hash.is_empty() || !hash.bytes().all(|b| b.is_ascii_alphanumeric()) {
            return None;
        }
        match repo {
            WORKER_REPO => Some(Self::Worker(hash)),
            NODE_TEST_REPO => Some(Self::NodeTestServer(hash)),
            _ => None,
        }
    }

    /// Its folder in the machine's workers folder `workers`
    /// (`MachineLayout::workers`): the program, the record of what its build
    /// left out, and the installs holding it.
    pub fn dir(&self, workers: &Path) -> PathBuf {
        match self {
            Self::Worker(hash) => workers.join(hash),
            Self::NodeTestServer(hash) => crate::layout::node_test_servers_in(workers).join(hash),
        }
    }

    /// The program file.
    pub fn file(&self, workers: &Path) -> PathBuf {
        self.dir(workers).join(host_build::PROGRAM_FILE)
    }

    /// What it is started with: a worker serves as it is; the test
    /// program's other subcommands are for running it by hand.
    // SYNC: the test server's start command <-> crates/weft-engine/src/test_runner.rs (Args::Serve)
    pub fn args(&self) -> &'static [&'static str] {
        match self {
            Self::Worker(_) => &[],
            Self::NodeTestServer(_) => &["serve"],
        }
    }
}

/// The binary hashes of the worker programs `install` (its label) holds
/// in the machine's workers folder `workers`.
pub fn held_workers(workers: &Path, install: &str) -> anyhow::Result<Vec<String>> {
    programs::held_workers(workers, install)
}

/// Remove the worker programs in the machine's workers folder `workers`
/// that nothing uses any more, answering their binary hashes: first every
/// hold of an install not in `installs` (the labels of the installs on the
/// machine) goes, then every worker no install holds and no install's
/// record names as its standard worker (the standard worker of a weft an
/// update replaced, a removed install's programs). A test server is never
/// one: the next build of its package replaces it.
pub fn reclaim_workers(workers: &Path, installs: &[String]) -> anyhow::Result<Vec<String>> {
    programs::reclaim_workers(workers, installs)
}

pub struct LocalImageBuilder {
    docker: Arc<dyn Docker>,
    toolchain: Arc<dyn Toolchain>,
    /// The install whose names the infra images go under
    /// (`Install::local_image_ref`), and whose label holds its programs.
    install: weft_core::infra::Install,
    /// The install's own folders: its build logs.
    install_dir: InstallLayout,
    /// The machine's folders: the worker programs, their compile caches
    /// and shared crates, which every install shares.
    machine: MachineLayout,
    /// How many compile lanes the machine has (`lanes::lane_count`).
    lanes: u32,
    builds: Arc<parking_lot::Mutex<HashMap<String, Build>>>,
}

enum Build {
    /// Its task, and for a program, the hold this install took on it
    /// before building: a build released while it runs lets go of it, since
    /// the task that would have is stopped.
    Running { task: tokio::task::AbortHandle, hold: Option<ProgramHold> },
    Ended(BuildStatus),
}

/// An install's hold on one program ([`programs::hold`]): what
/// [`programs::let_go`] needs to give it up.
#[derive(Clone)]
struct ProgramHold {
    workers: PathBuf,
    image_ref: String,
    label: String,
}

impl ProgramHold {
    /// Take the hold ([`programs::hold`]), on the blocking pool.
    async fn take(&self) -> anyhow::Result<()> {
        let hold = self.clone();
        blocking(move || programs::hold(&hold.workers, &hold.image_ref, &hold.label)).await
    }

    /// Give the hold up ([`programs::let_go`]), on the blocking pool.
    async fn let_go(self) -> anyhow::Result<ImageDeleted> {
        blocking(move || programs::let_go(&self.workers, &self.image_ref, &self.label)).await
    }
}

impl LocalImageBuilder {
    pub fn new(
        docker: Arc<dyn Docker>,
        toolchain: Arc<dyn Toolchain>,
        install: weft_core::infra::Install,
        install_dir: InstallLayout,
        machine: MachineLayout,
        lanes: u32,
    ) -> Self {
        Self { docker, toolchain, install, install_dir, machine, lanes, builds: Arc::default() }
    }

    /// This install's hold on the program `image_ref` names.
    fn hold_of(&self, image_ref: &str) -> ProgramHold {
        ProgramHold { workers: self.machine.workers(), image_ref: image_ref.to_string(), label: self.install.label_value().to_string() }
    }

    /// Build the program `req` names from the host recipe in its context,
    /// in a free lane of the machine (waiting for one), or take the one
    /// another install already built.
    async fn start_host(&self, req: BuildRequest) -> anyhow::Result<BuildHandle> {
        let name = req.name.clone();
        let hold = self.hold_of(&req.image_ref);
        let out_dir = Program::of(&hold.image_ref).expect("start routes only a program here").dir(&hold.workers);
        // This install's hold on the program, taken before anything is
        // built, so another install letting go of its own meanwhile never
        // removes the folder under this build.
        hold.take().await?;
        if out_dir.join(host_build::PROGRAM_FILE).is_file() {
            let status = match tokio::fs::read(out_dir.join(host_build::LEFT_OUT_FILE)).await {
                Ok(record) => BuildStatus::Succeeded { result: Some(record) },
                Err(e) => BuildStatus::Failed {
                    reason: format!("the program in {} has no record of what its build left out ({e}); remove the folder to build it again", out_dir.display()),
                },
            };
            self.builds.lock().insert(name.clone(), Build::Ended(status));
            return Ok(BuildHandle::named(name));
        }
        let log = self.install_dir.build_log(&req.image_ref);
        let (toolchain, machine, lanes, builds, task_name, task_log) =
            (self.toolchain.clone(), self.machine.clone(), self.lanes, self.builds.clone(), name.clone(), log.clone());
        // The task's own copy, for a build that fails; the table keeps the
        // other, for a build released while it runs.
        let task_hold = hold.clone();
        let mut table = self.builds.lock();
        let task = tokio::spawn(async move {
            let built = host_build::compile(
                toolchain.as_ref(),
                &machine,
                host_build::CompileIn::Lane { lanes },
                &req.context_dir,
                out_dir,
                &task_log,
                || tracing::info!(target: "weft_platform_local::images", "every compile lane of this machine is busy; a build waits for one"),
            )
            .await;
            // The context is read until the build ends, and with it goes
            // the build's claim on the shared crates, which the reclaim
            // waits for.
            drop(req.staging);
            if matches!(built, Ok(HostBuilt::Built(_))) {
                let reclaim = machine.clone();
                match tokio::task::spawn_blocking(move || host_build::reclaim_shared_crates(&reclaim)).await {
                    Ok(Ok(_)) => {}
                    Ok(Err(e)) => tracing::warn!(target: "weft_platform_local::images", error = %format!("{e:#}"), "could not remove the shared crates no program uses; the build is unaffected"),
                    Err(e) => tracing::warn!(target: "weft_platform_local::images", error = %e, "the shared crates reclaim panicked; the build is unaffected"),
                }
            }
            let status = match built {
                Ok(HostBuilt::Built(record)) => BuildStatus::Succeeded { result: Some(record) },
                Ok(HostBuilt::Failed(why)) => {
                    let _ = task_hold.let_go().await;
                    BuildStatus::Failed {
                        reason: match tokio::fs::read_to_string(&task_log).await {
                            Ok(text) => format!("{}\n{why}", tail(&text, FAILURE_LOG_LINES)),
                            Err(e) => format!("{why}, and its log {} could not be read: {e}", task_log.display()),
                        },
                    }
                }
                Err(e) => {
                    let _ = task_hold.let_go().await;
                    BuildStatus::Failed { reason: format!("{e:#}") }
                }
            };
            if let Some(entry) = builds.lock().get_mut(&task_name) {
                *entry = Build::Ended(status);
            }
        });
        table.insert(name.clone(), Build::Running { task: task.abort_handle(), hold: Some(hold) });
        Ok(BuildHandle { external_build_id: name, log_url: Some(log.display().to_string()) })
    }

    /// `docker build` of the infra image whose Dockerfile is in `req`'s
    /// context, or the image another install on this daemon already holds
    /// under the same tag.
    async fn start_docker(&self, req: BuildRequest) -> anyhow::Result<BuildHandle> {
        let name = req.name.clone();
        let image_ref = req.image_ref.clone();
        let docker = self.docker.clone();
        let builds = self.builds.clone();
        let task_name = name.clone();
        // An image another install on this daemon holds is taken as it is,
        // with nothing built, and its staging let go of at once.
        if adopt(docker.as_ref(), &image_ref).await? {
            self.builds.lock().insert(name.clone(), Build::Ended(BuildStatus::Succeeded { result: None }));
            return Ok(BuildHandle::named(name));
        }
        let log = self.install_dir.build_log(&image_ref);
        if let Some(dir) = log.parent() {
            tokio::fs::create_dir_all(dir).await.map_err(|e| anyhow::anyhow!("create {}: {e}", dir.display()))?;
        }
        let args = build_args(&req);
        // `docker build` reads the context while it runs, after the
        // request that staged it answered: the build task keeps the
        // staging until docker is done with it, and an abort (`release`)
        // drops it with the task; either way the directory goes on a
        // blocking thread (`Staging`).
        let staging = req.staging;
        // Held until the build is registered, so a fast build never
        // records its end before its start.
        let mut table = self.builds.lock();
        let task_log = log.clone();
        let task = tokio::spawn(async move {
            // The whole output is the build's log (`BuildHandle::log_url`),
            // written as docker prints it, so it can be followed while the
            // build runs.
            let code = docker.exec_logged(&args, &task_log).await;
            drop(staging);
            let status = match code {
                Ok(0) => BuildStatus::Succeeded { result: None },
                Ok(_) => BuildStatus::Failed {
                    reason: match tokio::fs::read_to_string(&task_log).await {
                        Ok(text) => tail(&text, FAILURE_LOG_LINES),
                        Err(e) => format!("docker build failed, and its log {} could not be read: {e}", task_log.display()),
                    },
                },
                Err(e) => BuildStatus::Failed { reason: format!("{e:#}") },
            };
            if let Some(entry) = builds.lock().get_mut(&task_name) {
                *entry = Build::Ended(status);
            }
        });
        table.insert(name.clone(), Build::Running { task: task.abort_handle(), hold: None });
        Ok(BuildHandle { external_build_id: name, log_url: Some(log.display().to_string()) })
    }
}

/// Run the blocking `f` (a file lock, a folder walk) off the async runtime.
async fn blocking<T: Send + 'static>(f: impl FnOnce() -> anyhow::Result<T> + Send + 'static) -> anyhow::Result<T> {
    tokio::task::spawn_blocking(f).await.map_err(|e| anyhow::anyhow!("a blocking step of a build stopped: {e}"))?
}

/// Which installs hold each program ([`Program`], by its ref): a file per
/// install in the program's `held-by/` folder, named by the install's
/// label. A program goes once no install holds it and, for a worker, no
/// install's image record names it as its standard worker. Every change
/// happens under one lock on the workers folder, so a build taking its
/// hold and a removal letting go never cross.
mod programs {
    use std::path::{Path, PathBuf};

    use weft_platform_traits::ImageDeleted;

    use super::Program;

    const HELD_BY: &str = "held-by";
    const LOCK: &str = ".lock";

    fn turn(workers: &Path) -> anyhow::Result<std::fs::File> {
        let path = workers.join(LOCK);
        weft_core::file_lock::hold(&path, || {
            tracing::info!(target: "weft_platform_local::images", "waiting for another install to finish with the machine's worker programs")
        })
        .map_err(|e| anyhow::anyhow!("lock {}: {e}", path.display()))
    }

    fn dir_of(workers: &Path, image_ref: &str) -> anyhow::Result<PathBuf> {
        Ok(Program::of(image_ref).ok_or_else(|| anyhow::anyhow!("{image_ref} names no program of weft's"))?.dir(workers))
    }

    /// `install` holds the program `image_ref` names from now on (its
    /// folder made if missing).
    pub fn hold(workers: &Path, image_ref: &str, install: &str) -> anyhow::Result<()> {
        let _turn = turn(workers)?;
        let held = dir_of(workers, image_ref)?.join(HELD_BY);
        std::fs::create_dir_all(&held).map_err(|e| anyhow::anyhow!("create {}: {e}", held.display()))?;
        std::fs::write(held.join(install), b"").map_err(|e| anyhow::anyhow!("write {}: {e}", held.join(install).display()))
    }

    /// Whether `install` holds the program `program`.
    pub fn holds(workers: &Path, program: Program<'_>, install: &str) -> bool {
        program.dir(workers).join(HELD_BY).join(install).is_file()
    }

    /// The binary hashes of the worker programs `install` holds.
    pub fn held_workers(workers: &Path, install: &str) -> anyhow::Result<Vec<String>> {
        let entries = match std::fs::read_dir(workers) {
            Ok(entries) => entries,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => anyhow::bail!("read {}: {e}", workers.display()),
        };
        let mut held = Vec::new();
        for entry in entries {
            let name = entry.map_err(|e| anyhow::anyhow!("read {}: {e}", workers.display()))?.file_name().to_string_lossy().to_string();
            if let Some(Program::Worker(hash)) = Program::of(&format!("{}:{name}", super::WORKER_REPO)) {
                if holds(workers, Program::Worker(hash), install) {
                    held.push(hash.to_string());
                }
            }
        }
        held.sort();
        Ok(held)
    }

    /// `install` no longer holds the program `image_ref` names: its
    /// folder goes when nobody else holds it and, for a worker, no install
    /// on the machine (`machine_dir`, and each folder of `installs/`)
    /// names it as its standard worker.
    pub fn let_go(workers: &Path, image_ref: &str, install: &str) -> anyhow::Result<ImageDeleted> {
        let machine_dir = workers.parent().expect("the workers folder is in weft's data folder");
        let program = Program::of(image_ref).ok_or_else(|| anyhow::anyhow!("{image_ref} names no program of weft's"))?;
        let _turn = turn(workers)?;
        let dir = program.dir(workers);
        if !dir.exists() {
            return Ok(ImageDeleted::Absent);
        }
        let mine = dir.join(HELD_BY).join(install);
        if let Err(e) = std::fs::remove_file(&mine) {
            if e.kind() != std::io::ErrorKind::NotFound {
                anyhow::bail!("remove {}: {e}", mine.display());
            }
        }
        let others = match std::fs::read_dir(dir.join(HELD_BY)) {
            Ok(entries) => entries.count(),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => 0,
            Err(e) => anyhow::bail!("read {}: {e}", dir.join(HELD_BY).display()),
        };
        let standard = match program {
            Program::Worker(hash) => a_standard_worker(machine_dir, hash)?,
            Program::NodeTestServer(_) => false,
        };
        if others == 0 && !standard {
            std::fs::remove_dir_all(&dir).map_err(|e| anyhow::anyhow!("remove {}: {e}", dir.display()))?;
        }
        Ok(ImageDeleted::Deleted)
    }

    /// [`super::reclaim_workers`].
    pub fn reclaim_workers(workers: &Path, installs: &[String]) -> anyhow::Result<Vec<String>> {
        let machine_dir = workers.parent().expect("the workers folder is in weft's data folder");
        if !workers.is_dir() {
            return Ok(Vec::new());
        }
        let _turn = turn(workers)?;
        let entries = std::fs::read_dir(workers).map_err(|e| anyhow::anyhow!("read {}: {e}", workers.display()))?;
        let mut removed = Vec::new();
        for entry in entries {
            let name = entry.map_err(|e| anyhow::anyhow!("read {}: {e}", workers.display()))?.file_name().to_string_lossy().to_string();
            let image_ref = format!("{}:{name}", super::WORKER_REPO);
            let Some(Program::Worker(hash)) = Program::of(&image_ref) else { continue };
            let dir = Program::Worker(hash).dir(workers);
            let held = dir.join(HELD_BY);
            let mut holders = 0;
            match std::fs::read_dir(&held) {
                Ok(entries) => {
                    for holder in entries {
                        let holder = holder.map_err(|e| anyhow::anyhow!("read {}: {e}", held.display()))?;
                        if installs.iter().any(|install| holder.file_name().to_str() == Some(install)) {
                            holders += 1;
                        } else {
                            std::fs::remove_file(holder.path()).map_err(|e| anyhow::anyhow!("remove {}: {e}", holder.path().display()))?;
                        }
                    }
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => anyhow::bail!("read {}: {e}", held.display()),
            }
            if holders == 0 && !a_standard_worker(machine_dir, hash)? {
                std::fs::remove_dir_all(&dir).map_err(|e| anyhow::anyhow!("remove {}: {e}", dir.display()))?;
                removed.push(hash.to_string());
            }
        }
        removed.sort();
        Ok(removed)
    }

    /// Whether any install on the machine names the program `hash` as its
    /// standard worker (`ImageRefs::standard_worker`).
    fn a_standard_worker(machine_dir: &Path, hash: &str) -> anyhow::Result<bool> {
        let tag = format!("{}:{hash}", super::WORKER_REPO);
        let mut dirs = vec![machine_dir.to_path_buf()];
        match std::fs::read_dir(machine_dir.join("installs")) {
            Ok(entries) => dirs.extend(entries.filter_map(Result::ok).map(|e| e.path())),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => anyhow::bail!("read {}: {e}", machine_dir.join("installs").display()),
        }
        for dir in dirs {
            if let Some(record) = weft_core::images::ImageRefs::load(&dir)? {
                if record.standard_worker.rsplit('/').next() == Some(tag.as_str()) {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }
}

/// Give `image_ref` to an image another install already holds under the
/// same content-addressed tag (`Install::tag_of_local_image`), so a
/// content one install built is never built again by the next. `false`
/// when no install holds it, or the one that did removed its name before
/// the tag landed: the caller builds it then.
async fn adopt(docker: &dyn Docker, image_ref: &str) -> anyhow::Result<bool> {
    let Some(tag) = weft_core::infra::Install::tag_of_local_image(image_ref) else {
        return Ok(false);
    };
    let listing = docker::run(
        docker,
        vec![
            "image".into(),
            "ls".into(),
            "--format".into(),
            "{{.Repository}}:{{.Tag}}".into(),
            "--filter".into(),
            format!("reference={tag}"),
            "--filter".into(),
            format!("reference=localhost/*/{tag}"),
        ],
    )
    .await?;
    for held in listing.lines().map(str::trim) {
        if held == image_ref || weft_core::infra::Install::tag_of_local_image(held) != Some(tag) {
            continue;
        }
        if docker.exec(&["tag".into(), held.into(), image_ref.into()]).await?.success() {
            return Ok(true);
        }
    }
    Ok(false)
}

/// `docker build` for `req`.
fn build_args(req: &BuildRequest) -> Vec<String> {
    vec![
        "build".to_string(),
        "--tag".into(),
        req.image_ref.clone(),
        "--file".into(),
        req.context_dir.join("Dockerfile").display().to_string(),
        req.context_dir.display().to_string(),
    ]
}

fn tail(text: &str, lines: usize) -> String {
    let all: Vec<&str> = text.lines().collect();
    all[all.len().saturating_sub(lines)..].join("\n")
}

#[async_trait]
impl ImageBuilder for LocalImageBuilder {
    /// A program's bare tag ([`Program`]); for an infra image, the
    /// install's own name for it (`Install::local_image_ref`).
    fn image_ref(&self, tag: &str) -> String {
        match Program::of(tag) {
            Some(_) => tag.to_string(),
            None => self.install.local_image_ref(tag),
        }
    }

    /// Workers here are programs compiled on the machine, sharing the
    /// machine's crates.
    fn worker_packaging(&self) -> WorkerPackaging {
        self.machine.host_packaging()
    }

    /// A worker is compiled on the machine (`host_build`), an infra image
    /// built with `docker build`; either is taken as it is when another
    /// install already made it.
    async fn start(&self, req: BuildRequest) -> anyhow::Result<BuildHandle> {
        match (&req.recipe, Program::of(&req.image_ref).is_some()) {
            (BuildRecipe::Host, true) => self.start_host(req).await,
            (BuildRecipe::Dockerfile { result_file: None }, false) => self.start_docker(req).await,
            (BuildRecipe::Host, false) => anyhow::bail!(
                "build {} asks for a host build of {}, which is none of weft's programs: only a worker or a node test server is compiled on the machine",
                req.name,
                req.image_ref
            ),
            (BuildRecipe::Dockerfile { .. }, true) => anyhow::bail!(
                "build {} asks for an image of the program {}, but a local install runs weft's programs as processes: \
                 the build must be planned as a host build (this builder's worker packaging is `host`)",
                req.name,
                req.image_ref
            ),
            (BuildRecipe::Dockerfile { result_file: Some(file) }, false) => anyhow::bail!(
                "build {} asks to read {file} out of the image {}, but a local install builds only infra images, which report nothing",
                req.name,
                req.image_ref
            ),
        }
    }

    async fn poll(&self, handle: &BuildHandle) -> anyhow::Result<BuildStatus> {
        Ok(match self.builds.lock().get(&handle.external_build_id) {
            Some(Build::Running { .. }) => BuildStatus::Pending,
            Some(Build::Ended(status)) => status.clone(),
            None => BuildStatus::Gone,
        })
    }

    /// A program is there when its file is and this install holds it
    /// (one only another install holds is taken by a build, at no cost).
    async fn image_exists(&self, image_ref: &str) -> anyhow::Result<bool> {
        if let Some(program) = Program::of(image_ref) {
            let workers = self.machine.workers();
            return Ok(program.file(&workers).is_file() && programs::holds(&workers, program, self.install.label_value()));
        }
        Ok(self.docker.exec(&["image".into(), "inspect".into(), image_ref.into()]).await?.success())
    }

    /// A build still running is stopped, and a program's lets go of the
    /// hold it took (the program folder goes with the last hold, a part
    /// copied in with it). The dispatcher joins same-image builds of one
    /// install into one build, so no other build of this install relies on
    /// that hold.
    async fn release(&self, handle: &BuildHandle) {
        let removed = self.builds.lock().remove(&handle.external_build_id);
        if let Some(Build::Running { task, hold }) = removed {
            task.abort();
            if let Some(hold) = hold {
                let image_ref = hold.image_ref.clone();
                if let Err(e) = hold.let_go().await {
                    tracing::warn!(target: "weft_platform_local::images", image = %image_ref, error = %format!("{e:#}"), "a released build could not let go of its program, so its folder stays in the machine's workers folder");
                }
            }
        }
    }

    /// Removes this install's name for the image: its hold on a program
    /// (the program goes with the last one, `programs::let_go`),
    /// or its Docker name on an infra image (`docker image rm` of that
    /// name: Docker deletes the image itself only with its last name, so
    /// an image another install still holds under its own name stays for
    /// that install).
    async fn delete_image(&self, image_ref: &str) -> anyhow::Result<ImageDeleted> {
        let deleted = match Program::of(image_ref) {
            Some(_) => self.hold_of(image_ref).let_go().await?,
            None => self.delete_docker_image(image_ref).await?,
        };
        if deleted != ImageDeleted::InUse {
            // Its log goes with the install's name for it (`build_log`).
            let log = self.install_dir.build_log(image_ref);
            if let Err(e) = tokio::fs::remove_file(&log).await {
                if e.kind() != std::io::ErrorKind::NotFound {
                    tracing::warn!(target: "weft_platform_local::images", log = %log.display(), error = %e, "could not remove a deleted image's build log");
                }
            }
        }
        Ok(deleted)
    }
}

impl LocalImageBuilder {
    async fn delete_docker_image(&self, image_ref: &str) -> anyhow::Result<ImageDeleted> {
        let args = vec!["image".to_string(), "rm".into(), image_ref.into()];
        let out = self.docker.exec(&args).await?;
        if out.success() {
            Ok(ImageDeleted::Deleted)
        } else if out.stderr.contains("No such image") {
            Ok(ImageDeleted::Absent)
        } else if out.stderr.contains("is using its referenced image") {
            Ok(ImageDeleted::InUse)
        } else {
            docker::DockerOutput::ok(out, &args).map(|_| ImageDeleted::Deleted)
        }
    }
}

/// The most BuildKit's cache may hold. Over it, the least recently used
/// records go first until it fits, so what the last builds used (the
/// infra images' layers, the runtime image's compile cache) is the last
/// to go.
// SYNC: the cap and the prune flags <-> setup.sh (the "bound BuildKit cache" prune)
pub const BUILD_CACHE_CAP: &str = "20GB";

/// `docker builder prune` holding BuildKit's cache to [`BUILD_CACHE_CAP`].
///
/// `--all`, because without it BuildKit only considers records it does not
/// count as shared or internal, and on a machine that has built many
/// versions of weft that left 2.4GB of 235GB reclaimable in place. No
/// `--filter until=`: that filter makes every record used inside its
/// window ineligible whatever the cap says, so a busy day of builds grew
/// the cache past the cap with nothing allowed to free it (measured: 221GB
/// freed without it, 20GB with it). A record a build is using right now is
/// never taken either way.
fn build_cache_bound_args() -> Vec<String> {
    ["builder", "prune", "--force", "--all", "--max-used-space", BUILD_CACHE_CAP].map(String::from).to_vec()
}

/// Keep BuildKit's cache under [`BUILD_CACHE_CAP`], least recently used
/// first. The same bound `setup.sh` applies, applied while the install
/// runs, since every infra image build adds to the cache between installs.
pub async fn bound_build_cache(docker: &dyn Docker) -> anyhow::Result<()> {
    docker::run(docker, build_cache_bound_args()).await.map(|_| ())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::docker::fake::FakeDocker;
    use crate::host_build::fake::FakeToolchain;
    use weft_core::builds::{HostRecipe, HOST_RECIPE_FILE};
    use weft_platform_traits::Staging;

    /// A staged directory, removed when dropped, as the dispatcher's
    /// staging temp dir is.
    struct Staged(std::path::PathBuf);

    impl Drop for Staged {
        fn drop(&mut self) {
            std::fs::remove_dir_all(&self.0).unwrap();
        }
    }

    /// A request for an infra image's staged context (a Dockerfile and a
    /// file it copies) that the request holds only through its staging,
    /// as a build request that answered does.
    fn req(name: &str) -> BuildRequest {
        let dir = std::env::temp_dir().join(format!("weft-staged-{}", uuid::Uuid::new_v4().simple()));
        std::fs::create_dir_all(dir.join("src")).unwrap();
        std::fs::write(dir.join("Dockerfile"), "FROM scratch\n").unwrap();
        std::fs::write(dir.join("src/main.rs"), "fn main() {}\n").unwrap();
        BuildRequest {
            name: name.into(),
            project_id: uuid::Uuid::from_u128(1),
            tenant: "local".into(),
            context_dir: dir.clone(),
            recipe: BuildRecipe::Dockerfile { result_file: None },
            staging: Staging::new(Staged(dir)),
            image_ref: "weft-infra-pg:abc".into(),
        }
    }

    /// A builder whose host builds nobody runs, for the Docker side, with
    /// its install and machine folders in a temporary folder of its own
    /// (kept until the returned folder drops), so tests run side by side
    /// never share a build log.
    fn docker_builder(docker: Arc<dyn Docker>, install: weft_core::infra::Install) -> (LocalImageBuilder, tempfile::TempDir) {
        let dirs = tempfile::tempdir().unwrap();
        (docker_builder_in(docker, install, dirs.path().to_path_buf()), dirs)
    }

    /// A builder whose host builds nobody runs, on the install folder the
    /// test reads back.
    fn docker_builder_in(docker: Arc<dyn Docker>, install: weft_core::infra::Install, install_dir: std::path::PathBuf) -> LocalImageBuilder {
        let machine = MachineLayout::new(install_dir.join("machine"));
        LocalImageBuilder::new(docker, Arc::new(FakeToolchain::default()), install, InstallLayout::new(install_dir), machine, 1)
    }

    async fn settled(b: &LocalImageBuilder, h: &BuildHandle) -> BuildStatus {
        loop {
            match b.poll(h).await.unwrap() {
                BuildStatus::Pending => tokio::task::yield_now().await,
                other => return other,
            }
        }
    }

    /// Wait for the staged directory to go, which happens on a blocking
    /// thread once the last holder lets go.
    async fn gone(dir: &std::path::Path) {
        for _ in 0..500 {
            if !dir.exists() {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        panic!("the staging {} was never let go of", dir.display());
    }

    /// A Docker whose `docker build` runs until the test lets it end.
    struct HeldBuilds {
        docker: FakeDocker,
        ends: tokio::sync::Semaphore,
    }

    #[async_trait]
    impl Docker for HeldBuilds {
        async fn exec(&self, args: &[String]) -> anyhow::Result<docker::DockerOutput> {
            let out = self.docker.exec(args).await;
            if args.first().is_some_and(|verb| verb == "build") {
                self.ends.acquire().await.unwrap().forget();
            }
            out
        }
    }

    /// `docker build` reads the staged context itself, untouched, and the
    /// build keeps it on disk while it runs, letting go once it ends; an
    /// infra image reports nothing.
    #[tokio::test]
    async fn a_build_runs_docker_build_on_the_staging_it_keeps() {
        let docker = Arc::new(HeldBuilds { docker: FakeDocker::new(), ends: tokio::sync::Semaphore::new(0) });
        let (b, _dirs) = docker_builder(docker.clone(), weft_core::infra::Install::default_install());
        let first = req("b1");
        let dir = first.context_dir.clone();
        let h = b.start(first).await.unwrap();
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        assert_eq!(b.poll(&h).await.unwrap(), BuildStatus::Pending);
        assert!(dir.join("src/main.rs").exists(), "the staging stays while the build runs");
        let call = &docker.docker.calls_to("build")[0];
        assert_eq!(
            call,
            &["build", "--tag", "weft-infra-pg:abc", "--file"]
                .into_iter()
                .map(String::from)
                .chain([dir.join("Dockerfile").display().to_string(), dir.display().to_string()])
                .collect::<Vec<_>>(),
            "built from the staged context itself"
        );
        docker.ends.add_permits(1);
        assert_eq!(settled(&b, &h).await, BuildStatus::Succeeded { result: None });
        gone(&dir).await;
        assert!(docker.docker.calls_to("create").is_empty(), "nothing is read out of an infra image");

        docker.docker.fail(&["build"], "error: cargo failed");
        docker.ends.add_permits(1);
        let h = b.start(req("b2")).await.unwrap();
        let ended = settled(&b, &h).await;
        assert!(matches!(&ended, BuildStatus::Failed { reason } if reason.contains("cargo failed")), "{ended:?}");

        b.release(&h).await;
        assert_eq!(b.poll(&h).await.unwrap(), BuildStatus::Gone, "a released build is gone");
        assert_eq!(b.poll(&BuildHandle::named("never")).await.unwrap(), BuildStatus::Gone);
    }

    /// A build stopped while it runs lets go of its staging with it.
    #[tokio::test]
    async fn a_stopped_build_lets_go_of_its_staging() {
        let docker = Arc::new(HeldBuilds { docker: FakeDocker::new(), ends: tokio::sync::Semaphore::new(0) });
        let (b, _dirs) = docker_builder(docker, weft_core::infra::Install::default_install());
        let r = req("b1");
        let dir = r.context_dir.clone();
        let h = b.start(r).await.unwrap();
        tokio::task::yield_now().await;
        assert!(dir.exists(), "kept while it runs");
        b.release(&h).await;
        gone(&dir).await;
        assert_eq!(b.poll(&h).await.unwrap(), BuildStatus::Gone);
    }

    /// A content another install already holds is given this install's
    /// name instead of being built; with nobody holding it, it is built.
    #[tokio::test]
    async fn a_content_another_install_holds_is_named_not_built() {
        let docker = Arc::new(FakeDocker::new());
        docker.answer(&["image", "ls"], "evil/weft-infra-pg:abc\nweft-infra-pg:abc\n");
        let (b, _dirs) = docker_builder(docker.clone(), weft_core::infra::Install::named("cell7").unwrap());
        let mut r = req("b1");
        r.image_ref = b.image_ref("weft-infra-pg:abc");
        assert_eq!(r.image_ref, "localhost/weft-cell7/weft-infra-pg:abc");
        let dir = r.context_dir.clone();
        let h = b.start(r.clone()).await.unwrap();
        assert_eq!(settled(&b, &h).await, BuildStatus::Succeeded { result: None });
        assert_eq!(docker.calls_to("tag"), vec![vec!["tag", "weft-infra-pg:abc", "localhost/weft-cell7/weft-infra-pg:abc"]]);
        assert!(docker.calls_to("build").is_empty(), "nothing built");
        assert!(dir.exists(), "the test's own clone still holds it");

        let docker = Arc::new(FakeDocker::new());
        docker.answer(&["image", "ls"], "evil/weft-infra-pg:abc\n");
        let (b, _dirs) = docker_builder(docker.clone(), weft_core::infra::Install::named("cell7").unwrap());
        let h = b.start(r).await.unwrap();
        assert_eq!(settled(&b, &h).await, BuildStatus::Succeeded { result: None });
        assert!(docker.calls_to("tag").is_empty(), "an image no install holds is never taken");
        assert_eq!(docker.calls_to("build").len(), 1);
        gone(&dir).await;
    }

    /// The bound caps every record, least recently used first: `--all`
    /// and no age filter, or most of the cache is never eligible.
    #[tokio::test]
    async fn the_build_cache_bound_caps_every_record() {
        let docker = FakeDocker::new();
        bound_build_cache(&docker).await.unwrap();
        assert_eq!(docker.calls_to("builder")[0], vec!["builder", "prune", "--force", "--all", "--max-used-space", "20GB"]);
    }

    /// Deleting an image removes this install's name for it, and its build
    /// log with it.
    #[tokio::test]
    async fn deleting_an_image_removes_its_name_and_its_log() {
        let dir = tempfile::tempdir().unwrap();
        let docker = Arc::new(FakeDocker::new());
        let b = docker_builder_in(docker.clone(), weft_core::infra::Install::default_install(), dir.path().to_path_buf());
        let log = InstallLayout::new(dir.path()).build_log("weft-infra-pg:abc");
        std::fs::create_dir_all(log.parent().unwrap()).unwrap();
        std::fs::write(&log, "output").unwrap();
        assert_eq!(b.delete_image("weft-infra-pg:abc").await.unwrap(), ImageDeleted::Deleted);
        assert_eq!(docker.calls(), vec![vec!["image", "rm", "weft-infra-pg:abc"]]);
        assert!(!log.exists(), "the log goes with the image");
    }

    #[tokio::test]
    async fn deleting_an_image_already_gone_is_fine() {
        let docker = Arc::new(FakeDocker::new());
        docker.fail(&["image", "rm"], "Error: No such image: x");
        let (b, _dirs) = docker_builder(docker.clone(), weft_core::infra::Install::default_install());
        assert_eq!(b.delete_image("x").await.unwrap(), ImageDeleted::Absent);
        docker.fail(&["image", "inspect"], "Error: No such image: x");
        assert!(!b.image_exists("x").await.unwrap());
    }

    #[tokio::test]
    async fn an_image_a_container_still_runs_from_is_kept() {
        let docker = Arc::new(FakeDocker::new());
        docker.fail(&["image", "rm"], "Error response from daemon: conflict: unable to delete x (must be forced) - container 9bcd46ba4da9 is using its referenced image 0e8952e178ee");
        let (b, _dirs) = docker_builder(docker, weft_core::infra::Install::default_install());
        assert_eq!(b.delete_image("x").await.unwrap(), ImageDeleted::InUse);
    }

    /// A machine for host builds: weft's data folder, installs' folders,
    /// and contexts holding a host recipe.
    struct HostMachine {
        machine: tempfile::TempDir,
        installs: tempfile::TempDir,
        toolchain: Arc<FakeToolchain>,
    }

    impl HostMachine {
        fn new() -> Self {
            Self { machine: tempfile::tempdir().unwrap(), installs: tempfile::tempdir().unwrap(), toolchain: Arc::new(FakeToolchain::default()) }
        }

        fn layout(&self) -> MachineLayout {
            MachineLayout::new(self.machine.path())
        }

        fn builder(&self, install: &str) -> LocalImageBuilder {
            self.builder_with(install, self.toolchain.clone())
        }

        fn builder_with(&self, install: &str, toolchain: Arc<dyn Toolchain>) -> LocalImageBuilder {
            let dir = self.installs.path().join(install);
            std::fs::create_dir_all(&dir).unwrap();
            LocalImageBuilder::new(
                Arc::new(FakeDocker::new()),
                toolchain,
                weft_core::infra::Install::named(install).unwrap(),
                InstallLayout::new(dir),
                self.layout(),
                1,
            )
        }

        /// A staged context for the worker `hash`, whose build (when the
        /// fake toolchain runs it, in lane 0) writes the program and
        /// `record`.
        fn request(&self, b: &LocalImageBuilder, hash: &str, record: &[u8]) -> BuildRequest {
            self.tagged_request(b, hash, record, None)
        }

        /// [`Self::request`], for a build whose nodes' `[build.env]` has
        /// the tag `cache_tag`.
        fn tagged_request(&self, b: &LocalImageBuilder, hash: &str, record: &[u8], cache_tag: Option<&str>) -> BuildRequest {
            let ctx = std::env::temp_dir().join(format!("weft-host-{}", uuid::Uuid::new_v4().simple()));
            let recipe = HostRecipe {
                crate_dir: ctx.join("crate"),
                isolate_dir: ctx.join("isolate"),
                isolate_candidates: ctx.join("crate/isolate.json"),
                left_out_record: ctx.join("crate/left-out.json"),
                seed_lock: ctx.join("seed.lock"),
                binary_name: "weft-worker".into(),
                build_env: [("RUSTFLAGS".to_string(), "--cfg from_a_node".to_string())].into(),
                build_env_set_by: [("RUSTFLAGS".to_string(), ["Native".to_string()].into())].into(),
                cache_tag: cache_tag.map(str::to_string),
            };
            for dir in [&recipe.crate_dir, &recipe.isolate_dir] {
                std::fs::create_dir_all(dir).unwrap();
            }
            std::fs::write(&recipe.seed_lock, "# lock").unwrap();
            std::fs::write(ctx.join(HOST_RECIPE_FILE), serde_json::to_vec(&recipe).unwrap()).unwrap();
            let isolate = self.layout().lane_cache(0, cache_tag).join("release/weft-isolate");
            self.toolchain.makes(&isolate, &recipe.left_out_record, record);
            self.toolchain.makes(&isolate, self.layout().lane_cache(0, cache_tag).join("release/weft-worker"), b"\x7fELF");
            BuildRequest {
                name: format!("b-{hash}"),
                project_id: uuid::Uuid::from_u128(1),
                tenant: "local".into(),
                context_dir: ctx.clone(),
                recipe: BuildRecipe::Host,
                staging: Staging::new(Staged(ctx)),
                image_ref: b.image_ref(&format!("weft-worker:{hash}")),
            }
        }

        fn program_dir(&self, hash: &str) -> std::path::PathBuf {
            Program::Worker(hash).dir(&self.layout().workers())
        }
    }

    /// A worker is compiled on the machine: the helper first, then the
    /// worker through it, both in the worker crate with its lane's cache;
    /// the program and its record land in the machine's workers folder, the
    /// record is the build's result, with the units the build used beside
    /// them.
    #[tokio::test]
    async fn a_worker_is_compiled_on_the_machine_into_its_folder() {
        let m = HostMachine::new();
        let b = m.builder("one");
        let req = m.request(&b, "abc", br#"{"packages":[]}"#);
        let (ctx, image) = (req.context_dir.clone(), req.image_ref.clone());
        assert_eq!(image, "weft-worker:abc", "a program's ref is its bare tag on every install");
        assert!(!b.image_exists(&image).await.unwrap());
        let h = b.start(req).await.unwrap();
        assert_eq!(settled(&b, &h).await, BuildStatus::Succeeded { result: Some(br#"{"packages":[]}"#.to_vec()) });
        let calls = m.toolchain.calls.lock().clone();
        let programs: Vec<String> = calls.iter().map(|c| c.program.display().to_string()).collect();
        let target = m.layout().lane_cache(0, None);
        assert_eq!(programs, ["cargo".to_string(), target.join("release/weft-isolate").display().to_string(), "cp".to_string()]);
        // With no seed on the machine (its standard worker came prebuilt),
        // the first lane to finish becomes it.
        assert_eq!(calls[2].args, ["-a".to_string(), format!("{}/.", target.display()), m.layout().seed_cache().display().to_string()]);
        assert!(m.layout().seed_mark().is_file() && m.layout().lane_mark(0, None).is_file());
        assert!(!m.layout().seed_cache().join(".weft-seeded").exists() && !target.join(".weft-seeded").exists(), "the marks are beside the caches, never in them");
        for c in &calls[..2] {
            assert_eq!(c.cwd, ctx.join("crate"));
            assert_eq!(
                c.env,
                vec![
                    ("RUSTFLAGS".to_string(), "--cfg from_a_node".to_string()),
                    ("CARGO_TARGET_DIR".to_string(), target.display().to_string()),
                ],
                "the nodes' build variables reach cargo itself, as an image's ENV lines do"
            );
        }
        assert_eq!(calls[0].args[..3], ["build", "--release", "--manifest-path"]);
        assert_eq!(calls[0].stdout, Some(ctx.join("cargo-messages.json")), "cargo's messages are what the sweep reads");
        assert_eq!(std::fs::read(m.program_dir("abc").join("worker")).unwrap(), b"\x7fELF");
        assert_eq!(std::fs::read(m.program_dir("abc").join("left-out.json")).unwrap(), br#"{"packages":[]}"#);
        assert!(m.program_dir("abc").join(host_build::USED_FILE).is_file());
        assert!(b.image_exists(&image).await.unwrap());
        gone(&ctx).await;
    }

    /// A build whose nodes set `[build.env]` compiles in its lane's cache
    /// for their tag, beside the one builds setting nothing share: seeded
    /// from the seed when that is whole, marked warm on its own, and never
    /// made the seed itself.
    #[tokio::test]
    async fn a_build_with_its_own_build_env_compiles_in_a_cache_of_its_own() {
        let m = HostMachine::new();
        let tag = "0123456789abcdef";
        let b = m.builder("one");
        let h = b.start(m.tagged_request(&b, "abc", b"{}", Some(tag))).await.unwrap();
        assert_eq!(settled(&b, &h).await, BuildStatus::Succeeded { result: Some(b"{}".to_vec()) });
        let calls = m.toolchain.calls.lock().clone();
        let target = m.layout().lane_cache(0, Some(tag));
        let programs: Vec<String> = calls.iter().map(|c| c.program.display().to_string()).collect();
        assert_eq!(programs, ["cargo".to_string(), target.join("release/weft-isolate").display().to_string()], "no seed is made from it");
        assert!(calls[0].env.contains(&("CARGO_TARGET_DIR".to_string(), target.display().to_string())));
        assert!(m.layout().lane_mark(0, Some(tag)).is_file() && !m.layout().lane_mark(0, None).exists());
        assert!(!m.layout().seed_mark().exists());

        std::fs::create_dir_all(m.layout().seed_cache()).unwrap();
        host_build::mark_seed(&m.layout()).unwrap();
        let other = "fedcba9876543210";
        m.toolchain.calls.lock().clear();
        let h = b.start(m.tagged_request(&b, "def", b"{}", Some(other))).await.unwrap();
        assert_eq!(settled(&b, &h).await, BuildStatus::Succeeded { result: Some(b"{}".to_vec()) });
        let calls = m.toolchain.calls.lock().clone();
        let target = m.layout().lane_cache(0, Some(other));
        assert_eq!(calls[0].program, std::path::Path::new("cp"));
        assert_eq!(calls[0].args, ["-a".to_string(), format!("{}/.", m.layout().seed_cache().display()), target.display().to_string()], "a cold cache of its own starts from the seed");
        assert!(m.layout().lane_mark(0, Some(other)).is_file());
    }

    /// After a build, its lane, the seed and the shared crates keep only
    /// what some program on the machine was built from: another program's
    /// stay, and what no program used goes.
    #[tokio::test]
    async fn a_build_leaves_its_caches_only_what_programs_on_the_machine_use() {
        let m = HostMachine::new();
        let (kept, dropped) = ("00000000000000a1", "00000000000000b2");
        for cache in [m.layout().lane_cache(0, None), m.layout().seed_cache()] {
            for hash in [kept, dropped] {
                let rlib = cache.join(format!("release/deps/libpkg_x-{hash}.rlib"));
                std::fs::create_dir_all(rlib.parent().unwrap()).unwrap();
                std::fs::write(rlib, b"rlib").unwrap();
            }
        }
        host_build::mark_seed(&m.layout()).unwrap();
        host_build::mark_lane(&m.layout(), 0, None).unwrap();
        std::fs::create_dir_all(m.program_dir("other")).unwrap();
        let used = host_build::Used { units: [kept.to_string()].into(), crates: ["pkg_kept-1".to_string()].into() };
        std::fs::write(m.program_dir("other").join(host_build::USED_FILE), serde_json::to_vec(&used).unwrap()).unwrap();
        for name in ["pkg_kept-1", "pkg_dropped-2"] {
            std::fs::create_dir_all(m.layout().shared_crates().join(name)).unwrap();
        }
        let b = m.builder("one");
        let h = b.start(m.request(&b, "abc", b"{}")).await.unwrap();
        assert_eq!(settled(&b, &h).await, BuildStatus::Succeeded { result: Some(b"{}".to_vec()) });
        for cache in [m.layout().lane_cache(0, None), m.layout().seed_cache()] {
            let deps = cache.join("release/deps");
            assert!(deps.join(format!("libpkg_x-{kept}.rlib")).exists(), "{}: another program's piece stays", cache.display());
            assert!(!deps.join(format!("libpkg_x-{dropped}.rlib")).exists(), "{}: a piece no program used goes", cache.display());
        }
        assert!(m.layout().shared_crates().join("pkg_kept-1").is_dir(), "another program's crate stays");
        assert!(!m.layout().shared_crates().join("pkg_dropped-2").exists(), "a crate no program used goes");
    }

    /// While a build holds the shared crates (it staged into them and is
    /// not compiled yet), a reclaim removes nothing: its crates are in no
    /// program's record yet.
    #[test]
    fn a_reclaim_waits_for_no_build_to_hold_the_shared_crates() {
        let m = HostMachine::new();
        let crate_dir = m.layout().shared_crates().join("pkg_staged-1");
        std::fs::create_dir_all(&crate_dir).unwrap();
        let staging = weft_core::file_lock::hold_shared(&weft_core::builds::shared_crates_lock(&m.layout().shared_crates())).unwrap();
        assert_eq!(host_build::reclaim_shared_crates(&m.layout()).unwrap(), None);
        assert!(crate_dir.is_dir());
        drop(staging);
        assert_eq!(host_build::reclaim_shared_crates(&m.layout()).unwrap(), Some(vec!["pkg_staged-1".to_string()]));
    }

    /// A toolchain that, at every command, notes whether lane 0 and the
    /// seed are held by someone (the build that ran it).
    struct Watching {
        inner: Arc<FakeToolchain>,
        machine: MachineLayout,
        seen: parking_lot::Mutex<Vec<(String, bool, bool)>>,
    }

    #[async_trait]
    impl Toolchain for Watching {
        async fn run(&self, cmd: &host_build::HostCommand, log: &std::path::Path) -> anyhow::Result<i32> {
            let lane_held = crate::lanes::Lane::try_take(&self.machine, 0).unwrap().is_none();
            let seed_held = weft_core::file_lock::try_hold(&self.machine.seed_lock()).unwrap().is_none();
            self.seen.lock().push((cmd.program.display().to_string(), lane_held, seed_held));
            self.inner.run(cmd, log).await
        }
    }

    /// A build holds its lane from the seed copy through the compile, and
    /// the seed's lock while it copies the seed (never while it compiles);
    /// the lane is free once the build ended.
    #[tokio::test]
    async fn a_host_build_holds_its_lane_through_the_seed_copy_and_the_compile() {
        let m = HostMachine::new();
        std::fs::create_dir_all(m.layout().seed_cache()).unwrap();
        host_build::mark_seed(&m.layout()).unwrap();
        let watching = Arc::new(Watching { inner: m.toolchain.clone(), machine: m.layout(), seen: Default::default() });
        let b = m.builder_with("one", watching.clone());
        let h = b.start(m.request(&b, "abc", b"{}")).await.unwrap();
        assert_eq!(settled(&b, &h).await, BuildStatus::Succeeded { result: Some(b"{}".to_vec()) });
        let seen = watching.seen.lock().clone();
        let programs: Vec<&str> = seen.iter().map(|(p, _, _)| p.rsplit('/').next().unwrap()).collect();
        assert_eq!(programs, ["cp", "cargo", "weft-isolate"], "the cold lane is seeded first");
        assert!(seen.iter().all(|(_, lane, _)| *lane), "the lane is held at every step: {seen:?}");
        assert_eq!(seen.iter().map(|(_, _, seed)| *seed).collect::<Vec<_>>(), [true, false, false], "the seed only while it is copied");
        assert!(crate::lanes::Lane::try_take(&m.layout(), 0).unwrap().is_some(), "let go once the build ended");
    }

    /// A toolchain whose first `cp` fails, as a copy cut short does.
    struct FirstCopyFails {
        inner: Arc<FakeToolchain>,
        failed: std::sync::atomic::AtomicBool,
    }

    #[async_trait]
    impl Toolchain for FirstCopyFails {
        async fn run(&self, cmd: &host_build::HostCommand, log: &std::path::Path) -> anyhow::Result<i32> {
            let code = self.inner.run(cmd, log).await?;
            if cmd.program == std::path::Path::new("cp") && !self.failed.swap(true, std::sync::atomic::Ordering::SeqCst) {
                return Ok(1);
            }
            Ok(code)
        }
    }

    /// A seed copy cut short leaves the lane unmarked, so the next build
    /// copies the seed again over what it left, and marks the lane once
    /// that copy is whole.
    #[tokio::test]
    async fn a_seed_copy_cut_short_leaves_the_lane_unmarked() {
        let m = HostMachine::new();
        std::fs::create_dir_all(m.layout().seed_cache()).unwrap();
        host_build::mark_seed(&m.layout()).unwrap();
        let b = m.builder_with("one", Arc::new(FirstCopyFails { inner: m.toolchain.clone(), failed: Default::default() }));
        let h = b.start(m.request(&b, "abc", b"{}")).await.unwrap();
        assert!(matches!(settled(&b, &h).await, BuildStatus::Failed { .. }));
        assert!(!m.layout().lane_mark(0, None).exists(), "a lane whose copy failed is not warm");
        let h = b.start(m.request(&b, "abc", b"{}")).await.unwrap();
        assert_eq!(settled(&b, &h).await, BuildStatus::Succeeded { result: Some(b"{}".to_vec()) });
        let copies = m.toolchain.calls.lock().iter().filter(|c| c.program == std::path::Path::new("cp")).count();
        assert_eq!(copies, 2, "copied again");
        assert!(m.layout().lane_mark(0, None).is_file());
    }

    /// A lane that finished a build is warm even when it could not become
    /// the seed, so its next build never copies a seed over it.
    #[tokio::test]
    async fn a_lane_that_built_is_marked_whatever_the_seed_did() {
        let m = HostMachine::new();
        m.toolchain.fails("cp");
        let b = m.builder("one");
        let h = b.start(m.request(&b, "abc", b"{}")).await.unwrap();
        assert_eq!(settled(&b, &h).await, BuildStatus::Succeeded { result: Some(b"{}".to_vec()) }, "a seed that cannot be made costs only speed");
        assert!(!m.layout().seed_mark().exists(), "no seed was made");
        assert!(m.layout().lane_mark(0, None).is_file(), "the lane is warm");
    }

    /// A toolchain whose commands never end, as a long compile.
    struct NeverEnds;

    #[async_trait]
    impl Toolchain for NeverEnds {
        async fn run(&self, _cmd: &host_build::HostCommand, _log: &std::path::Path) -> anyhow::Result<i32> {
            std::future::pending().await
        }
    }

    /// A host build released while it runs lets go of the hold it took, so
    /// the program's empty folder does not stay held by this install.
    #[tokio::test]
    async fn a_released_host_build_lets_go_of_its_program() {
        let m = HostMachine::new();
        let b = m.builder_with("one", Arc::new(NeverEnds));
        let h = b.start(m.request(&b, "abc", b"{}")).await.unwrap();
        assert!(m.program_dir("abc").join("held-by/one").is_file(), "held while it builds");
        assert_eq!(b.poll(&h).await.unwrap(), BuildStatus::Pending);
        b.release(&h).await;
        assert!(!m.program_dir("abc").exists(), "let go of, and nobody else holds it");
        assert_eq!(b.poll(&h).await.unwrap(), BuildStatus::Gone);
    }

    /// The standard worker compiles in the seed itself, holding the seed's
    /// lock through every step and no lane.
    #[tokio::test]
    async fn the_seed_is_compiled_holding_its_own_lock_and_no_lane() {
        let m = HostMachine::new();
        let watching = Watching { inner: m.toolchain.clone(), machine: m.layout(), seen: Default::default() };
        let b = m.builder("one");
        let req = m.request(&b, "abc", b"{}");
        let isolate = m.layout().seed_cache().join("release/weft-isolate");
        m.toolchain.makes(&isolate, req.context_dir.join("crate/left-out.json"), b"{}");
        m.toolchain.makes(&isolate, m.layout().seed_cache().join("release/weft-worker"), b"\x7fELF");
        let log = m.machine.path().join("seed.log");
        let built = host_build::compile(&watching, &m.layout(), host_build::CompileIn::Seed, &req.context_dir, m.program_dir("abc"), &log, || {})
            .await
            .unwrap();
        assert_eq!(built, HostBuilt::Built(b"{}".to_vec()));
        let seen = watching.seen.lock().clone();
        assert_eq!(seen.iter().map(|(p, _, _)| p.rsplit('/').next().unwrap()).collect::<Vec<_>>(), ["cargo", "weft-isolate"], "the seed copies nothing");
        assert!(seen.iter().all(|(_, lane, seed)| !lane && *seed), "{seen:?}");
        assert!(m.layout().seed_mark().is_file(), "a seed that left nothing out is whole");
    }

    /// A program one install built is taken by the next with nothing
    /// built, and goes from the machine only with the last install holding
    /// it.
    #[tokio::test]
    async fn a_program_is_shared_by_installs_and_goes_with_the_last() {
        let m = HostMachine::new();
        let (one, two) = (m.builder("one"), m.builder("two"));
        let h = one.start(m.request(&one, "abc", b"{}")).await.unwrap();
        settled(&one, &h).await;
        let req = m.request(&two, "abc", b"{}");
        let image = req.image_ref.clone();
        assert!(!two.image_exists(&image).await.unwrap(), "this install does not hold it yet");
        let ran = m.toolchain.calls.lock().len();
        let h = two.start(req).await.unwrap();
        assert_eq!(settled(&two, &h).await, BuildStatus::Succeeded { result: Some(b"{}".to_vec()) });
        assert_eq!(m.toolchain.calls.lock().len(), ran, "nothing built");
        assert!(two.image_exists(&image).await.unwrap());

        assert_eq!(one.delete_image(&image).await.unwrap(), ImageDeleted::Deleted);
        assert!(m.program_dir("abc").join("worker").exists(), "the other install still holds it");
        assert!(!one.image_exists(&image).await.unwrap());
        assert_eq!(two.delete_image(&image).await.unwrap(), ImageDeleted::Deleted);
        assert!(!m.program_dir("abc").exists(), "gone with the last");
        assert_eq!(two.delete_image(&image).await.unwrap(), ImageDeleted::Absent);
    }

    /// The standard worker an install's record names stays, held or not,
    /// whether the record is this weft's (a program) or an older weft's (a
    /// registry image of the same hash).
    #[tokio::test]
    async fn an_installs_standard_worker_stays() {
        for named in ["weft-worker:abc", "ghcr.io/weavemindai/weft-worker:abc"] {
            let m = HostMachine::new();
            let b = m.builder("one");
            let h = b.start(m.request(&b, "abc", b"{}")).await.unwrap();
            settled(&b, &h).await;
            let two = m.machine.path().join("installs").join("two");
            std::fs::create_dir_all(&two).unwrap();
            weft_core::images::ImageRefs {
                runtime: "weft-runtime:r".into(),
                standard_worker: named.into(),
                builder_bases: Default::default(),
                compile_cache_keys: Default::default(),
            }
            .save(&two)
            .unwrap();
            b.delete_image("weft-worker:abc").await.unwrap();
            assert!(m.program_dir("abc").join("worker").exists(), "{named}");
        }
    }

    /// A reclaim drops the holds of installs no longer on the machine, then
    /// every worker nobody holds that no install names as its standard
    /// worker, and leaves the test servers alone.
    #[tokio::test]
    async fn a_reclaim_takes_the_workers_nothing_uses() {
        let m = HostMachine::new();
        let one = m.builder("one");
        let gone = m.builder("gone");
        for (b, hash) in [(&one, "kept"), (&gone, "orphan")] {
            let h = b.start(m.request(b, hash, b"{}")).await.unwrap();
            settled(b, &h).await;
        }
        for hash in ["standard", "replaced"] {
            std::fs::create_dir_all(m.program_dir(hash)).unwrap();
            std::fs::write(m.program_dir(hash).join("worker"), b"\x7fELF").unwrap();
        }
        weft_core::images::ImageRefs {
            runtime: "weft-runtime:r".into(),
            standard_worker: "weft-worker:standard".into(),
            builder_bases: Default::default(),
            compile_cache_keys: Default::default(),
        }
        .save(m.machine.path())
        .unwrap();
        let server = m.layout().node_test_servers().join("abc");
        std::fs::create_dir_all(&server).unwrap();

        let removed = reclaim_workers(&m.layout().workers(), &["one".to_string()]).unwrap();
        assert_eq!(removed, ["orphan", "replaced"]);
        assert!(m.program_dir("kept").join("worker").exists() && m.program_dir("standard").join("worker").exists());
        assert!(server.exists(), "a test server is its package's to replace");
    }

    /// A build that fails answers its log's last lines, and lets go of the
    /// folder it held.
    #[tokio::test]
    async fn a_failed_host_build_answers_its_log_and_leaves_nothing() {
        let m = HostMachine::new();
        let b = m.builder("one");
        m.toolchain.fails("cargo");
        let h = b.start(m.request(&b, "abc", b"{}")).await.unwrap();
        let BuildStatus::Failed { reason } = settled(&b, &h).await else { panic!("a failed cargo succeeded") };
        assert!(reason.contains("ran cargo") && reason.contains("exit code 1"), "{reason}");
        assert!(!m.program_dir("abc").exists());
    }

    /// A worker is compiled here, never built as an image, only a worker
    /// is compiled, and nothing is read out of an infra image.
    #[tokio::test]
    async fn the_recipe_must_fit_the_ref() {
        let m = HostMachine::new();
        let b = m.builder("one");
        let mut req = m.request(&b, "abc", b"{}");
        req.recipe = BuildRecipe::Dockerfile { result_file: None };
        let e = format!("{:#}", b.start(req.clone()).await.unwrap_err());
        assert!(e.contains("host build"), "{e}");
        req.recipe = BuildRecipe::Host;
        req.image_ref = "weft-infra-pg:abc".into();
        let e = format!("{:#}", b.start(req.clone()).await.unwrap_err());
        assert!(e.contains("none of weft's programs"), "{e}");
        req.recipe = BuildRecipe::Dockerfile { result_file: Some(weft_core::builds::LEFT_OUT_RECORD_PATH.into()) };
        let e = format!("{:#}", b.start(req).await.unwrap_err());
        assert!(e.contains("report nothing"), "{e}");
        assert_eq!(b.worker_packaging(), WorkerPackaging::Host { shared_crates: m.layout().shared_crates() });
    }

    /// A program's ref is its bare tag; an install's name on an image (an
    /// older weft's worker image) is no program, and neither is a path.
    #[test]
    fn a_program_ref_is_its_bare_tag() {
        assert_eq!(Program::of("weft-worker:ab12"), Some(Program::Worker("ab12")));
        assert_eq!(Program::of("weft-node-tests:cd34"), Some(Program::NodeTestServer("cd34")));
        assert_eq!(Program::of("localhost/weft-cell7/weft-worker:ab12"), None);
        assert_eq!(Program::of("ghcr.io/weavemindai/weft-worker:ab12"), None);
        assert_eq!(Program::of("weft-infra-pg:ab12"), None);
        assert_eq!(Program::of("weft-worker:.."), None, "never a path out of the folder");
        assert_eq!(Program::of("weft-node-tests:"), None);
        let w = std::path::Path::new("/w");
        assert_eq!(Program::Worker("ab").file(w), std::path::PathBuf::from("/w/ab/worker"));
        assert_eq!(Program::NodeTestServer("cd").file(w), std::path::PathBuf::from("/w/node-tests/cd/worker"));
        assert_eq!(Program::Worker("ab").args(), &[] as &[&str]);
        assert_eq!(Program::NodeTestServer("cd").args(), ["serve"]);
        let (b, _dirs) = docker_builder(Arc::new(FakeDocker::new()), weft_core::infra::Install::named("cell7").unwrap());
        assert_eq!(b.image_ref("weft-worker:ab12"), "weft-worker:ab12");
        assert_eq!(b.image_ref("weft-infra-pg:ab12"), "localhost/weft-cell7/weft-infra-pg:ab12");
    }

    /// A node test server is compiled on the machine like a worker, into
    /// its own folder, and goes with the last install holding it.
    #[tokio::test]
    async fn a_node_test_server_is_compiled_into_its_own_folder() {
        let m = HostMachine::new();
        let b = m.builder("one");
        let mut req = m.request(&b, "abc", b"{}");
        req.image_ref = b.image_ref("weft-node-tests:abc");
        let image = req.image_ref.clone();
        let h = b.start(req).await.unwrap();
        assert_eq!(settled(&b, &h).await, BuildStatus::Succeeded { result: Some(b"{}".to_vec()) });
        let dir = m.layout().node_test_servers().join("abc");
        assert_eq!(std::fs::read(dir.join("worker")).unwrap(), b"\x7fELF");
        assert!(!m.program_dir("abc").exists(), "never in a worker's folder");
        assert!(b.image_exists(&image).await.unwrap());
        assert_eq!(b.delete_image(&image).await.unwrap(), ImageDeleted::Deleted);
        assert!(!dir.exists());
    }
}
