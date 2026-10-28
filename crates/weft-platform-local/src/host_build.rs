//! A program compiled on the machine that runs it (`BuildRecipe::Host`):
//! a project's worker, a package's node-test server, the standard worker.
//! The steps a worker image's builder stage takes
//! (`weft_compiler::worker_image`, `CARGO_BUILD_RUN_FRAGMENT`), run with
//! the person's own Rust toolchain in one of the machine's compile lanes
//! (`lanes`), whose cache every install on the machine shares. The program
//! lands in its folder in the machine's workers folder
//! (`images::Program::dir`), beside the record of the node packages its
//! build left out, where the runner starts it from. [`compile`] is the one
//! way any of them is compiled.
//!
//! The commands run behind [`Toolchain`], so the steps are tested against
//! a fake that records them; only [`MachineToolchain`] runs anything.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::process::Stdio;

use async_trait::async_trait;
use weft_core::builds::HostRecipe;

use crate::layout::MachineLayout;

/// One command of a host build.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostCommand {
    pub program: PathBuf,
    pub args: Vec<String>,
    pub cwd: PathBuf,
    /// Set on top of the runtime's own environment (whose `PATH` and
    /// `HOME` find the person's toolchain).
    pub env: Vec<(String, String)>,
    /// Where its stdout goes instead of the build log, when somewhere
    /// else.
    pub stdout: Option<PathBuf>,
}

#[async_trait]
pub trait Toolchain: Send + Sync {
    /// Run `cmd`, everything it prints appended to `log` as it prints it
    /// (its stdout to `cmd.stdout` when that is set), so the log can be
    /// followed while it runs. Answers its exit code. The command stops
    /// when the caller stops waiting (a released build).
    async fn run(&self, cmd: &HostCommand, log: &Path) -> anyhow::Result<i32>;
}

/// The person's own toolchain, through the `PATH` the runtime has.
pub struct MachineToolchain;

#[async_trait]
impl Toolchain for MachineToolchain {
    async fn run(&self, cmd: &HostCommand, log: &Path) -> anyhow::Result<i32> {
        let open = |path: &Path| {
            std::fs::OpenOptions::new().create(true).append(true).open(path).map_err(|e| anyhow::anyhow!("open {}: {e}", path.display()))
        };
        let stderr = open(log)?;
        let stdout = match &cmd.stdout {
            Some(path) => std::fs::File::create(path).map_err(|e| anyhow::anyhow!("create {}: {e}", path.display()))?,
            None => stderr.try_clone().map_err(|e| anyhow::anyhow!("open {} twice: {e}", log.display()))?,
        };
        let status = tokio::process::Command::new(&cmd.program)
            .args(&cmd.args)
            .envs(cmd.env.iter().map(|(k, v)| (k, v)))
            .current_dir(&cmd.cwd)
            .stdin(Stdio::null())
            .stdout(stdout)
            .stderr(stderr)
            .kill_on_drop(true)
            .status()
            .await
            .map_err(|e| match e.kind() {
                std::io::ErrorKind::NotFound if cmd.program == Path::new("cargo") => anyhow::anyhow!(
                    "`cargo` is not on the PATH weft's runtime has, and a local install compiles each project's worker on this machine; \
                     install Rust with rustup (https://rustup.rs), which picks the toolchain weft's rust-toolchain.toml pins, \
                     then start the install again (`./setup.sh`) so its runtime gets the new PATH"
                ),
                _ => anyhow::anyhow!("run {} in {}: {e}", cmd.program.display(), cmd.cwd.display()),
            })?;
        Ok(status.code().unwrap_or(-1))
    }
}

/// The seed (`MachineLayout::seed_cache`) is whole: the standard worker
/// compiled in it, leaving nothing out, or the cache of the first lane to
/// finish a build on a machine whose standard worker came prebuilt.
/// [`compile`] marks it so (`MachineLayout::seed_mark`), still holding the
/// seed's lock.
pub(crate) fn mark_seed(machine: &MachineLayout) -> anyhow::Result<()> {
    let mark = machine.seed_mark();
    std::fs::write(&mark, b"").map_err(|e| anyhow::anyhow!("mark the compile cache's seed whole ({}): {e}", mark.display()))
}

/// Lane `number`'s cache is warm (`MachineLayout::lane_mark`): the seed was
/// copied whole into it, or a build finished in it. Written only once
/// that is done, so a copy cut short leaves the lane unmarked and the next
/// build copies the seed again over it.
pub(crate) fn mark_lane(machine: &MachineLayout, number: u32) -> anyhow::Result<()> {
    let mark = machine.lane_mark(number);
    std::fs::write(&mark, b"").map_err(|e| anyhow::anyhow!("mark compile lane {number} warm ({}): {e}", mark.display()))
}

/// Which of the machine's compile caches a build compiles in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompileIn {
    /// The seed every lane starts from: the standard worker's, compiled
    /// holding the seed's lock, and marked whole once a build in it left
    /// nothing out.
    Seed,
    /// A free lane of `lanes`, held for the build (waiting politely for one
    /// when every one is held): any other program. A lane whose cache is
    /// still cold copies the seed into it first, holding the seed's lock,
    /// so a project's first build compiles only its own packages, the way a
    /// worker image's cache starts from the builder base's compiled tree.
    Lane { lanes: u32 },
}

/// Compile the host build staged in `context_dir` (its recipe is
/// `HOST_RECIPE_FILE` there) into `out_dir`, in `cache`, with its output in
/// `log` (written fresh). The lane or the seed's lock is held from before
/// the seed copy until after the cache sweep, so no other build and no
/// cleanup touches that cache meanwhile. `on_wait` is said once when every
/// lane, or the seed, is held by another build.
pub async fn compile(
    toolchain: &dyn Toolchain,
    machine: &MachineLayout,
    cache: CompileIn,
    context_dir: &Path,
    out_dir: PathBuf,
    log: &Path,
    on_wait: impl FnOnce() + Send + 'static,
) -> anyhow::Result<HostBuilt> {
    let recipe_path = context_dir.join(weft_core::builds::HOST_RECIPE_FILE);
    let recipe: HostRecipe = serde_json::from_slice(
        &tokio::fs::read(&recipe_path).await.map_err(|e| anyhow::anyhow!("read the host recipe {}: {e}", recipe_path.display()))?,
    )
    .map_err(|e| anyhow::anyhow!("{} is not a host recipe: {e}", recipe_path.display()))?;
    let held = match cache {
        CompileIn::Seed => Held::Seed { _lock: crate::lanes::hold(&machine.seed_lock(), on_wait).await? },
        CompileIn::Lane { lanes } => Held::Lane(crate::lanes::Lane::take(machine, lanes, on_wait).await?),
    };
    if let Some(dir) = log.parent() {
        tokio::fs::create_dir_all(dir).await.map_err(|e| anyhow::anyhow!("create {}: {e}", dir.display()))?;
    }
    tokio::fs::write(log, b"").await.map_err(|e| anyhow::anyhow!("write {}: {e}", log.display()))?;
    let built = build(toolchain, machine, &held, HostBuild { recipe: &recipe, context_dir, out_dir, log }).await?;
    match (&held, &built) {
        (Held::Seed { .. }, HostBuilt::Built(record)) => {
            if weft_core::builds::LeftOut::parse(record).is_ok_and(|record| record.is_empty()) {
                mark_seed(machine)?;
            }
        }
        // The lane is warm now, whatever the seed is, so it never copies
        // the seed over itself. A machine whose standard worker came
        // prebuilt has no seed: the first lane to finish a build becomes
        // it, so every other lane starts warm too. A mark that cannot be
        // written or a cache that cannot be copied costs only speed, so it
        // is said and the build stands.
        (Held::Lane(lane), HostBuilt::Built(_)) => {
            if let Err(e) = mark_lane(machine, lane.number()) {
                tracing::warn!(target: "weft_platform_local::host_build", error = %format!("{e:#}"), "could not mark this build's compile lane warm; its next build copies the seed into it again");
            }
            if let Err(e) = warm_seed_from(toolchain, machine, &machine.lane_cache(lane.number()), log).await {
                tracing::warn!(target: "weft_platform_local::host_build", error = %format!("{e:#}"), "could not make this build's compile cache the machine's seed; the next cold lane compiles from nothing");
            }
        }
        _ => {}
    }
    Ok(built)
}

/// Make the lane cache `lane` (held by the caller, just built in) the
/// machine's seed when there is none yet and nobody is making one: copied
/// whole, then marked, so a copy cut short leaves a seed nothing copies.
async fn warm_seed_from(toolchain: &dyn Toolchain, machine: &MachineLayout, lane: &Path, log: &Path) -> anyhow::Result<()> {
    let seed = machine.seed_cache();
    if machine.seed_mark().exists() {
        return Ok(());
    }
    let lock_path = machine.seed_lock();
    let Some(_seed_lock) = weft_core::file_lock::try_hold(&lock_path).map_err(|e| anyhow::anyhow!("lock {}: {e}", lock_path.display()))? else {
        return Ok(());
    };
    if machine.seed_mark().exists() {
        return Ok(());
    }
    match tokio::fs::remove_dir_all(&seed).await {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => anyhow::bail!("remove the unfinished seed {}: {e}", seed.display()),
    }
    tokio::fs::create_dir_all(&seed).await.map_err(|e| anyhow::anyhow!("create {}: {e}", seed.display()))?;
    let copy = HostCommand {
        program: "cp".into(),
        args: vec!["-a".into(), format!("{}/.", lane.display()), seed.display().to_string()],
        cwd: seed.clone(),
        env: Vec::new(),
        stdout: None,
    };
    let code = toolchain.run(&copy, log).await?;
    anyhow::ensure!(code == 0, "copy the compile cache {} into the seed {} failed with exit code {code}", lane.display(), seed.display());
    mark_seed(machine)
}

/// The cache a build holds for its whole length.
enum Held {
    Seed { _lock: std::fs::File },
    Lane(crate::lanes::Lane),
}

impl Held {
    fn target_dir(&self, machine: &MachineLayout) -> PathBuf {
        match self {
            Held::Seed { .. } => machine.seed_cache(),
            Held::Lane(lane) => machine.lane_cache(lane.number()),
        }
    }
}

/// The program's file name in its folder.
pub const PROGRAM_FILE: &str = "worker";

/// The record of what the build left out, beside the program.
pub const LEFT_OUT_FILE: &str = "left-out.json";

/// What the program's build used ([`Used`]), beside the program.
pub const USED_FILE: &str = "used.json";

/// What a program's build used: everything here stays on the machine as
/// long as the program does, so building it again compiles nothing new.
#[derive(Debug, Default, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Used {
    /// The compiled units, by the hash cargo names their files with: kept
    /// in every compile cache ([`sweep`]).
    pub units: BTreeSet<String>,
    /// The folders of the shared crates it was compiled from
    /// (`MachineLayout::shared_crates`): kept there ([`reclaim_shared_crates`]).
    pub crates: BTreeSet<String>,
}

impl Used {
    /// Everything the programs on `machine` were built from. A program from
    /// before builds kept this record keeps nothing.
    pub fn of_programs(machine: &MachineLayout) -> anyhow::Result<Self> {
        let mut used = Self::default();
        for programs in [machine.workers(), machine.node_test_servers()] {
            let Ok(entries) = std::fs::read_dir(&programs) else { continue };
            for entry in entries {
                let dir = entry.map_err(|e| anyhow::anyhow!("read {}: {e}", programs.display()))?.path();
                if !dir.is_dir() {
                    continue;
                }
                let file = dir.join(USED_FILE);
                let bytes = match std::fs::read(&file) {
                    Ok(bytes) => bytes,
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                    Err(e) => anyhow::bail!("read {}: {e}", file.display()),
                };
                let one: Self = serde_json::from_slice(&bytes).map_err(|e| anyhow::anyhow!("{} is not a record of what a build used: {e}", file.display()))?;
                used.units.extend(one.units);
                used.crates.extend(one.crates);
            }
        }
        Ok(used)
    }
}

/// Remove from the machine's shared crates every folder no program on the
/// machine was built from, and anything a stopped build left
/// half-written; answers the names it removed. While any build holds the
/// folder (`weft_core::builds::shared_crates_lock`) it removes nothing and
/// answers `None`: that build's crates are in no program's record yet, and
/// a later reclaim takes what is left.
pub fn reclaim_shared_crates(machine: &MachineLayout) -> anyhow::Result<Option<Vec<String>>> {
    let shared = machine.shared_crates();
    let lock = weft_core::builds::shared_crates_lock(&shared);
    let Some(_alone) = weft_core::file_lock::try_hold(&lock).map_err(|e| anyhow::anyhow!("lock {}: {e}", lock.display()))? else {
        return Ok(None);
    };
    let kept = Used::of_programs(machine)?.crates;
    let entries = match std::fs::read_dir(&shared) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Some(Vec::new())),
        Err(e) => anyhow::bail!("read {}: {e}", shared.display()),
    };
    let mut removed = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|e| anyhow::anyhow!("read {}: {e}", shared.display()))?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if kept.contains(&name) {
            continue;
        }
        let path = entry.path();
        let gone = if path.is_dir() { std::fs::remove_dir_all(&path) } else { std::fs::remove_file(&path) };
        gone.map_err(|e| anyhow::anyhow!("remove {}: {e}", path.display()))?;
        removed.push(name);
    }
    removed.sort();
    Ok(Some(removed))
}

/// What a host build needs to know besides its recipe and its cache.
struct HostBuild<'a> {
    recipe: &'a HostRecipe,
    /// The build's context folder: where cargo's messages are written.
    context_dir: &'a Path,
    /// Where the program and its record land (`images::Program::dir`).
    out_dir: PathBuf,
    log: &'a Path,
}

/// What a host build ended with.
#[derive(Debug, PartialEq, Eq)]
pub enum HostBuilt {
    /// The record of what it left out, its bytes.
    Built(Vec<u8>),
    /// A step failed; what the log said is the reason.
    Failed(String),
}

/// Run the build in the cache `held`: the seed into a cold lane, the
/// helper, then the worker through it, then the program and its record
/// into `out_dir`, then the cache sweep. The program is put in place last
/// and by a rename, so a folder holding it always holds its record too.
async fn build(toolchain: &dyn Toolchain, machine: &MachineLayout, held: &Held, b: HostBuild<'_>) -> anyhow::Result<HostBuilt> {
    let r = b.recipe;
    let target_dir = held.target_dir(machine);
    for crate_dir in [&r.crate_dir, &r.isolate_dir] {
        let lock = crate_dir.join("Cargo.lock");
        tokio::fs::copy(&r.seed_lock, &lock)
            .await
            .map_err(|e| anyhow::anyhow!("copy the seed lock {} to {}: {e}", r.seed_lock.display(), lock.display()))?;
    }
    if let Held::Lane(lane) = held {
        seed(toolchain, machine, lane.number(), b.log).await?;
    }
    let messages = b.context_dir.join("cargo-messages.json");
    let target = target_dir.display().to_string();
    let env = vec![("CARGO_TARGET_DIR".to_string(), target.clone())];
    let release = target_dir.join("release");
    let step = |program: PathBuf, args: Vec<String>, stdout: Option<PathBuf>| HostCommand {
        program,
        args,
        cwd: r.crate_dir.clone(),
        env: env.clone(),
        stdout,
    };
    let helper = step(
        "cargo".into(),
        vec![
            "build".into(),
            "--release".into(),
            "--manifest-path".into(),
            r.isolate_dir.join("Cargo.toml").display().to_string(),
            "--message-format=json-render-diagnostics".into(),
        ],
        Some(messages.clone()),
    );
    let worker = step(
        release.join("weft-isolate"),
        vec![r.isolate_candidates.display().to_string(), r.left_out_record.display().to_string(), messages.display().to_string()],
        None,
    );
    for cmd in [helper, worker] {
        let code = toolchain.run(&cmd, b.log).await?;
        if code != 0 {
            return Ok(HostBuilt::Failed(format!("`{}` failed with exit code {code}", cmd.program.display())));
        }
    }
    let record = tokio::fs::read(&r.left_out_record)
        .await
        .map_err(|e| anyhow::anyhow!("the build ended without its record of what it left out at {}: {e}", r.left_out_record.display()))?;
    let used = used(&messages, &release, &machine.shared_crates()).await?;
    let used = serde_json::to_vec(&used).map_err(|e| anyhow::anyhow!("the record of what the build used: {e}"))?;
    place_bytes(&b.out_dir, USED_FILE, &used).await?;
    place(&b.out_dir, LEFT_OUT_FILE, &r.left_out_record).await?;
    place(&b.out_dir, PROGRAM_FILE, &release.join(&r.binary_name)).await?;
    // The sweep only bounds the cache: the build is done whatever it did.
    if let Err(e) = sweep(machine, &release) {
        tracing::warn!(target: "weft_platform_local::host_build", error = %format!("{e:#}"), "the compile cache sweep failed; the build is unaffected");
    }
    Ok(HostBuilt::Built(record))
}

/// What a build used, off the cargo messages it wrote: its units in
/// `release`, and the folders of `shared` its units were compiled from.
async fn used(messages: &Path, release: &Path, shared: &Path) -> anyhow::Result<Used> {
    let bytes = tokio::fs::read(messages).await.map_err(|e| anyhow::anyhow!("read the build's cargo messages {}: {e}", messages.display()))?;
    Ok(Used { units: crate::cargo_cache::used_units(&bytes, release)?, crates: crate::cargo_cache::source_folders(&bytes, shared) })
}

/// Keep in the cache `release` just built in, and in the seed when no
/// build holds it, only the units some program on the machine was built
/// from ([`Used`]): what any of them would reuse when built again. A
/// program removed from the machine takes its units with it at the next
/// build.
fn sweep(machine: &MachineLayout, release: &Path) -> anyhow::Result<()> {
    let kept = Used::of_programs(machine)?.units;
    crate::cargo_cache::keep_used(release, &kept)?;
    let lock_path = machine.seed_lock();
    if let Some(_seed_lock) = weft_core::file_lock::try_hold(&lock_path).map_err(|e| anyhow::anyhow!("lock {}: {e}", lock_path.display()))? {
        let seed = machine.seed_cache().join("release");
        if seed.is_dir() {
            crate::cargo_cache::keep_used(&seed, &kept)?;
        }
    }
    Ok(())
}

/// Give lane `number`'s cache the seed's compiled tree, once: when the
/// lane is not warm yet ([`mark_lane`]) and the seed is whole
/// ([`mark_seed`]). The copy holds the seed's lock, so it never copies a
/// seed while it is compiled. A lane with no whole seed beside it builds
/// from what it holds.
async fn seed(toolchain: &dyn Toolchain, machine: &MachineLayout, number: u32, log: &Path) -> anyhow::Result<()> {
    if machine.lane_mark(number).exists() {
        return Ok(());
    }
    let _seed_lock = crate::lanes::hold(&machine.seed_lock(), || {
        tracing::info!(target: "weft_platform_local::host_build", "a build waits for the standard worker's compile to copy its cache")
    })
    .await?;
    if !machine.seed_mark().exists() {
        return Ok(());
    }
    let (seed, target) = (machine.seed_cache(), machine.lane_cache(number));
    tokio::fs::create_dir_all(&target).await.map_err(|e| anyhow::anyhow!("create {}: {e}", target.display()))?;
    let copy = HostCommand {
        program: "cp".into(),
        args: vec!["-a".into(), format!("{}/.", seed.display()), target.display().to_string()],
        cwd: target.clone(),
        env: Vec::new(),
        stdout: None,
    };
    let code = toolchain.run(&copy, log).await?;
    anyhow::ensure!(code == 0, "copy the compile cache's seed {} into {} failed with exit code {code}", seed.display(), target.display());
    mark_lane(machine, number)
}

/// Write `bytes` to `name` in `dir` the way [`place`] copies a file.
async fn place_bytes(dir: &Path, name: &str, bytes: &[u8]) -> anyhow::Result<()> {
    tokio::fs::create_dir_all(dir).await.map_err(|e| anyhow::anyhow!("create {}: {e}", dir.display()))?;
    let partial = dir.join(format!("{name}.{}.tmp", uuid::Uuid::new_v4().simple()));
    let to = dir.join(name);
    let written = async {
        tokio::fs::write(&partial, bytes).await?;
        tokio::fs::rename(&partial, &to).await
    }
    .await;
    if let Err(e) = written {
        let _ = tokio::fs::remove_file(&partial).await;
        anyhow::bail!("write {}: {e}", to.display());
    }
    Ok(())
}

/// Copy `from` to `name` in `dir`: to a file beside it first, then renamed
/// over it, so a reader never sees half of it.
async fn place(dir: &Path, name: &str, from: &Path) -> anyhow::Result<()> {
    tokio::fs::create_dir_all(dir).await.map_err(|e| anyhow::anyhow!("create {}: {e}", dir.display()))?;
    let partial = dir.join(format!("{name}.{}.tmp", uuid::Uuid::new_v4().simple()));
    let to = dir.join(name);
    let copied = async {
        tokio::fs::copy(from, &partial).await?;
        tokio::fs::rename(&partial, &to).await
    }
    .await;
    if let Err(e) = copied {
        let _ = tokio::fs::remove_file(&partial).await;
        anyhow::bail!("copy {} to {}: {e}", from.display(), to.display());
    }
    Ok(())
}

#[cfg(test)]
pub mod fake {
    use super::*;
    use parking_lot::Mutex;

    /// Records every command; answers 0 unless told a program fails, and
    /// writes what a step makes when told to (`makes`).
    #[derive(Default)]
    pub struct FakeToolchain {
        pub calls: Mutex<Vec<HostCommand>>,
        fails: Mutex<Vec<PathBuf>>,
        makes: Mutex<Vec<(PathBuf, PathBuf, Vec<u8>)>>,
    }

    impl FakeToolchain {
        /// `program` exits 1.
        pub fn fails(&self, program: impl Into<PathBuf>) {
            self.fails.lock().push(program.into());
        }

        /// When `program` runs, it writes `bytes` to `file`.
        pub fn makes(&self, program: impl Into<PathBuf>, file: impl Into<PathBuf>, bytes: &[u8]) {
            self.makes.lock().push((program.into(), file.into(), bytes.to_vec()));
        }
    }

    #[async_trait]
    impl Toolchain for FakeToolchain {
        async fn run(&self, cmd: &HostCommand, log: &Path) -> anyhow::Result<i32> {
            self.calls.lock().push(cmd.clone());
            std::fs::write(log, format!("ran {}\n", cmd.program.display()))?;
            if let Some(stdout) = &cmd.stdout {
                std::fs::write(stdout, b"")?;
            }
            for (program, file, bytes) in self.makes.lock().iter() {
                if *program == cmd.program {
                    std::fs::create_dir_all(file.parent().unwrap())?;
                    std::fs::write(file, bytes)?;
                }
            }
            Ok(if self.fails.lock().contains(&cmd.program) { 1 } else { 0 })
        }
    }
}
