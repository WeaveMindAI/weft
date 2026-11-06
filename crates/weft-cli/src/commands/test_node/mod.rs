//! `weft test-node`: run node self-tests.
//!
//! Basic/fake tiers run RIGHT HERE: every targeted package's test
//! crate is emitted against the real weft checkout
//! (`EmitPaths::Local`) as a member of one cargo workspace, built with
//! plain cargo (no docker, no install, no broker), and executed.
//! A broken `main.weft` never blocks this, and neither does a broken
//! package nobody targeted: only what you asked to test joins the
//! build. Target a package and it is the only one that has to compile.
//! However many packages are targeted, they build in ONE cargo
//! invocation, then their binaries run one at a time or, under
//! `--parallel`, side by side.
//!
//! The live tier runs the PRODUCTION credential path (connection
//! resolution, relaying, metering, billing), which only exists next to
//! the broker, so live tests always run on this machine's own install:
//! the CLI compiles each package's test server as a program on this
//! machine (no Docker), in the machine's workers folder where the
//! install's runner starts it from, checks the machine meets what the
//! tested nodes need while they run, asks the install to run one test per
//! call (`/projects/{id}/node-tests/run`), and polls the outcome. Live
//! needs the project registered with the dispatcher (cost has to
//! attribute to something) and a grant for each test's declared
//! service.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use anyhow::{bail, Context, Result};

use weft_catalog::FsCatalog;
use weft_core::access::spec::{AccessSpec, CredentialField, Door, LoginWay};
use weft_core::access::wire::ConnectDirect;
use weft_core::node::MetadataCatalog;
use weft_core::node_test::{
    concurrency_limit, report_line, NodeTestsListing, RunAllReport, TestListing, TestReport,
};
use weft_core::task::TaskStatus;
use weft_core::TestTier;

use super::Ctx;

mod progress;
use progress::LiveProgress;

pub struct TestNodeArgs {
    /// Package names or node types; empty = every package.
    pub targets: Vec<String>,
    /// Run only the test with this name.
    pub test: Option<String>,
    /// Tiers to run; empty = basic and fake (never live implicitly).
    pub tiers: Vec<TierArg>,
    /// Live: create an ephemeral pasted-key grant for each named
    /// service, deleted after the run. Each field comes from
    /// `WEFT_NODE_TEST_<SERVICE>_<FIELD>` when set, else a stdin
    /// prompt; never argv.
    pub key: Vec<String>,
    /// Live: use these exact grant ids, each `<service>=<grant id>`
    /// (repeatable; a bare `<grant id>` is accepted when the selected
    /// tests need exactly one service).
    pub connection: Vec<String>,
    /// Skip the live-run confirmation for this invocation.
    pub yes: bool,
    /// Concurrency: `None` = one at a time, `Some(0)` = all at once
    /// (bare `--parallel`), `Some(n)` = at most n in flight. Caps the
    /// package binaries running side by side, the tests in flight
    /// inside each (forwarded to the binary), and the live runs.
    pub parallel: Option<usize>,
}

/// The clap face of a tier choice; mapped to [`TestTier`] here so the
/// core type stays clap-free.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum TierArg {
    Basic,
    Fake,
    Live,
}

impl TierArg {
    fn tier(self) -> TestTier {
        match self {
            Self::Basic => TestTier::Basic,
            Self::Fake => TestTier::Fake,
            Self::Live => TestTier::Live,
        }
    }
}

/// One package to test, with the node types to keep when the targets
/// named node types of it rather than the package itself.
#[derive(Debug, PartialEq)]
struct TargetPackage {
    name: String,
    /// `None` = every node of the package.
    nodes: Option<BTreeSet<String>>,
}

impl TargetPackage {
    fn tests_node(&self, node_type: &str) -> bool {
        self.nodes.as_ref().is_none_or(|nodes| nodes.contains(node_type))
    }

    /// Fold one more target resolving to this package in: the whole
    /// package wins over any node of it.
    fn widen(&mut self, node: Option<String>) {
        match (&mut self.nodes, node) {
            (Some(nodes), Some(node)) => {
                nodes.insert(node);
            }
            (nodes, None) => *nodes = None,
            (None, Some(_)) => {}
        }
    }
}

/// One test's report, with the package it belongs to (the unit a
/// scripted sweep records and resumes by).
#[derive(Debug, serde::Serialize)]
struct PackageReport {
    package: String,
    #[serde(flatten)]
    report: TestReport,
}

pub async fn run(ctx: Ctx, args: TestNodeArgs) -> Result<()> {
    let project = ctx.project()?;
    let catalog = weft_compiler::build::build_project_catalog(&project.root)
        .map_err(|e| anyhow::anyhow!("discover nodes: {e}"))?;

    let targets = resolve_targets(&catalog, &args.targets)?;
    if targets.is_empty() {
        eprintln!(
            "no package in this project (nodes/ or src/) declares tests (a node declares tests in a \
             tests.rs next to its code)"
        );
        return Ok(());
    }

    // Requested tiers, deduped in order; no --tier means basic + fake
    // (live never runs implicitly).
    let mut tiers: Vec<TestTier> = Vec::new();
    for t in if args.tiers.is_empty() {
        vec![TierArg::Basic, TierArg::Fake]
    } else {
        args.tiers.clone()
    } {
        if !tiers.contains(&t.tier()) {
            tiers.push(t.tier());
        }
    }
    let live = tiers.contains(&TestTier::Live);
    let local: Vec<TestTier> = tiers.iter().copied().filter(|t| *t != TestTier::Live).collect();
    if !live && (!args.key.is_empty() || !args.connection.is_empty()) {
        bail!("--key and --connection only apply to the live tier; add --tier live");
    }

    // The cargo cache is machine-wide and lives outside the project,
    // so the engine compiles once per MACHINE rather than once per
    // project, and a project folder never grows a build cache. The
    // local tiers RUN the per-package binary, so they build it; the
    // live tier only needs each package's test LISTING, which
    // `cached_listing` answers without a build when the package's
    // content hash has not moved.
    let cache_root = node_test_cache_root();
    let cache_lock = node_test_cache_lock();
    bound_build_cache(&cache_root, &cache_lock, cache_cap_bytes()?)?;
    // Held, shared with every other run, until this one returns: its
    // build and its tests use the cache, and no wipe may take the folder
    // away meanwhile.
    let _using_cache = weft_core::file_lock::hold_shared(&cache_lock)
        .with_context(|| format!("lock {}", cache_lock.display()))?;
    let build_dirs =
        weft_compiler::build::TestBuildDirs::shared(&project.root, &cache_root);
    // Every package this run will test is emitted BEFORE any of them
    // builds, so cargo unifies features across them and compiles each
    // shared dependency once instead of once per feature union.
    let package_names: Vec<String> = targets.iter().map(|t| t.name.clone()).collect();
    let workspace =
        weft_compiler::build::prepare_test_workspace(&catalog, &package_names, &build_dirs)
            .map_err(|e| anyhow::anyhow!("{e}"))?;
    let mut reports: Vec<PackageReport> = Vec::new();
    // Packages whose binary could not be run at all (not a failing
    // test: a broken invocation), each with why.
    let mut broken: Vec<(String, anyhow::Error)> = Vec::new();
    if !local.is_empty() {
        // Every package compiles before any test runs: nothing to run
        // until the code builds, and cargo's own diagnostics have
        // already named whatever broke.
        let binaries = weft_compiler::build::build_node_test_binaries(&workspace)
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        let local_run = LocalRun { test_filter: args.test.as_deref(), tiers: &local, parallel: args.parallel };
        for (target, outcome) in targets.iter().zip(run_local_packages(&targets, &binaries, &local_run)) {
            match outcome {
                Ok(run) => reports.extend(run.into_iter().map(|report| PackageReport { package: target.name.clone(), report })),
                Err(e) => broken.push((target.name.clone(), e)),
            }
        }
    }
    // When a package's binary could not even run, the run has already
    // failed, so nothing goes on to spend money live.
    if live && broken.is_empty() {
        run_live(&ctx, &catalog, &targets, &workspace, &build_dirs, &args, &mut reports).await?;
    }

    render(&ctx, &targets, &reports, &broken)?;
    let failed = reports.iter().filter(|r| !r.report.passed).count();
    if !broken.is_empty() {
        let why: Vec<String> = broken.iter().map(|(package, e)| format!("  {package}: {e:#}")).collect();
        bail!(
            "the tests of {} package(s) could not run{}:\n{}",
            broken.len(),
            if failed > 0 { format!(", and {failed} of {} node tests failed", reports.len()) } else { String::new() },
            why.join("\n")
        );
    }
    if failed > 0 {
        bail!("{failed} of {} node tests failed", reports.len());
    }
    if reports.is_empty() {
        // A --test filter that matches nothing is an error (a typo'd
        // name must never pass green). A tier selection that finds
        // nothing is just "nothing to run": scripted sweeps hand every
        // package the same tiers, and a package with no live tests is
        // a normal state, not a failure.
        let tier_names: Vec<&str> = tiers.iter().map(|t| t.as_str()).collect();
        if let Some(f) = args.test.as_deref() {
            bail!(
                "no test named '{f}' in tier(s) {} on the targeted node(s)",
                tier_names.join(", ")
            );
        }
        if args.targets.is_empty() {
            eprintln!("no matching node tests declared (a node declares tests in its tests.rs)");
        } else {
            eprintln!(
                "{} declare(s) no tests in tier(s) {}: nothing to run",
                args.targets.iter().map(|t| format!("'{t}'")).collect::<Vec<_>>().join(", "),
                tier_names.join(", ")
            );
        }
    }
    Ok(())
}

/// `weft node-test-hash`: print the content hash naming a package's
/// test OUTCOME inputs (`node_test_cache_hash`: the package's own
/// sources plus the catalog's type registry; narrower than the image
/// tag, so an engine or image-recipe edit rebuilds the image but does
/// not move this hash). A scripted runner records it per package
/// after a fully green live run and skips packages whose hash has not
/// moved since; live tests cost money. With no target, prints
/// `<package> <hash>` for every test-declaring package.
pub fn hash(ctx: Ctx, target: Option<String>) -> Result<()> {
    let project = ctx.project()?;
    let catalog = weft_compiler::build::build_project_catalog(&project.root)
        .map_err(|e| anyhow::anyhow!("discover nodes: {e}"))?;
    match target {
        Some(t) => {
            // Accept a node type too, resolved to its package, so the
            // argument surface matches `weft test-node`.
            let targets = resolve_targets(&catalog, std::slice::from_ref(&t))?;
            let package = &targets.first().expect("resolve_targets errors instead of empty").name;
            let hash = weft_compiler::build::node_test_cache_hash(&catalog, package)
                .map_err(|e| anyhow::anyhow!("{e}"))?;
            println!("{hash}");
        }
        None => {
            for target in resolve_targets(&catalog, &[])? {
                let hash =
                    weft_compiler::build::node_test_cache_hash(&catalog, &target.name)
                        .map_err(|e| anyhow::anyhow!("{e}"))?;
                println!("{} {hash}", target.name);
            }
        }
    }
    Ok(())
}

/// The machine-wide node-test cache: one compiled engine for every
/// project on this machine, beside the rest of weft's own state rather
/// than inside anybody's project.
fn node_test_cache_root() -> PathBuf {
    super::daemon::data_dir().join("node-test-cache")
}

/// The lock every user of the node-test cache holds shared while it
/// builds and runs there, and a wipe holds alone. Beside the cache
/// rather than in it: a lock file inside would go with the wipe, and the
/// next run would lock a new file nobody else holds.
fn node_test_cache_lock() -> PathBuf {
    super::daemon::data_dir().join("node-test-cache.lock")
}

/// This project's own slice of the shared node-test cache (its emitted
/// crates and listings), the part `weft rm --local` reclaims.
pub fn project_cache_slice(project_root: &std::path::Path) -> PathBuf {
    weft_compiler::build::TestBuildDirs::shared(project_root, &node_test_cache_root()).crates_root
}

/// What the node-test cache takes on disk, for `weft clean --images`'s
/// size report: `None` when nothing has been built on this machine.
pub fn cache_size_bytes() -> Result<Option<u64>> {
    let root = node_test_cache_root();
    if !root.exists() {
        return Ok(None);
    }
    dir_size_bytes(&root)
        .map(Some)
        .with_context(|| format!("measure the node-test cache at {}", root.display()))
}

/// Throw the whole node-test cache away (`weft clean --build-cache`):
/// every project's emitted crates and the shared compiled engine. The
/// next `weft test-node` builds cold and re-makes all of it. Waits for
/// every `weft test-node` using the cache to finish first.
pub fn wipe_cache() -> Result<()> {
    let root = node_test_cache_root();
    if !root.exists() {
        return Ok(());
    }
    let lock = node_test_cache_lock();
    let _alone = weft_core::file_lock::hold(&lock, || println!("waiting for the weft test-node runs using it to end…"))
        .with_context(|| format!("lock {}", lock.display()))?;
    match std::fs::remove_dir_all(&root) {
        Err(e) if e.kind() != std::io::ErrorKind::NotFound => Err(e).with_context(|| format!("remove {}", root.display())),
        _ => Ok(()),
    }
}

/// The cap on the node-test cache, in bytes. Override with
/// `WEFT_TEST_CACHE_CAP_GB`; 0 disables the bound.
fn cache_cap_bytes() -> Result<u64> {
    // A full sweep of the 28-package stdlib settles at 1.7GB, so the
    // cap has to sit above that or it would wipe on every ordinary
    // run. 6GB leaves room for a project's own packages on top and
    // still trips long before accumulated junk is the user's problem.
    const DEFAULT_CAP_GB: u64 = 6;
    let Ok(raw) = std::env::var("WEFT_TEST_CACHE_CAP_GB") else {
        return Ok(DEFAULT_CAP_GB * 1024 * 1024 * 1024);
    };
    let gb: u64 = raw.trim().parse().with_context(|| {
        format!("WEFT_TEST_CACHE_CAP_GB must be a whole number of gigabytes, got '{raw}'")
    })?;
    Ok(gb * 1024 * 1024 * 1024)
}

/// Keep the node-test build cache under `cap_bytes`, wiping it whole
/// when it is over.
///
/// Cargo's target dir is append-only: an artifact's filename carries a
/// hash of its features, flags and dependency graph, so a build with
/// any of those changed writes a NEW file beside the old one and
/// nothing ever deletes the old one. Left alone it grows until the
/// disk is full (one project's cache reached 16GB before the cache
/// was shared and bounded). Under the cap the cache is untouched and
/// reruns stay fast; over it, one cold rebuild buys the space back.
/// `cap_bytes` of 0 disables the bound.
///
/// Another run building or testing in the cache holds `lock` shared,
/// so the wipe only tries for it alone and never waits: while anyone
/// uses the cache it is left over the cap this time, and a later run
/// that finds it free wipes it.
fn bound_build_cache(root: &std::path::Path, lock: &std::path::Path, cap_bytes: u64) -> Result<()> {
    if cap_bytes == 0 || !root.exists() {
        return Ok(());
    }
    // `weft clean --build-cache` may remove the whole cache between the
    // check above and the walk: nothing is left, so nothing is over the cap.
    let size = match dir_size_bytes(root) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound && !root.exists() => 0,
        measured => measured.with_context(|| format!("measure the node-test build cache at {}", root.display()))?,
    };
    let alone = if size > cap_bytes {
        weft_core::file_lock::try_hold(lock).with_context(|| format!("lock {}", lock.display()))?
    } else {
        None
    };
    let gb = |bytes: u64| bytes as f64 / 1024.0 / 1024.0 / 1024.0;
    match bound_decision(size, cap_bytes, alone.is_some()) {
        BoundDecision::Keep => {}
        BoundDecision::Busy => println!(
            "node-test build cache is {:.1}GB (cap {:.0}GB), but another weft test-node is using it; \
             leaving it for a later run to clear. The cap is WEFT_TEST_CACHE_CAP_GB.",
            gb(size),
            gb(cap_bytes),
        ),
        BoundDecision::Wipe => {
            println!(
                "node-test build cache is {:.1}GB (cap {:.0}GB); clearing it, so this run builds cold. \
                 The cap is WEFT_TEST_CACHE_CAP_GB.",
                gb(size),
                gb(cap_bytes),
            );
            std::fs::remove_dir_all(root).with_context(|| format!("remove {}", root.display()))?;
        }
    }
    Ok(())
}

/// What [`bound_build_cache`] does with the cache.
#[derive(Debug, PartialEq)]
enum BoundDecision {
    /// Under the cap (or no cap): leave it.
    Keep,
    /// Over the cap and nobody else is using it: wipe it.
    Wipe,
    /// Over the cap, but another run is using it: leave it this time.
    Busy,
}

/// `alone` says whether this run got the cache's lock to itself; it only
/// matters over the cap.
fn bound_decision(size: u64, cap_bytes: u64, alone: bool) -> BoundDecision {
    if cap_bytes == 0 || size <= cap_bytes {
        BoundDecision::Keep
    } else if alone {
        BoundDecision::Wipe
    } else {
        BoundDecision::Busy
    }
}

/// Total bytes of every file under `root`, following no symlinks (a
/// target dir symlinked onto another disk is still that disk's
/// problem, and following one could wander out of the tree). A file or
/// folder that vanishes mid-walk counts as nothing: another run's cargo
/// may be building in the tree, and it removes its temporary files.
pub(crate) fn dir_size_bytes(root: &std::path::Path) -> std::io::Result<u64> {
    let gone = |e: &std::io::Error| e.kind() == std::io::ErrorKind::NotFound;
    let mut total = 0;
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let entries = match std::fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(e) if gone(&e) && dir != root => continue,
            Err(e) => return Err(e),
        };
        for entry in entries {
            let entry = entry?;
            let meta = match entry.metadata() {
                Ok(meta) => meta,
                Err(e) if gone(&e) => continue,
                Err(e) => return Err(e),
            };
            if meta.is_dir() {
                stack.push(entry.path());
            } else if meta.is_file() {
                total += meta.len();
            }
        }
    }
    Ok(total)
}

/// Resolve the positional targets to packages, in the order first
/// named. A package name wins over a node type on the (unlikely)
/// collision, since the package is the build unit. Several targets in
/// one package fold into one entry: the package once, testing the named
/// node types, or all of it when the package itself was named.
///
/// A node that failed to load is left out of the catalog, and so out of
/// its package's test binary: its tests would never run while the rest
/// reads green, and a package whose every node failed is not in the
/// catalog at all. So what did not load refuses here, for every caller
/// (`weft test-node` and `weft node-test-hash`): under the targeted
/// package's folder when one is named, anywhere in the project for the
/// whole-project sweep, and listed beside "no such package" when the
/// name matches nothing (it may be one of them).
fn resolve_targets(catalog: &FsCatalog, targets: &[String]) -> Result<Vec<TargetPackage>> {
    let left_out = |within: Option<&std::path::Path>| -> Vec<String> {
        catalog
            .problems()
            .iter()
            .filter(|problem| within.is_none_or(|root| problem.error.paths().iter().any(|path| path.starts_with(root))))
            .map(|problem| format!("  {}", problem.error))
            .collect()
    };
    if targets.is_empty() {
        let broken = left_out(None);
        if !broken.is_empty() {
            bail!("some nodes of this project did not load, so their tests cannot run:\n{}", broken.join("\n"));
        }
        // The whole-project sweep covers exactly the packages that
        // declare tests (the catalog's own predicate, shared with
        // every other consumer): building a test binary for a
        // test-less package would be wasted compilation.
        let mut names: Vec<String> = Vec::new();
        for package in catalog.packages() {
            if catalog.package_declares_tests(package)? {
                names.push(package.name.clone());
            }
        }
        names.sort();
        return Ok(names.into_iter().map(|name| TargetPackage { name, nodes: None }).collect());
    }
    let mut resolved: Vec<TargetPackage> = Vec::new();
    for t in targets {
        let found = match catalog.packages().find(|p| p.name == *t) {
            Some(package) => Some((package, None)),
            None => catalog.package_of(t).map(|package| (package, Some(t.clone()))),
        };
        let Some((package, node)) = found else {
            let mut known: Vec<&str> = catalog.packages().map(|p| p.name.as_str()).collect();
            known.sort();
            let broken = left_out(None);
            let broken = if broken.is_empty() {
                String::new()
            } else {
                format!("; these did not load, and it may be one of them:\n{}", broken.join("\n"))
            };
            bail!(
                "'{t}' is neither a package nor a node type in this project (nodes/ or src/) \
                 (packages: {}){broken}",
                known.join(", ")
            );
        };
        let broken = left_out(Some(&package.root));
        if !broken.is_empty() {
            bail!(
                "some nodes of package '{}' did not load, so its tests cannot run (a package is \
                 built and tested whole):\n{}",
                package.name,
                broken.join("\n")
            );
        }
        match resolved.iter_mut().find(|r| r.name == package.name) {
            Some(existing) => existing.widen(node),
            None => resolved.push(TargetPackage { name: package.name.clone(), nodes: node.map(|n| BTreeSet::from([n])) }),
        }
    }
    Ok(resolved)
}

/// What every package's local run shares: the test filter, the tiers,
/// and the concurrency cap.
struct LocalRun<'a> {
    test_filter: Option<&'a str>,
    tiers: &'a [TestTier],
    parallel: Option<usize>,
}

/// Run every target package's local tiers through its built binary,
/// up to `--parallel` packages side by side, and give back each
/// package's outcome in `targets`' order.
///
/// Side by side, a package's own output (its binary's stderr: node
/// logs, tracing) is held until it finishes and printed as one block
/// under its name with its outcome line, so two packages never
/// interleave; one at a time, it streams straight through under its
/// name. Each binary is deleted once its package has run.
fn run_local_packages(
    targets: &[TargetPackage],
    binaries: &BTreeMap<String, PathBuf>,
    run: &LocalRun<'_>,
) -> Vec<Result<Vec<TestReport>>> {
    let lanes = concurrency_limit(run.parallel, targets.len()).min(targets.len());
    let side_by_side = lanes > 1;
    let several = targets.len() > 1;
    let next = std::sync::atomic::AtomicUsize::new(0);
    let outcomes: std::sync::Mutex<Vec<Option<Result<Vec<TestReport>>>>> =
        std::sync::Mutex::new(targets.iter().map(|_| None).collect());
    std::thread::scope(|scope| {
        for _ in 0..lanes {
            scope.spawn(|| loop {
                let i = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let Some(target) = targets.get(i) else { break };
                if several && !side_by_side {
                    eprintln!("\n──── node tests: {} ────", target.name);
                }
                let mut log = side_by_side.then(Vec::new);
                let outcome = binaries
                    .get(&target.name)
                    .with_context(|| format!("no test binary was built for package '{}'", target.name))
                    .and_then(|binary| {
                        let tested = run_local(binary, target, run, log.as_mut());
                        // The binary has served its purpose. It is ~110MB
                        // (mostly the line tables), it relinks in seconds
                        // from the dependency cache that stays, and keeping
                        // one per package held 3.2GB of the cache between
                        // runs for nothing.
                        weft_compiler::build::drop_built_binary(binary);
                        tested
                    });
                if let Some(log) = log {
                    use std::io::Write as _;
                    let mut err = std::io::stderr().lock();
                    let _ = writeln!(err, "\n──── node tests: {} ────", target.name);
                    let _ = err.write_all(&log);
                    let _ = writeln!(err, "{}", package_line(&target.name, &outcome_view(&outcome)));
                }
                outcomes.lock().expect("no lane panics while holding the outcomes")[i] = Some(outcome);
            });
        }
    });
    outcomes
        .into_inner()
        .expect("every lane has finished")
        .into_iter()
        .map(|outcome| outcome.expect("every package was taken by a lane"))
        .collect()
}

/// How one package's run went, as its outcome line tells it.
enum PackageOutcome {
    /// It ran: how many tests, how many failed.
    Ran { tests: usize, failed: usize },
    /// Its binary could not run at all.
    Broken,
}

impl PackageOutcome {
    fn of<'a>(reports: impl Iterator<Item = &'a TestReport>) -> Self {
        let (tests, failed) = reports.fold((0, 0), |(tests, failed), r| (tests + 1, failed + usize::from(!r.passed)));
        Self::Ran { tests, failed }
    }
}

fn outcome_view(outcome: &Result<Vec<TestReport>>) -> PackageOutcome {
    match outcome {
        Ok(reports) => PackageOutcome::of(reports.iter()),
        Err(_) => PackageOutcome::Broken,
    }
}

/// One package's outcome line. A passing package's line starts with
/// `✓ package <name>:`, which a scripted sweep reads to learn which
/// packages passed.
// SYNC: package_line <-> scripts/run-node-tests.sh (the `✓ package` lines)
fn package_line(package: &str, outcome: &PackageOutcome) -> String {
    match outcome {
        PackageOutcome::Ran { tests: 0, .. } => format!("✓ package {package}: no test in the selected tiers"),
        PackageOutcome::Ran { tests, failed: 0 } => format!("✓ package {package}: {tests} passed"),
        PackageOutcome::Ran { tests, failed } => format!("✗ package {package}: {failed} of {tests} failed"),
        PackageOutcome::Broken => format!("✗ package {package}: could not run (why is at the end)"),
    }
}

/// Run one package's local-tier (basic/fake) tests through its built
/// binary. The caller decides whether an empty result is an error.
/// `log`, when given, takes the binary's stderr instead of the
/// terminal.
fn run_local(
    binary: &PathBuf,
    target: &TargetPackage,
    run: &LocalRun<'_>,
    mut log: Option<&mut Vec<u8>>,
) -> Result<Vec<TestReport>> {
    // Unfiltered: one `run-all` invocation covers the package.
    if target.nodes.is_none() && run.test_filter.is_none() {
        let mut cmd: Vec<&str> = vec!["run-all"];
        for t in run.tiers {
            cmd.extend(["--tier", t.as_str()]);
        }
        let parallel_arg = run.parallel.map(|n| n.to_string());
        if let Some(n) = parallel_arg.as_deref() {
            cmd.extend(["--parallel", n]);
        }
        let out = run_binary(binary, &cmd, log)?;
        let parsed: RunAllReport =
            serde_json::from_str(&out.report).context("parse run-all report")?;
        return Ok(parsed.tests);
    }

    // Filtered: list, select, run each individually.
    let listing = list_tests(binary, log.as_deref_mut())?;
    let mut reports = Vec::new();
    for node in &listing {
        if !target.tests_node(&node.node_type) {
            continue;
        }
        for t in &node.tests {
            if run.test_filter.is_some_and(|f| f != t.name) {
                continue;
            }
            if !run.tiers.contains(&t.tier) {
                continue;
            }
            let out = run_binary(
                binary,
                &["run", "--node", &node.node_type, "--test", &t.name],
                log.as_deref_mut(),
            )?;
            reports.push(
                serde_json::from_str(&out.report).context("parse test report")?,
            );
        }
    }
    Ok(reports)
}

fn list_tests(binary: &PathBuf, log: Option<&mut Vec<u8>>) -> Result<Vec<NodeTestsListing>> {
    let out = run_binary(binary, &["list"], log)?;
    let parsed: TestListing =
        serde_json::from_str(&out.report).context("parse test listing")?;
    Ok(parsed.nodes)
}

/// Every targeted package's test listing, built at most ONCE for the
/// whole set: a live-only sweep must not pay a cargo build per
/// package just to learn "no live tests here". The listing is a pure
/// function of the package's code, so it is cached under the
/// project's cache slice keyed by `node_test_cache_hash` (the
/// package's sources plus the type registry; an engine edit changes
/// the binary but never the listing). A hash hit reads the stored
/// answer; every miss is collected first, then one workspace build
/// produces all the missing binaries, each is asked, and its answer
/// stored.
fn cached_listings(
    catalog: &FsCatalog,
    targets: &[TargetPackage],
    build_dirs: &weft_compiler::build::TestBuildDirs,
    workspace: &weft_compiler::build::TestWorkspace,
) -> Result<BTreeMap<String, Vec<NodeTestsListing>>> {
    #[derive(serde::Serialize, serde::Deserialize)]
    struct StoredListing {
        hash: String,
        nodes: Vec<NodeTestsListing>,
    }
    let mut listings = BTreeMap::new();
    // (package, its hash, where its listing is stored) for every
    // package whose stored listing is missing or stale.
    let mut stale: Vec<(String, String, PathBuf)> = Vec::new();
    for target in targets {
        if listings.contains_key(&target.name) {
            continue;
        }
        let hash = weft_compiler::build::node_test_cache_hash(catalog, &target.name)
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        let path = build_dirs.crates_root.join("listings").join(format!(
            "{}.json",
            weft_compiler::build::sanitize_crate_name(&target.name)
        ));
        // A stale, unreadable, or old-format entry is simply rebuilt
        // below; the file is a pure cache, never a source of truth.
        let stored = std::fs::read(&path)
            .ok()
            .and_then(|bytes| serde_json::from_slice::<StoredListing>(&bytes).ok())
            .filter(|stored| stored.hash == hash);
        match stored {
            Some(stored) => {
                listings.insert(target.name.clone(), stored.nodes);
            }
            None => stale.push((target.name.clone(), hash, path)),
        }
    }
    if stale.is_empty() {
        return Ok(listings);
    }
    let binaries = weft_compiler::build::build_node_test_binaries(workspace)
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    for (package, hash, path) in stale {
        let binary = binaries
            .get(&package)
            .with_context(|| format!("no test binary was built for package '{package}'"))?;
        let nodes = list_tests(binary, None)?;
        weft_compiler::build::drop_built_binary(binary);
        std::fs::create_dir_all(path.parent().expect("the listings path has a parent"))?;
        std::fs::write(
            &path,
            serde_json::to_vec(&StoredListing { hash, nodes: nodes.clone() })
                .expect("a listing serializes"),
        )?;
        listings.insert(package, nodes);
    }
    Ok(listings)
}

struct BinaryOutput {
    /// The runner's report line (its stdout protocol: one JSON
    /// document on the last sentinel-prefixed line, extracted through
    /// `report_line`).
    report: String,
}

/// Run the test binary; stdout is the JSON channel, and stderr (node
/// logs, tracing) streams through to the terminal, or is appended to
/// `log` when one is given. A non-zero exit with parseable stdout is NOT
/// an error here (a failing test reports through the JSON); a broken
/// invocation surfaces via empty stdout.
fn run_binary(binary: &PathBuf, args: &[&str], log: Option<&mut Vec<u8>>) -> Result<BinaryOutput> {
    let stderr = if log.is_some() { std::process::Stdio::piped() } else { std::process::Stdio::inherit() };
    let out = std::process::Command::new(binary)
        .args(args)
        .stderr(stderr)
        .output()
        .with_context(|| format!("run {}", binary.display()))?;
    if let Some(log) = log {
        log.extend_from_slice(&out.stderr);
    }
    let stdout = String::from_utf8_lossy(&out.stdout);
    let Some(report) = report_line(&stdout) else {
        bail!(
            "{} {} produced no output (exit {})",
            binary.display(),
            args.join(" "),
            out.status
        );
    };
    Ok(BinaryOutput { report: report.to_string() })
}

fn render(ctx: &Ctx, targets: &[TargetPackage], reports: &[PackageReport], broken: &[(String, anyhow::Error)]) -> Result<()> {
    if ctx.json_out(&reports)? {
        return Ok(());
    }
    // Per-test lines for the LOCAL tiers only: a live test already
    // printed its line the moment it finished (the progress block), so
    // repeating it here would double every result in the scrollback.
    for r in reports.iter().map(|r| &r.report).filter(|r| r.tier != TestTier::Live) {
        let mark = if r.passed { "✓" } else { "✗" };
        println!("{mark} {} / {} ({})", r.node, r.test, r.tier.as_str());
    }
    // Then every failure in full, so a red run ends with the errors on
    // screen instead of scrolled away behind other tests' output.
    let failed: Vec<&TestReport> = reports.iter().map(|r| &r.report).filter(|r| !r.passed).collect();
    if !failed.is_empty() {
        println!("\n──── failures ────");
        for r in &failed {
            println!(
                "✗ {} / {} ({})\n  {}",
                r.node,
                r.test,
                r.tier.as_str(),
                r.error.as_deref().unwrap_or("no error message").replace('\n', "\n  ")
            );
            if !r.run_ids.is_empty() {
                println!(
                    "  live run cost is recorded under run(s): {}",
                    r.run_ids.join(", ")
                );
            }
        }
    }
    // With several packages, one line each, every tier counted, so a
    // red run says which packages to look at.
    if targets.len() > 1 {
        println!();
        for target in targets {
            let outcome = if broken.iter().any(|(package, _)| *package == target.name) {
                PackageOutcome::Broken
            } else {
                PackageOutcome::of(reports.iter().filter(|r| r.package == target.name).map(|r| &r.report))
            };
            println!("{}", package_line(&target.name, &outcome));
        }
    }
    // The failure count line is the caller's bail (the process error),
    // so it prints exactly once.
    if !reports.is_empty() && failed.is_empty() && broken.is_empty() {
        println!("\nall {} node tests passed", reports.len());
    }
    Ok(())
}

// ----- Live tier ------------------------------------------------------

/// One live test to run, with its resolved package + service.
struct LiveRun {
    package: String,
    node: String,
    test: String,
    service: String,
}

async fn run_live(
    ctx: &Ctx,
    catalog: &FsCatalog,
    targets: &[TargetPackage],
    workspace: &weft_compiler::build::TestWorkspace,
    build_dirs: &weft_compiler::build::TestBuildDirs,
    args: &TestNodeArgs,
    reports: &mut Vec<PackageReport>,
) -> Result<()> {
    // Discover the live tests per target package (through each
    // package's listing: the registry is the single source, read via
    // the hash-keyed cache so unchanged packages skip the build).
    let mut runs: Vec<LiveRun> = Vec::new();
    // Declared fixtures each selected test needs, validated up front
    // against the environment: every miss is collected and reported at
    // once, BEFORE any consent prompt, grant, or run. (The test server's
    // `rig.fixture` read stays the runtime backstop.)
    let mut missing_fixtures: Vec<String> = Vec::new();
    let listings = cached_listings(catalog, targets, build_dirs, workspace)?;
    for target in targets {
        for node in &listings[&target.name] {
            if !target.tests_node(&node.node_type) {
                continue;
            }
            for t in &node.tests {
                if t.tier != TestTier::Live {
                    continue;
                }
                if args.test.as_deref().is_some_and(|f| f != t.name) {
                    continue;
                }
                let service = t.service.clone().with_context(|| {
                    format!("live test {}/{} declares no service", node.node_type, t.name)
                })?;
                for fixture in &t.fixtures {
                    if !fixture.required {
                        continue;
                    }
                    let var = format!("WEFT_NODE_TEST_{}", fixture.name);
                    if std::env::var(&var).map_or(true, |v| v.is_empty()) {
                        missing_fixtures.push(format!(
                            "  {var}  ({}/{}: {})",
                            node.node_type,
                            t.name,
                            fixture.label.as_deref().unwrap_or(&fixture.name)
                        ));
                    }
                }
                runs.push(LiveRun {
                    package: target.name.clone(),
                    node: node.node_type.clone(),
                    test: t.name.clone(),
                    service,
                });
            }
        }
    }
    if runs.is_empty() {
        // Nothing selected: the caller's empty-report check decides
        // whether an explicit filter matching nothing is an error.
        return Ok(());
    }
    if !missing_fixtures.is_empty() {
        bail!(
            "the selected live tests declare fixtures that are not set in the \
             environment:\n{}\nset each variable (the repo .env for scripted runs) and \
             re-run",
            missing_fixtures.join("\n")
        );
    }
    let client = ctx.client()?;
    // The test server is compiled here and runs as a program on this
    // machine, which only this machine's own install starts programs on;
    // and what the tested nodes need while they run is on this machine.
    // Both settled before any consent, grant or run.
    let Some(install) = crate::credentials::this_machines_install(client.base())? else {
        bail!(
            "live node tests run on this machine's own install: the test server is compiled on this machine and \
             runs here as a program, which {} cannot start. Run them without `--on` or `--dispatcher`, from a \
             folder whose install is this machine's (`weft daemon which` says which).",
            client.base()
        );
    };
    refuse_unmet_needs(catalog, &runs).await?;
    ensure_live_consent(args)?;

    let project_id = super::resolve_project_id(ctx, None)?;

    // Resolve every service's connection in TWO phases: first a pure
    // plan (validations + grant lookups, no side effects), then the
    // one side-effecting step (the ephemeral grant), so a bail during
    // planning can never leave a pasted credential behind.
    enum Planned {
        Existing(uuid::Uuid),
        NeedsEphemeral,
    }
    let services: std::collections::BTreeSet<String> =
        runs.iter().map(|r| r.service.clone()).collect();
    for k in &args.key {
        if !services.contains(k) {
            bail!(
                "--key names service '{k}', but the selected live tests need: {}",
                services.iter().cloned().collect::<Vec<_>>().join(", ")
            );
        }
        if args.key.iter().filter(|other| *other == k).count() > 1 {
            bail!("--key names service '{k}' twice");
        }
    }
    // `--connection <service>=<id>` pins one service's grant; a bare
    // `<id>` is accepted only when exactly one service is in play.
    let mut pinned: BTreeMap<String, uuid::Uuid> = BTreeMap::new();
    for raw in &args.connection {
        let (service, id) = match raw.split_once('=') {
            Some((s, id)) => (s.to_string(), id.to_string()),
            None if services.len() == 1 => {
                (services.iter().next().cloned().expect("one service"), raw.clone())
            }
            None => bail!(
                "--connection '{raw}' is ambiguous across several services ({}); \
                 qualify it as --connection <service>=<grant id>",
                services.iter().cloned().collect::<Vec<_>>().join(", ")
            ),
        };
        if !services.contains(&service) {
            bail!(
                "--connection names service '{service}', but the selected live tests \
                 need: {}",
                services.iter().cloned().collect::<Vec<_>>().join(", ")
            );
        }
        if args.key.contains(&service) {
            bail!(
                "--key and --connection both answer how to sign '{service}'; pick one \
                 (an ephemeral pasted key, or the existing connection)"
            );
        }
        // Typed HERE, where the flag is read: everything downstream
        // (the plan, the run requests) carries a real Uuid.
        let id: uuid::Uuid = id
            .parse()
            .with_context(|| format!("--connection {id} is not a connection id (a UUID)"))?;
        if pinned.insert(service.clone(), id).is_some() {
            bail!("--connection names service '{service}' twice");
        }
    }
    let mut plan: BTreeMap<String, Planned> = BTreeMap::new();
    for service in &services {
        let planned = if args.key.contains(service) {
            Planned::NeedsEphemeral
        } else if let Some(conn) = pinned.get(service) {
            // Validate NOW, in the plan phase: a typo'd or
            // wrong-service grant id must fail before any side effect
            // (and before a run spends money discovering it).
            require_grant_for_service(ctx, service, *conn).await?;
            Planned::Existing(*conn)
        } else {
            match sole_grant_for_service(ctx, service).await? {
                Some(id) => Planned::Existing(id),
                // No grant at all: environment-provided key fields
                // (the scripted stand-in for --key) promote to an
                // ephemeral grant; otherwise name every way in.
                None if env_key_present(service) => Planned::NeedsEphemeral,
                None => bail!(
                    "no connection for service '{service}'; connect one in the editor, \
                     pass --key {service} to paste a throwaway key for this run, or set \
                     WEFT_NODE_TEST_{}_<FIELD> in the environment",
                    env_component(service)
                ),
            }
        };
        plan.insert(service.clone(), planned);
    }

    // Every WEFT_NODE_TEST_* variable rides to the test as
    // a live fixture (`LiveRig::fixture`): values a test cannot
    // self-provision, like the chat id a bot may message. EXCEPT the
    // key variables: a credential field reaches the test through the
    // grant via the broker, never as env, so every catalog service's
    // key variables are excluded (not just this run's; a scripted
    // sweep exports the whole .env).
    let key_vars: std::collections::BTreeSet<String> = catalog
        .all()
        .into_iter()
        .filter_map(|m| m.service.as_ref())
        .flat_map(|spec| {
            let service = spec.service.clone();
            key_fields(spec)
                .into_iter()
                .map(move |f| key_env_var(&service, &f.name))
        })
        .collect();
    let fixtures: BTreeMap<String, String> = std::env::vars()
        .filter(|(k, v)| {
            k.starts_with("WEFT_NODE_TEST_") && !v.is_empty() && !key_vars.contains(k)
        })
        .collect();

    // ALL prompting happens HERE, before any grant exists and before
    // any signal handler is armed: the credential prompts keep the
    // default die-on-Ctrl+C, and a Ctrl+C at any prompt leaks nothing
    // because nothing has been created yet.
    let mut connections: BTreeMap<String, uuid::Uuid> = BTreeMap::new();
    let mut prepared: Vec<(String, PreparedKey)> = Vec::new();
    for (service, planned) in plan {
        match planned {
            Planned::Existing(id) => {
                connections.insert(service, id);
            }
            Planned::NeedsEphemeral => {
                let key = prepare_ephemeral_key(catalog, &service)?;
                prepared.push((service, key));
            }
        }
    }

    // The prompts are done: NOW create the grants and run, both inside
    // one Ctrl+C select, so from the instant the first grant exists no
    // exit (a later creation failing, the run failing, Ctrl+C at any
    // point) can skip the cleanup loop below. Every grant whose id
    // came back is deleted unconditionally, and a failed delete names
    // the id and the recovery. (The gap: a create interrupted or
    // unparsed IN FLIGHT may have stored a grant whose id nobody
    // holds; only `weft connect --list` can find it, and the interrupt
    // message says to check it.)
    let mut ephemeral_grants: Vec<uuid::Uuid> = Vec::new();
    let result = if prepared.is_empty() {
        run_live_tests(ctx, &install, catalog, &project_id, &runs, &connections, &fixtures, args.parallel, reports).await
    } else {
        let create_and_run = async {
            for (service, key) in prepared {
                let id = create_ephemeral_grant(ctx, key).await?;
                ephemeral_grants.push(id);
                connections.insert(service, id);
            }
            run_live_tests(ctx, &install, catalog, &project_id, &runs, &connections, &fixtures, args.parallel, reports).await
        };
        tokio::select! {
            r = create_and_run => r,
            _ = tokio::signal::ctrl_c() => Err(anyhow::anyhow!(
                "interrupted (Ctrl+C); any test already started keeps running, and a \
                 connection being stored at that moment may have survived: check \
                 `weft connect --list`"
            )),
        }
    };
    for grant_id in ephemeral_grants {
        // Bounded: a hung delete must not keep a finished run (and its
        // WARNING) from ever reaching the terminal.
        let outcome = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            super::connect::forget_grant(&client, grant_id),
        )
        .await;
        let failure = match outcome {
            Ok(Ok(())) => None,
            Ok(Err(e)) => Some(e.to_string()),
            Err(_) => Some("timed out after 10s".to_string()),
        };
        if let Some(e) = failure {
            eprintln!(
                "WARNING: could not delete the ephemeral key grant {grant_id}: {e}\n\
                 remove it with `weft connect --forget {grant_id} --yes`"
            );
        }
    }
    result
}

/// The environment variable a `<service>` credential field reads in a
/// scripted run: `WEFT_NODE_TEST_<SERVICE>_<FIELD>`.
fn key_env_var(service: &str, field: &str) -> String {
    format!("WEFT_NODE_TEST_{}_{}", env_component(service), env_component(field))
}

/// Every credential field a `<service>` key can carry: the paste
/// variant's, plus the fields of every way to sign in the spec itself
/// offers (either set may be absent).
fn key_fields(spec: &AccessSpec) -> Vec<CredentialField> {
    let ways_of = |spec: &AccessSpec| spec.ways().iter().flat_map(|w| w.fields.clone()).collect::<Vec<_>>();
    let mut fields: Vec<CredentialField> = spec.paste_variant().as_ref().map(ways_of).unwrap_or_default();
    fields.extend(ways_of(spec));
    fields
}

/// The way to sign in a `<service>` key fills: the service's only way,
/// else, for a scripted run, the one way whose fields the environment
/// sets, else the one the person picks.
fn key_way<'s>(spec: &'s AccessSpec, service: &str, env_driven: bool) -> Result<&'s LoginWay> {
    let ways = spec.ways();
    if let [only] = ways {
        return Ok(only);
    }
    if env_driven {
        let set: Vec<&LoginWay> = ways
            .iter()
            .filter(|w| w.fields.iter().any(|f| std::env::var(key_env_var(service, &f.name)).is_ok_and(|v| !v.is_empty())))
            .collect();
        return match set.as_slice() {
            [one] => Ok(one),
            [] => bail!("the environment sets no field of any of '{service}'s ways to sign in"),
            _ => bail!(
                "the environment sets fields of several of '{service}'s ways to sign in ({}); set only one way's",
                set.iter().map(|w| w.id.as_str()).collect::<Vec<_>>().join(", ")
            ),
        };
    }
    let menu: String = ways.iter().enumerate().map(|(i, w)| format!("\n  [{}] {}", i + 1, w.display_label())).collect();
    let choice = crate::prompt::prompt_line(
        &format!("How does the '{service}' key sign in?{menu}\nWay [1-{}]: ", ways.len()),
        &format!("one way's {} variables in the environment", key_env_var(service, "<field>")),
    )?;
    choice
        .trim()
        .parse::<usize>()
        .ok()
        .and_then(|n| n.checked_sub(1))
        .and_then(|i| ways.get(i))
        .with_context(|| format!("'{}' is not one of the ways; answer 1 to {}", choice.trim(), ways.len()))
}

/// True when any `WEFT_NODE_TEST_<SERVICE>_*` variable is set: the
/// scripted stand-in for `--key`, promoted to an ephemeral grant whose
/// fields read those variables.
fn env_key_present(service: &str) -> bool {
    let prefix = format!("WEFT_NODE_TEST_{}_", env_component(service));
    std::env::vars().any(|(k, _)| k.starts_with(&prefix))
}

/// A service or field name as an environment-variable component:
/// uppercased, every non-alphanumeric collapsed to `_`.
fn env_component(name: &str) -> String {
    name.chars()
        .map(|c| if c.is_ascii_alphanumeric() { c.to_ascii_uppercase() } else { '_' })
        .collect()
}

/// Plan-phase check that `grant_id` exists AND answers `service`:
/// the run request would accept any string and fail deep inside the
/// process otherwise. Uses the service-scoped list (the API's read
/// surface for grants), so a wrong-service id fails naming both.
async fn require_grant_for_service(ctx: &Ctx, service: &str, grant_id: uuid::Uuid) -> Result<()> {
    let grants = super::connect::list_grants(&ctx.client()?, super::connect::Doorway::Owner, Some(service)).await?;
    if !grants.iter().any(|g| g.id == grant_id) {
        bail!(
            "--connection {grant_id} is not a grant for service '{service}'; list the \
             service's connections with `weft connect --list` (or run without \
             --connection to pick the only one automatically)"
        );
    }
    Ok(())
}

/// Returns `Ok` only once the user has consented to live spending,
/// persisting the never-ask-again choice when accepted.
fn ensure_live_consent(args: &TestNodeArgs) -> Result<()> {
    // --yes first: an explicit flag must not be blocked by an
    // unrelated problem in the settings file it makes irrelevant.
    if args.yes {
        return Ok(());
    }
    let mut config = crate::user_config::load()?;
    if config.skip_live_test_confirmation {
        return Ok(());
    }
    let ok = crate::prompt::confirm(
        "Live tests call the real provider with a real credential and can spend money. \
         Run them? Type 'yes' to continue: ",
        "--yes",
    )?;
    if !ok {
        bail!("live run declined");
    }
    // This prompt is only reachable interactively (the first confirm
    // bails on a non-tty), so its hint names the persistent knob a
    // scripted user would set, not a flag that cannot reach it.
    if crate::prompt::confirm(
        "Skip this confirmation from now on? Type 'yes' to never ask again: ",
        "skip_live_test_confirmation = true in ~/.config/weft/cli.toml",
    )? {
        config.skip_live_test_confirmation = true;
        crate::user_config::save(&config)?;
    }
    Ok(())
}

/// The id of the tenant's sole grant for `service`: exactly one is
/// unambiguous; zero is `None` (the caller knows the other ways in);
/// several name the fix.
async fn sole_grant_for_service(ctx: &Ctx, service: &str) -> Result<Option<uuid::Uuid>> {
    let grants = super::connect::list_grants(&ctx.client()?, super::connect::Doorway::Owner, Some(service)).await?;
    match grants.as_slice() {
        [] => Ok(None),
        [only] => Ok(Some(only.id)),
        many => {
            let list: Vec<String> = many
                .iter()
                .map(|g| {
                    format!(
                        "  {}  {}",
                        g.id,
                        g.identity
                            .as_deref()
                            .or(g.label.as_deref())
                            .unwrap_or("(unlabeled)")
                    )
                })
                .collect();
            bail!(
                "several connections exist for '{service}'; pick one with \
                 --connection <id>:\n{}",
                list.join("\n")
            );
        }
    }
}

/// One service's fully-prompted ephemeral-grant ingredients, resolved
/// BEFORE any grant exists: the prompting (with its default Ctrl+C
/// behavior) runs outside the signal-guarded create-and-run window.
/// Fields are prompted on stdin, NEVER read from argv (shell history).
struct PreparedKey {
    spec: AccessSpec,
    values: BTreeMap<String, String>,
    paste: bool,
    /// The way to sign in the values fill, for a pasted-credential
    /// service (`None` for the paste section, whose one way is implied).
    way: Option<String>,
}

fn prepare_ephemeral_key(catalog: &FsCatalog, service: &str) -> Result<PreparedKey> {
    let spec = catalog
        .all()
        .into_iter()
        .find_map(|m| m.service.as_ref().filter(|s| s.service == service))
        .with_context(|| {
            format!("no node in this project declares the '{service}' service recipe")
        })?;

    // The paste surface: a consent service's ownPage.paste fields, or
    // a static (key-paste) service's own declared fields.
    // Once ANY field for this service comes from the environment the
    // whole grant is env-driven: a required gap fails loud naming the
    // variable and an optional gap is skipped, never prompted (a
    // scripted sweep must not stop on stdin).
    let env_driven = env_key_present(service);
    let (fields, paste_flag, way) = match spec.paste_variant() {
        Some(variant) => (variant.ways().iter().flat_map(|w| w.fields.clone()).collect::<Vec<_>>(), true, None),
        None if !spec.ways().is_empty() => {
            let way = key_way(spec, service, env_driven)?;
            (way.fields.clone(), false, Some(way.id.clone()))
        }
        None => bail!(
            "service '{service}' has no paste flow (it connects through a sign-in); \
             connect it in the editor and re-run without --key"
        ),
    };
    let mut values: BTreeMap<String, String> = BTreeMap::new();
    for field in &fields {
        let label = field.label.as_deref().unwrap_or(&field.name);
        let hint = if field.optional { " (optional, Enter to skip)" } else { "" };
        let env_var = key_env_var(service, &field.name);
        let value = match std::env::var(&env_var) {
            Ok(v) if !v.is_empty() => v,
            _ if env_driven => {
                if field.optional {
                    continue;
                }
                bail!(
                    "'{label}' is required for a '{service}' key; set {env_var} in the \
                     environment"
                );
            }
            // An accidental empty Enter on a required field re-asks
            // that one field instead of discarding everything already
            // pasted.
            _ => loop {
                let value = crate::prompt::prompt_line(
                    &format!("{label}{hint}: "),
                    &format!("{env_var} in the environment for scripted runs"),
                )?;
                if !value.is_empty() || field.optional {
                    break value;
                }
                eprintln!("'{label}' is required for a '{service}' key (Ctrl+C to abort)");
            },
        };
        if value.is_empty() {
            continue;
        }
        values.insert(field.name.clone(), value);
    }
    Ok(PreparedKey { spec: spec.clone(), values, paste: paste_flag, way })
}

/// The side-effecting half: store the prepared credential as an
/// ephemeral grant. No prompting in here, so it can run inside the
/// Ctrl+C select.
async fn create_ephemeral_grant(ctx: &Ctx, key: PreparedKey) -> Result<uuid::Uuid> {
    let grant = super::connect::connect_direct(
        &ctx.client()?,
        super::connect::Doorway::Owner,
        ConnectDirect {
            spec: key.spec,
            door: Door::Own,
            values: key.values,
            label: Some("weft test-node (ephemeral)".into()),
            permissions: Vec::new(),
            registration: None,
            paste: key.paste,
            way: key.way,
            project_id: None,
            instance: None,
        },
        None,
    )
    .await
    .context("create the ephemeral key grant")?;
    Ok(grant.id)
}

async fn run_live_tests(
    ctx: &Ctx,
    install: &weft_core::infra::Install,
    catalog: &FsCatalog,
    project_id: &str,
    runs: &[LiveRun],
    connections: &BTreeMap<String, uuid::Uuid>,
    fixtures: &BTreeMap<String, String>,
    parallel: Option<usize>,
    reports: &mut Vec<PackageReport>,
) -> Result<()> {
    let client = ctx.client()?;
    let project = ctx.project()?;

    // One test server per involved package, content-addressed and
    // skipped when already present, by its ref.
    let mut servers: BTreeMap<String, String> = BTreeMap::new();
    let packages: std::collections::BTreeSet<&str> =
        runs.iter().map(|r| r.package.as_str()).collect();
    for package in packages {
        servers.insert(
            package.to_string(),
            ensure_test_server(install, project, catalog, package).await?,
        );
    }

    // Each run is its own call with its own run identity, so runs
    // interleave freely; the index restores declaration order in the
    // report. A run that itself breaks (not a failing test) surfaces
    // after the in-flight batch settles.
    let limit = concurrency_limit(parallel, runs.len());
    // The progress block's lifetime is a SCOPE, not a pair of
    // statements: the caller's Ctrl+C select drops this whole future,
    // and the display must still stop ticking and erase itself before
    // the grant-cleanup messages print (a detached ticker would keep
    // repainting over the one WARNING that matters). The guard's Drop
    // runs on the normal path and on cancellation alike.
    struct ProgressSession {
        progress: std::sync::Arc<LiveProgress>,
        ticker: tokio::task::JoinHandle<()>,
    }
    impl Drop for ProgressSession {
        fn drop(&mut self) {
            // abort() is best-effort (a ticker mid-render finishes its
            // pass); clear() latches the display DONE, so even that
            // last pass paints nothing.
            self.ticker.abort();
            self.progress.clear();
        }
    }
    let progress = std::sync::Arc::new(LiveProgress::new(
        &runs.iter().map(|r| (r.package.clone(), r.test.clone())).collect::<Vec<_>>(),
    ));
    // A 1s ticker keeps the bars' elapsed times moving between events.
    let session = ProgressSession {
        progress: progress.clone(),
        ticker: tokio::spawn({
            let progress = progress.clone();
            async move {
                loop {
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    progress.tick();
                }
            }
        }),
    };
    use futures::StreamExt as _;
    let settled: Vec<(usize, Result<TestReport>)> =
        futures::stream::iter(runs.iter().enumerate())
            .map(|(i, run)| {
                // A broken resolve invariant here must fail the RUN,
                // never panic: the caller still has ephemeral grants
                // to delete after the batch settles.
                let image = servers.get(&run.package).cloned().with_context(|| {
                    format!("no test server resolved for package '{}'", run.package)
                });
                let connection = connections.get(&run.service).cloned().with_context(|| {
                    format!("no connection resolved for service '{}'", run.service)
                });
                let client = &client;
                let progress = &progress;
                async move {
                    let outcome = match (image, connection) {
                        (Ok(image), Ok(connection)) => {
                            run_one_live_test(
                                client, project_id, run, &image, connection, fixtures, progress,
                            )
                            .await
                        }
                        (Err(e), _) | (_, Err(e)) => Err(e),
                    };
                    if let Err(e) = &outcome {
                        // The RUN broke (not a failing test): the bar
                        // still records the slot so counts stay honest
                        // while the error propagates after the batch.
                        progress.finished(&run.package, &run.test, false, Some(&e.to_string()));
                    }
                    (i, outcome)
                }
            })
            .buffer_unordered(limit)
            .collect()
            .await;
    drop(session);
    let mut settled = settled;
    settled.sort_by_key(|(i, _)| *i);
    for (i, outcome) in settled {
        reports.push(PackageReport { package: runs[i].package.clone(), report: outcome? });
    }
    Ok(())
}

/// Start one live test and poll it to terminal. No deadline: a
/// live test may take as long as the provider takes; Ctrl+C is the way
/// out, and a periodic breadcrumb keeps a long wait legible. A status
/// this CLI does not know is a loud contract break, never an endless
/// silent wait.
async fn run_one_live_test(
    client: &crate::client::DispatcherClient,
    project_id: &str,
    run: &LiveRun,
    image: &str,
    connection: uuid::Uuid,
    fixtures: &BTreeMap<String, String>,
    progress: &LiveProgress,
) -> Result<TestReport> {
    progress.started(&run.package, &run.test);
    let resp = client
        .post_json(
            &format!("/projects/{project_id}/node-tests/run"),
            &serde_json::to_value(weft_core::node_test::RunNodeTestRequest {
                image_ref: image.to_string(),
                node: run.node.clone(),
                test: run.test.clone(),
                live_connection: Some(connection.to_string()),
                fixtures: fixtures.clone(),
            })?,
        )
        .await
        .context("start the node-test run")?;
    let task_id = serde_json::from_value::<weft_core::node_test::RunNodeTestResponse>(resp)
        .context("read the started node-test run")?
        .task_id;

    let started = std::time::Instant::now();
    let mut last_breadcrumb = std::time::Instant::now();
    let report = loop {
        // Held by the dispatcher until the run finishes or the hold runs
        // out, so the loop asks again at once either way. A look the
        // install did not answer is said and looked at again.
        let looked = client
            .waiting_up_to(RUN_HOLD)
            .get_json(&format!("/projects/{project_id}/node-tests/runs/{task_id}?wait_ms={}", RUN_HOLD.as_millis()))
            .await;
        let status = match looked {
            Ok(status) => status,
            Err(e) => {
                let why = crate::commands::ensure::look_again_after(e).map_err(|e| e.context("wait on the node-test run"))?;
                progress.note(&format!("could not look at the node-test run just now ({why}); looking again"));
                tokio::time::sleep(crate::commands::follow::TASK_LOOK_AGAIN_AFTER).await;
                continue;
            }
        };
        let status: weft_core::node_test::NodeTestRunStatus = serde_json::from_value(status).context("read the node-test run's status")?;
        match status.status {
            TaskStatus::Complete => break status.report.context("completed without a report")?,
            TaskStatus::Failed => bail!(
                "the node-test run itself failed (not the test): {}",
                status.error.as_deref().unwrap_or("unknown")
            ),
            in_progress @ (TaskStatus::Pending | TaskStatus::Claimed) => {
                if last_breadcrumb.elapsed() >= std::time::Duration::from_secs(15) {
                    last_breadcrumb = std::time::Instant::now();
                    progress.note(&format!(
                        "still running {} / {}: task {task_id} is {in_progress} \
                         ({}s elapsed); Ctrl+C to stop waiting",
                        run.node,
                        run.test,
                        started.elapsed().as_secs(),
                    ));
                }
            }
        }
    };
    progress.finished(&run.package, &run.test, report.passed, report.error.as_deref());
    Ok(report)
}

/// How long one wait on a node-test run is held by the dispatcher
/// before the CLI asks again, and so how often a still-running run gets
/// its breadcrumb checked.
const RUN_HOLD: std::time::Duration = std::time::Duration::from_secs(15);

/// Refuse the live runs `runs` when this machine lacks what one of the
/// node types they test needs while it runs (`weft_platform_local::LocalNeeds`,
/// the check an install makes before it starts a program), naming the
/// `weft deps-install` that installs it.
async fn refuse_unmet_needs(catalog: &FsCatalog, runs: &[LiveRun]) -> Result<()> {
    use weft_platform_traits::MachineNeeds as _;
    let mut wanted = Vec::new();
    let packages: std::collections::BTreeSet<&str> = runs.iter().map(|r| r.package.as_str()).collect();
    for package in packages {
        let needs = weft_compiler::build::package_run_needs(catalog, package).map_err(|e| anyhow::anyhow!("{e}"))?;
        wanted.extend(tested_needs(&needs, runs.iter().filter(|r| r.package == package).map(|r| r.node.as_str())));
    }
    if wanted.is_empty() {
        return Ok(());
    }
    let needs = weft_platform_local::LocalNeeds;
    let missing = needs.missing(&wanted).await?;
    if missing.is_empty() {
        return Ok(());
    }
    bail!("{}", weft_core::needs::refusal(&missing, needs.needs_from()))
}

/// The needs in `needs` of the node types `tested` names, one row per need,
/// each naming its node type as the node a test runs.
fn tested_needs<'a>(needs: &weft_core::needs::NodeNeeds, tested: impl Iterator<Item = &'a str>) -> Vec<weft_core::needs::NodeNeed> {
    let tested: std::collections::BTreeSet<&str> = tested.collect();
    needs
        .iter()
        .filter(|(node_type, _)| tested.contains(node_type.as_str()))
        .flat_map(|(node_type, list)| {
            list.iter().map(|need| weft_core::needs::NodeNeed { node_type: node_type.clone(), nodes: vec![node_type.clone()], need: need.clone() })
        })
        .collect()
}

/// Make the package's test server a program on this machine
/// (`weft_platform_local::images::Program::NodeTestServer`), in the
/// machine's workers folder where the install's runner starts it from:
/// compiled here, in a free compile lane, when its content is not there
/// yet, its log under the install's build logs. Answers its ref,
/// `weft-node-tests:<test hash>`.
async fn ensure_test_server(
    install: &weft_core::infra::Install,
    project: &weft_compiler::project::Project,
    catalog: &FsCatalog,
    package: &str,
) -> Result<String> {
    use weft_platform_local::host_build;
    let hash = weft_compiler::build::node_test_content_hash(catalog, package)
        .map_err(|e| anyhow::anyhow!("the test server's hash for '{package}': {e}"))?;
    let image = weft_compiler::build::node_test_image_tag(&hash);
    let machine = weft_platform_local::MachineLayout::here();
    let workers = machine.workers();
    let program = weft_platform_local::images::Program::of(&image)
        .with_context(|| format!("{image} names no test server (`weft_core::builds::NODE_TEST_REPO`)"))?;
    let dir = program.dir(&workers);
    let file = program.file(&workers);
    let owner = test_server_owner(project, package);
    let servers = machine.node_test_servers();
    // Its folder is named by the package's content, so another project may
    // have compiled it already: claimed under the owners' lock, which keeps
    // a claim and another project's removal of the folder from crossing.
    let owners_lock = workers.join(TEST_SERVER_OWNERS_LOCK);
    let waiting = "another weft is updating the machine's test servers; waiting for it";
    {
        let _turn = crate::images::hold_file_lock(owners_lock.clone(), waiting).await?;
        if claim_test_server(&servers, &dir, &file, &owner)? {
            return Ok(image);
        }
    }
    let staged = tokio::task::block_in_place(|| {
        weft_compiler::build::build_test_artifact(project, catalog, package, &machine.shared_crates())
    })
    .map_err(|e| anyhow::anyhow!("stage the test server for '{package}': {e}"))?;
    anyhow::ensure!(
        staged.content_hash == hash,
        "the test server for '{package}' staged as {}, not {hash}: its sources changed while it was staged; run it again",
        staged.content_hash
    );
    let log = weft_platform_local::InstallLayout::new(install.dir()).build_log(&image);
    eprintln!("compiling the test server for '{package}' on this machine (its log: {})", log.display());
    match crate::host_programs::compile(&staged, dir.clone(), &log, crate::host_programs::any_lane()?).await? {
        // Its one package is what it tests, so a package that does not
        // compile fails the build itself, never a record of it.
        host_build::HostBuilt::Built(_) => {}
        host_build::HostBuilt::Failed(reason) => {
            bail!("the test server for '{package}' did not compile: {reason}\nits log: {}", log.display())
        }
    }
    let _turn = crate::images::hold_file_lock(owners_lock, waiting).await?;
    // A folder with no owner is never removed, so the one just compiled is
    // still there.
    anyhow::ensure!(
        claim_test_server(&servers, &dir, &file, &owner)?,
        "the test server for '{package}' was compiled into {}, and is not there any more; run it again",
        dir.display()
    );
    Ok(image)
}

/// The file in a test server's folder naming, one per line, each project
/// and package it tests ([`test_server_owner`]): several projects may share
/// one, since its folder is named by the package's content. It is how the
/// next build of a package finds the servers it replaces.
const TEST_SERVER_OWNER_FILE: &str = "tests";

/// The lock, in the machine's workers folder, every change to the test
/// servers' owner files, and every removal of a server, is made under.
const TEST_SERVER_OWNERS_LOCK: &str = "node-tests.lock";

fn test_server_owner(project: &weft_compiler::project::Project, package: &str) -> String {
    format!("{} {package}", project.id())
}

/// The owners in `dir`'s owner file, `None` when it has none.
fn test_server_owners(dir: &std::path::Path) -> Result<Option<Vec<String>>> {
    let path = dir.join(TEST_SERVER_OWNER_FILE);
    match std::fs::read_to_string(&path) {
        Ok(text) => Ok(Some(text.lines().filter(|line| !line.is_empty()).map(str::to_string).collect())),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e).with_context(|| format!("read {}", path.display())),
    }
}

/// Write `owners` as `dir`'s owner file, whole (beside it, then renamed),
/// so a reader never sees half of it.
fn write_test_server_owners(dir: &std::path::Path, owners: &[String]) -> Result<()> {
    let body: String = owners.iter().map(|owner| format!("{owner}\n")).collect();
    weft_core::infra::write_whole(dir, TEST_SERVER_OWNER_FILE, body.as_bytes(), 0o644)
        .with_context(|| format!("write {}", dir.join(TEST_SERVER_OWNER_FILE).display()))
}

/// Claim the test server in `fresh` for `owner`, once its program `file`
/// is there: `owner` is added to its owner file (when not there yet), then
/// [`drop_stale_test_servers`] lets go of the ones it replaces. `false`, and
/// nothing changed, when the program is not there. The caller holds
/// [`TEST_SERVER_OWNERS_LOCK`].
fn claim_test_server(servers: &std::path::Path, fresh: &std::path::Path, file: &std::path::Path, owner: &str) -> Result<bool> {
    if !file.is_file() {
        return Ok(false);
    }
    let mut owners = test_server_owners(fresh)?.unwrap_or_default();
    if !owners.iter().any(|of| of == owner) {
        owners.push(owner.to_string());
        write_test_server_owners(fresh, &owners)?;
    }
    drop_stale_test_servers(servers, fresh, owner)?;
    Ok(true)
}

/// Let go, for `owner`, of every test server in `servers` but `fresh`: one
/// is compiled per version of a package's sources, and `owner` asks for an
/// older one no more. Its line leaves each other server's owner file, and a
/// server is removed once no owner is left in it; one another project still
/// tests with stays. A folder with no owner file is not one this command
/// made, and stays. The caller holds [`TEST_SERVER_OWNERS_LOCK`].
fn drop_stale_test_servers(servers: &std::path::Path, fresh: &std::path::Path, owner: &str) -> Result<()> {
    let entries = match std::fs::read_dir(servers) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e).with_context(|| format!("read {}", servers.display())),
    };
    for entry in entries {
        let dir = entry.with_context(|| format!("read {}", servers.display()))?.path();
        if dir == fresh || !dir.is_dir() {
            continue;
        }
        let Some(mut owners) = test_server_owners(&dir)? else { continue };
        let before = owners.len();
        owners.retain(|of| of != owner);
        match (owners.len() == before, owners.is_empty()) {
            (true, _) => {}
            (false, true) => std::fs::remove_dir_all(&dir).with_context(|| format!("remove the old test server {}", dir.display()))?,
            (false, false) => write_test_server_owners(&dir, &owners)?,
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// What did not load refuses the run that would have tested it: a
    /// package with a broken node, a package whose every node broke
    /// (named, it is in no package list, so the refusal lists what did
    /// not load), and the whole-project sweep. A healthy package beside a
    /// broken one still runs.
    #[test]
    fn what_did_not_load_refuses_the_tests_it_would_have_run() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let node = |dir: &std::path::Path, metadata: &str| {
            std::fs::create_dir_all(dir).expect("mkdir");
            std::fs::write(dir.join("metadata.json"), metadata).expect("write");
            std::fs::write(dir.join("mod.rs"), "// node\n").expect("write");
        };
        for package in ["pkg", "gone", "other"] {
            let root = tmp.path().join(package);
            std::fs::create_dir_all(&root).expect("mkdir");
            std::fs::write(root.join("package.toml"), format!("[package]\nname = \"{package}\"\n")).expect("write");
        }
        let good = |name: &str| format!(r#"{{ "type": "{name}", "label": "{name}", "description": "" }}"#);
        let broken = |name: &str| {
            format!(
                r#"{{ "type": "{name}", "label": "{name}", "description": "", "validate": [{{ "when": {{ "kind": "no_such_kind" }}, "then": {{ "message": "m" }} }}] }}"#
            )
        };
        node(&tmp.path().join("pkg/good"), &good("Good"));
        node(&tmp.path().join("pkg/broken"), &broken("Broken"));
        node(&tmp.path().join("gone/only"), &broken("Only"));
        node(&tmp.path().join("other/fine"), &good("Fine"));
        let catalog = FsCatalog::discover(tmp.path()).expect("discover");
        let refusal = |targets: &[&str]| {
            resolve_targets(&catalog, &targets.iter().map(|t| t.to_string()).collect::<Vec<_>>())
                .expect_err("refused")
                .to_string()
        };
        let cases: [&[&str]; 5] = [&["pkg"], &["Good"], &["gone"], &[], &["other", "pkg"]];
        for targets in cases {
            let err = refusal(targets);
            assert!(err.contains("no_such_kind"), "{targets:?}: {err}");
        }
        assert!(refusal(&["gone"]).contains("gone/only"), "the package that vanished is named by its folder");
        let fine = resolve_targets(&catalog, &["other".to_string()]).expect("another package's broken node is not this one's");
        assert_eq!(fine[0].name, "other");
    }

    /// Several targets resolve to each package once, in the order first
    /// named: node types of one package fold into its node set, naming
    /// the package itself widens it to every node whichever comes first,
    /// and a name that is neither a package nor a node type refuses the
    /// whole call.
    #[test]
    fn several_targets_fold_into_one_entry_per_package() {
        let tmp = tempfile::tempdir().expect("tempdir");
        for (package, nodes) in [("alpha", &["One", "Two"][..]), ("beta", &["Three"])] {
            let root = tmp.path().join(package);
            std::fs::create_dir_all(&root).expect("mkdir");
            std::fs::write(root.join("package.toml"), format!("[package]\nname = \"{package}\"\n")).expect("write");
            for node in nodes {
                let dir = root.join(node.to_lowercase());
                std::fs::create_dir_all(&dir).expect("mkdir");
                std::fs::write(dir.join("metadata.json"), format!(r#"{{ "type": "{node}", "label": "{node}", "description": "" }}"#))
                    .expect("write");
                std::fs::write(dir.join("mod.rs"), "// node\n").expect("write");
            }
        }
        let catalog = FsCatalog::discover(tmp.path()).expect("discover");
        let resolve = |targets: &[&str]| resolve_targets(&catalog, &targets.iter().map(|t| t.to_string()).collect::<Vec<_>>());
        let nodes = |names: &[&str]| Some(names.iter().map(|n| n.to_string()).collect::<BTreeSet<_>>());
        let package = |name: &str, nodes: Option<BTreeSet<String>>| TargetPackage { name: name.to_string(), nodes };

        assert_eq!(
            resolve(&["Three", "One", "Two"]).expect("resolves"),
            vec![package("beta", nodes(&["Three"])), package("alpha", nodes(&["One", "Two"]))]
        );
        assert_eq!(resolve(&["One", "alpha"]).expect("resolves"), vec![package("alpha", None)]);
        assert_eq!(resolve(&["alpha", "One"]).expect("resolves"), vec![package("alpha", None)]);
        assert_eq!(resolve(&["beta", "beta"]).expect("resolves"), vec![package("beta", None)]);
        let err = resolve(&["alpha", "Nope"]).expect_err("refused").to_string();
        assert!(err.contains("'Nope' is neither a package nor a node type"), "{err}");

        let one = package("alpha", nodes(&["One"]));
        assert!(one.tests_node("One") && !one.tests_node("Two"));
        assert!(package("alpha", None).tests_node("Two"));
    }

    /// A passing package's outcome line starts `✓ package <name>:`, the
    /// prefix scripts/run-node-tests.sh reads to record which packages
    /// passed; every other outcome does not.
    #[test]
    fn only_a_passing_package_line_reads_as_passed() {
        let passed = |outcome: PackageOutcome| package_line("web", &outcome).starts_with("✓ package web:");
        assert!(passed(PackageOutcome::Ran { tests: 3, failed: 0 }));
        assert!(passed(PackageOutcome::Ran { tests: 0, failed: 0 }));
        assert!(!passed(PackageOutcome::Ran { tests: 3, failed: 1 }));
        assert!(!passed(PackageOutcome::Broken));
    }

    /// Write `bytes` bytes at `path`, creating parents.
    fn file_of(path: &std::path::Path, bytes: usize) {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, vec![0u8; bytes]).unwrap();
    }

    #[test]
    fn dir_size_counts_every_file_at_every_depth() {
        let dir = tempfile::tempdir().unwrap();
        file_of(&dir.path().join("a"), 100);
        file_of(&dir.path().join("deps/b"), 250);
        file_of(&dir.path().join("incremental/x/c"), 650);
        assert_eq!(dir_size_bytes(dir.path()).unwrap(), 1000);
    }

    #[test]
    fn a_cache_under_the_cap_is_left_alone() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("test");
        file_of(&root.join("deps/a"), 100);
        bound_build_cache(&root, &dir.path().join("test.lock"), 1000).unwrap();
        assert!(root.join("deps/a").exists());
    }

    #[test]
    fn a_cache_over_the_cap_is_wiped_whole() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("test");
        file_of(&root.join("deps/a"), 2000);
        bound_build_cache(&root, &dir.path().join("test.lock"), 1000).unwrap();
        assert!(!root.exists());
    }

    /// Over the cap, a cache another run holds is left where it is; once
    /// nobody holds it, the next run wipes it.
    #[test]
    fn a_cache_in_use_is_never_wiped() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("test");
        let lock = dir.path().join("test.lock");
        file_of(&root.join("deps/a"), 2000);
        let other_run = weft_core::file_lock::hold_shared(&lock).unwrap();
        bound_build_cache(&root, &lock, 1000).unwrap();
        assert!(root.join("deps/a").exists(), "another run is building in it");
        drop(other_run);
        bound_build_cache(&root, &lock, 1000).unwrap();
        assert!(!root.exists(), "free again, it goes");
    }

    #[test]
    fn the_wipe_happens_only_over_the_cap_and_alone() {
        assert_eq!(bound_decision(500, 1000, false), BoundDecision::Keep);
        assert_eq!(bound_decision(1000, 1000, true), BoundDecision::Keep);
        assert_eq!(bound_decision(2000, 0, true), BoundDecision::Keep);
        assert_eq!(bound_decision(2000, 1000, true), BoundDecision::Wipe);
        assert_eq!(bound_decision(2000, 1000, false), BoundDecision::Busy);
    }

    #[test]
    fn a_zero_cap_disables_the_bound() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("test");
        file_of(&root.join("deps/a"), 2000);
        bound_build_cache(&root, &dir.path().join("test.lock"), 0).unwrap();
        assert!(root.join("deps/a").exists());
    }

    /// A package's fresh test server replaces its older ones, and leaves
    /// every other package's, and any folder it did not make, alone.
    #[test]
    fn a_fresh_test_server_drops_only_its_packages_older_ones() {
        let servers = tempfile::tempdir().unwrap();
        let server = |hash: &str, owner: Option<&str>| {
            let dir = servers.path().join(hash);
            file_of(&dir.join("worker"), 1);
            if let Some(owner) = owner {
                std::fs::write(dir.join(TEST_SERVER_OWNER_FILE), owner).unwrap();
            }
            dir
        };
        let fresh = server("new", Some("p llm"));
        let old = server("old", Some("p llm"));
        let sibling = server("sib", Some("p format"));
        let other_project = server("far", Some("q llm"));
        let unknown = server("odd", None);
        drop_stale_test_servers(servers.path(), &fresh, "p llm").unwrap();
        assert!(fresh.exists() && !old.exists());
        assert!(sibling.exists() && other_project.exists() && unknown.exists());
        drop_stale_test_servers(&servers.path().join("absent"), &fresh, "p llm").unwrap();
    }

    /// Two projects testing the same package share its server: one moving
    /// to a new version takes only its own line out of the old server, which
    /// stays for the other, and is removed once the other moves on too.
    #[test]
    fn a_shared_test_server_stays_until_its_last_owner_moves_on() {
        let servers = tempfile::tempdir().unwrap();
        let server = |hash: &str| {
            let dir = servers.path().join(hash);
            file_of(&dir.join("worker"), 1);
            dir
        };
        let shared = server("v1");
        assert!(claim_test_server(servers.path(), &shared, &shared.join("worker"), "p llm").unwrap());
        // The second project finds it compiled, and is recorded too, once.
        assert!(claim_test_server(servers.path(), &shared, &shared.join("worker"), "q llm").unwrap());
        assert!(claim_test_server(servers.path(), &shared, &shared.join("worker"), "q llm").unwrap());
        assert_eq!(test_server_owners(&shared).unwrap().unwrap(), ["p llm", "q llm"]);

        let p_next = server("v2");
        assert!(claim_test_server(servers.path(), &p_next, &p_next.join("worker"), "p llm").unwrap());
        assert_eq!(test_server_owners(&shared).unwrap().unwrap(), ["q llm"]);

        let q_next = server("v3");
        assert!(claim_test_server(servers.path(), &q_next, &q_next.join("worker"), "q llm").unwrap());
        assert!(!shared.exists() && p_next.exists() && q_next.exists());
    }

    /// A folder whose program is not there is not claimed, and nothing is
    /// written into it or removed around it.
    #[test]
    fn a_test_server_without_its_program_is_not_claimed() {
        let servers = tempfile::tempdir().unwrap();
        let old = servers.path().join("old");
        file_of(&old.join("worker"), 1);
        std::fs::write(old.join(TEST_SERVER_OWNER_FILE), "p llm\n").unwrap();
        let missing = servers.path().join("new");
        assert!(!claim_test_server(servers.path(), &missing, &missing.join("worker"), "p llm").unwrap());
        assert!(!missing.exists() && old.exists());
    }

    /// Only the needs of a node type a run tests are asked about, each
    /// row naming its node type.
    #[test]
    fn only_the_tested_node_types_needs_are_asked() {
        let need = |name: &str| weft_core::needs::Need { name: name.into(), stage: weft_core::needs::NeedStage::Run, check: vec![name.into()], install: Default::default(), how: None, recipe: None };
        let needs: weft_core::needs::NodeNeeds = [("ExecPython".to_string(), vec![need("python3")]), ("Ffmpeg".to_string(), vec![need("ffmpeg")])].into();
        let rows = tested_needs(&needs, ["ExecPython", "ExecPython", "Plain"].into_iter());
        assert_eq!(rows.len(), 1);
        assert_eq!((rows[0].node_type.as_str(), rows[0].nodes.as_slice(), rows[0].need.name.as_str()), ("ExecPython", &["ExecPython".to_string()][..], "python3"));
        assert!(tested_needs(&needs, std::iter::empty()).is_empty());
    }

    #[test]
    fn a_missing_cache_is_not_an_error() {
        let dir = tempfile::tempdir().unwrap();
        bound_build_cache(&dir.path().join("absent"), &dir.path().join("absent.lock"), 1).unwrap();
    }
}
