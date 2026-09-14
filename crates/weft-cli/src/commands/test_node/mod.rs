//! `weft test-node`: run node self-tests.
//!
//! Basic/fake tiers run RIGHT HERE: every targeted package's test
//! crate is emitted against the real weft checkout
//! (`EmitPaths::Local`) as a member of one cargo workspace, built with
//! plain cargo (no docker, no cluster, no broker), and executed.
//! A broken `main.weft` never blocks this, and neither does a broken
//! package nobody targeted: only what you asked to test joins the
//! build. Target a package and it is the only one that has to compile.
//!
//! The live tier runs the PRODUCTION credential path (connection
//! resolution, relaying, metering, billing), which only exists next to
//! the broker, so live tests always run as a short-lived test pod in
//! the cluster: the CLI builds + loads the per-package test image,
//! asks the dispatcher to run one test per pod
//! (`/projects/{id}/node-tests/run`), and polls the outcome. Live
//! needs the project registered with the dispatcher (cost has to
//! attribute to something) and a grant for each test's declared
//! service.

use std::collections::BTreeMap;
use std::path::PathBuf;

use anyhow::{bail, Context, Result};
use serde_json::{json, Value};

use weft_catalog::FsCatalog;
use weft_core::access::spec::{AccessSpec, Acquisition, CredentialField, Door};
use weft_core::access::wire::ConnectDirect;
use weft_core::node::MetadataCatalog;
use weft_core::node_test::{
    concurrency_limit, report_line, NodeTestsListing, RunAllReport, TestListing, TestReport,
};
use weft_core::TestTier;

use super::Ctx;

mod progress;
use progress::LiveProgress;

pub struct TestNodeArgs {
    /// A package name or a node type; absent = every package.
    pub target: Option<String>,
    /// Run only the test with this name (needs a node/package target).
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
    /// Concurrency: `None` = one test at a time, `Some(0)` = all at
    /// once (bare `--parallel`), `Some(n)` = at most n in flight.
    /// Applies to the local tiers (forwarded to the package binary)
    /// and to live pod runs alike.
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

/// One package to test, with an optional single-node filter (set when
/// the target named a node type rather than a package).
struct TargetPackage {
    name: String,
    node_filter: Option<String>,
}

pub async fn run(ctx: Ctx, args: TestNodeArgs) -> Result<()> {
    let project = ctx.project()?;
    let catalog = weft_compiler::build::build_project_catalog(&project.root)
        .map_err(|e| anyhow::anyhow!("discover nodes: {e}"))?;

    let targets = resolve_targets(&catalog, args.target.as_deref())?;
    if targets.is_empty() {
        eprintln!(
            "no package under nodes/ declares tests (a node declares tests in a \
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
    bound_build_cache(&cache_root, cache_cap_bytes()?)?;
    let build_dirs =
        weft_compiler::build::TestBuildDirs::shared(&project.root, &cache_root);
    // Every package this run will test is emitted BEFORE any of them
    // builds, so cargo unifies features across them and compiles each
    // shared dependency once instead of once per feature union.
    let package_names: Vec<String> = targets.iter().map(|t| t.name.clone()).collect();
    let workspace =
        weft_compiler::build::prepare_test_workspace(&catalog, &package_names, &build_dirs)
            .map_err(|e| anyhow::anyhow!("{e}"))?;
    let mut reports: Vec<TestReport> = Vec::new();
    if !local.is_empty() {
        // Every package compiles before any test runs: nothing to run
        // until the code builds, and cargo's own diagnostics have
        // already named whatever broke.
        let binaries = weft_compiler::build::build_node_test_binaries(&workspace)
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        for target in &targets {
            let binary = binaries.get(&target.name).with_context(|| {
                format!("no test binary was built for package '{}'", target.name)
            })?;
            let run = run_local(binary, target, args.test.as_deref(), &local, args.parallel);
            // The binary has served its purpose. It is ~110MB (mostly
            // the line tables), it relinks in seconds from the
            // dependency cache that stays, and keeping one per package
            // held 3.2GB of the cache between runs for nothing.
            weft_compiler::build::drop_built_binary(binary);
            reports.extend(run?);
        }
    }
    if live {
        run_live(&ctx, &catalog, &targets, &workspace, &build_dirs, &args, &mut reports).await?;
    }

    render(&ctx, &reports)?;
    let failed = reports.iter().filter(|r| !r.passed).count();
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
        match args.target.as_deref() {
            Some(t) => eprintln!("'{t}' declares no tests in tier(s) {}: nothing to run", tier_names.join(", ")),
            None => eprintln!("no matching node tests declared (a node declares tests in its tests.rs)"),
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
            let targets = resolve_targets(&catalog, Some(&t))?;
            let package = &targets.first().expect("resolve_targets errors instead of empty").name;
            let hash = weft_compiler::build::node_test_cache_hash(project, &catalog, package)
                .map_err(|e| anyhow::anyhow!("{e}"))?;
            println!("{hash}");
        }
        None => {
            for target in resolve_targets(&catalog, None)? {
                let hash =
                    weft_compiler::build::node_test_cache_hash(project, &catalog, &target.name)
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
/// next `weft test-node` builds cold and re-makes all of it.
pub fn wipe_cache() -> Result<()> {
    let root = node_test_cache_root();
    if !root.exists() {
        return Ok(());
    }
    std::fs::remove_dir_all(&root).with_context(|| format!("remove {}", root.display()))
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
fn bound_build_cache(root: &std::path::Path, cap_bytes: u64) -> Result<()> {
    if cap_bytes == 0 || !root.exists() {
        return Ok(());
    }
    let size = dir_size_bytes(root)
        .with_context(|| format!("measure the node-test build cache at {}", root.display()))?;
    if size <= cap_bytes {
        return Ok(());
    }
    println!(
        "node-test build cache is {:.1}GB (cap {:.0}GB); clearing it, so this run builds cold. \
         The cap is WEFT_TEST_CACHE_CAP_GB.",
        size as f64 / 1024.0 / 1024.0 / 1024.0,
        cap_bytes as f64 / 1024.0 / 1024.0 / 1024.0,
    );
    std::fs::remove_dir_all(root).with_context(|| format!("remove {}", root.display()))?;
    Ok(())
}

/// Total bytes of every file under `root`, following no symlinks (a
/// target dir symlinked onto another disk is still that disk's
/// problem, and following one could wander out of the tree).
fn dir_size_bytes(root: &std::path::Path) -> std::io::Result<u64> {
    let mut total = 0;
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(&dir)? {
            let entry = entry?;
            let meta = entry.metadata()?;
            if meta.is_dir() {
                stack.push(entry.path());
            } else if meta.is_file() {
                total += meta.len();
            }
        }
    }
    Ok(total)
}

/// Resolve the positional target to packages. A package name wins over
/// a node type on the (unlikely) collision, since the package is the
/// build unit.
fn resolve_targets(catalog: &FsCatalog, target: Option<&str>) -> Result<Vec<TargetPackage>> {
    match target {
        None => {
            // The whole-project sweep covers exactly the packages that
            // declare tests (the catalog's own predicate, shared with
            // every other consumer): building a test binary for a
            // test-less package would be wasted compilation.
            let mut names: Vec<String> = catalog
                .packages()
                .filter(|p| catalog.package_declares_tests(p))
                .map(|p| p.name.clone())
                .collect();
            names.sort();
            Ok(names
                .into_iter()
                .map(|name| TargetPackage { name, node_filter: None })
                .collect())
        }
        Some(t) => {
            if catalog.packages().any(|p| p.name == t) {
                return Ok(vec![TargetPackage { name: t.to_string(), node_filter: None }]);
            }
            if let Some(pkg) = catalog.package_of(t) {
                return Ok(vec![TargetPackage {
                    name: pkg.name.clone(),
                    node_filter: Some(t.to_string()),
                }]);
            }
            let mut known: Vec<&str> = catalog.packages().map(|p| p.name.as_str()).collect();
            known.sort();
            bail!(
                "'{t}' is neither a package nor a node type in this project's nodes/ \
                 (packages: {})",
                known.join(", ")
            );
        }
    }
}

/// Run one package's local-tier (basic/fake) tests through its built
/// binary. The caller decides whether an empty result is an error.
fn run_local(
    binary: &PathBuf,
    target: &TargetPackage,
    test_filter: Option<&str>,
    tiers: &[TestTier],
    parallel: Option<usize>,
) -> Result<Vec<TestReport>> {
    // Unfiltered: one `run-all` invocation covers the package.
    if target.node_filter.is_none() && test_filter.is_none() {
        let mut cmd: Vec<&str> = vec!["run-all"];
        for t in tiers {
            cmd.extend(["--tier", t.as_str()]);
        }
        let parallel_arg = parallel.map(|n| n.to_string());
        if let Some(n) = parallel_arg.as_deref() {
            cmd.extend(["--parallel", n]);
        }
        let out = run_binary(binary, &cmd)?;
        let parsed: RunAllReport =
            serde_json::from_str(&out.report).context("parse run-all report")?;
        return Ok(parsed.tests);
    }

    // Filtered: list, select, run each individually.
    let listing = list_tests(binary)?;
    let mut reports = Vec::new();
    for node in &listing {
        if target.node_filter.as_deref().is_some_and(|f| f != node.node_type) {
            continue;
        }
        for t in &node.tests {
            if test_filter.is_some_and(|f| f != t.name) {
                continue;
            }
            if !tiers.contains(&t.tier) {
                continue;
            }
            let out = run_binary(
                binary,
                &["run", "--node", &node.node_type, "--test", &t.name],
            )?;
            reports.push(
                serde_json::from_str(&out.report).context("parse test report")?,
            );
        }
    }
    Ok(reports)
}

fn list_tests(binary: &PathBuf) -> Result<Vec<NodeTestsListing>> {
    let out = run_binary(binary, &["list"])?;
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
    project: &weft_compiler::project::Project,
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
        let hash = weft_compiler::build::node_test_cache_hash(project, catalog, &target.name)
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
        let nodes = list_tests(binary)?;
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

/// Run the test binary; stderr (node logs, tracing) streams through to
/// the terminal, stdout is the JSON channel. A non-zero exit with
/// parseable stdout is NOT an error here (a failing test reports
/// through the JSON); a broken invocation surfaces via empty stdout.
fn run_binary(binary: &PathBuf, args: &[&str]) -> Result<BinaryOutput> {
    let out = std::process::Command::new(binary)
        .args(args)
        .stderr(std::process::Stdio::inherit())
        .output()
        .with_context(|| format!("run {}", binary.display()))?;
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

fn render(ctx: &Ctx, reports: &[TestReport]) -> Result<()> {
    if ctx.json_out(&reports)? {
        return Ok(());
    }
    // Per-test lines for the LOCAL tiers only: a live test already
    // printed its line the moment it finished (the progress block), so
    // repeating it here would double every result in the scrollback.
    for r in reports.iter().filter(|r| r.tier != TestTier::Live) {
        let mark = if r.passed { "✓" } else { "✗" };
        println!("{mark} {} / {} ({})", r.node, r.test, r.tier.as_str());
    }
    // Then every failure in full, so a red run ends with the errors on
    // screen instead of scrolled away behind other tests' output.
    let failed: Vec<&TestReport> = reports.iter().filter(|r| !r.passed).collect();
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
            if !r.colors.is_empty() {
                println!(
                    "  live run cost is recorded under execution color(s): {}",
                    r.colors.join(", ")
                );
            }
        }
    }
    // The failure count line is the caller's bail (the process error),
    // so it prints exactly once.
    if !reports.is_empty() && failed.is_empty() {
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
    reports: &mut Vec<TestReport>,
) -> Result<()> {
    let project = ctx.project()?;
    // Discover the live tests per target package (through each
    // package's listing: the registry is the single source, read via
    // the hash-keyed cache so unchanged packages skip the build).
    let mut runs: Vec<LiveRun> = Vec::new();
    // Declared fixtures each selected test needs, validated up front
    // against the environment: every miss is collected and reported at
    // once, BEFORE any consent prompt, grant, or pod. (The pod-side
    // `rig.fixture` read stays the runtime backstop.)
    let mut missing_fixtures: Vec<String> = Vec::new();
    let listings = cached_listings(project, catalog, targets, build_dirs, workspace)?;
    for target in targets {
        for node in &listings[&target.name] {
            if target.node_filter.as_deref().is_some_and(|f| f != node.node_type) {
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
    ensure_live_consent(args)?;

    let client = ctx.client();
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
        // (the plan, the pod requests) carries a real Uuid.
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
            // (and before a pod spends money discovering it).
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

    // Every WEFT_NODE_TEST_* variable rides into the test pod's env as
    // a live fixture (`LiveRig::fixture`): values a test cannot
    // self-provision, like the chat id a bot may message. EXCEPT the
    // key variables: a credential field reaches the pod through the
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
        run_live_pods(ctx, catalog, &project_id, &runs, &connections, &fixtures, args.parallel, reports).await
    } else {
        let create_and_run = async {
            for (service, key) in prepared {
                let id = create_ephemeral_grant(ctx, key).await?;
                ephemeral_grants.push(id);
                connections.insert(service, id);
            }
            run_live_pods(ctx, catalog, &project_id, &runs, &connections, &fixtures, args.parallel, reports).await
        };
        tokio::select! {
            r = create_and_run => r,
            _ = tokio::signal::ctrl_c() => Err(anyhow::anyhow!(
                "interrupted (Ctrl+C); any started test pod keeps running, and a \
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

/// A spec's static-acquisition credential fields, when it has any.
fn static_fields(spec: &AccessSpec) -> Option<Vec<CredentialField>> {
    match &spec.acquisition {
        Acquisition::Static { fields } => Some(fields.clone()),
        _ => None,
    }
}

/// Every credential field a `<service>` key can carry: the paste
/// variant's static fields plus the spec's own static-acquisition
/// fields (either set may be absent).
fn key_fields(spec: &AccessSpec) -> Vec<CredentialField> {
    let mut fields: Vec<CredentialField> = spec
        .paste_variant()
        .as_ref()
        .and_then(static_fields)
        .unwrap_or_default();
    fields.extend(static_fields(spec).unwrap_or_default());
    fields
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
/// pod otherwise. Uses the service-scoped list (the API's read
/// surface for grants), so a wrong-service id fails naming both.
async fn require_grant_for_service(ctx: &Ctx, service: &str, grant_id: uuid::Uuid) -> Result<()> {
    let grants = super::connect::list_grants(&ctx.client(), Some(service)).await?;
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
    let grants = super::connect::list_grants(&ctx.client(), Some(service)).await?;
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
}

fn prepare_ephemeral_key(catalog: &FsCatalog, service: &str) -> Result<PreparedKey> {
    let spec = catalog
        .all()
        .into_iter()
        .find_map(|m| m.service.as_ref().filter(|s| s.service == service))
        .with_context(|| {
            format!("no node in this project declares the '{service}' service recipe")
        })?;

    // The paste surface: a consent service's own_page.paste fields, or
    // a static (key-paste) service's own declared fields.
    let (fields, paste_flag) = match spec.paste_variant() {
        Some(variant) => (
            static_fields(&variant)
                .expect("paste_variant always yields a static acquisition"),
            true,
        ),
        None => match static_fields(spec) {
            Some(fields) => (fields, false),
            None => bail!(
                "service '{service}' has no paste flow (it connects through a sign-in); \
                 connect it in the editor and re-run without --key"
            ),
        },
    };

    // Once ANY field for this service comes from the environment the
    // whole grant is env-driven: a required gap fails loud naming the
    // variable and an optional gap is skipped, never prompted (a
    // scripted sweep must not stop on stdin).
    let env_driven = env_key_present(service);
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
    Ok(PreparedKey { spec: spec.clone(), values, paste: paste_flag })
}

/// The side-effecting half: store the prepared credential as an
/// ephemeral grant. No prompting in here, so it can run inside the
/// Ctrl+C select.
async fn create_ephemeral_grant(ctx: &Ctx, key: PreparedKey) -> Result<uuid::Uuid> {
    let grant = super::connect::connect_direct(
        &ctx.client(),
        ConnectDirect {
            spec: key.spec,
            door: Door::Own,
            values: key.values,
            label: Some("weft test-node (ephemeral)".into()),
            permissions: Vec::new(),
            registration: None,
            paste: key.paste,
            project_id: None,
        },
        None,
    )
    .await
    .context("create the ephemeral key grant")?;
    Ok(grant.id)
}

async fn run_live_pods(
    ctx: &Ctx,
    catalog: &FsCatalog,
    project_id: &str,
    runs: &[LiveRun],
    connections: &BTreeMap<String, uuid::Uuid>,
    fixtures: &BTreeMap<String, String>,
    parallel: Option<usize>,
    reports: &mut Vec<TestReport>,
) -> Result<()> {
    let client = ctx.client();
    let project = ctx.project()?;

    // One image per involved package, content-addressed and skipped
    // when already present.
    let mut images: BTreeMap<String, String> = BTreeMap::new();
    let packages: std::collections::BTreeSet<&str> =
        runs.iter().map(|r| r.package.as_str()).collect();
    for package in packages {
        images.insert(
            package.to_string(),
            ensure_test_image(project, catalog, package).await?,
        );
    }

    // Each run is its own pod with its own execution identity, so runs
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
                let image = images.get(&run.package).cloned().with_context(|| {
                    format!("no test image resolved for package '{}'", run.package)
                });
                let connection = connections.get(&run.service).cloned().with_context(|| {
                    format!("no connection resolved for service '{}'", run.service)
                });
                let client = &client;
                let progress = &progress;
                async move {
                    let outcome = match (image, connection) {
                        (Ok(image), Ok(connection)) => {
                            run_one_live_pod(
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
    for (_, outcome) in settled {
        reports.push(outcome?);
    }
    Ok(())
}

/// Start one live test's pod and poll it to terminal. No deadline: a
/// live test may take as long as the provider takes; Ctrl+C is the way
/// out, and a periodic breadcrumb keeps a long wait legible. A status
/// this CLI does not know is a loud contract break, never an endless
/// silent wait.
async fn run_one_live_pod(
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
            &json!({
                "imageRef": image,
                "node": run.node,
                "test": run.test,
                "liveConnection": connection,
                "fixtures": fixtures,
            }),
        )
        .await
        .context("start the node-test run")?;
    let task_id = resp
        .get("taskId")
        .and_then(Value::as_str)
        .context("run answered without a task id")?
        .to_string();

    let started = std::time::Instant::now();
    let mut last_breadcrumb = std::time::Instant::now();
    let report = loop {
        let status = client
            .get_json(&format!("/projects/{project_id}/node-tests/runs/{task_id}"))
            .await
            .context("poll the node-test run")?;
        match status.get("status").and_then(Value::as_str) {
            Some("complete") => {
                break status.get("report").cloned().context("completed without a report")?
            }
            Some("failed") => bail!(
                "the node-test run itself failed (not the test): {}",
                status.get("error").and_then(Value::as_str).unwrap_or("unknown")
            ),
            Some(in_progress @ ("pending" | "claimed")) => {
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
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            }
            other => bail!(
                "unexpected node-test run status {other:?} for task {task_id}; \
                 the dispatcher and this CLI disagree on the task states"
            ),
        }
    };
    let report: TestReport = serde_json::from_value(report).context("parse live test report")?;
    progress.finished(&run.package, &run.test, report.passed, report.error.as_deref());
    Ok(report)
}

/// Build + load the package's content-addressed test image, skipping
/// on a tag hit (the same skip rule worker images use).
async fn ensure_test_image(
    project: &weft_compiler::project::Project,
    catalog: &FsCatalog,
    package: &str,
) -> Result<String> {
    let builder_base = crate::images::ensure_worker_builder_base().await?;
    let artifact = tokio::task::block_in_place(|| {
        weft_compiler::build::build_test_artifact(project, catalog, package, &builder_base)
    })
    .map_err(|e| anyhow::anyhow!("stage test image for '{package}': {e}"))?;
    let tag = weft_compiler::build::node_test_image_tag(&artifact.content_hash);

    if !crate::images::image_present(&tag).await? {
        // The package rides its own label so the stale-image GC can
        // scope to "this package's older test images": every package's
        // test image shares the one scratch project id, so the project
        // label alone would make packages evict each other's images.
        crate::images::docker_build(
            &tag,
            &artifact.build_context.join("Dockerfile"),
            &artifact.build_context,
            &[
                format!("weft.dev/project={}", project.id()),
                format!("weft.dev/node-test-package={package}"),
            ],
            None,
        )
        .await?;
    }
    // Load onto the local cluster so the pod's IfNotPresent pull hits.
    let cfg = crate::commands::daemon::cluster_config();
    match cfg.backend {
        crate::commands::daemon::ClusterBackend::Kind => {
            if crate::commands::build::kind_available(&cfg.cluster_name).await {
                crate::images::kind_load(&cfg.cluster_name, &tag, false).await?;
            } else {
                bail!(
                    "no running kind cluster '{}' to load {tag} onto; start it with \
                     `weft daemon start`",
                    cfg.cluster_name
                );
            }
        }
        // The SAME failure worker images surface on this backend: the
        // image ref the pod spawns is what the user pushed, so a
        // local-only tag must stop here with the push recipe rather
        // than ImagePullBackOff in the cluster.
        crate::commands::daemon::ClusterBackend::K8s => {
            return Err(crate::commands::build::bail_k8s_push_needed(&tag));
        }
    }
    // Test images are content-addressed per package-source version and
    // pile up fast across a suite run; drop this PACKAGE's stale ones
    // now that its fresh tag is ensured (package-scoped: every package
    // shares the one scratch project id). Test pods are short-lived
    // and never restart from an old tag, so beyond the fresh tag
    // nothing is referenced by design: a tag a still-running test pod
    // uses refuses its node-side remove and survives.
    crate::commands::build::gc_stale_images(
        std::slice::from_ref(&tag),
        &[
            format!("weft.dev/project={}", project.id()),
            format!("weft.dev/node-test-package={package}"),
        ],
        Some(&crate::images::ReferencedImages::default()),
    )
    .await;
    Ok(tag)
}

#[cfg(test)]
mod tests {
    use super::*;

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
        bound_build_cache(&root, 1000).unwrap();
        assert!(root.join("deps/a").exists());
    }

    #[test]
    fn a_cache_over_the_cap_is_wiped_whole() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("test");
        file_of(&root.join("deps/a"), 2000);
        bound_build_cache(&root, 1000).unwrap();
        assert!(!root.exists());
    }

    #[test]
    fn a_zero_cap_disables_the_bound() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("test");
        file_of(&root.join("deps/a"), 2000);
        bound_build_cache(&root, 0).unwrap();
        assert!(root.join("deps/a").exists());
    }

    #[test]
    fn a_missing_cache_is_not_an_error() {
        let dir = tempfile::tempdir().unwrap();
        bound_build_cache(&dir.path().join("absent"), 1).unwrap();
    }
}
