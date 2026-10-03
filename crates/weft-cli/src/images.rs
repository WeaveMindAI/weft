//! Image naming, build and registry pull/push helpers. Owned by the CLI
//! so `weft daemon start` makes the images an install runs exist. No
//! external shell scripts.
//!
//! Every shared image (the runtime, the worker builder base, and the
//! standard worker every project that uses only the base catalog runs on) is
//! content-addressed: the tag is a hash of everything the
//! image is built from, so a present tag IS the right content. Ensuring
//! one is a three-step ladder: already present locally -> done; pull
//! the same tag from the registry (CI pushes every tag it builds from a
//! clean checkout, so an unmodified tree hits this) -> done; otherwise
//! build locally under the same tag (a modified tree hashes to a tag
//! the registry has never seen, so local changes always build).

use std::fmt::Write as _;
use std::os::fd::AsFd;
use std::path::{Path, PathBuf};

use anyhow::Result;
use sha2::{Digest, Sha256};
use tokio::process::Command;

/// Registry the prebuilt shared images are published to (by the
/// release workflow, on every push to the release branch). Override
/// with `WEFT_IMAGE_REGISTRY`; set it to the empty string to name
/// images bare (`weft-dispatcher:<hash>`), which also disables the
/// pull step.
const DEFAULT_IMAGE_REGISTRY: &str = "ghcr.io/weavemindai";

/// The registry prefix for shared image refs, `None` when disabled.
fn image_registry() -> Option<String> {
    parse_registry(std::env::var("WEFT_IMAGE_REGISTRY").ok().as_deref())
}

/// The registry rules, separated from the env read so they are
/// testable: unset means the default registry, blank means disabled,
/// anything else is trimmed of whitespace and a trailing slash.
fn parse_registry(raw: Option<&str>) -> Option<String> {
    match raw {
        None => Some(DEFAULT_IMAGE_REGISTRY.to_string()),
        Some(v) if v.trim().is_empty() => None,
        Some(v) => Some(v.trim().trim_end_matches('/').to_string()),
    }
}

/// `<registry>/<repo>:<tag>`, or `<repo>:<tag>` with the registry
/// disabled.
fn qualified_ref(repo: &str, tag: &str) -> String {
    qualified_ref_with(image_registry().as_deref(), repo, tag)
}

fn qualified_ref_with(registry: Option<&str>, repo: &str, tag: &str) -> String {
    match registry {
        Some(reg) => format!("{reg}/{repo}:{tag}"),
        None => format!("{repo}:{tag}"),
    }
}

/// Split a ref into its bare repo (registry prefix stripped) and tag.
/// Errors on a ref with no tag, and on a digest-pinned ref (`@sha256:`):
/// neither can be compared for staleness (a digest would read as a
/// "tag" no listing line ever matches, condemning every image of the
/// repo), and every weft ref carries a content tag by construction, so
/// either shape means a malformed override.
pub(crate) fn ref_repo_tag(image_ref: &str) -> Result<(&str, &str)> {
    anyhow::ensure!(
        !image_ref.contains('@'),
        "image ref '{image_ref}' is digest-pinned; use <repo>:<tag> \
         (weft tags are content-addressed already)"
    );
    let repo_tag = image_ref.rsplit_once('/').map_or(image_ref, |(_, t)| t);
    repo_tag.rsplit_once(':').ok_or_else(|| {
        anyhow::anyhow!("image ref '{image_ref}' carries no tag (expected <repo>:<tag>)")
    })
}

/// The docker platform string for THIS machine, `None` on an
/// architecture docker has no images for anyway.
fn host_docker_platform() -> Option<&'static str> {
    match std::env::consts::ARCH {
        "x86_64" => Some("linux/amd64"),
        "aarch64" => Some("linux/arm64"),
        _ => None,
    }
}

/// A `program` invocation that can never write to this process's
/// stdout: the child's stdout is pre-routed to OUR stderr. The CLI's
/// stdout is a data stream at two surfaces (`weft build-images` prints
/// image refs the release workflow captures; `weft build --json`
/// prints NDJSON the editor extension parses), and tools like docker,
/// buildx print progress lines to THEIR stdout, so every such
/// child in this crate goes through here. A caller that `.output()`s
/// overrides the redirect with a pipe (tokio always pipes for
/// `output()`), which is equally safe: the bytes land in the result,
/// not on our stdout. Only a `.status()` child actually streams to
/// stderr through the redirect.
pub(crate) fn quiet_stdout(program: &str) -> Command {
    let mut cmd = Command::new(program);
    // The dup cannot realistically fail (fd table exhaustion); a quiet
    // fall-through to inherited stdout would be the exact corruption
    // this function exists to prevent, so it panics instead.
    let fd = std::io::stderr().as_fd().try_clone_to_owned().expect("duplicate the stderr fd");
    cmd.stdout(std::process::Stdio::from(fd));
    cmd
}

/// `quiet_stdout("docker")`, the crate's only way to run docker.
pub(crate) fn docker() -> Command {
    quiet_stdout("docker")
}

/// `docker pull`, answering whether the image is now local. `Ok(false)`
/// covers every way the pull can come up empty (tag not in the
/// registry, no network, bare ref with no registry component): the
/// caller's next rung is a local build, and the reason is printed so a
/// registry outage doesn't silently turn every install into a compile.
/// The pull pins the host's platform, so a tag that only exists for
/// another architecture is a loud miss (and a local build) instead of
/// a silently emulated wrong-arch image.
async fn docker_pull(image_ref: &str) -> Result<bool> {
    // A ref without a registry component would resolve against docker
    // hub, which never hosts weft images.
    if !image_ref.contains('/') {
        return Ok(false);
    }
    let Err(reason) = pull_one(image_ref).await? else {
        return Ok(true);
    };
    // Tags are a hash of the image's contents, so the same repo and tag
    // in the published registry is the same image. A registry of one's
    // own (an install's Artifact Registry) starts empty: when the release
    // already published this exact image, copy it instead of building.
    if let Some(published) = published_copy(image_ref)? {
        if pull_one(&published).await?.is_ok() {
            let status = docker().args(["tag", &published, image_ref]).status().await?;
            anyhow::ensure!(status.success(), "docker tag {published} {image_ref} failed with {status}");
            eprintln!("using the published {published} as {image_ref}");
            return Ok(true);
        }
    }
    eprintln!("pull {image_ref} unavailable ({reason}); building locally");
    Ok(false)
}

/// The same image in the registry the release publishes to, when
/// `image_ref` names another registry; `None` when it already is that one.
fn published_copy(image_ref: &str) -> Result<Option<String>> {
    if image_ref.starts_with(&format!("{DEFAULT_IMAGE_REGISTRY}/")) {
        return Ok(None);
    }
    let (repo, tag) = ref_repo_tag(image_ref)?;
    Ok(Some(qualified_ref_with(Some(DEFAULT_IMAGE_REGISTRY), repo, tag)))
}

/// One `docker pull`: `Err` carries docker's reason when the image is
/// not there.
async fn pull_one(image_ref: &str) -> Result<std::result::Result<(), String>> {
    eprintln!("pulling {image_ref}");
    let mut cmd = docker();
    cmd.arg("pull");
    if let Some(platform) = host_docker_platform() {
        cmd.args(["--platform", platform]);
    }
    let out = cmd
        .arg(image_ref)
        .output()
        .await
        .map_err(|e| anyhow::anyhow!("docker not reachable on PATH: {e}"))?;
    if out.status.success() {
        return Ok(Ok(()));
    }
    let stderr = String::from_utf8_lossy(&out.stderr);
    Ok(Err(stderr.lines().last().unwrap_or("unknown error").trim().to_string()))
}

/// `docker push`, loud on failure. Used by `weft build-images` (the
/// release workflow); a local install never pushes.
pub async fn docker_push(image_ref: &str) -> Result<()> {
    let status = docker().args(["push", image_ref]).status().await?;
    anyhow::ensure!(status.success(), "docker push {image_ref} failed with {status}");
    Ok(())
}


/// The builder base's content-addressed ref for the current checkout.
pub fn builder_base_ref() -> Result<String> {
    let root = weft_compiler::build::resolve_weft_root()
        .map_err(|e| anyhow::anyhow!("resolve weft repo root: {e}"))?;
    let hash = weft_compiler::hash::compute_builder_base_hash(&root)?;
    let short = hash.chars().take(16).collect::<String>();
    Ok(qualified_ref(weft_compiler::worker_image::BUILDER_BASE_REPO, &short))
}

/// Ensure the shared pre-built worker builder base image exists.
/// Returns its content-addressed ref. An engine / toolchain bump
/// produces a fresh tag and per-project worker Dockerfiles
/// automatically pick it up via their `FROM {{builder_base_image}}`
/// line.
///
/// The base image bakes debian + rustup + the workspace's pinned
/// toolchain, plus the engine workspace at `/weft/`. Per-project
/// worker builds FROM this image and skip the apt + rustup install
/// cycle, paying only per-project costs (per-node apt packages,
/// cargo fetch + compile inside the shared BuildKit cache mounts).
pub async fn ensure_worker_builder_base() -> Result<String> {
    let image_ref = builder_base_ref()?;
    ensure_builder_base_at(&image_ref, false).await?;
    Ok(image_ref)
}

/// Materialize the builder base under `image_ref` (present -> pull ->
/// build; `rebuild` skips straight to the build, same contract as
/// `ensure_system_image`). Split from the ref computation so the
/// release workflow can materialize an arch-suffixed name while
/// everything local uses the bare one.
pub async fn ensure_builder_base_at(image_ref: &str, rebuild: bool) -> Result<()> {
    let root = weft_compiler::build::resolve_weft_root()
        .map_err(|e| anyhow::anyhow!("resolve weft repo root: {e}"))?;
    // The ref is content-addressed (the hash covers every input the
    // build context reads, via `compute_builder_base_hash`), so a
    // present ref IS the right content: no stamp file needed.
    if rebuild || !image_present(image_ref).await? {
        // The staging dir is one FIXED path (wiped and rewritten), and
        // parallel `weft test-node` processes all funnel here, so the
        // stage-and-build holds an exclusive file lock: the loser
        // blocks, then finds the ref present and skips. The lock file
        // lives BESIDE the staging dir (staging wipes the dir itself).
        // Acquired on the blocking pool: a plain block would freeze
        // this worker thread's OTHER futures (the sibling system-image
        // ensures joined with this one) for as long as another weft
        // process holds the lock.
        let ctx_dir = root.join(weft_compiler::worker_image::BASE_CONTEXT_DIR);
        if let Some(parent) = ctx_dir.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let lock_path = ctx_dir.with_extension("lock");
        let _lock = tokio::task::spawn_blocking(move || -> Result<std::fs::File> {
            let lock = std::fs::File::create(&lock_path)?;
            // A held lock means another weft process is building the
            // base (minutes on a clean machine); say so before
            // blocking, or this process sits silent the whole time.
            if lock.try_lock().is_err() {
                eprintln!(
                    "another weft process is building the shared builder base; \
                     waiting for it to finish"
                );
                lock.lock()
                    .map_err(|e| anyhow::anyhow!("lock the builder-base staging dir: {e}"))?;
            }
            Ok(lock)
        })
        .await??;
        if rebuild || (!image_present(image_ref).await? && !docker_pull(image_ref).await?) {
            // Stage the base build context: the worker-linked workspace
            // slice + toolchain pin + generated warm-up crate + the
            // RENDERED Dockerfile (target-cache key substituted), and
            // nothing else, so the baked image (and the docker tarball)
            // carry exactly what the base hash covers. See
            // `build::stage_builder_base_context`.
            let ctx = weft_compiler::build::stage_builder_base_context(&root)
                .map_err(|e| anyhow::anyhow!("stage builder-base context: {e}"))?;
            docker_build(image_ref, &ctx.join("Dockerfile"), &ctx, &[], &[]).await?;
        }
    }
    // Builder-base images are large (~1GB+: debian + rustup +
    // staged workspace). Earlier shape GC'd every prior tag after a
    // fresh ensure, but that races with in-flight per-project
    // builds: a docker build FROMing an older base ref sees the tag
    // yanked mid-build. Disk-pressure cleanup is an explicit
    // `weft clean --images` operation, not an implicit side-effect of
    // every `weft run`.
    Ok(())
}

/// The Dockerfile the runtime image builds from.
// SYNC: RUNTIME_DOCKERFILE <-> deploy/docker/runtime.Dockerfile
const RUNTIME_DOCKERFILE: &str = "runtime.Dockerfile";

/// The runtime image's repository.
pub const RUNTIME_REPO: &str = "weft-runtime";

/// The env var that overrides the runtime image's ref wholesale (an
/// operator running their own registry sets the full ref there).
pub const RUNTIME_IMAGE_ENV: &str = "WEFT_RUNTIME_IMAGE";

/// The content-addressed ref of the runtime image:
/// `<registry>/weft-runtime:<hash16>`, the hash covering the runtime
/// crate's workspace closure, the workspace manifests, the toolchain pin,
/// the Dockerfile and `.dockerignore`, and the weft source the image
/// carries to compile project versions against. `WEFT_RUNTIME_IMAGE`
/// wins verbatim when set.
pub fn runtime_image_ref() -> Result<String> {
    if let Ok(v) = std::env::var(RUNTIME_IMAGE_ENV) {
        let v = v.trim();
        if !v.is_empty() {
            anyhow::ensure!(
                !v.contains(char::is_whitespace) && !v.contains(['"', '\'']),
                "{RUNTIME_IMAGE_ENV} is '{v}', which is not a well-formed image ref (whitespace or quotes)"
            );
            ref_repo_tag(v).map_err(|e| anyhow::anyhow!("{RUNTIME_IMAGE_ENV}: {e}"))?;
            return Ok(v.to_string());
        }
    }
    let root = weft_compiler::build::resolve_weft_root().map_err(|e| anyhow::anyhow!("resolve weft repo root: {e}"))?;
    let closure = weft_compiler::codegen::workspace_crate_closure(&root, &[RUNTIME_REPO.to_string()])
        .map_err(|e| anyhow::anyhow!("runtime image crate closure: {e}"))?;
    let mut inputs: Vec<(String, PathBuf)> =
        closure.iter().map(|name| (format!("crates/{name}"), root.join("crates").join(name))).collect();
    for rel in ["Cargo.toml", "Cargo.lock", "rust-toolchain.toml"] {
        inputs.push((rel.to_string(), root.join(rel)));
    }
    inputs.push((RUNTIME_DOCKERFILE.to_string(), root.join("deploy/docker").join(RUNTIME_DOCKERFILE)));
    // .dockerignore shapes the build context the Dockerfile reads.
    inputs.push((".dockerignore".to_string(), root.join(".dockerignore")));
    let mut hasher = Sha256::new();
    for (label, path) in &inputs {
        weft_compiler::hash::hash_path_skipping(&mut hasher, label, path, &|name, at_top| {
            at_top && NOT_IN_THE_BINARY.contains(&name)
        })?;
    }
    // The weft source a version compiles against, every file of it (the
    // Dockerfile copies these whole), minus what `.dockerignore` keeps
    // out, which is the node-tree exclusion every hash walk already
    // applies (`weft_catalog::is_node_tree_excluded`).
    for rel in ["crates", "catalog", weft_compiler::hash::BUILDER_BASE_DOCKERFILE, weft_compiler::hash::BUILDER_BASE_SPLIT_SCRIPT] {
        weft_compiler::hash::hash_path(&mut hasher, &format!("carried:{rel}"), &root.join(rel))?;
    }
    Ok(qualified_ref(RUNTIME_REPO, &short_hex(&hasher.finalize())))
}

/// Make the runtime image exist locally under `image_ref`: present ->
/// done; pull -> done; build `deploy/docker/runtime.Dockerfile`.
/// `rebuild` skips straight to the build, for when a present image is
/// corrupt or hand-modified.
pub async fn ensure_runtime_image(image_ref: &str, rebuild: bool) -> Result<()> {
    if !rebuild {
        if image_present(image_ref).await? {
            eprintln!("image {image_ref} present; skipping");
            return Ok(());
        }
        if docker_pull(image_ref).await? {
            return Ok(());
        }
    }
    let root = weft_compiler::build::resolve_weft_root().map_err(|e| anyhow::anyhow!("resolve weft repo root: {e}"))?;
    let dockerfile = root.join("deploy/docker").join(RUNTIME_DOCKERFILE);
    docker_build(image_ref, &dockerfile, &root, &[], &[]).await
}

/// Every shared image one ensure materialized: the runtime, the builder
/// base every project's worker compiles on, and the standard worker.
pub struct SharedImages {
    pub runtime: String,
    pub builder_base: String,
    /// The worker built from the whole base catalog. A project whose
    /// nodes are all base catalog nodes builds to exactly this image
    /// (its hash names no project), so its first run finds it ready
    /// instead of compiling.
    pub worker: String,
}

impl SharedImages {
    /// Every bare (unsuffixed) ref, one per shared image.
    /// SYNC: `weft build-images` stdout = one registry-qualified bare
    ///       ref per line, in this order <-> .github/workflows/release.yml
    ///       (build + push shared images; the images-manifest job diffs
    ///       and stitches every line), .github/workflows/install-gcp.yml
    ///       (build the runtime's images: line 1 is the runtime, line 2
    ///       the builder base)
    pub fn bare_refs(&self) -> Vec<&str> {
        vec![self.runtime.as_str(), self.builder_base.as_str(), self.worker.as_str()]
    }
}

/// `<ref><suffix>`: the name a per-architecture half is materialized
/// and pushed under. One definition so the ensure and the push can
/// never disagree on the concatenation.
pub fn suffixed_ref(bare: &str, ref_suffix: Option<&str>) -> String {
    match ref_suffix {
        Some(suffix) => format!("{bare}{suffix}"),
        None => bare.to_string(),
    }
}

/// Make both shared images exist locally, side by side. Returns the bare
/// content-addressed refs. With `ref_suffix`, each image is materialized
/// under `<ref><suffix>` instead of its bare name: the release workflow's
/// per-architecture halves, checked and pulled by their OWN names.
///
/// `tokio::join!`, NOT `try_join!`: an early bail would drop the sibling
/// future while its `docker build` child keeps running detached; join!
/// lets both finish, then every error is reported together.
pub async fn ensure_all_shared_images(rebuild: bool, ref_suffix: Option<&str>) -> Result<SharedImages> {
    let runtime = runtime_image_ref()?;
    let base = builder_base_ref()?;
    let (runtime_target, base_target) = (suffixed_ref(&runtime, ref_suffix), suffixed_ref(&base, ref_suffix));
    let (r, b) = tokio::join!(
        ensure_runtime_image(&runtime_target, rebuild),
        ensure_builder_base_at(&base_target, rebuild),
    );
    let failures: Vec<String> = [("runtime", r), ("builder-base", b)]
        .into_iter()
        .filter_map(|(name, res)| res.err().map(|e| format!("{name}: {e:#}")))
        .collect();
    anyhow::ensure!(failures.is_empty(), "shared image build failed:\n  {}", failures.join("\n  "));
    // After the base: the standard worker is built FROM it.
    let worker = ensure_standard_worker(&base, rebuild, ref_suffix).await.map_err(|e| anyhow::anyhow!("standard worker: {e:#}"))?;
    Ok(SharedImages { runtime, builder_base: base, worker })
}

/// The standard worker's ref for this checkout:
/// `<registry>/weft-worker:<hash>` (`weft_compiler::build::standard_worker_hash`).
pub fn standard_worker_ref() -> Result<String> {
    let hash = weft_compiler::build::standard_worker_hash()?;
    Ok(qualified_ref(weft_compiler::build::WORKER_IMAGE_REPO, &hash))
}

/// Make the standard worker exist under its ref (present, else pulled,
/// else built FROM `base`, the builder base this process just ensured).
/// An install holds it under a name of its own ([`hold_standard_worker`]).
async fn ensure_standard_worker(base: &str, rebuild: bool, suffix: Option<&str>) -> Result<String> {
    let image = standard_worker_ref()?;
    let target = suffixed_ref(&image, suffix);
    if rebuild || (!image_present(&target).await? && !docker_pull(&target).await?) {
        let stock = weft_compiler::build::StockProject::materialize()?;
        let bases = weft_compiler::worker_image::BaseImages {
            builder: suffixed_ref(base, suffix),
            runtime: weft_compiler::worker_image::DEFAULT_BASE_IMAGE.to_string(),
        };
        let build = weft_compiler::build::build_project(
            &stock.project,
            &stock.definition,
            &stock.catalog,
            &bases,
            weft_core::builds::NodeSet::Full,
        )?;
        docker_build_worker(&target, &build.build_context.join("Dockerfile"), &build.build_context, &[]).await?;
    }
    Ok(image)
}

/// Give `install` its own name for the standard worker `image`
/// (`Install::local_image_ref`), the name its builds look for, so a stock
/// project's build finds it and compiles nothing. The install records it
/// and removes that name once a newer weft names another standard worker;
/// the image goes with its last name.
pub async fn hold_standard_worker(install: &weft_core::infra::Install, image: &str) -> Result<()> {
    let (repo, tag) = ref_repo_tag(image)?;
    let held = install.local_image_ref(&format!("{repo}:{tag}"));
    if held != image && !image_present(&held).await? {
        let out = docker().args(["tag", image, &held]).output().await?;
        anyhow::ensure!(out.status.success(), "docker tag {image} {held}: {}", String::from_utf8_lossy(&out.stderr).trim());
    }
    Ok(())
}

/// One BuildKit record of a worker compile cache mount, as `docker
/// buildx du --verbose` lists it: there is one per cache key (build
/// environment and builder), and a key retired by a toolchain bump keeps
/// its record, and its bytes, until something prunes it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompileCacheRecord {
    pub id: String,
    /// The cache key (`hash::compute_worker_cache_key`) in the mount id,
    /// without the compile lane after it: every lane of a key is that
    /// key's cache.
    pub key: String,
    /// The compile lane after the key, `None` when the mount id carries
    /// no lane number.
    pub lane: Option<u32>,
    /// What the mount id carries after the key instead of a lane number
    /// (empty when nothing): a mount id this build does not make, kept so
    /// the size report shows its bytes instead of hiding them.
    pub unreadable_lane: Option<String>,
    pub size: String,
    /// Whole days since BuildKit last used it, read off its "Last used"
    /// line (`0` for anything under a day).
    pub idle_days: u64,
}

/// Every worker compile cache mount on this host.
pub async fn worker_compile_caches() -> Result<Vec<CompileCacheRecord>> {
    let output = docker().args(["buildx", "du", "--verbose"]).output().await?;
    anyhow::ensure!(
        output.status.success(),
        "docker buildx du exited {}: {}",
        output.status,
        String::from_utf8_lossy(&output.stderr).trim()
    );
    Ok(compile_caches_in(&String::from_utf8_lossy(&output.stdout)))
}

/// Drop one BuildKit record by id (`docker buildx prune --filter id=`),
/// and check it is gone: prune exits 0 whether or not it reclaimed
/// anything, so a record it declined to touch would otherwise be
/// reported as dropped on every run.
pub async fn prune_build_record(id: &str) -> Result<()> {
    let output = docker().args(["buildx", "prune", "--force", "--filter", &format!("id={id}")]).output().await?;
    anyhow::ensure!(
        output.status.success(),
        "docker buildx prune {id} exited {}: {}",
        output.status,
        String::from_utf8_lossy(&output.stderr).trim()
    );
    anyhow::ensure!(
        worker_compile_caches().await?.iter().all(|cache| cache.id != id),
        "docker buildx prune left record {id} in place; `docker buildx prune --all` removes it by hand"
    );
    Ok(())
}

/// The worker compile cache records in `docker buildx du --verbose`
/// output: blank-line separated `Key: value` blocks whose description
/// names a mount id starting with `WORKER_CACHE_MOUNT_ID_PREFIX`.
fn compile_caches_in(du_verbose: &str) -> Vec<CompileCacheRecord> {
    let wanted = format!("with id \"/{}", weft_compiler::worker_image::WORKER_CACHE_MOUNT_ID_PREFIX);
    du_verbose.split("\n\n").filter_map(|record| {
        let field = |key: &str| record.lines().find_map(|line| line.strip_prefix(key)).map(str::trim);
        let description = field("Description:")?;
        let key_start = description.find(&wanted)? + wanted.len();
        // `<key>-<lane>`; the key is hex, so the first dash ends it.
        let id = description[key_start..].split('"').next()?;
        let (key, lane, unreadable_lane) = match id.split_once('-') {
            Some((key, suffix)) => match suffix.parse() {
                Ok(lane) => (key.to_string(), Some(lane), None),
                Err(_) => (key.to_string(), None, Some(suffix.to_string())),
            },
            None => (id.to_string(), None, Some(String::new())),
        };
        Some(CompileCacheRecord {
            id: field("ID:")?.to_string(),
            key,
            lane,
            unreadable_lane,
            size: field("Size:")?.to_string(),
            idle_days: idle_days(field("Last used:").unwrap_or("")),
        })
    }).collect()
}

/// Whole days in a BuildKit "Last used" phrase ("56 minutes ago", "3
/// days ago", "2 weeks ago", "About an hour ago", "Never"). Anything not
/// counted in days or longer is under a day.
fn idle_days(last_used: &str) -> u64 {
    let mut words = last_used.split_whitespace();
    let Some(count) = words.next().and_then(|n| n.parse::<u64>().ok()) else { return 0 };
    match words.next().map(|unit| unit.trim_end_matches('s')) {
        Some("day") => count,
        Some("week") => count * 7,
        Some("month") => count * 30,
        Some("year") => count * 365,
        _ => 0,
    }
}

/// THE one `docker build` invocation, BuildKit on. Every image the CLI
/// builds (builder base, runtime, worker, infra, node-test)
/// funnels here. `labels` stamp the `weft.dev/*` filters later GC
/// selects on (empty for the shared images, which GC by repo+tag).
pub(crate) async fn docker_build(
    image_ref: &str,
    dockerfile: &Path,
    context: &Path,
    labels: &[String],
    build_args: &[String],
) -> Result<()> {
    eprintln!(
        "building image {image_ref} (this may take several minutes on first run; \
         subsequent builds are incremental)"
    );
    // We DO want docker's layer cache: combined with the buildkit
    // cargo cache mounts the Dockerfiles declare, an unchanged crate
    // set short-circuits to seconds. Deeper source changes are
    // caught by cargo's own fingerprinting inside the cache mount.
    let mut cmd = docker();
    cmd.env("DOCKER_BUILDKIT", "1")
        .args(["build", "-t", image_ref, "-f"])
        .arg(dockerfile);
    for l in labels {
        cmd.args(["--label", l]);
    }
    for arg in build_args {
        cmd.args(["--build-arg", arg]);
    }
    let status = cmd.arg(context).status().await?;
    if !status.success() {
        anyhow::bail!("docker build {image_ref} failed with {status}");
    }
    Ok(())
}

/// Build a node-test image (a Dockerfile from the worker templates, whose
/// compile cache is split into lanes) on this machine: [`docker_build`]
/// holding one of this host's compile lanes for the whole build, named to
/// the build through [`weft_compiler::worker_image::COMPILE_LANE_ARG`].
/// Project images are built by the install (`weft-dispatcher::build`),
/// which holds lanes of its own the same way.
pub(crate) async fn docker_build_worker(
    image_ref: &str,
    dockerfile: &Path,
    context: &Path,
    labels: &[String],
) -> Result<()> {
    let lane = CompileLane::hold().await?;
    let arg = format!("{}={}", weft_compiler::worker_image::COMPILE_LANE_ARG, lane.number);
    docker_build(image_ref, dockerfile, context, labels, &[arg]).await
}

/// How many worker builds compile side by side when nobody says
/// otherwise. Each lane keeps a compile cache of its own (about 2GB), so
/// this is also how many of those are kept.
const DEFAULT_COMPILE_LANES: u32 = 4;

/// Overrides [`DEFAULT_COMPILE_LANES`]: more for a machine that builds
/// many projects at once (and has the disk), 1 to keep a single cache.
const COMPILE_LANES_ENV: &str = "WEFT_COMPILE_LANES";

/// One of this host's compile lanes, held (an exclusive lock on its file
/// under the weft data dir) until dropped. Every weft process on the host
/// picks from the same lock files, so no two builds hold one lane.
struct CompileLane {
    number: u32,
    _lock: std::fs::File,
}

impl CompileLane {
    /// The first free lane, waiting for one to free up when all are held.
    async fn hold() -> Result<Self> {
        let lanes = compile_lanes()?;
        let dir = crate::commands::daemon::data_dir().join("compile-lanes");
        std::fs::create_dir_all(&dir)?;
        let files = (0..lanes)
            .map(|n| {
                let file = std::fs::OpenOptions::new()
                    .create(true)
                    .truncate(false)
                    .write(true)
                    .open(dir.join(format!("{n}.lock")))?;
                Ok((n, file))
            })
            .collect::<Result<Vec<_>>>()?;
        let mut said = false;
        loop {
            for (number, file) in &files {
                match file.try_lock() {
                    Ok(()) => {
                        let _lock = file.try_clone()?;
                        return Ok(Self { number: *number, _lock });
                    }
                    Err(std::fs::TryLockError::WouldBlock) => {}
                    Err(std::fs::TryLockError::Error(e)) => {
                        return Err(anyhow::anyhow!("lock compile lane {number}: {e}"));
                    }
                }
            }
            if !said {
                eprintln!(
                    "all {lanes} compile lanes are busy with other builds; waiting for one \
                     ({COMPILE_LANES_ENV} sets how many)"
                );
                said = true;
            }
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        }
    }
}

/// How many compile lanes there are: [`COMPILE_LANES_ENV`], or
/// [`DEFAULT_COMPILE_LANES`]. This host's node-test builds hold one each,
/// and `weft daemon start` hands the same count to the install's BuildKit.
pub(crate) fn compile_lanes() -> Result<u32> {
    match std::env::var(COMPILE_LANES_ENV) {
        Err(_) => Ok(DEFAULT_COMPILE_LANES),
        Ok(raw) => match raw.trim().parse::<u32>() {
            Ok(n) if n >= 1 => Ok(n),
            _ => anyhow::bail!("{COMPILE_LANES_ENV}={raw:?}: expected a whole number of at least 1"),
        },
    }
}

/// Directories inside a crate that the image build never compiles, so a change
/// in one must not rebuild the runtime image.
/// SYNC: what the runtime image's hash covers <-> .dockerignore (what the
///       repo-root build context ships). The two need not be byte-equal
///       (only the compiled binaries + catalog reach the final stage),
///       but a path that changes what a binary compiles TO must be in
///       both, or two trees with one hash build different images.
const NOT_IN_THE_BINARY: &[&str] = &["tests", "benches", "examples"];

/// The first 8 bytes of a digest, hex: 64 bits, plenty for a
/// content-addressed tag.
fn short_hex(digest: &[u8]) -> String {
    let mut out = String::with_capacity(16);
    for b in digest.iter().take(8) {
        let _ = write!(&mut out, "{:02x}", b);
    }
    out
}

pub async fn image_present(image_ref: &str) -> Result<bool> {
    let out = docker()
        .args(["image", "inspect", image_ref])
        .output()
        .await
        .map_err(|e| anyhow::anyhow!("docker not reachable on PATH: {e}"))?;
    if out.status.success() {
        return Ok(true);
    }
    // `docker image inspect` exits non-zero in two distinct cases:
    // 1. image truly absent (stderr: "Error: No such image: ...").
    //    This is the answer Ok(false) the caller wants.
    // 2. daemon unreachable (stderr: "Cannot connect to the Docker
    //    daemon ...") or any other infra failure. We refuse to
    //    treat that as "image absent" because the caller would
    //    silently rebuild on every invocation, masking the real
    //    problem. Surface as Err.
    let stderr = String::from_utf8_lossy(&out.stderr);
    if stderr.contains("No such image") || stderr.contains("no such image") {
        Ok(false)
    } else {
        anyhow::bail!(
            "docker image inspect failed (image='{image_ref}'): {}",
            stderr.trim()
        )
    }
}

/// The predicate a "current ref" sweep condemns with: every tag of
/// `current_ref`'s repo except the current one, under any registry
/// spelling. Owning the ref split here means no caller hand-rolls it,
/// and a `current_ref` that cannot be split is a loud error, never an
/// everything-condemned sweep. Compose it with a referenced-set hold
/// where one applies.
pub fn outside_current(current_ref: &str) -> Result<impl Fn(&str, &str) -> bool + '_> {
    let (repo, current) = ref_repo_tag(current_ref)?;
    Ok(move |r: &str, t: &str| r == repo && t != current)
}

/// The standard workers in `listing` other than the current one
/// (`current_ref`). The standard worker is the one worker this host pulls
/// or builds under a registry name, and each weft version names a new one,
/// so every older one is dead. Only weft's own registry names go: the
/// current ref's registry and the one the release publishes to (the copy
/// it may have been tagged from), never another repository that happens to
/// be called `weft-worker`. The current hash is current under any suffix
/// (`<hash>-amd64`, the release's per-architecture halves): content hashes
/// have one length, so no other hash starts with it. An install's own name
/// for a worker (`Install::local_image_ref`) carries no registry, and that
/// install reclaims it.
pub fn stale_standard_workers(listing: &str, current_ref: &str) -> Result<Vec<String>> {
    let (_, current) = ref_repo_tag(current_ref)?;
    // With the registry disabled the current ref is bare, the default
    // install's own name, and only the published registry is left to sweep.
    let registry = current_ref.rsplit_once('/').map_or(DEFAULT_IMAGE_REGISTRY, |(registry, _)| registry);
    let repos = [registry, DEFAULT_IMAGE_REGISTRY].map(|r| format!("{r}/{}:", weft_compiler::build::WORKER_IMAGE_REPO));
    Ok(listing
        .lines()
        .map(str::trim)
        .filter(|full| {
            repos.iter().any(|repo| full.strip_prefix(repo.as_str()).is_some_and(|tag| !tag.is_empty() && !tag.starts_with(current)))
        })
        .map(str::to_string)
        .collect())
}

/// Host `docker images` lines (one `repo:tag` per line) that a sweep
/// may delete: those whose prefix-stripped `(repo, tag)` parse and
/// satisfy `condemn`. THE host-side matcher (the runtime image, builder
/// bases, worker and infra reclaims all pass their own predicate over
/// the same split), the host sibling of `node_images_matching`, pure
/// for the same reason: the two matchers decide what a sweep may
/// delete. Untagged/dangling lines (`<none>:<none>`) parse to a repo no
/// weft predicate names.
pub fn host_images_matching(listing: &str, condemn: impl Fn(&str, &str) -> bool) -> Vec<String> {
    listing
        .lines()
        .map(str::trim)
        .filter(|full| ref_repo_tag(full).is_ok_and(|(r, t)| condemn(r, t)))
        .map(str::to_string)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Records as `docker buildx du --verbose` printed them on a host that
    /// had built workers under two cache keys, one of them in compile
    /// lane 2 (captured verbatim, fields this parser ignores trimmed).
    #[test]
    fn every_compile_cache_key_is_read_off_the_buildkit_records() {
        let du = "ID:           p490dco22moj9u55682dm3enh\nParents:\n - x\nCreated at:   2026-09-13 17:34:56 +0000 UTC\nMutable:      true\nReclaimable:  true\nShared:       false\nSize:         421.7MB\n\
                  Description:  cached mount /root/.cargo/registry from exec /bin/sh -c cargo build --release with id \"/weft-worker-cargo-registry\"\nUsage count:  6\nLast used:    11 seconds ago\nType:         exec.cachemount\n\n\
                  ID:           z5k0h7hhdmaktflss7bsyjs7u\nMutable:      true\nSize:         1.982GB\n\
                  Description:  cached mount /cache/target from exec /bin/sh -c ( [ -f /cache/target/.weft-seeded ] || ! [ -d /weft/target ] ) && cargo build --release with id \"/weft-worker-target-9af3f2c5f9da6eaa-2\"\nUsage count:  6\nLast used:    11 seconds ago\nType:         exec.cachemount\n\n\
                  ID:           hve80t8pw8n1ackgqj82wn30e\nSize:         6.1GB\n\
                  Description:  cached mount /cache/target from exec /bin/sh -c cargo build --release with id \"/weft-worker-target-0123456789abcdef\"\nUsage count:  2\nLast used:    5 weeks ago\nType:         exec.cachemount\n\n\
                  ID:           q1\nSize:         3MB\n\
                  Description:  cached mount /cache/target with id \"/weft-worker-target-0123456789abcdef-x2\"\nLast used:    2 minutes ago\n";
        assert_eq!(compile_caches_in(du), vec![
            CompileCacheRecord { id: "z5k0h7hhdmaktflss7bsyjs7u".into(), key: "9af3f2c5f9da6eaa".into(), lane: Some(2), unreadable_lane: None, size: "1.982GB".into(), idle_days: 0 },
            CompileCacheRecord { id: "hve80t8pw8n1ackgqj82wn30e".into(), key: "0123456789abcdef".into(), lane: None, unreadable_lane: Some(String::new()), size: "6.1GB".into(), idle_days: 35 },
            CompileCacheRecord { id: "q1".into(), key: "0123456789abcdef".into(), lane: None, unreadable_lane: Some("x2".into()), size: "3MB".into(), idle_days: 0 },
        ]);
        assert!(compile_caches_in("ID: c\nSize: 1MB\nDescription: something else\n").is_empty());
        assert_eq!(idle_days("About an hour ago"), 0);
        assert_eq!(idle_days("3 days ago"), 3);
        assert_eq!(idle_days("2 months ago"), 60);
        assert_eq!(idle_days("Never"), 0);
    }

    /// The ref is `<registry>/<repo>:<tag>` and drops to a bare
    /// `<repo>:<tag>` when the registry is disabled: the bare shape is
    /// what keeps `docker_pull` from ever dialing docker hub for a
    /// weft image.
    #[test]
    fn refs_qualify_with_and_without_a_registry() {
        assert_eq!(
            qualified_ref_with(Some("ghcr.io/weavemindai"), "weft-dispatcher", "abc123"),
            "ghcr.io/weavemindai/weft-dispatcher:abc123"
        );
        assert_eq!(
            qualified_ref_with(None, "weft-dispatcher", "abc123"),
            "weft-dispatcher:abc123"
        );
    }

    /// An image wanted in another registry is looked for under the same
    /// repo and tag in the published one.
    #[test]
    fn the_published_copy_is_the_same_repo_and_tag() {
        assert_eq!(
            published_copy("us-central1-docker.pkg.dev/p/weft/weft-runtime:abc").unwrap().as_deref(),
            Some("ghcr.io/weavemindai/weft-runtime:abc")
        );
        assert_eq!(published_copy("ghcr.io/weavemindai/weft-runtime:abc").unwrap(), None);
    }

    /// Unset means the default registry, blank means disabled, and a
    /// value is trimmed of whitespace and a trailing slash.
    #[test]
    fn registry_parsing_rules() {
        assert_eq!(parse_registry(None), Some(DEFAULT_IMAGE_REGISTRY.to_string()));
        assert_eq!(parse_registry(Some("")), None);
        assert_eq!(parse_registry(Some("   ")), None);
        assert_eq!(parse_registry(Some("ghcr.io/x/")), Some("ghcr.io/x".to_string()));
        assert_eq!(parse_registry(Some(" ghcr.io/x ")), Some("ghcr.io/x".to_string()));
    }

    /// The (repo, tag) split ignores any registry prefix (ports
    /// included) and refuses an untagged ref: a sweep that cannot name
    /// the current tag must skip, never condemn everything.
    #[test]
    fn ref_splitting_handles_registries_and_refuses_untagged() {
        assert_eq!(
            ref_repo_tag("ghcr.io/weavemindai/weft-dispatcher:abc").unwrap(),
            ("weft-dispatcher", "abc")
        );
        assert_eq!(
            ref_repo_tag("localhost:5000/weft-dispatcher:abc").unwrap(),
            ("weft-dispatcher", "abc")
        );
        assert_eq!(ref_repo_tag("weft-dispatcher:local").unwrap(), ("weft-dispatcher", "local"));
        assert!(ref_repo_tag("myreg.example/weft-dispatcher").is_err());
        assert!(ref_repo_tag("weft-dispatcher").is_err());
        // A digest-pinned ref would split into a "tag" no listing line
        // ever equals, condemning the whole repo; refused instead.
        assert!(ref_repo_tag("ghcr.io/weavemindai/weft-dispatcher@sha256:0011").is_err());
    }

    /// Only weft's registry names of older standard workers go: never the
    /// current one under any suffix, never an install's own name, never
    /// another repository called `weft-worker`.
    #[test]
    fn only_older_standard_workers_are_stale() {
        let listing = "ghcr.io/weavemindai/weft-worker:now\nghcr.io/weavemindai/weft-worker:now-amd64\n\
                       ghcr.io/weavemindai/weft-worker:old\nghcr.io/weavemindai/weft-worker:old-arm64\n\
                       eu.gcr.io/p/weft-worker:older\neu.gcr.io/p/weft-worker:now\nweft-worker:project\n\
                       localhost/weft-cell7/weft-worker:old\nsomeone/weft-worker:old\nghcr.io/other/weft-worker:old\n\
                       ghcr.io/weavemindai/weft-runtime:old\n<none>:<none>\n";
        assert_eq!(
            stale_standard_workers(listing, "ghcr.io/weavemindai/weft-worker:now").unwrap(),
            vec!["ghcr.io/weavemindai/weft-worker:old".to_string(), "ghcr.io/weavemindai/weft-worker:old-arm64".to_string()]
        );
        assert_eq!(
            stale_standard_workers(listing, "eu.gcr.io/p/weft-worker:now").unwrap(),
            vec![
                "ghcr.io/weavemindai/weft-worker:old".to_string(),
                "ghcr.io/weavemindai/weft-worker:old-arm64".to_string(),
                "eu.gcr.io/p/weft-worker:older".to_string()
            ]
        );
        assert_eq!(
            stale_standard_workers(listing, "weft-worker:now").unwrap(),
            vec!["ghcr.io/weavemindai/weft-worker:old".to_string(), "ghcr.io/weavemindai/weft-worker:old-arm64".to_string()],
            "with the registry disabled, the published registry is the only one swept"
        );
    }

    /// The host sweep must remove every other tag of a system repo
    /// (the pre-registry `:local` spelling included), keep the tag the
    /// install runs under EITHER spelling, skip untagged/dangling
    /// lines, and never touch foreign repos (`weft-infra-supervisor`
    /// vs a `weft-infra-<node>` image included).
    #[test]
    fn host_sweep_condemns_only_stale_system_tags() {
        let listing = "\
ghcr.io/weavemindai/weft-dispatcher:abc123
weft-dispatcher:abc123
weft-dispatcher:local
ghcr.io/weavemindai/weft-dispatcher:0ld0ld
weft-worker:abc123
weft-infra-supervisor:abc123
weft-infra-postgres:zzz999
<none>:<none>
";
        let stale = host_images_matching(
            listing,
            outside_current("ghcr.io/weavemindai/weft-dispatcher:abc123").unwrap(),
        );
        assert_eq!(
            stale,
            vec![
                "weft-dispatcher:local".to_string(),
                "ghcr.io/weavemindai/weft-dispatcher:0ld0ld".to_string(),
            ]
        );
        let infra =
            host_images_matching(listing, outside_current("weft-infra-supervisor:abc123").unwrap());
        assert!(infra.is_empty(), "{infra:?}");
        // Composed with another hold: a non-current tag it protects
        // survives.
        let current = outside_current("weft-dispatcher:abc123").unwrap();
        let held = host_images_matching(listing, |r, t| current(r, t) && t != "0ld0ld");
        assert_eq!(held, vec!["weft-dispatcher:local".to_string()]);
        // A current ref that cannot be split is a loud error, never an
        // everything-condemned sweep.
        assert!(outside_current("weft-dispatcher").is_err());
    }
}
