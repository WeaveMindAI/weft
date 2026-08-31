//! Image naming, build, registry pull/push and kind-load helpers. Owned
//! by the CLI so the user runs `weft daemon start` / `weft infra up` and
//! the right images land in the cluster. No external shell scripts.
//!
//! Every shared image (the four system services + the worker builder
//! base) is content-addressed: the tag is a hash of everything the
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
/// crictl and kind print progress lines to THEIR stdout, so every such
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
        return Ok(true);
    }
    let stderr = String::from_utf8_lossy(&out.stderr);
    let reason = stderr.lines().last().unwrap_or("unknown error").trim();
    eprintln!("pull {image_ref} unavailable ({reason}); building locally");
    Ok(false)
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
            docker_build(image_ref, &ctx.join("Dockerfile"), &ctx, &[], None).await?;
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

/// The one Dockerfile every system image builds from: a shared builder stage
/// compiles all four binaries in ONE cargo invocation (one target cache, one
/// pass over the workspace instead of four), and each image is a named runtime
/// stage selected with `docker build --target <stage>`.
const SYSTEM_IMAGES_DOCKERFILE: &str = "system-images.Dockerfile";

/// The four long-running system services and everything image-shaped
/// about each: its repo name, its stage in the shared Dockerfile, the
/// binary crate whose closure keys its content hash, and the env var
/// that overrides its ref wholesale (an operator running their own
/// registry sets the full ref there and this module's naming steps
/// aside).
/// SYNC: the binary crate names and the dockerfile_stage names <-> the
///       `-p ... --bin ...` package list and the `AS <stage>` names in
///       deploy/docker/system-images.Dockerfile
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SystemService {
    Dispatcher,
    Listener,
    Broker,
    Supervisor,
}

impl SystemService {
    pub const ALL: [SystemService; 4] =
        [Self::Dispatcher, Self::Listener, Self::Broker, Self::Supervisor];

    pub fn repo(self) -> &'static str {
        match self {
            Self::Dispatcher => "weft-dispatcher",
            Self::Listener => "weft-listener",
            Self::Broker => "weft-broker",
            Self::Supervisor => "weft-infra-supervisor",
        }
    }

    /// The named runtime stage in `system-images.Dockerfile`.
    fn dockerfile_stage(self) -> &'static str {
        match self {
            Self::Dispatcher => "dispatcher",
            Self::Listener => "listener",
            Self::Broker => "broker",
            Self::Supervisor => "supervisor",
        }
    }

    /// The binary crate whose workspace closure keys this image's
    /// content hash. Same string as the repo today, but the two answer
    /// different questions.
    fn binary_crate(self) -> &'static str {
        self.repo()
    }

    /// The env var that both OVERRIDES this service's ref (read in
    /// `system_image_ref`) and CARRIES it into the manifests (the
    /// substitution var name in `manifest_template_vars`), one name so
    /// the two can never disagree.
    /// SYNC: WEFT_*_IMAGE names <-> deploy/k8s/dispatcher.yaml (env block),
    ///       deploy/k8s/broker.yaml (image line),
    ///       crates/weft-dispatcher/src/app.rs (listener/supervisor backend reads)
    pub fn image_env(self) -> &'static str {
        match self {
            Self::Dispatcher => "WEFT_DISPATCHER_IMAGE",
            Self::Listener => "WEFT_LISTENER_IMAGE",
            Self::Broker => "WEFT_BROKER_IMAGE",
            Self::Supervisor => "WEFT_SUPERVISOR_IMAGE",
        }
    }
}

/// The resolved image ref of every system service, computed once per
/// daemon start/restart and threaded everywhere a ref is needed
/// (ensure, kind load, manifest substitution, pooled-tier reconcile),
/// so no two consumers can disagree on what "the dispatcher image" is.
#[derive(Debug, Clone)]
pub struct SystemImages {
    pub dispatcher: String,
    pub listener: String,
    pub broker: String,
    pub supervisor: String,
}

impl SystemImages {
    pub fn resolve() -> Result<Self> {
        Ok(Self {
            dispatcher: system_image_ref(SystemService::Dispatcher)?,
            listener: system_image_ref(SystemService::Listener)?,
            broker: system_image_ref(SystemService::Broker)?,
            supervisor: system_image_ref(SystemService::Supervisor)?,
        })
    }

    pub fn get(&self, svc: SystemService) -> &str {
        match svc {
            SystemService::Dispatcher => &self.dispatcher,
            SystemService::Listener => &self.listener,
            SystemService::Broker => &self.broker,
            SystemService::Supervisor => &self.supervisor,
        }
    }
}

/// The content-addressed ref for one system service:
/// `<registry>/<repo>:<hash16>` where the hash covers the binary
/// crate's workspace closure + workspace manifests + toolchain pin +
/// the shared Dockerfile + `.dockerignore`. A change
/// confined to one service moves only that service's ref; a shared
/// crate is in every closure, so it moves all four. The env override
/// (`WEFT_<SVC>_IMAGE`) wins verbatim when set.
pub fn system_image_ref(svc: SystemService) -> Result<String> {
    if let Ok(v) = std::env::var(svc.image_env()) {
        let v = v.trim();
        if !v.is_empty() {
            // The ref lands verbatim in rendered k8s manifests and in
            // tag comparisons, so reject shapes that would corrupt
            // either: whitespace/quotes break the YAML, a missing tag
            // breaks the staleness compare (see `ref_repo_tag`).
            anyhow::ensure!(
                !v.contains(char::is_whitespace) && !v.contains(['"', '\'']),
                "{} is '{v}', which is not a well-formed image ref \
                 (whitespace or quotes)",
                svc.image_env()
            );
            ref_repo_tag(v).map_err(|e| anyhow::anyhow!("{}: {e}", svc.image_env()))?;
            return Ok(v.to_string());
        }
    }
    let root = weft_compiler::build::resolve_weft_root()
        .map_err(|e| anyhow::anyhow!("resolve weft repo root: {e}"))?;
    let closure =
        weft_compiler::codegen::workspace_crate_closure(&root, &[svc.binary_crate().to_string()])
            .map_err(|e| anyhow::anyhow!("system-image crate closure: {e}"))?;
    let mut inputs: Vec<(String, PathBuf)> = closure
        .iter()
        .map(|name| (format!("crates/{name}"), root.join("crates").join(name)))
        .collect();
    for rel in ["Cargo.toml", "Cargo.lock", "rust-toolchain.toml"] {
        inputs.push((rel.to_string(), root.join(rel)));
    }
    inputs.push((
        SYSTEM_IMAGES_DOCKERFILE.to_string(),
        root.join("deploy/docker").join(SYSTEM_IMAGES_DOCKERFILE),
    ));
    // .dockerignore shapes the build context the Dockerfile reads, so
    // it is an input like the Dockerfile itself; without it two trees
    // hashing identically could ship different contexts.
    inputs.push((".dockerignore".to_string(), root.join(".dockerignore")));
    Ok(qualified_ref(svc.repo(), &hash_inputs(&inputs)?))
}

/// Make a system service's image exist locally under `image_ref`:
/// present -> done; pull -> done; build the service's stage of
/// `deploy/docker/system-images.Dockerfile`. `rebuild` skips straight
/// to the build, for when a present image is corrupt or hand-modified.
pub async fn ensure_system_image(
    svc: SystemService,
    image_ref: &str,
    rebuild: bool,
) -> Result<()> {
    if !rebuild {
        if image_present(image_ref).await? {
            // Progress to stderr (data on stdout, progress on stderr).
            eprintln!("image {image_ref} present; skipping");
            return Ok(());
        }
        if docker_pull(image_ref).await? {
            return Ok(());
        }
    }
    let root = weft_compiler::build::resolve_weft_root()
        .map_err(|e| anyhow::anyhow!("resolve weft repo root: {e}"))?;
    let dockerfile = root.join("deploy/docker").join(SYSTEM_IMAGES_DOCKERFILE);
    docker_build(image_ref, &dockerfile, &root, &[], Some(svc.dockerfile_stage())).await
}

/// How `ensure_all_shared_images` treats a builder-base failure. The
/// base is a pre-warm for future `weft run`s in the daemon's boot
/// (which re-ensure it with the user present), but the one artifact
/// the release workflow exists to publish.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum BaseFailure {
    Fatal,
    Warn,
}

/// Every shared image one ensure materialized: the four system refs
/// plus the builder base, `None` when its ensure failed under
/// `BaseFailure::Warn` (never under `Fatal`, which fails the ensure).
pub struct SharedImages {
    pub system: SystemImages,
    pub builder_base: Option<String>,
}

impl SharedImages {
    /// Every bare (unsuffixed) ref, one per shared image.
    /// SYNC: `weft build-images` stdout = one registry-qualified bare
    ///       ref per line <-> .github/workflows/release.yml (images
    ///       job: the smoke-run step greps by repo name; the
    ///       images-manifest job diffs and stitches every line)
    pub fn bare_refs(&self) -> Vec<&str> {
        let mut refs = vec![
            self.system.dispatcher.as_str(),
            self.system.listener.as_str(),
            self.system.broker.as_str(),
            self.system.supervisor.as_str(),
        ];
        if let Some(base) = &self.builder_base {
            refs.push(base);
        }
        refs
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

/// Make every shared image (the four system services + the worker
/// builder base) exist locally, concurrently (independent input sets,
/// per-image buildkit cache mounts). Returns the bare
/// content-addressed refs. With `ref_suffix`, each image is
/// materialized under `<ref><suffix>` instead of its bare name: the
/// release workflow's per-architecture halves, checked and pulled by
/// their OWN names so a bare ref that predates multi-arch can never
/// satisfy (and then poison) an architecture leg.
///
/// `tokio::join!`, NOT `try_join!`: an early bail would drop the
/// sibling futures while their `docker build` children keep running
/// detached (orphaned builds churning CPU with nobody reading the
/// result). join! lets every build finish, then all errors are
/// aggregated into one loud failure.
pub async fn ensure_all_shared_images(
    rebuild: bool,
    base_failure: BaseFailure,
    ref_suffix: Option<&str>,
) -> Result<SharedImages> {
    let imgs = SystemImages::resolve()?;
    let base_ref = builder_base_ref()?;
    let base_target = suffixed_ref(&base_ref, ref_suffix);
    let base_ensure = async {
        ensure_builder_base_at(&base_target, rebuild).await.map(|()| base_ref.clone())
    };
    let dispatcher_ref = suffixed_ref(&imgs.dispatcher, ref_suffix);
    let listener_ref = suffixed_ref(&imgs.listener, ref_suffix);
    let broker_ref = suffixed_ref(&imgs.broker, ref_suffix);
    let supervisor_ref = suffixed_ref(&imgs.supervisor, ref_suffix);
    let (dispatcher, listener, broker, supervisor, base) = tokio::join!(
        ensure_system_image(SystemService::Dispatcher, &dispatcher_ref, rebuild),
        ensure_system_image(SystemService::Listener, &listener_ref, rebuild),
        ensure_system_image(SystemService::Broker, &broker_ref, rebuild),
        ensure_system_image(SystemService::Supervisor, &supervisor_ref, rebuild),
        base_ensure,
    );
    let mut failures: Vec<String> = Vec::new();
    for (name, res) in [
        ("dispatcher", dispatcher),
        ("listener", listener),
        ("broker", broker),
        ("supervisor", supervisor),
    ] {
        if let Err(e) = res {
            failures.push(format!("{name}: {e:#}"));
        }
    }
    let base = match base {
        Ok(bare) => Some(bare),
        Err(e) => {
            match base_failure {
                BaseFailure::Fatal => failures.push(format!("builder-base: {e:#}")),
                BaseFailure::Warn => tracing::warn!(
                    target: "weft_cli::images",
                    error = %e,
                    "pre-warm of worker builder base failed; next `weft run` will retry"
                ),
            }
            None
        }
    };
    anyhow::ensure!(
        failures.is_empty(),
        "shared image build failed:\n  {}",
        failures.join("\n  ")
    );
    Ok(SharedImages { system: imgs, builder_base: base })
}

/// THE one `docker build` invocation, BuildKit on. Every image the CLI
/// builds (builder base, system images, worker, infra, node-test)
/// funnels here. `labels` stamp the `weft.dev/*` filters later GC
/// selects on (empty for the shared images, which GC by repo+tag);
/// `target` selects a named stage of a multi-target Dockerfile (the
/// system images); `None` builds the final stage.
pub(crate) async fn docker_build(
    image_ref: &str,
    dockerfile: &Path,
    context: &Path,
    labels: &[String],
    target: Option<&str>,
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
    if let Some(stage) = target {
        cmd.args(["--target", stage]);
    }
    let status = cmd.arg(context).status().await?;
    if !status.success() {
        anyhow::bail!("docker build {image_ref} failed with {status}");
    }
    Ok(())
}

/// Directories inside a crate that the image build never compiles, so a change
/// in one must not roll four images and the pods running them.
/// SYNC: what a system image's hash covers <-> .dockerignore (what the
///       repo-root build context ships). The two need not be byte-equal
///       (only the compiled binaries + catalog reach the final stage),
///       but a path that changes what a binary compiles TO must be in
///       both, or two trees with one hash build different images.
const NOT_IN_THE_BINARY: &[&str] = &["tests", "benches", "examples"];

/// Hash every regular file under each labeled input path. Shares
/// framing rules with the project source-hash function
/// (`hash::hash_path`) so the two hashers can't drift; both use
/// SHA-256 with explicit `file:` / `dir:` / `path:` / `missing:`
/// prefixes over machine-independent labels. Returns a 16-char hex
/// prefix (64 bits), plenty for a content-addressed tag.
fn hash_inputs(inputs: &[(String, PathBuf)]) -> Result<String> {
    let mut hasher = Sha256::new();
    for (label, path) in inputs {
        weft_compiler::hash::hash_path_skipping(&mut hasher, label, path, NOT_IN_THE_BINARY)?;
    }
    let digest = hasher.finalize();
    let mut out = String::with_capacity(16);
    for b in digest.iter().take(8) {
        let _ = write!(&mut out, "{:02x}", b);
    }
    Ok(out)
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

/// Load a locally-present image into the named kind cluster so its
/// Pods can run it without reaching a registry.
///
/// Every ref that goes through here is content-addressed
/// (`<repo>:<hash>`, worker / infra / system), so a ref already
/// present on the node IS the right content and the load
/// short-circuits.
///
/// We never compare image IDs across the docker/containerd boundary:
/// the two runtimes digest the same image differently (docker's config
/// blob vs containerd's), so an ID comparison never matches.
///
/// `force` skips the presence short-circuit and loads unconditionally:
/// the repair path for a node image whose BYTES are wrong under a
/// still-matching tag (a `--rebuild` re-made the image under the same
/// content ref, so tag presence would wrongly say "already right").
pub async fn kind_load(cluster: &str, image_ref: &str, force: bool) -> Result<()> {
    if !force && kind_node_has_tag(cluster, image_ref).await {
        return Ok(());
    }
    let status = quiet_stdout("kind")
        .args(["load", "docker-image", image_ref, "--name", cluster])
        .status()
        .await?;
    if !status.success() {
        anyhow::bail!("kind load docker-image {image_ref} failed");
    }
    Ok(())
}

/// Reclaim every system-service image whose tag is NOT the one the
/// cluster now runs, on host docker and (kind only) the node's
/// containerd. Content-addressed tags accumulate one image per engine
/// change with nothing else evicting them; the pods were just rolled
/// onto `current`, so every other tag of these repos is dead weight.
/// Also sweeps the pre-registry `:local` spellings. Warn-only: a
/// busy image (an old pod still terminating) just survives until the
/// next run.
pub async fn gc_stale_system_images(kind_cluster: Option<&str>, current: &SystemImages) {
    // A current ref whose tag cannot be parsed must SKIP its repo, not
    // condemn everything of that repo (`system_image_ref` validates
    // overrides, so this is belt over suspenders).
    let current_tag = |svc: SystemService| ref_repo_tag(current.get(svc)).ok().map(|(_, t)| t);
    let host = async {
        let out = docker().args(["images", "--format", "{{.Repository}}:{{.Tag}}"]).output().await;
        let out = match out {
            Ok(o) if o.status.success() => o,
            _ => return,
        };
        let listing = String::from_utf8_lossy(&out.stdout);
        for svc in SystemService::ALL {
            let Ok(stale) = host_images_condemned(current.get(svc), &listing, |_| true) else {
                continue;
            };
            for stale in stale {
                remove_image_warn_only(&["rmi", &stale]).await;
            }
        }
    };
    let node = async {
        let Some(cluster) = kind_cluster else { return };
        let Ok(groups) = kind_node_image_tag_groups(cluster).await else { return };
        let node = format!("{cluster}-control-plane");
        for svc in SystemService::ALL {
            let Some(tag) = current_tag(svc) else { continue };
            for image in node_images_condemned(svc.repo(), &groups, |t| t != tag) {
                remove_image_warn_only(&["exec", &node, "crictl", "rmi", &image]).await;
            }
        }
    };
    tokio::join!(host, node);
}

/// Host `docker images` lines (one `repo:tag` per line) that a sweep
/// may delete: same repo as `current_ref` under any registry prefix,
/// tag differing from `current_ref`'s, and `condemn_extra(tag)` true
/// (pass `|_| true` when "not current" is the whole rule). Owning the
/// ref split here means no caller ever hand-rolls it, and a
/// `current_ref` that cannot be split is a loud error, never an
/// everything-condemned sweep. The host-side sibling of
/// `node_images_condemned`, pure for the same reason: the two matchers
/// decide what a sweep may delete.
pub fn host_images_condemned(
    current_ref: &str,
    listing: &str,
    condemn_extra: impl Fn(&str) -> bool,
) -> Result<Vec<String>> {
    let (repo, current) = ref_repo_tag(current_ref)?;
    Ok(listing
        .lines()
        .map(str::trim)
        .filter(|full| {
            ref_repo_tag(full).is_ok_and(|(r, t)| r == repo && t != current && condemn_extra(t))
        })
        .map(str::to_string)
        .collect())
}

/// One best-effort image removal: a busy image (an old pod still
/// terminating) legitimately survives to the next run, but the refusal
/// is still worth a line so a wedged runtime or a permission problem
/// is not invisible forever.
async fn remove_image_warn_only(docker_args: &[&str]) {
    match docker().args(docker_args).output().await {
        Ok(out) if out.status.success() => {}
        Ok(out) => tracing::warn!(
            target: "weft_cli::images",
            "docker {} failed: {}",
            docker_args.join(" "),
            String::from_utf8_lossy(&out.stderr).trim()
        ),
        Err(e) => tracing::warn!(
            target: "weft_cli::images",
            error = %e,
            "docker {} could not run",
            docker_args.join(" ")
        ),
    }
}

/// The node's images as one `repoTags` group PER IMAGE, via `crictl
/// images -o json` on the control-plane container. THE one
/// node-image-list reader: tag-presence checks (`kind_node_has_tag`)
/// and image reclamation (`weft clean --images`) both parse through
/// here, so the two cannot drift on how a node ref is spelled. The
/// grouping preserves which refs share content: `crictl rmi` removes
/// the whole image behind a ref (every tag on it, not just the named
/// one), so any node-side removal must decide per IMAGE, never per
/// tag. Digest-only images list an empty `repoTags` array and come
/// back as empty groups.
pub async fn kind_node_image_tag_groups(cluster: &str) -> Result<Vec<Vec<String>>> {
    let node = format!("{cluster}-control-plane");
    let out = docker()
        .args(["exec", &node, "crictl", "images", "-o", "json"])
        .output()
        .await
        .map_err(|e| anyhow::anyhow!("docker not reachable on PATH: {e}"))?;
    if !out.status.success() {
        anyhow::bail!(
            "crictl images on {node} failed: {}",
            String::from_utf8_lossy(&out.stderr).trim()
        );
    }
    let parsed: serde_json::Value = serde_json::from_str(&String::from_utf8_lossy(&out.stdout))
        .map_err(|e| anyhow::anyhow!("parse crictl images output from {node}: {e}"))?;
    // A listing without an `images` array is an unreadable listing,
    // not an empty node; treating it as empty would make a reclaim
    // silently reclaim nothing and report success.
    let images = parsed.get("images").and_then(|v| v.as_array()).ok_or_else(|| {
        anyhow::anyhow!("crictl images on {node} returned no `images` array: {parsed}")
    })?;
    Ok(images
        .iter()
        .filter_map(|img| img.get("repoTags").and_then(|v| v.as_array()))
        .map(|tags| tags.iter().filter_map(|t| t.as_str()).map(str::to_string).collect())
        .collect())
}

/// The hashes of every image the dispatcher still counts on: running
/// projects' binary hashes UNION non-terminal worker pods' hashes and
/// pending/claimed task hashes (a pod draining in-flight work may run
/// an image its project no longer points at; deleting it would strand
/// a restart). The ONE fetch every image reclaim subtracts before
/// deleting anything; a failure is the caller's cue to skip the
/// reclaim, never to guess "nothing is referenced".
/// SYNC: response shape (JSON array of bare hash strings) <->
///       crates/weft-dispatcher/src/api/project.rs referenced_images
pub async fn referenced_image_hashes(
    client: &crate::client::DispatcherClient,
) -> Result<std::collections::BTreeSet<String>> {
    let json = client
        .get_json("/images/referenced")
        .await
        .map_err(|e| anyhow::anyhow!("fetch referenced images (is the daemon up?): {e}"))?;
    Ok(json
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|v| v.as_str())
        .map(str::to_string)
        .collect())
}

/// One ref per NODE IMAGE that is safe to `crictl rmi`: every one of
/// the image's tags is a `repo` tag (ignoring any registry prefix)
/// satisfying `condemn`. `crictl rmi` removes the whole image behind
/// a ref, every tag included, so an image carrying even one live tag
/// (a fresh tag whose content matches a stale one, or another repo's
/// tag) must survive untouched; condemning per tag once deleted a
/// freshly loaded test image whose bits matched the stale tag being
/// dropped. Pure so it is unit-testable; only images made purely of
/// `repo`'s refs ever leave, which is the guarantee that keeps system
/// images (listener & co) safe from every node cleanup. Handles bare
/// (`repo:<tag>`), docker-canonical (`docker.io/library/...`), and
/// registry-qualified (`host:port/path/repo:<tag>`) spellings.
pub fn node_images_condemned(
    repo: &str,
    image_tag_groups: &[Vec<String>],
    condemn: impl Fn(&str) -> bool,
) -> Vec<String> {
    let prefix = format!("{repo}:");
    image_tag_groups
        .iter()
        .filter(|group| {
            !group.is_empty()
                && group.iter().all(|full| {
                    let repo_tag = full.rsplit_once('/').map_or(full.as_str(), |(_, t)| t);
                    repo_tag.strip_prefix(&prefix).is_some_and(&condemn)
                })
        })
        .filter_map(|group| group.first().cloned())
        .collect()
}

/// Whether the kind node already has an image under `image_ref`. Ref
/// presence alone is the answer: tags are content-addressed (the
/// suffix is the source hash), so a present ref is the right content.
/// We match on `repoTags`, not the image id, precisely because docker
/// and containerd report different ids for the same image. Any listing
/// failure reads as "absent" so the caller just re-loads. A bare ref
/// (no registry) is stored by containerd under its docker-canonical
/// `docker.io/library/` spelling, so both spellings match.
async fn kind_node_has_tag(cluster: &str, image_ref: &str) -> bool {
    let Ok(groups) = kind_node_image_tag_groups(cluster).await else {
        return false;
    };
    let canonical =
        (!image_ref.contains('/')).then(|| format!("docker.io/library/{image_ref}"));
    groups
        .iter()
        .flatten()
        .any(|t| t == image_ref || Some(t.as_str()) == canonical.as_deref())
}

#[cfg(test)]
mod tests {
    use super::*;

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

    /// The host sweep must remove every other tag of a system repo
    /// (the pre-registry `:local` spelling included), keep the tag the
    /// cluster runs under EITHER spelling, skip untagged/dangling
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
        let stale =
            host_images_condemned("ghcr.io/weavemindai/weft-dispatcher:abc123", listing, |_| true)
                .expect("well-formed current ref");
        assert_eq!(
            stale,
            vec![
                "weft-dispatcher:local".to_string(),
                "ghcr.io/weavemindai/weft-dispatcher:0ld0ld".to_string(),
            ]
        );
        let infra = host_images_condemned("weft-infra-supervisor:abc123", listing, |_| true)
            .expect("well-formed current ref");
        assert!(infra.is_empty(), "{infra:?}");
        // The extra predicate narrows further (a referenced-set hold):
        // a non-current tag it protects survives.
        let held =
            host_images_condemned("weft-dispatcher:abc123", listing, |t| t != "0ld0ld")
                .expect("well-formed current ref");
        assert_eq!(held, vec!["weft-dispatcher:local".to_string()]);
        // A current ref that cannot be split is a loud error, never an
        // everything-condemned sweep.
        assert!(host_images_condemned("weft-dispatcher", listing, |_| true).is_err());
    }

    /// The node-side sweep must remove every other tag of a system
    /// repo (the pre-registry `:local` spelling included), keep the
    /// tag the cluster runs, and never touch foreign repos.
    #[test]
    fn gc_condemns_only_stale_system_tags() {
        let one = |s: &str| vec![s.to_string()];
        let groups = vec![
            one("ghcr.io/weavemindai/weft-dispatcher:abc123"),
            one("docker.io/library/weft-dispatcher:local"),
            one("ghcr.io/weavemindai/weft-dispatcher:0ld0ld"),
            one("docker.io/library/weft-worker:abc123"),
        ];
        let stale = node_images_condemned("weft-dispatcher", &groups, |tag| tag != "abc123");
        assert_eq!(
            stale,
            vec![
                "docker.io/library/weft-dispatcher:local".to_string(),
                "ghcr.io/weavemindai/weft-dispatcher:0ld0ld".to_string(),
            ]
        );
    }

    /// Each system image's hash closure covers its own binary and what that
    /// binary links, and nothing else. Two properties matter. A crate none of
    /// them link (weft-e2e, weft-cli) must gate no image, or every CLI edit
    /// rebuilds and re-rolls four pods for nothing. And a service's own crate
    /// must gate only its own image, so a dispatcher change does not roll the
    /// listener, the broker and the supervisor with it.
    #[test]
    fn each_system_image_hashes_its_own_binary_and_its_links() {
        let root = weft_compiler::build::resolve_weft_root().expect("resolve weft root");
        let closure_of = |seed: &str| {
            weft_compiler::codegen::workspace_crate_closure(&root, &[seed.to_string()])
                .expect("compute system closure")
        };

        let dispatcher = closure_of("weft-dispatcher");
        for absent in ["weft-e2e", "weft-cli"] {
            assert!(
                !dispatcher.contains(&absent.to_string()),
                "{absent} must not gate an image; closure: {dispatcher:?}"
            );
        }
        // The shared brain every service pulls, so a change there does move
        // all four.
        for present in ["weft-dispatcher", "weft-compiler", "weft-core"] {
            assert!(
                dispatcher.contains(&present.to_string()),
                "{present} missing from the dispatcher closure; closure: {dispatcher:?}"
            );
        }

        for other in ["weft-listener", "weft-broker", "weft-infra-supervisor"] {
            let closure = closure_of(other);
            assert!(closure.contains(&other.to_string()), "{other} misses itself");
            assert!(
                !closure.contains(&"weft-dispatcher".to_string()),
                "a dispatcher-only change must not roll {other}; closure: {closure:?}"
            );
        }
    }
}
