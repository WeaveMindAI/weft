//! Image naming, build and registry pull/push helpers, and the sweeps of
//! what Docker holds on this machine. Owned by the CLI so `weft daemon
//! start` makes the runtime image a local install runs exist, and `weft
//! build-images` makes the images a cloud install runs. No external shell
//! scripts.
//!
//! Every image the release publishes is content-addressed: the tag is a
//! hash of everything the image is built from, so a present tag IS the
//! right content. A local install uses only the runtime image (what its
//! runtime and its infra units' agents run); the builder base and the
//! standard worker image are the cloud's (`weft build-images`, on CI or a
//! cloud install's runner). Ensuring one is a three-step ladder: already
//! present locally -> done; pull the same tag from the registry (CI pushes
//! every tag it builds from a clean checkout, so an unmodified tree hits
//! this) -> done; otherwise build locally under the same tag (a modified
//! tree hashes to a tag the registry has never seen, so local changes
//! always build).

use std::fmt::Write as _;
use std::os::fd::AsFd;
use std::path::{Path, PathBuf};

use anyhow::Result;
use sha2::{Digest, Sha256};
use tokio::process::Command;

/// Registry the prebuilt shared images are published to (by the
/// release workflow, on every push to the release branch). Override
/// with `WEFT_IMAGE_REGISTRY`; set it to the empty string to name
/// images bare (`weft-runtime:<hash>`), which also disables the
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


/// The builder base's content-addressed ref for the current checkout: what
/// a cloud install's worker images compile in.
pub fn builder_base_ref() -> Result<String> {
    let root = weft_compiler::build::resolve_weft_root()
        .map_err(|e| anyhow::anyhow!("resolve weft repo root: {e}"))?;
    let hash = weft_compiler::hash::compute_builder_base_hash(&root)?;
    let short = hash.chars().take(16).collect::<String>();
    Ok(qualified_ref(weft_compiler::worker_image::BUILDER_BASE_REPO, &short))
}

/// Materialize the builder base under `image_ref` (present -> pull ->
/// build). Split from the ref computation so the release workflow can
/// materialize an arch-suffixed name.
pub async fn ensure_builder_base_at(image_ref: &str) -> Result<()> {
    let root = weft_compiler::build::resolve_weft_root()
        .map_err(|e| anyhow::anyhow!("resolve weft repo root: {e}"))?;
    // The ref is content-addressed (the hash covers every input the
    // build context reads, via `compute_builder_base_hash`), so a
    // present ref IS the right content: no stamp file needed.
    if image_present(image_ref).await? {
        return Ok(());
    }
    // The staging dir is one FIXED path (wiped and rewritten), so the
    // stage-and-build holds an exclusive file lock: a second `weft
    // build-images` blocks, then finds the ref present and skips. The lock
    // file lives BESIDE the staging dir (staging wipes the dir itself).
    let ctx_dir = root.join(weft_compiler::worker_image::BASE_CONTEXT_DIR);
    let _lock = hold_file_lock(
        ctx_dir.with_extension("lock"),
        "another weft process is building the builder base; waiting for it to finish",
    )
    .await?;
    if !image_present(image_ref).await? && !docker_pull(image_ref).await? {
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
    if let Some(v) = runtime_image_override()? {
        return Ok(v);
    }
    Ok(qualified_ref(RUNTIME_REPO, &short_hex(&runtime_source_hash()?.finalize())))
}

/// `WEFT_RUNTIME_IMAGE`, when it is set, checked to be a well-formed ref.
fn runtime_image_override() -> Result<Option<String>> {
    let Ok(v) = std::env::var(RUNTIME_IMAGE_ENV) else { return Ok(None) };
    let v = v.trim();
    if v.is_empty() {
        return Ok(None);
    }
    anyhow::ensure!(
        !v.contains(char::is_whitespace) && !v.contains(['"', '\'']),
        "{RUNTIME_IMAGE_ENV} is '{v}', which is not a well-formed image ref (whitespace or quotes)"
    );
    ref_repo_tag(v).map_err(|e| anyhow::anyhow!("{RUNTIME_IMAGE_ENV}: {e}"))?;
    Ok(Some(v.to_string()))
}

/// The hash [`runtime_image_ref`] is cut from, before it is finished:
/// everything the image is built from in this checkout.
fn runtime_source_hash() -> Result<Sha256> {
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
        }, &weft_compiler::hash::DISK)?;
    }
    // The weft source a version compiles against, every file of it (the
    // Dockerfile copies these whole), minus what `.dockerignore` keeps
    // out, which is the node-tree exclusion every hash walk already
    // applies (`weft_catalog::is_node_tree_excluded`).
    for rel in ["crates", "catalog", weft_compiler::hash::BUILDER_BASE_DOCKERFILE, weft_compiler::hash::BUILDER_BASE_SPLIT_SCRIPT] {
        weft_compiler::hash::hash_path(&mut hasher, &format!("carried:{rel}"), &root.join(rel), &weft_compiler::hash::DISK)?;
    }
    Ok(hasher)
}

/// The glibc each Debian release the runtime image has been based on
/// ships, as (major, minor). The release itself is named once, in the
/// runtime Dockerfile's last `FROM` ([`runtime_image_release`]); a base
/// this table does not know is refused rather than guessed.
const DEBIAN_GLIBC: &[(&str, (u32, u32))] = &[("bookworm", (2, 36)), ("trixie", (2, 41))];

/// The Debian release the runtime image runs on, and its glibc: read off
/// the last `FROM` of the runtime Dockerfile (`FROM debian:trixie-slim`),
/// the stage the image is.
fn runtime_image_release(dockerfile: &str) -> Result<(String, (u32, u32))> {
    let base = dockerfile
        .lines()
        .rev()
        .find_map(|line| line.trim().strip_prefix("FROM "))
        .and_then(|rest| rest.split_whitespace().next())
        .ok_or_else(|| anyhow::anyhow!("the runtime Dockerfile has no FROM line"))?;
    let release = base
        .strip_prefix("debian:")
        .map(|tag| tag.split('-').next().unwrap_or(tag))
        .ok_or_else(|| anyhow::anyhow!("the runtime image is based on {base}, not on a Debian release weft knows the glibc of"))?;
    let glibc = DEBIAN_GLIBC.iter().find(|(name, _)| *name == release).map(|(_, v)| *v).ok_or_else(|| {
        anyhow::anyhow!("the runtime image is based on Debian {release}, whose glibc is not in DEBIAN_GLIBC (crates/weft-cli/src/images.rs); add it there")
    })?;
    Ok((release.to_string(), glibc))
}

/// [`runtime_image_release`] of this checkout's runtime Dockerfile.
fn this_runtime_image_release(root: &Path) -> Result<(String, (u32, u32))> {
    let path = root.join("deploy/docker").join(RUNTIME_DOCKERFILE);
    let text = std::fs::read_to_string(&path).map_err(|e| anyhow::anyhow!("read {}: {e}", path.display()))?;
    runtime_image_release(&text)
}

/// This machine's glibc, as (major, minor); `None` when it has none (not
/// Linux, or a musl Linux), which `getconf` answers by failing.
fn host_glibc() -> Option<(u32, u32)> {
    if !cfg!(target_os = "linux") {
        return None;
    }
    let out = std::process::Command::new("getconf").arg("GNU_LIBC_VERSION").output().ok()?;
    out.status.success().then(|| parse_glibc(&String::from_utf8_lossy(&out.stdout))).flatten()
}

/// `glibc 2.41` (what `getconf GNU_LIBC_VERSION` prints) as (2, 41).
fn parse_glibc(text: &str) -> Option<(u32, u32)> {
    let version = text.trim().strip_prefix("glibc ")?;
    let mut parts = version.split('.');
    Some((parts.next()?.parse().ok()?, parts.next()?.parse().ok()?))
}

/// Where a local install's runtime image gets its `weft-runtime`.
#[derive(Debug, PartialEq, Eq)]
enum RuntimeSource {
    /// The binary `./setup.sh` just built on this machine, copied in:
    /// a binary built against a glibc no newer than the image's starts
    /// there.
    Host,
    /// Compiled inside Docker, for the reason given: a binary built here
    /// may need a glibc symbol the image does not have.
    CompileInImage(String),
}

/// Decided up front from this machine's glibc against the image's, so a
/// binary that could not start in the image is never tried.
fn runtime_source(host: Option<(u32, u32)>, release: &str, image: (u32, u32)) -> RuntimeSource {
    let show = |(major, minor): (u32, u32)| format!("{major}.{minor}");
    match host {
        Some(host) if host <= image => RuntimeSource::Host,
        Some(host) => RuntimeSource::CompileInImage(format!(
            "this machine's glibc {} is newer than the {} of Debian {release}, which the runtime image runs on, \
             so the weft-runtime built here may not start there",
            show(host),
            show(image)
        )),
        None => RuntimeSource::CompileInImage(
            "this machine has no glibc (it is not Linux, or its libc is musl), so the weft-runtime built here \
             cannot run in the runtime image"
                .to_string(),
        ),
    }
}

/// The runtime image a local install runs its infra units' agents on,
/// and how it is made.
pub struct LocalRuntimeImage {
    pub image_ref: String,
    source: RuntimeSource,
    host_binary: PathBuf,
}

/// The runtime image for a local install whose runtime is `host_binary`.
///
/// When that binary can run in the image ([`runtime_source`]), the image
/// is built around it, and its tag hashes the binary on top of the
/// source: a binary built again (another profile, another toolchain on
/// the host) makes a new image, never a stale one reused under the
/// source's tag. Otherwise the binary is compiled inside Docker, under
/// the source's tag ([`runtime_image_ref`]), as the release workflow's
/// would be. `WEFT_RUNTIME_IMAGE` wins verbatim either way.
pub fn local_runtime_image(host_binary: &Path) -> Result<LocalRuntimeImage> {
    let root = weft_compiler::build::resolve_weft_root().map_err(|e| anyhow::anyhow!("resolve weft repo root: {e}"))?;
    let (release, image_glibc) = this_runtime_image_release(&root)?;
    let source = runtime_source(host_glibc(), &release, image_glibc);
    let image_ref = match (runtime_image_override()?, &source) {
        (Some(v), _) => v,
        (None, RuntimeSource::CompileInImage(_)) => runtime_image_ref()?,
        (None, RuntimeSource::Host) => {
            let mut hasher = runtime_source_hash()?;
            weft_compiler::hash::hash_path(&mut hasher, "prebuilt:weft-runtime", host_binary, &weft_compiler::hash::DISK)?;
            qualified_ref(RUNTIME_REPO, &short_hex(&hasher.finalize()))
        }
    };
    Ok(LocalRuntimeImage { image_ref, source, host_binary: host_binary.to_path_buf() })
}

impl LocalRuntimeImage {
    /// Make it exist ([`ensure_runtime_image`]). When it has to be
    /// compiled inside Docker, why is said once, right before.
    pub async fn ensure(&self, rebuild: bool) -> Result<()> {
        match &self.source {
            RuntimeSource::Host => ensure_runtime_image(&self.image_ref, rebuild, RuntimeBinary::Host(&self.host_binary)).await,
            RuntimeSource::CompileInImage(why) => {
                ensure_runtime_image(&self.image_ref, rebuild, RuntimeBinary::Compile { why: Some(why) }).await
            }
        }
    }
}

/// Where [`ensure_runtime_image`] gets the `weft-runtime` it puts in the
/// image when it has to build one.
pub enum RuntimeBinary<'a> {
    /// Compiled inside Docker, the Dockerfile's `compiled` stage; `why`,
    /// when given, is said before the compile starts.
    Compile { why: Option<&'a str> },
    /// This binary, built from this same checkout outside Docker (the
    /// release workflow's, under the source's tag).
    Prebuilt(&'a Path),
    /// The binary of this machine's own install, under a tag that hashes
    /// it ([`local_runtime_image`]): no registry holds that tag, so it is
    /// never pulled.
    Host(&'a Path),
}

/// The build context a prebuilt runtime binary rides in, and the file
/// name it has there.
// SYNC: PREBUILT_CONTEXT + PREBUILT_BINARY + the RUNTIME_BINARY arg <-> deploy/docker/runtime.Dockerfile (the prebuilt stage)
const PREBUILT_CONTEXT: &str = "weft-prebuilt";
const PREBUILT_BINARY: &str = "weft-runtime";

/// The build arg naming the compile cache a runtime compiled in the image
/// builds in: the install's own (`weft_compiler::build::install_cache_key`).
// SYNC: TARGET_CACHE_ARG <-> deploy/docker/runtime.Dockerfile (WEFT_TARGET_CACHE)
const TARGET_CACHE_ARG: &str = "WEFT_TARGET_CACHE";

/// What the id of the runtime's compile cache mount starts with, before
/// its [`TARGET_CACHE_ARG`].
// SYNC: RUNTIME_CACHE_MOUNT_ID_PREFIX <-> deploy/docker/runtime.Dockerfile (weft-cargo-target-runtime-${WEFT_TARGET_CACHE})
const RUNTIME_CACHE_MOUNT_ID_PREFIX: &str = "weft-cargo-target-runtime-";

/// Make the runtime image exist locally under `image_ref`: present ->
/// done; pull -> done; build `deploy/docker/runtime.Dockerfile`.
/// `rebuild` skips straight to the build, for when a present image is
/// corrupt or hand-modified.
///
/// With [`RuntimeBinary::Prebuilt`], the build copies that `weft-runtime`
/// binary in instead of compiling one. It must be built from this same
/// checkout: the ref hashes the source, and nothing can check a binary
/// against it. Whether it RUNS in the image (this machine's
/// architecture, a glibc no newer than the image's Debian release) is
/// checked by starting it there once, and an image it cannot run in is
/// removed, never pushed.
pub async fn ensure_runtime_image(image_ref: &str, rebuild: bool, binary: RuntimeBinary<'_>) -> Result<()> {
    if !rebuild {
        if image_present(image_ref).await? {
            eprintln!("image {image_ref} present; skipping");
            return Ok(());
        }
        if !matches!(binary, RuntimeBinary::Host(_)) && docker_pull(image_ref).await? {
            return Ok(());
        }
    }
    let root = weft_compiler::build::resolve_weft_root().map_err(|e| anyhow::anyhow!("resolve weft repo root: {e}"))?;
    let dockerfile = root.join("deploy/docker").join(RUNTIME_DOCKERFILE);
    let (binary, instead) = match binary {
        RuntimeBinary::Prebuilt(binary) => (binary, "`weft build-images` without --runtime-binary compiles it in the image instead"),
        // Chosen because this machine's glibc is no newer than the
        // image's (`runtime_source`), so a refusal here is something
        // that check does not see.
        RuntimeBinary::Host(binary) => (
            binary,
            "this machine's glibc is no newer than the image's, so something else keeps it from starting \
             (the reason above); please report it",
        ),
        RuntimeBinary::Compile { why } => {
            if let Some(why) = why {
                eprintln!("compiling weft-runtime inside Docker for the runtime image: {why}");
            }
            let install = weft_core::infra::Install::from_env().map_err(anyhow::Error::msg)?;
            let cache = weft_compiler::build::install_cache_key(&root, &install).map_err(|e| anyhow::anyhow!("the runtime's compile cache key: {e}"))?;
            docker_build(image_ref, &dockerfile, &root, &[format!("{TARGET_CACHE_ARG}={cache}")], &[]).await?;
            drop_superseded_install_build_caches(&install, &cache).await;
            return Ok(());
        }
    };
    // Its own context holding only the binary, under the name the
    // Dockerfile copies, whatever the file was called on disk.
    let staged = tempfile::tempdir()?;
    std::fs::copy(binary, staged.path().join(PREBUILT_BINARY))
        .map_err(|e| anyhow::anyhow!("stage the prebuilt runtime {}: {e}", binary.display()))?;
    docker_build(
        image_ref,
        &dockerfile,
        &root,
        &["RUNTIME_BINARY=prebuilt".to_string()],
        &[(PREBUILT_CONTEXT, staged.path())],
    )
    .await?;
    let probe = docker().args(["run", "--rm", image_ref, "--version"]).output().await?;
    if !probe.status.success() {
        let removed = docker().args(["rmi", image_ref]).output().await?;
        anyhow::ensure!(
            removed.status.success(),
            "remove {image_ref} after its runtime failed to start: {}",
            String::from_utf8_lossy(&removed.stderr).trim()
        );
        let (release, (major, minor)) = this_runtime_image_release(&root)?;
        anyhow::bail!(
            "the prebuilt runtime {} does not run in the image (removed {image_ref}): {}\n\
             it has to be built for this machine's architecture against a glibc no newer than \
             Debian {release}'s ({major}.{minor}; Ubuntu 22.04's is older); {instead}",
            binary.display(),
            String::from_utf8_lossy(&probe.stderr).trim()
        );
    }
    Ok(())
}

/// Every image the release publishes (`weft build-images`): the runtime,
/// which a local install pulls, and the builder base a cloud install's
/// worker images compile on and the standard worker image a cloud
/// install's untouched projects run.
pub struct ReleaseImages {
    pub runtime: String,
    pub builder_base: String,
    /// The worker image built from the whole base catalog. A cloud
    /// project whose nodes are all base catalog nodes builds to exactly
    /// this image (its hash names no project), so its first run finds it
    /// ready instead of compiling.
    pub worker: String,
}

impl ReleaseImages {
    /// The refs the current checkout's content hashes to, touching no
    /// image.
    pub fn current() -> Result<Self> {
        let builder_base = builder_base_ref()?;
        let worker = standard_worker_ref(&builder_base)?;
        Ok(Self { runtime: runtime_image_ref()?, builder_base, worker })
    }

    /// Every bare (unsuffixed) ref, one per image.
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

/// Make the images `wanted` names exist locally, the runtime and the
/// builder base side by side, then the standard worker FROM that base.
/// With `ref_suffix`, each image is materialized under `<ref><suffix>`
/// instead of its bare name: the release workflow's per-architecture
/// halves, checked and pulled by their OWN names. `prebuilt_runtime` is
/// the binary of [`RuntimeBinary::Prebuilt`]; without it the runtime is
/// compiled in the image.
///
/// `tokio::join!`, NOT `try_join!`: an early bail would drop the sibling
/// future while its `docker build` child keeps running detached; join!
/// lets both finish, then every error is reported together.
pub async fn ensure_release_images(wanted: &ReleaseImages, ref_suffix: Option<&str>, prebuilt_runtime: Option<&Path>) -> Result<()> {
    let (runtime_target, base_target) = (suffixed_ref(&wanted.runtime, ref_suffix), suffixed_ref(&wanted.builder_base, ref_suffix));
    let runtime_binary = prebuilt_runtime.map_or(RuntimeBinary::Compile { why: None }, RuntimeBinary::Prebuilt);
    let (r, b) = tokio::join!(ensure_runtime_image(&runtime_target, false, runtime_binary), ensure_builder_base_at(&base_target));
    let failures: Vec<String> = [("runtime", r), ("builder-base", b)]
        .into_iter()
        .filter_map(|(name, res)| res.err().map(|e| format!("{name}: {e:#}")))
        .collect();
    anyhow::ensure!(failures.is_empty(), "image build failed:\n  {}", failures.join("\n  "));
    // After the base: the standard worker is built FROM it.
    ensure_standard_worker(&suffixed_ref(&wanted.worker, ref_suffix), &base_target)
        .await
        .map_err(|e| anyhow::anyhow!("standard worker: {e:#}"))
}

/// The standard worker image's ref for this checkout, compiled FROM
/// `builder_base`: `<registry>/weft-worker:<hash>`
/// (`weft_compiler::build::stock_binary_hash` of an image).
pub fn standard_worker_ref(builder_base: &str) -> Result<String> {
    let hash = weft_compiler::build::stock_binary_hash(&weft_core::builds::WorkerPackaging::Image { builder: builder_base.to_string() })?;
    Ok(qualified_ref(weft_core::builds::WORKER_REPO, &hash))
}

/// Make the standard worker image exist under `target` (present, else
/// pulled, else built FROM `base`, the builder base this process just
/// ensured).
async fn ensure_standard_worker(target: &str, base: &str) -> Result<()> {
    if image_present(target).await? || docker_pull(target).await? {
        return Ok(());
    }
    let stock = weft_compiler::build::StockProject::materialize()?;
    let build = weft_compiler::build::build_project(
        &stock.project,
        &stock.definition,
        &stock.contents,
        &stock.catalog,
        // `StockProject::materialize` compiled it from disk.
        weft_compiler::CompileFs::disk(&stock.project.root),
        &weft_core::builds::WorkerPackaging::Image { builder: base.to_string() },
        weft_core::builds::NodeSet::Full,
        &weft_compiler::hash::DISK,
    )?;
    docker_build(target, &build.build_context.join("Dockerfile"), &build.build_context, &[], &[]).await?;
    // Every install uses it as built, reading no record of it
    // (`ledger::note_shared`), so it must leave nothing out: a stock
    // node that does not compile fails it here, naming the package.
    require_whole_worker(target).await
}

/// Refuse a worker image whose build left a node package out
/// (`weft_core::builds::LeftOut`, read from the image without running it).
async fn require_whole_worker(image: &str) -> Result<()> {
    let created = docker().args(["create", image, "weft-never-run"]).output().await?;
    anyhow::ensure!(created.status.success(), "docker create {image}: {}", String::from_utf8_lossy(&created.stderr).trim());
    let id = String::from_utf8_lossy(&created.stdout).trim().to_string();
    let copy = tempfile::NamedTempFile::new()?;
    let copied = docker().args(["cp", &format!("{id}:{}", weft_core::builds::LEFT_OUT_RECORD_PATH)]).arg(copy.path()).output().await;
    let removed = docker().args(["rm", "--force", &id]).output().await;
    let copied = copied?;
    anyhow::ensure!(
        copied.status.success(),
        "read {} from {image}: {}",
        weft_core::builds::LEFT_OUT_RECORD_PATH,
        String::from_utf8_lossy(&copied.stderr).trim()
    );
    if let Err(e) = removed {
        tracing::warn!(container = %id, error = %e, "could not remove the container the worker's record was read from");
    }
    let record = weft_core::builds::LeftOut::parse(&std::fs::read(copy.path())?).map_err(anyhow::Error::msg)?;
    anyhow::ensure!(
        record.is_empty(),
        "the standard worker left packages out because they did not compile:\n  {}",
        record.packages.iter().map(|p| p.reason(None)).collect::<Vec<_>>().join("\n  ")
    );
    Ok(())
}

/// One BuildKit record of a worker image's compile cache mount, as
/// `docker buildx du --verbose` lists it: what a `weft build-images` run on
/// this machine, or an older weft's worker image builds, left. There is
/// one per cache key (build environment, builder and the nodes'
/// `[build.env]`), and a key retired by
/// a toolchain bump keeps its record, and its bytes, until something
/// prunes it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompileCacheRecord {
    pub id: String,
    /// The cache key (`hash::compute_worker_cache_key`) in the mount id,
    /// without the compile lane after it: every lane of a key is that
    /// key's cache.
    pub key: String,
    /// The compile lane after the key, on a mount an older weft's worker
    /// image build made in one of the machine's lanes; `None` when the
    /// mount id carries no lane (a `weft build-images` cache).
    pub lane: Option<u32>,
    /// What the mount id carries after the key that is no lane number: a
    /// mount id no weft makes, kept so the size report shows its bytes
    /// instead of hiding them.
    pub unreadable_lane: Option<String>,
    pub size: String,
    /// Whole days since BuildKit last used it, read off its "Last used"
    /// line (`0` for anything under a day).
    pub idle_days: u64,
}

/// Every worker image compile cache mount on this host.
pub async fn worker_compile_caches() -> Result<Vec<CompileCacheRecord>> {
    Ok(compile_caches_in(&buildx_du().await?))
}

/// One BuildKit record of a cache mount that compiles weft's own crates:
/// the runtime image's, keyed by `weft_compiler::build::install_cache_key`
/// for the install it was built for, and the builder base's, keyed by the
/// toolchain alone (`weft_compiler::build::builder_base_cache_key`) or, on
/// one an older weft left, by an install too.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstallCacheRecord {
    pub id: String,
    /// Its whole key, empty for the runtime's mount from before it had one.
    pub key: String,
    /// The label of the install it is for (`default`, or its name);
    /// `None` for a mount no install owns (a builder base's under this
    /// weft, or one from before each install had caches of its own).
    pub install: Option<String>,
    pub size: String,
    /// Whether BuildKit may drop it now: `false` while a build mounts it.
    pub reclaimable: bool,
}

/// Every cache mount on this host that compiles weft's own crates.
pub async fn install_build_caches() -> Result<Vec<InstallCacheRecord>> {
    Ok(install_caches_in(&buildx_du().await?))
}

/// Drop every cache mount compiling weft's own crates for `install`
/// ([`install_build_caches`]) but those under the key `current`: with
/// none, the caches of an install being taken off this machine; with its
/// current key, the ones its builds left under a key they no longer use
/// (the toolchain moved), which no build mounts again. Each failure is
/// said, and the call fails naming them all.
pub async fn drop_install_build_caches(install: &weft_core::infra::Install, current: Option<&str>) -> Result<()> {
    let mut failures = Vec::new();
    let caches = install_build_caches().await?;
    let dead = caches.iter().filter(|c| c.install.as_deref() == Some(install.label_value()) && Some(c.key.as_str()) != current);
    for cache in dead {
        if !cache.reclaimable {
            failures.push(format!("a build mounts the compile cache {} right now; run this again once it ends", cache.id));
            continue;
        }
        println!("dropping the compile cache of install '{}' ({})", install.label_value(), cache.size);
        if let Err(error) = prune_build_record(&cache.id).await {
            failures.push(format!("{error:#}"));
        }
    }
    anyhow::ensure!(failures.is_empty(), "{}", failures.join("; "));
    Ok(())
}

/// After a build compiled in `install`'s cache under `key`: drop the
/// caches its builds left under an older key ([`drop_install_build_caches`]).
/// Nothing ever mounts those again, and nothing else drops them while the
/// install stays on the machine. A failure is said and the build stands.
async fn drop_superseded_install_build_caches(install: &weft_core::infra::Install, key: &str) {
    if let Err(error) = drop_install_build_caches(install, Some(key)).await {
        eprintln!("could not drop the compile caches install '{}' left under an older key: {error:#}", install.label_value());
    }
}

/// `docker buildx du --verbose`: every BuildKit record on this host.
async fn buildx_du() -> Result<String> {
    let output = docker().args(["buildx", "du", "--verbose"]).output().await?;
    anyhow::ensure!(
        output.status.success(),
        "docker buildx du exited {}: {}",
        output.status,
        String::from_utf8_lossy(&output.stderr).trim()
    );
    Ok(String::from_utf8_lossy(&output.stdout).into_owned())
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
        !buildx_du().await?.lines().filter_map(|line| line.strip_prefix("ID:")).any(|listed| listed.trim() == id),
        "docker buildx prune left record {id} in place; `docker buildx prune --all` removes it by hand"
    );
    Ok(())
}

/// One record of `docker buildx du --verbose` output whose description
/// names a cache mount id starting with a prefix: what follows the prefix
/// in the id, and the record's own fields.
struct MountRecord<'a> {
    id: String,
    mount: &'a str,
    size: String,
    idle_days: u64,
    reclaimable: bool,
}

/// The records of `du_verbose` (blank-line separated `Key: value` blocks)
/// whose mount id starts with `prefix`.
fn mount_records_in<'a>(du_verbose: &'a str, prefix: &str) -> Vec<MountRecord<'a>> {
    let wanted = format!("with id \"/{prefix}");
    du_verbose.split("\n\n").filter_map(|record| {
        let field = |key: &str| record.lines().find_map(|line| line.strip_prefix(key)).map(str::trim);
        let description = field("Description:")?;
        let start = description.find(&wanted)? + wanted.len();
        Some(MountRecord {
            id: field("ID:")?.to_string(),
            mount: description[start..].split('"').next()?,
            size: field("Size:")?.to_string(),
            idle_days: idle_days(field("Last used:").unwrap_or("")),
            reclaimable: field("Reclaimable:") != Some("false"),
        })
    }).collect()
}

/// The worker compile cache records in `docker buildx du --verbose`
/// output: those whose mount id starts with `WORKER_CACHE_MOUNT_ID_PREFIX`.
fn compile_caches_in(du_verbose: &str) -> Vec<CompileCacheRecord> {
    mount_records_in(du_verbose, weft_compiler::worker_image::WORKER_CACHE_MOUNT_ID_PREFIX).into_iter().map(|record| {
        // `<key>`, or `<key>-<lane>` on an older weft's; the key is hex,
        // so the first dash ends it.
        let (key, lane, unreadable_lane) = match record.mount.split_once('-') {
            Some((key, suffix)) => match suffix.parse() {
                Ok(lane) => (key.to_string(), Some(lane), None),
                Err(_) => (key.to_string(), None, Some(suffix.to_string())),
            },
            None => (record.mount.to_string(), None, None),
        };
        CompileCacheRecord { id: record.id, key, lane, unreadable_lane, size: record.size, idle_days: record.idle_days }
    }).collect()
}

/// The records in `docker buildx du --verbose` output of the cache mounts
/// that compile weft's own crates: the runtime's and the builder base's,
/// each mount id ending in its key. The runtime's mount from before it had
/// a key at all is the prefix without its dash.
fn install_caches_in(du_verbose: &str) -> Vec<InstallCacheRecord> {
    let runtime = mount_records_in(du_verbose, RUNTIME_CACHE_MOUNT_ID_PREFIX.trim_end_matches('-'))
        .into_iter()
        .filter_map(|record| match record.mount {
            "" => Some((record, "")),
            mount => mount.strip_prefix('-').map(|key| (record, key)),
        });
    let base = mount_records_in(du_verbose, weft_compiler::build::BUILDER_BASE_CACHE_MOUNT_ID_PREFIX)
        .into_iter()
        .map(|record| {
            let key = record.mount;
            (record, key)
        });
    runtime
        .chain(base)
        .map(|(record, key)| InstallCacheRecord {
            install: weft_compiler::build::install_of_cache_key(key).map(str::to_string),
            key: key.to_string(),
            id: record.id,
            size: record.size,
            reclaimable: record.reclaimable,
        })
        .collect()
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
/// builds (the runtime, and for the release the builder base and the
/// standard worker image) funnels here; a sweep finds them by repo and
/// tag. `named_contexts` are extra build contexts the Dockerfile reads
/// with `COPY --from=<name>`.
pub(crate) async fn docker_build(
    image_ref: &str,
    dockerfile: &Path,
    context: &Path,
    build_args: &[String],
    named_contexts: &[(&str, &Path)],
) -> Result<()> {
    eprintln!(
        "building image {image_ref} (this may take several minutes on first run; \
         subsequent builds are incremental)"
    );
    // We DO want docker's layer cache: combined with the buildkit
    // cargo cache mounts the Dockerfiles declare, an unchanged crate
    // set short-circuits to seconds. Deeper source changes are
    // caught by cargo's own fingerprinting inside the cache mount, which
    // holds one install's build of the runtime only
    // (`weft_compiler::build::install_cache_key`), or one keyed by the
    // toolchain or the content: cargo goes by modification times, which
    // another branch's artifacts fool.
    let mut cmd = docker();
    cmd.env("DOCKER_BUILDKIT", "1")
        .args(["build", "-t", image_ref, "-f"])
        .arg(dockerfile);
    for arg in build_args {
        cmd.args(["--build-arg", arg]);
    }
    for (name, dir) in named_contexts {
        cmd.arg("--build-context").arg(format!("{name}={}", dir.display()));
    }
    let status = cmd.arg(context).status().await?;
    if !status.success() {
        anyhow::bail!("docker build {image_ref} failed with {status}");
    }
    Ok(())
}

/// Hold the machine-wide lock on the file at `path`, waiting (on the
/// blocking pool, so this thread's other futures keep going) while
/// another weft process holds it, after saying `waiting` once.
pub(crate) async fn hold_file_lock(path: PathBuf, waiting: &'static str) -> Result<std::fs::File> {
    weft_platform_local::lanes::hold(&path, move || eprintln!("{waiting}")).await
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

/// The predicate a sweep of `repo` condemns with: every tag of `repo`
/// but those of the `kept` refs, under any registry spelling (an
/// install's local name `localhost/weft-<name>/<repo>:<tag>` included).
/// Owning the ref split here means no caller hand-rolls it, and a kept
/// ref that cannot be split, or names another repo, is a loud error,
/// never an everything-condemned sweep. Compose it with a
/// referenced-set hold where one applies.
pub fn outside_kept(repo: &str, kept: &[&str]) -> Result<impl Fn(&str, &str) -> bool> {
    let mut tags = std::collections::BTreeSet::new();
    for &kept_ref in kept {
        let (kept_repo, tag) = ref_repo_tag(kept_ref)?;
        anyhow::ensure!(kept_repo == repo, "the kept image '{kept_ref}' is not a {repo} image");
        tags.insert(tag.to_string());
    }
    let repo = repo.to_string();
    Ok(move |r: &str, t: &str| r == repo && !tags.contains(t))
}

/// [`outside_kept`] keeping only `current_ref`.
pub fn outside_current(current_ref: &str) -> Result<impl Fn(&str, &str) -> bool> {
    let (repo, _) = ref_repo_tag(current_ref)?;
    outside_kept(repo, &[current_ref])
}

/// The standard worker images in `listing` other than the `kept` ones:
/// what an older weft on this machine pulled or built for its install, or
/// a `weft build-images` run built, under a registry name. Each weft
/// version names a new one, so every one no install uses is dead (this
/// weft's own standard worker is a program, never an image). Only weft's own registry names go: the kept
/// refs' registries and the one the release publishes to (the copy one
/// may have been tagged from), never another repository that happens to
/// be called `weft-worker`. A kept hash is kept under any suffix
/// (`<hash>-amd64`, the release's per-architecture halves): content
/// hashes have one length, so no other hash starts with it. An install's
/// own name for a worker image (`Install::local_image_ref`) carries no
/// registry, and that install's start reclaims it ([`docker_workers_of`]).
pub fn stale_standard_workers<'a>(listing: &str, kept: impl IntoIterator<Item = &'a str>) -> Result<Vec<String>> {
    let mut registries = std::collections::BTreeSet::from([DEFAULT_IMAGE_REGISTRY]);
    let mut tags = Vec::new();
    for kept_ref in kept {
        tags.push(ref_repo_tag(kept_ref)?.1);
        // A bare ref (the registry disabled) is an install's own name,
        // and adds no registry to sweep.
        if let Some((registry, _)) = kept_ref.rsplit_once('/') {
            registries.insert(registry);
        }
    }
    let repos: Vec<String> = registries.iter().map(|r| format!("{r}/{}:", weft_core::builds::WORKER_REPO)).collect();
    Ok(listing
        .lines()
        .map(str::trim)
        .filter(|full| {
            repos.iter().any(|repo| {
                full.strip_prefix(repo.as_str()).is_some_and(|tag| !tag.is_empty() && !tags.iter().any(|kept| tag.starts_with(kept)))
            })
        })
        .map(str::to_string)
        .collect())
}

/// The label an older weft's node-test image carries naming the TAG of
/// the builder base it was built on, which is how a sweep keeps the
/// node-test images of every builder base an install uses and drops the
/// rest. This weft builds no node-test image (a live test's server is a
/// program compiled on the machine).
pub const NODE_TEST_BASE_LABEL: &str = "weft.dev/builder-base";

/// The node-test images an older weft left in `listing` that are not in
/// `kept` (the ones built on a builder base an install uses, listed
/// through [`NODE_TEST_BASE_LABEL`]): one built on a base no install uses,
/// or before images carried the label, is dead.
pub fn stale_node_test_images(listing: &str, kept: &str) -> Vec<String> {
    let kept: std::collections::BTreeSet<&str> = kept.lines().map(str::trim).collect();
    host_images_matching(listing, |r, _| r == weft_core::builds::NODE_TEST_REPO)
        .into_iter()
        .filter(|image| !kept.contains(image.as_str()))
        .collect()
}

/// Host `docker images` lines (one `repo:tag` per line) that a sweep
/// may delete: those whose prefix-stripped `(repo, tag)` parse and
/// satisfy `condemn`. THE host-side matcher (the runtime image, builder
/// bases, worker and infra reclaims all pass their own predicate over
/// the same split), pure because it decides what a sweep may delete.
/// Untagged/dangling lines (`<none>:<none>`) parse to a repo no
/// weft predicate names.
pub fn host_images_matching(listing: &str, condemn: impl Fn(&str, &str) -> bool) -> Vec<String> {
    listing
        .lines()
        .map(str::trim)
        .filter(|full| ref_repo_tag(full).is_ok_and(|(r, t)| condemn(r, t)))
        .map(str::to_string)
        .collect()
}

/// Every host image (narrowed by docker's `filters`, `label=k=v` and
/// the like) as one `repo:tag` line, the listing the host-side matcher
/// reads.
pub(crate) async fn host_image_listing(filters: &[String]) -> anyhow::Result<String> {
    let mut images = docker();
    images.arg("images");
    for filter in filters {
        images.args(["--filter", filter]);
    }
    let listing = images
        .args(["--format", "{{.Repository}}:{{.Tag}}"])
        .output()
        .await?;
    anyhow::ensure!(
        listing.status.success(),
        "docker images exited {}: {}",
        listing.status,
        String::from_utf8_lossy(&listing.stderr).trim()
    );
    Ok(String::from_utf8_lossy(&listing.stdout).into_owned())
}

/// What one removal pass did with its condemned refs. `already_gone`
/// is a ref a concurrent clean reclaimed between our listing and the
/// delete; `in_use` is a ref the runtime refused to drop because a
/// container still runs it (kept; the next clean gets it).
#[derive(Debug, Default, PartialEq)]
pub(crate) struct Reclaimed {
    pub(crate) removed: usize,
    pub(crate) already_gone: usize,
    pub(crate) in_use: usize,
}

impl Reclaimed {
    /// The one report line every host sweep prints, so the report sites
    /// cannot drift in wording: "removed N <what>", then only the
    /// tails that happened.
    pub(crate) fn report(&self, what: &str) -> String {
        let mut line = format!("removed {} {what}", self.removed);
        if self.already_gone > 0 {
            line.push_str(&format!(", {} already reclaimed", self.already_gone));
        }
        if self.in_use > 0 {
            line.push_str(&format!(", {} still in use (kept)", self.in_use));
        }
        line
    }
}

/// Remove host docker images one by one: a tag a concurrent clean already reclaimed is a
/// success, and a tag docker refuses to drop because a container still
/// uses it is kept, both detected by observing presence rather than
/// parsing error prose. No `-f`: force would untag a live container's
/// image anyway and strand its restart. "Absent" is only a verdict
/// while the daemon still answers.
pub(crate) async fn reclaim_host_images(images: &[String]) -> anyhow::Result<Reclaimed> {
    let mut done = Reclaimed::default();
    for image in images {
        let out = docker().args(["rmi", image]).output().await?;
        if out.status.success() {
            done.removed += 1;
            continue;
        }
        let present = docker()
            .args(["image", "inspect", image])
            .output()
            .await?
            .status
            .success();
        if present {
            println!("kept {image}: {}", String::from_utf8_lossy(&out.stderr).trim());
            done.in_use += 1;
            continue;
        }
        let daemon_up = docker()
            .args(["version", "--format", "{{.Server.Version}}"])
            .output()
            .await?
            .status
            .success();
        if daemon_up {
            done.already_gone += 1;
            continue;
        }
        anyhow::bail!(
            "docker stopped answering while removing {image}; check the daemon \
             and rerun `weft clean --images`"
        );
    }
    Ok(done)
}

/// The images in `listing` (one `repo:tag` per line) a named install
/// holds under its own names (`Install::local_image_ref`,
/// `localhost/weft-<name>/<repo>:<tag>`) whose install `owned` picks.
/// Removing such a name never takes the image from another install: that
/// one holds a name of its own on it, and Docker deletes an image with
/// its last name. A line with no tag (`<none>`) names nothing to remove.
pub fn images_owned_by(listing: &str, owned: impl Fn(&weft_core::infra::Install) -> bool) -> Vec<String> {
    listing
        .lines()
        .map(str::trim)
        .filter(|full| !full.ends_with(":<none>"))
        .filter(|full| weft_core::infra::Install::owner_of_local_image(full).is_some_and(|owner| owned(&owner)))
        .map(str::to_string)
        .collect()
}

/// The worker images in `listing` under `install`'s own names
/// (`Install::local_image_ref`: a named install's
/// `localhost/weft-<name>/...`, the default install's bare names), which a
/// weft from before workers ran as processes built and nothing on this
/// machine runs any more. Node-test images are not: that weft named them
/// bare for every install, and their own sweep takes them by the builder
/// base they were built on ([`stale_node_test_images`]).
pub fn docker_workers_of(listing: &str, install: &weft_core::infra::Install) -> Vec<String> {
    use weft_core::infra::Install;
    listing
        .lines()
        .map(str::trim)
        .filter(|full| {
            let Some(tag) = Install::tag_of_local_image(full) else { return false };
            let ours = match Install::owner_of_local_image(full) {
                Some(owner) => &owner == install,
                None => install.name().is_none() && tag == *full,
            };
            ours && tag.split_once(':').is_some_and(|(repo, version)| version != "<none>" && repo == weft_core::builds::WORKER_REPO)
        })
        .map(str::to_string)
        .collect()
}

/// Remove every image `install` holds under its own names
/// ([`images_owned_by`]): the images of an install being taken off this
/// machine (its worker and infra images, and its names on the shared
/// ones). Its containers must be gone first, or Docker keeps what they
/// run.
pub async fn remove_install_images(install: &weft_core::infra::Install) -> Result<()> {
    let listing = host_image_listing(&[]).await?;
    let images = images_owned_by(&listing, |owner| owner == install);
    if images.is_empty() {
        return Ok(());
    }
    let done = reclaim_host_images(&images).await?;
    println!("{}", done.report(&format!("image(s) of install '{}'", install.label_value())));
    anyhow::ensure!(
        done.in_use == 0,
        "{} image(s) of install '{}' are still in use by a container; remove it and run this again",
        done.in_use,
        install.label_value()
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A binary built on this machine goes into the runtime image when
    /// this machine's glibc is no newer than the image's, and is compiled
    /// inside Docker otherwise, saying why; a machine with no glibc
    /// always compiles inside Docker.
    #[test]
    fn the_host_binary_is_used_only_when_its_glibc_fits_the_image() {
        assert_eq!(runtime_source(Some((2, 41)), "trixie", (2, 41)), RuntimeSource::Host);
        assert_eq!(runtime_source(Some((2, 35)), "trixie", (2, 41)), RuntimeSource::Host);
        let RuntimeSource::CompileInImage(why) = runtime_source(Some((2, 42)), "trixie", (2, 41)) else {
            panic!("a newer glibc compiles in the image")
        };
        assert!(why.contains("glibc 2.42 is newer than the 2.41 of Debian trixie"), "{why}");
        // 2.100 is newer than 2.41: versions compare as numbers.
        assert!(matches!(runtime_source(Some((2, 100)), "trixie", (2, 41)), RuntimeSource::CompileInImage(_)));
        assert!(matches!(runtime_source(Some((3, 0)), "trixie", (2, 41)), RuntimeSource::CompileInImage(_)));
        assert!(matches!(runtime_source(None, "trixie", (2, 41)), RuntimeSource::CompileInImage(_)));
    }

    #[test]
    fn glibc_versions_are_read_off_getconf() {
        assert_eq!(parse_glibc("glibc 2.41\n"), Some((2, 41)));
        assert_eq!(parse_glibc("glibc 2.39"), Some((2, 39)));
        assert_eq!(parse_glibc("musl 1.2"), None);
        assert_eq!(parse_glibc(""), None);
    }

    /// The image's release is its Dockerfile's last FROM, the stage the
    /// image is; an earlier stage's base does not count, and a base whose
    /// glibc is not known is refused.
    #[test]
    fn the_image_release_is_the_last_from_of_its_dockerfile() {
        let text = "FROM rust:1 AS compiled\nFROM ${RUNTIME_BINARY} AS binary\nFROM debian:trixie-slim\nCMD [\"serve\"]\n";
        assert_eq!(runtime_image_release(text).unwrap(), ("trixie".to_string(), (2, 41)));
        assert!(runtime_image_release("FROM debian:forky-slim\n").unwrap_err().to_string().contains("Debian forky"));
        assert!(runtime_image_release("FROM alpine:3\n").is_err());
        assert!(runtime_image_release("RUN true\n").is_err());
    }

    /// The real runtime Dockerfile names a release whose glibc is known,
    /// so `weft daemon start` can always decide.
    #[test]
    fn this_checkouts_runtime_image_release_is_known() {
        let root = weft_compiler::build::resolve_weft_root().unwrap();
        this_runtime_image_release(&root).unwrap();
    }

    /// An install's images are the ones under its own local names, worker
    /// and infra alike, and its names on the shared images; never another
    /// install's, never the default install's bare names, never a
    /// registry's, never an untagged line. Read off `docker images
    /// --format '{{.Repository}}:{{.Tag}}'` as a host printed it.
    #[test]
    fn an_installs_images_are_the_ones_under_its_own_names() {
        let listing = "\
localhost/weft-e2e1a2b3c/weft-worker:9f8e7d6c5b4a3921
localhost/weft-e2e1a2b3c/weft-infra-mini_service:0011aabb
localhost/weft-e2e1a2b3c/weft-runtime:deployhash
localhost/weft-e2e1a2b3c/weft-worker:<none>
localhost/weft-deploy/weft-worker:9f8e7d6c5b4a3921
localhost/weft-Bad/weft-worker:x
localhost/other/weft-worker:x
weft-worker:9f8e7d6c5b4a3921
weft-infra-mini_service:0011aabb
ghcr.io/weavemindai/weft-runtime:deployhash
<none>:<none>
";
        let gone = weft_core::infra::Install::named("e2e1a2b3c").unwrap();
        assert_eq!(
            images_owned_by(listing, |owner| owner == &gone),
            vec![
                "localhost/weft-e2e1a2b3c/weft-worker:9f8e7d6c5b4a3921".to_string(),
                "localhost/weft-e2e1a2b3c/weft-infra-mini_service:0011aabb".to_string(),
                "localhost/weft-e2e1a2b3c/weft-runtime:deployhash".to_string(),
            ]
        );
        // A machine-wide sweep: every install still here keeps its own.
        let here = [weft_core::infra::Install::default_install(), weft_core::infra::Install::named("deploy").unwrap()];
        assert_eq!(images_owned_by(listing, |owner| !here.contains(owner)), images_owned_by(listing, |owner| owner == &gone));
        assert!(images_owned_by(listing, |owner| owner.name() == Some("deploy")).len() == 1);
    }

    /// One report line per sweep: the count always, each tail only
    /// when it happened.
    #[test]
    fn reclaim_report_names_only_what_happened() {
        let plain = Reclaimed { removed: 2, already_gone: 0, in_use: 0 };
        assert_eq!(plain.report("stale infra image(s)"), "removed 2 stale infra image(s)");
        let busy = Reclaimed { removed: 1, already_gone: 1, in_use: 3 };
        assert_eq!(
            busy.report("stale worker image(s)"),
            "removed 1 stale worker image(s), 1 already reclaimed, 3 still in use (kept)"
        );
    }


    /// Records as `docker buildx du --verbose` printed them on a host that
    /// had built worker images under two cache keys, one of them by an
    /// older weft in compile lane 2, one by `weft build-images` in no lane
    /// (captured verbatim, fields this parser ignores trimmed).
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
            CompileCacheRecord { id: "hve80t8pw8n1ackgqj82wn30e".into(), key: "0123456789abcdef".into(), lane: None, unreadable_lane: None, size: "6.1GB".into(), idle_days: 35 },
            CompileCacheRecord { id: "q1".into(), key: "0123456789abcdef".into(), lane: None, unreadable_lane: Some("x2".into()), size: "3MB".into(), idle_days: 0 },
        ]);
        assert!(compile_caches_in("ID: c\nSize: 1MB\nDescription: something else\n").is_empty());
        assert_eq!(idle_days("About an hour ago"), 0);
        assert_eq!(idle_days("3 days ago"), 3);
        assert_eq!(idle_days("2 months ago"), 60);
        assert_eq!(idle_days("Never"), 0);
    }

    /// The caches that compiled weft's own crates are read with the
    /// install their key names, or none for a mount no install owns (the
    /// runtime's unkeyed one, a builder base's keyed by the toolchain); a
    /// worker cache and a mount that only starts like the runtime's are
    /// not among them.
    #[test]
    fn every_install_cache_is_read_with_its_install() {
        let record = |id: &str, mount: &str, reclaimable: bool| {
            format!("ID:           {id}\nReclaimable:  {reclaimable}\nSize:         1GB\nDescription:  cached mount /x from exec /bin/sh -c cargo build with id \"/{mount}\"\nLast used:    2 days ago\n")
        };
        let du = [
            record("r1", "weft-cargo-target-runtime-gcpfixe0499b-0011223344556677", false),
            record("r2", "weft-cargo-target-runtime", true),
            record("r3", "weft-cargo-target-runtimes", true),
            record("b1", "weft-builder-base-target-default-0011223344556677", true),
            record("b2", "weft-builder-base-target-0011223344556677", true),
            record("w1", "weft-worker-target-9af3f2c5f9da6eaa-2", true),
        ]
        .join("\n");
        let read: Vec<(String, String, Option<String>, bool)> =
            install_caches_in(&du).into_iter().map(|c| (c.id, c.key, c.install, c.reclaimable)).collect();
        assert_eq!(read, vec![
            ("r1".to_string(), "gcpfixe0499b-0011223344556677".to_string(), Some("gcpfixe0499b".to_string()), false),
            ("r2".to_string(), String::new(), None, true),
            ("b1".to_string(), "default-0011223344556677".to_string(), Some("default".to_string()), true),
            ("b2".to_string(), "0011223344556677".to_string(), None, true),
        ]);
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

    /// Only weft's registry names of standard workers no install uses
    /// go: never a kept one under any suffix, never an install's own
    /// name, never another repository called `weft-worker`.
    #[test]
    fn only_standard_workers_no_install_uses_are_stale() {
        let listing = "ghcr.io/weavemindai/weft-worker:now\nghcr.io/weavemindai/weft-worker:now-amd64\n\
                       ghcr.io/weavemindai/weft-worker:old\nghcr.io/weavemindai/weft-worker:old-arm64\n\
                       eu.gcr.io/p/weft-worker:older\neu.gcr.io/p/weft-worker:now\nweft-worker:project\n\
                       localhost/weft-cell7/weft-worker:old\nsomeone/weft-worker:old\nghcr.io/other/weft-worker:old\n\
                       ghcr.io/weavemindai/weft-runtime:old\n<none>:<none>\n";
        assert_eq!(
            stale_standard_workers(listing, ["ghcr.io/weavemindai/weft-worker:now"]).unwrap(),
            vec!["ghcr.io/weavemindai/weft-worker:old".to_string(), "ghcr.io/weavemindai/weft-worker:old-arm64".to_string()]
        );
        assert_eq!(
            stale_standard_workers(listing, ["eu.gcr.io/p/weft-worker:now"]).unwrap(),
            vec![
                "ghcr.io/weavemindai/weft-worker:old".to_string(),
                "ghcr.io/weavemindai/weft-worker:old-arm64".to_string(),
                "eu.gcr.io/p/weft-worker:older".to_string()
            ]
        );
        assert_eq!(
            stale_standard_workers(listing, ["weft-worker:now"]).unwrap(),
            vec!["ghcr.io/weavemindai/weft-worker:old".to_string(), "ghcr.io/weavemindai/weft-worker:old-arm64".to_string()],
            "with the registry disabled, the published registry is the only one swept"
        );
        // Two installs on two versions keep both.
        assert_eq!(
            stale_standard_workers(listing, ["ghcr.io/weavemindai/weft-worker:now", "ghcr.io/weavemindai/weft-worker:old"]).unwrap(),
            Vec::<String>::new()
        );
        assert_eq!(
            stale_standard_workers(listing, Vec::<&str>::new()).unwrap().len(),
            4,
            "no install uses any: every one in the published registry goes"
        );
    }

    /// A shared-image sweep removes every tag of its repo that no install
    /// uses, under any name (a registry's, a bare one, an install's local
    /// one), keeps every tag one does, skips untagged lines, and never
    /// touches another repo (`weft-runtime` beside `weft-runtime-x`).
    #[test]
    fn a_shared_image_sweep_keeps_every_tag_an_install_uses() {
        let listing = "\
ghcr.io/weavemindai/weft-runtime:mainhash
weft-runtime:mainhash
localhost/weft-deploy/weft-runtime:deployhash
ghcr.io/weavemindai/weft-runtime:deployhash
weft-runtime:local
ghcr.io/weavemindai/weft-runtime:0ld0ld
localhost/weft-deploy/weft-runtime:0ld0ld
weft-runtime-x:0ld0ld
weft-builder-base:0ld0ld
<none>:<none>
";
        let kept = ["ghcr.io/weavemindai/weft-runtime:mainhash", "weft-runtime:deployhash"];
        let stale = host_images_matching(listing, outside_kept(RUNTIME_REPO, &kept).unwrap());
        assert_eq!(
            stale,
            vec![
                "weft-runtime:local".to_string(),
                "ghcr.io/weavemindai/weft-runtime:0ld0ld".to_string(),
                "localhost/weft-deploy/weft-runtime:0ld0ld".to_string(),
            ]
        );
        // Composed with another hold: a tag it protects survives.
        let outside = outside_current("weft-runtime:mainhash").unwrap();
        let held = host_images_matching(listing, |r, t| outside(r, t) && t != "0ld0ld");
        assert_eq!(
            held,
            vec![
                "localhost/weft-deploy/weft-runtime:deployhash".to_string(),
                "ghcr.io/weavemindai/weft-runtime:deployhash".to_string(),
                "weft-runtime:local".to_string(),
            ]
        );
        // No install uses any: the whole repo goes.
        assert_eq!(host_images_matching(listing, outside_kept(RUNTIME_REPO, &[]).unwrap()).len(), 7);
        // A kept ref that cannot be split, or is another repo's, is a
        // loud error, never an everything-condemned sweep.
        assert!(outside_current("weft-runtime").is_err());
        assert!(outside_kept(RUNTIME_REPO, &["weft-builder-base:x"]).is_err());
    }

    /// An install's worker images are the ones under its own names, never
    /// a node-test image (shared by every install of the weft that made
    /// them), a registry's, or another install's.
    #[test]
    fn an_installs_docker_workers_are_its_own_named_worker_images() {
        let listing = "weft-worker:a1\nweft-node-tests:b2\nweft-runtime:r\nweft-infra-pg:x\nghcr.io/weavemindai/weft-worker:s\n\
            localhost/weft-cell7/weft-worker:c3\nlocalhost/weft-other/weft-worker:d4\nweft-worker:<none>\n";
        let default = weft_core::infra::Install::default_install();
        assert_eq!(docker_workers_of(listing, &default), ["weft-worker:a1"]);
        let cell7 = weft_core::infra::Install::named("cell7").unwrap();
        assert_eq!(docker_workers_of(listing, &cell7), ["localhost/weft-cell7/weft-worker:c3"]);
    }

    /// The node-test images built on a builder base no install uses go,
    /// and so do those from before the images carried their base.
    #[test]
    fn node_test_images_of_a_base_no_install_uses_are_stale() {
        let listing = "weft-node-tests:a1\nweft-node-tests:b2\nweft-node-tests:c3\nweft-builder-base:old\n<none>:<none>\n";
        let kept = "weft-node-tests:a1\nweft-node-tests:c3\n";
        assert_eq!(stale_node_test_images(listing, kept), vec!["weft-node-tests:b2".to_string()]);
        assert_eq!(stale_node_test_images(listing, "").len(), 3);
    }
}
