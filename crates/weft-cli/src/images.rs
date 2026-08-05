//! Image build + kind-load helpers. Owned by the CLI so the user
//! runs `weft daemon start` / `weft infra up` and the right images
//! land in the cluster. No external shell scripts.
//!
//! A registry-backed build flips these same helpers to registry push;
//! only one place changes.

use std::path::{Path, PathBuf};

use anyhow::Result;
use tokio::process::Command;


/// Ensure the shared pre-built worker builder base image exists.
/// Returns its tag (`weft-builder-base:<short-hash>`). The tag is
/// content-addressed so an engine / toolchain bump produces a fresh
/// tag and per-project worker Dockerfiles automatically pick it up
/// via their `FROM {{builder_base_image}}` line.
///
/// The base image bakes debian + rustup + the workspace's pinned
/// toolchain, plus the engine workspace at `/weft/`. Per-project
/// worker builds FROM this image and skip the apt + rustup install
/// cycle, paying only per-project costs (per-node apt packages,
/// cargo fetch + compile inside the shared BuildKit cache mounts).
pub async fn ensure_worker_builder_base() -> Result<String> {
    let root = weft_compiler::build::resolve_weft_root()
        .map_err(|e| anyhow::anyhow!("resolve weft repo root: {e}"))?;
    let hash = weft_compiler::hash::compute_builder_base_hash(&root)?;
    let short = hash.chars().take(16).collect::<String>();
    let tag = weft_compiler::worker_image::builder_base_tag(&short);
    // The tag is content-addressed (the hash covers every input the
    // build context reads, via `compute_builder_base_hash`), so a
    // present tag IS the right content: no stamp file needed.
    if !image_present(&tag).await? {
        // Stage the base build context: the worker-linked workspace slice
        // + toolchain pin + generated warm-up crate + the RENDERED
        // Dockerfile (target-cache key substituted), and nothing else, so
        // the baked image (and the docker tarball) carry exactly what the
        // base hash covers. See `build::stage_builder_base_context`.
        let ctx = weft_compiler::build::stage_builder_base_context(&root)
            .map_err(|e| anyhow::anyhow!("stage builder-base context: {e}"))?;
        build_image(&tag, &ctx.join("Dockerfile"), &ctx, None).await?;
    }
    // Builder-base images are large (~1GB+: debian + rustup +
    // staged workspace). Earlier shape GC'd every prior tag after a
    // fresh ensure, but that races with in-flight per-project
    // builds: a docker build referencing `FROM weft-builder-base:<old>`
    // sees the tag yanked mid-build. Disk-pressure cleanup is an
    // explicit `weft clean --images` operation, not an implicit
    // side-effect of every `weft run`.
    Ok(tag)
}

/// The one Dockerfile every system image builds from: a shared builder stage
/// compiles all four binaries in ONE cargo invocation (one target cache, one
/// pass over the workspace instead of four), and each image is a named runtime
/// stage selected with `docker build --target <stage>`.
const SYSTEM_IMAGES_DOCKERFILE: &str = "system-images.Dockerfile";

/// The four system binaries the unified Dockerfile compiles. Seeds the
/// crate-closure the staleness stamps hash: an edit to a crate NONE of the
/// four link (weft-e2e, weft-cli) must not rebuild + re-roll the system pods.
// SYNC: SYSTEM_IMAGE_BINARIES <-> deploy/docker/system-images.Dockerfile (the
//       `cargo build -p ... --bin ...` line)
const SYSTEM_IMAGE_BINARIES: &[&str] = &[
    "weft-dispatcher",
    "weft-listener",
    "weft-broker",
    "weft-infra-supervisor",
];

/// Build (if stale) a system image: the `target` stage of
/// `deploy/docker/system-images.Dockerfile`. The input set is the four system
/// binaries' crate closure + the workspace manifests + toolchain pin, plus
/// that Dockerfile, plus `extra_input_rels` (paths relative to the weft root
/// the image additionally stages: the dispatcher bundles `catalog/` for its
/// describe / compile endpoints, the others stage nothing extra, so a
/// catalog-only edit doesn't invalidate them).
/// Returns `true` if a rebuild actually happened, `false` on a cache hit.
pub async fn ensure_system_image(
    tag: &str,
    target: &str,
    extra_input_rels: &[&str],
    rebuild: bool,
) -> Result<bool> {
    let root = weft_compiler::build::resolve_weft_root()
        .map_err(|e| anyhow::anyhow!("resolve weft repo root: {e}"))?;
    let dockerfile = root.join("deploy/docker").join(SYSTEM_IMAGES_DOCKERFILE);
    let seeds: Vec<String> = SYSTEM_IMAGE_BINARIES.iter().map(|s| s.to_string()).collect();
    let closure = weft_compiler::codegen::workspace_crate_closure(&root, &seeds)
        .map_err(|e| anyhow::anyhow!("system-image crate closure: {e}"))?;
    let mut inputs: Vec<(String, PathBuf)> = closure
        .iter()
        .map(|name| (format!("crates/{name}"), root.join("crates").join(name)))
        .collect();
    for rel in ["Cargo.toml", "Cargo.lock", "rust-toolchain.toml"] {
        inputs.push((rel.to_string(), root.join(rel)));
    }
    inputs.push((SYSTEM_IMAGES_DOCKERFILE.to_string(), dockerfile.clone()));
    for rel in extra_input_rels {
        inputs.push((rel.to_string(), root.join(rel)));
    }

    // System images use static `:local` tags, so tag presence says
    // nothing about content; a stamp file holding the input hash of
    // the last successful build decides staleness.
    let want_hash = hash_inputs(&inputs)?;
    let stamp_path = stamp_path_for(tag);
    let have_hash = std::fs::read_to_string(&stamp_path).ok().map(|s| s.trim().to_string());
    let image_exists = image_present(tag).await?;

    // `rebuild` is the force switch for when the stamp is lying (a
    // corrupted or hand-modified image): it must bypass the staleness
    // gate entirely, not just reword its message.
    if !rebuild && image_exists && have_hash.as_deref() == Some(want_hash.as_str()) {
        // Progress to stderr so `weft build-base --quiet` can capture only the tag
        // on stdout (data on stdout, progress on stderr).
        eprintln!("image {tag} up to date (image cached); skipping rebuild");
        return Ok(false);
    }

    build_image(tag, &dockerfile, &root, Some(target)).await?;
    if let Some(parent) = stamp_path.parent() {
        if let Err(e) = std::fs::create_dir_all(parent) {
            eprintln!(
                "warning: could not create stamp dir {} ({e}); \
                 the next ensure will rebuild {tag} even when unchanged",
                parent.display()
            );
        }
    }
    if let Err(e) = std::fs::write(&stamp_path, want_hash) {
        eprintln!(
            "warning: could not write image stamp {} ({e}); \
             the next ensure will rebuild {tag} even when unchanged",
            stamp_path.display()
        );
    }
    Ok(true)
}

/// One `docker build` invocation, BuildKit on. Used by both the
/// content-addressed builder base and the stamp-gated system images;
/// staleness decisions live in the callers. `target` selects a named
/// stage of a multi-target Dockerfile (the system images); `None`
/// builds the final stage (the builder base).
async fn build_image(
    tag: &str,
    dockerfile: &Path,
    context: &Path,
    target: Option<&str>,
) -> Result<()> {
    // Progress to stderr (data on stdout, progress on stderr) so a `--quiet`
    // caller capturing the resulting tag gets only the tag.
    eprintln!(
        "building image {tag} (this may take several minutes on first run; \
         subsequent builds are incremental)"
    );
    // We DO want docker's layer cache: combined with the buildkit
    // cargo cache mounts the Dockerfiles declare, an unchanged crate
    // set short-circuits to seconds. Deeper source changes are
    // caught by cargo's own fingerprinting inside the cache mount;
    // the callers' staleness gates handle the OUTER correctness (we
    // never reach this RUN when nothing changed).
    let mut cmd = Command::new("docker");
    cmd.env("DOCKER_BUILDKIT", "1")
        .args(["build", "-t", tag, "-f"])
        .arg(dockerfile);
    if let Some(stage) = target {
        cmd.args(["--target", stage]);
    }
    let status = cmd.arg(context).status().await?;
    if !status.success() {
        anyhow::bail!("docker build {tag} failed with {status}");
    }
    Ok(())
}

/// Stable per-tag stamp file. `weft-dispatcher:local` ->
/// `~/.local/share/weft/image-hashes/weft-dispatcher__local.hash`.
fn stamp_path_for(tag: &str) -> PathBuf {
    let safe_tag = tag.replace([':', '/'], "__");
    let base = std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".local/share/weft/image-hashes");
    base.join(format!("{safe_tag}.hash"))
}

/// Hash every regular file under each labeled input path. Shares
/// framing rules with the project source-hash function
/// (`hash::hash_path`) so the two hashers can't drift; both use
/// SHA-256 with explicit `file:` / `dir:` / `path:` / `missing:`
/// prefixes over machine-independent labels. Returns a 16-char hex
/// prefix (64 bits) which is plenty for image-stamp cache identity.
fn hash_inputs(inputs: &[(String, PathBuf)]) -> Result<String> {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    for (label, path) in inputs {
        weft_compiler::hash::hash_path(&mut hasher, label, path)?;
    }
    let digest = hasher.finalize();
    let mut out = String::with_capacity(16);
    for b in digest.iter().take(8) {
        use std::fmt::Write;
        let _ = write!(&mut out, "{:02x}", b);
    }
    Ok(out)
}

pub async fn image_present(tag: &str) -> Result<bool> {
    let out = Command::new("docker")
        .args(["image", "inspect", tag])
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
            "docker image inspect failed (image='{tag}'): {}",
            stderr.trim()
        )
    }
}

/// Load a locally-built image into the named kind cluster so its
/// Pods can pull it without a registry.
///
/// Content-addressed tags (worker / infra: `<repo>:<hash>`) can short-
/// circuit when the tag is already present on the node, because the
/// hash IS in the tag, so a present tag is the right content. Static
/// tags (`:local` for the four system images: dispatcher / listener
/// / broker / supervisor) CANNOT short-circuit on tag presence: the
/// tag is reused across builds, so "present" tells us nothing about
/// content. The caller distinguishes via `content_addressed_tag`.
///
/// We never compare image IDs across the docker/containerd boundary:
/// the two runtimes digest the same image differently (docker's config
/// blob vs containerd's), so an ID comparison never matches.
pub async fn kind_load(cluster: &str, tag: &str) -> Result<()> {
    kind_load_inner(cluster, tag, true).await
}

/// `kind_load` variant for static (reused) tags. Always re-loads,
/// because tag presence does not imply matching content for these.
pub async fn kind_load_force(cluster: &str, tag: &str) -> Result<()> {
    kind_load_inner(cluster, tag, false).await
}

async fn kind_load_inner(cluster: &str, tag: &str, allow_tag_skip: bool) -> Result<()> {
    if allow_tag_skip && kind_node_has_tag(cluster, tag).await {
        return Ok(());
    }
    let status = Command::new("kind")
        .args(["load", "docker-image", tag, "--name", cluster])
        .status()
        .await?;
    if !status.success() {
        anyhow::bail!("kind load docker-image {tag} failed");
    }
    Ok(())
}

/// Every image ref (`repoTags` entry) present on the kind node, via
/// `crictl images -o json` on the control-plane container. THE one
/// node-image-list reader: tag-presence checks (`kind_node_has_tag`) and
/// image reclamation (`weft clean --images`) both parse through here, so
/// the two cannot drift on how a node ref is spelled. Digest-only images
/// (null `repoTags`) are skipped.
pub async fn kind_node_repo_tags(cluster: &str) -> Result<Vec<String>> {
    let node = format!("{cluster}-control-plane");
    let out = Command::new("docker")
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
    Ok(parsed
        .get("images")
        .and_then(|v| v.as_array())
        .into_iter()
        .flatten()
        .filter_map(|img| img.get("repoTags").and_then(|v| v.as_array()))
        .flatten()
        .filter_map(|t| t.as_str())
        .map(str::to_string)
        .collect())
}

/// Whether the kind node already has an image tagged `tag`. Tag
/// presence alone is the answer: tags are content-addressed (the
/// suffix is the source hash), so a present tag is the right content.
/// We match on `repoTags`, not the image id, precisely because docker
/// and containerd report different ids for the same image. Any listing
/// failure reads as "absent" so the caller just re-loads.
async fn kind_node_has_tag(cluster: &str, tag: &str) -> bool {
    let (repo, version) = tag.split_once(':').unwrap_or((tag, "latest"));
    let Ok(tags) = kind_node_repo_tags(cluster).await else {
        return false;
    };
    tags.iter()
        .any(|t| t == &format!("{repo}:{version}") || t == &format!("docker.io/library/{repo}:{version}"))
}

#[cfg(test)]
mod tests {
    use super::SYSTEM_IMAGE_BINARIES;

    /// The system-image stamp closure must EXCLUDE the crates none of the four
    /// binaries link (weft-e2e, weft-cli): otherwise every e2e / CLI edit
    /// rebuilds + re-rolls all four system pods for nothing (the exact
    /// slowdown the closure scoping removed), and INCLUDE the binaries
    /// themselves plus the shared brain they all pull (weft-compiler via the
    /// dispatcher).
    #[test]
    fn system_stamp_closure_skips_non_system_crates() {
        let root = weft_compiler::build::resolve_weft_root().expect("resolve weft root");
        let seeds: Vec<String> = SYSTEM_IMAGE_BINARIES.iter().map(|s| s.to_string()).collect();
        let closure = weft_compiler::codegen::workspace_crate_closure(&root, &seeds)
            .expect("compute system closure");
        for absent in ["weft-e2e", "weft-cli"] {
            assert!(
                !closure.contains(&absent.to_string()),
                "{absent} must not gate system-image staleness; closure: {closure:?}"
            );
        }
        for present in ["weft-dispatcher", "weft-listener", "weft-broker",
                        "weft-infra-supervisor", "weft-compiler", "weft-core"] {
            assert!(
                closure.contains(&present.to_string()),
                "{present} missing from the system closure; closure: {closure:?}"
            );
        }
    }
}
