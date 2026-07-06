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
    let dockerfile = root.join(weft_compiler::hash::BUILDER_BASE_DOCKERFILE);
    // The tag is content-addressed (the hash covers every input the
    // build context reads, via `compute_builder_base_hash`), so a
    // present tag IS the right content: no stamp file needed.
    if !image_present(&tag).await? {
        // Generate the warm-up crate into the base build context. The base
        // compiles it to precook the rlibs every worker reuses (see
        // `codegen::emit_warmup_crate`). It is derived purely from
        // `fixed_worker_deps()`, so its content changes only when the engine /
        // dep set does, which already moves the base hash via `crates/`.
        let warmup_dir = root.join(weft_compiler::worker_image::WARMUP_CRATE_DIR);
        weft_compiler::codegen::emit_warmup_crate(&warmup_dir, &root)
            .map_err(|e| anyhow::anyhow!("emit builder-base warm-up crate: {e}"))?;
        build_image(&tag, &dockerfile, &root).await?;
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

/// Build (if stale) a system image from `deploy/docker/<dockerfile_name>`.
/// The input set is the shared workspace source list plus the
/// image's own Dockerfile plus `extra_input_rels` (paths relative to
/// the weft root the image additionally stages: the dispatcher
/// bundles `catalog/` for its describe / compile endpoints, the
/// others stage nothing extra, so a catalog-only edit doesn't
/// invalidate them).
/// Returns `true` if a rebuild actually happened, `false` on a cache hit.
pub async fn ensure_system_image(
    tag: &str,
    dockerfile_name: &str,
    extra_input_rels: &[&str],
    rebuild: bool,
) -> Result<bool> {
    let root = weft_compiler::build::resolve_weft_root()
        .map_err(|e| anyhow::anyhow!("resolve weft repo root: {e}"))?;
    let dockerfile = root.join("deploy/docker").join(dockerfile_name);
    let mut inputs = weft_compiler::hash::workspace_source_inputs(&root);
    inputs.push((dockerfile_name.to_string(), dockerfile.clone()));
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

    if image_exists && have_hash.as_deref() == Some(want_hash.as_str()) {
        let reason = if rebuild { "no source changes" } else { "image cached" };
        // Progress to stderr so `weft build-base --quiet` can capture only the tag
        // on stdout (data on stdout, progress on stderr).
        eprintln!("image {tag} up to date ({reason}); skipping rebuild");
        return Ok(false);
    }

    build_image(tag, &dockerfile, &root).await?;
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
/// staleness decisions live in the callers.
async fn build_image(tag: &str, dockerfile: &Path, context: &Path) -> Result<()> {
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
    let status = Command::new("docker")
        .env("DOCKER_BUILDKIT", "1")
        .args(["build", "-t", tag, "-f"])
        .arg(dockerfile)
        .arg(context)
        .status()
        .await?;
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

/// Whether the kind node already has an image tagged `tag`. Tag
/// presence alone is the answer: tags are content-addressed (the
/// suffix is the source hash), so a present tag is the right content.
/// We match on `repoTags`, not the image id, precisely because docker
/// and containerd report different ids for the same image.
async fn kind_node_has_tag(cluster: &str, tag: &str) -> bool {
    let node = format!("{cluster}-control-plane");
    let (repo, version) = tag.split_once(':').unwrap_or((tag, "latest"));
    let out = match Command::new("docker")
        .args(["exec", &node, "crictl", "images", "-o", "json"])
        .output()
        .await
    {
        Ok(o) if o.status.success() => o,
        _ => return false,
    };
    let text = String::from_utf8_lossy(&out.stdout);
    let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&text) else {
        return false;
    };
    parsed
        .get("images")
        .and_then(|v| v.as_array())
        .into_iter()
        .flatten()
        .filter_map(|img| img.get("repoTags").and_then(|v| v.as_array()))
        .flatten()
        .filter_map(|t| t.as_str())
        .any(|t| t == format!("{repo}:{version}") || t == format!("docker.io/library/{repo}:{version}"))
}
