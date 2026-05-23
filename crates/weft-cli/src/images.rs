//! Image build + kind-load helpers. Owned by the CLI so the user
//! runs `weft daemon start` / `weft infra up` and the right images
//! land in the cluster. No external shell scripts.
//!
//! For cloud deploy these same helpers flip to registry push; only
//! one place changes.

use std::path::{Path, PathBuf};

use anyhow::Result;
use tokio::process::Command;


/// Build (if stale) a system image from `deploy/docker/<dockerfile_name>`.
/// All four system images (dispatcher / listener / broker /
/// infra-supervisor) share the same build inputs (workspace
/// manifests + every crate + the catalog); the only per-image
/// difference is which Dockerfile drives the build. Adding a
/// build input is a one-line change here, not four. Returns `true`
/// if a rebuild actually happened, `false` on a cache hit.
pub async fn ensure_system_image(
    tag: &str,
    dockerfile_name: &str,
    rebuild: bool,
) -> Result<bool> {
    let root = weft_compiler::build::resolve_weft_root()
        .map_err(|e| anyhow::anyhow!("resolve weft repo root: {e}"))?;
    let dockerfile = root.join("deploy/docker").join(dockerfile_name);
    let inputs = vec![
        root.join("Cargo.toml"),
        root.join("Cargo.lock"),
        root.join("rust-toolchain.toml"),
        dockerfile.clone(),
        root.join("crates"),
        root.join("catalog"),
    ];
    ensure_image(tag, &dockerfile, &root, &inputs, rebuild).await
}

async fn ensure_image(
    tag: &str,
    dockerfile: &Path,
    context: &Path,
    inputs: &[PathBuf],
    rebuild: bool,
) -> Result<bool> {
    let want_hash = hash_inputs(inputs)?;
    let stamp_path = stamp_path_for(tag);
    let have_hash = std::fs::read_to_string(&stamp_path).ok().map(|s| s.trim().to_string());
    let image_exists = image_present(tag).await?;

    if image_exists && have_hash.as_deref() == Some(want_hash.as_str()) {
        let reason = if rebuild { "no source changes" } else { "image cached" };
        println!("image {tag} up to date ({reason}); skipping rebuild");
        return Ok(false);
    }

    println!(
        "building image {tag} (this may take several minutes on first run; \
         subsequent builds are incremental)"
    );
    let status = Command::new("docker")
        .args(["build", "-t", tag, "-f"])
        .arg(dockerfile)
        .arg(context)
        .status()
        .await?;
    if !status.success() {
        anyhow::bail!("docker build {tag} failed with {status}");
    }
    if let Some(parent) = stamp_path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    let _ = std::fs::write(&stamp_path, want_hash);
    Ok(true)
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

/// Hash every regular file under each input path. Shares framing
/// rules with the project source-hash function (`hash::hash_path`)
/// so the two hashers can't drift; both use SHA-256 with explicit
/// `file:` / `path:` / `missing:` prefixes. Returns a 16-char hex
/// prefix (64 bits) which is plenty for image-stamp cache identity.
fn hash_inputs(inputs: &[PathBuf]) -> Result<String> {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    for input in inputs {
        crate::hash::hash_path(&mut hasher, input)?;
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
pub async fn kind_load(cluster: &str, tag: &str) -> Result<()> {
    // Compare host docker's image ID against what's in the kind
    // node. `kind load docker-image` skips itself when a same-tag
    // image already exists there, even if the content diverged
    // (tag present + ID mismatch silently leaves the old image in
    // place). We detect that, delete the stale node-side image,
    // then load fresh.
    let host_id = docker_image_id(tag).await?;
    let node_id = kind_node_image_id(cluster, tag).await.unwrap_or(None);
    if host_id.is_some() && host_id == node_id {
        return Ok(());
    }
    if node_id.is_some() {
        let node = format!("{cluster}-control-plane");
        let _ = Command::new("docker")
            .args(["exec", &node, "crictl", "rmi", tag])
            .status()
            .await;
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

async fn docker_image_id(tag: &str) -> Result<Option<String>> {
    let out = Command::new("docker")
        .args(["image", "inspect", "--format", "{{.Id}}", tag])
        .output()
        .await?;
    if !out.status.success() {
        return Ok(None);
    }
    let id = String::from_utf8_lossy(&out.stdout).trim().to_string();
    Ok(if id.is_empty() { None } else { Some(id) })
}

async fn kind_node_image_id(cluster: &str, tag: &str) -> Result<Option<String>> {
    let node = format!("{cluster}-control-plane");
    let (repo, version) = tag.split_once(':').unwrap_or((tag, "latest"));
    let out = Command::new("docker")
        .args(["exec", &node, "crictl", "images", "-q", "-o", "json"])
        .output()
        .await?;
    if !out.status.success() {
        return Ok(None);
    }
    // `crictl images -o json` emits {"images": [{"id": "...", "repoTags": [...]}]}
    let text = String::from_utf8_lossy(&out.stdout);
    let parsed: serde_json::Value = match serde_json::from_str(&text) {
        Ok(v) => v,
        Err(_) => return Ok(None),
    };
    for img in parsed.get("images").and_then(|v| v.as_array()).into_iter().flatten() {
        let tags = img
            .get("repoTags")
            .and_then(|v| v.as_array())
            .map(|a| a.iter().filter_map(|t| t.as_str()).collect::<Vec<_>>())
            .unwrap_or_default();
        if tags.iter().any(|t| *t == format!("{repo}:{version}") || *t == format!("docker.io/library/{repo}:{version}")) {
            if let Some(id) = img.get("id").and_then(|v| v.as_str()) {
                return Ok(Some(format!("sha256:{}", id.trim_start_matches("sha256:"))));
            }
        }
    }
    Ok(None)
}
