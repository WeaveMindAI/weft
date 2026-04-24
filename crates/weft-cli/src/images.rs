//! Image build + kind-load helpers. Owned by the CLI so the user
//! runs `weft daemon start` / `weft infra up` and the right images
//! land in the cluster. No external shell scripts.
//!
//! For cloud deploy these same helpers flip to registry push; only
//! one place changes.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use tokio::process::Command;

/// Return the repo root: walks up from the `weft` CLI binary's
/// ancestor directories looking for the Cargo workspace root.
/// Falls back to `WEFT_REPO_ROOT` env var for overrides.
pub fn repo_root() -> Result<PathBuf> {
    if let Ok(p) = std::env::var("WEFT_REPO_ROOT") {
        return Ok(PathBuf::from(p));
    }
    // Ascend from CWD until we see a Cargo.toml that names the
    // `weft` workspace.
    let mut cur = std::env::current_dir().context("cwd")?;
    loop {
        let candidate = cur.join("Cargo.toml");
        if candidate.is_file() {
            if let Ok(body) = std::fs::read_to_string(&candidate) {
                if body.contains("weft-dispatcher") && body.contains("weft-listener") {
                    return Ok(cur);
                }
            }
        }
        if !cur.pop() {
            anyhow::bail!(
                "cannot locate weft repo root (no Cargo.toml with weft workspace found \
                 from cwd upwards). Set WEFT_REPO_ROOT to point at the weft source tree."
            );
        }
    }
}

/// Ensure `weft-dispatcher:local` exists in the local docker image
/// cache. Rebuild if `rebuild == true` or if it's missing.
pub async fn ensure_dispatcher_image(tag: &str, rebuild: bool) -> Result<()> {
    let root = repo_root()?;
    ensure_image(
        tag,
        &root.join("deploy/docker/dispatcher.Dockerfile"),
        &root,
        rebuild,
    )
    .await
}

pub async fn ensure_listener_image(tag: &str, rebuild: bool) -> Result<()> {
    let root = repo_root()?;
    ensure_image(
        tag,
        &root.join("deploy/docker/listener.Dockerfile"),
        &root,
        rebuild,
    )
    .await
}

async fn ensure_image(tag: &str, dockerfile: &Path, context: &Path, rebuild: bool) -> Result<()> {
    if !rebuild && image_present(tag).await? {
        return Ok(());
    }
    println!("building image {tag} (this may take several minutes on first run)");
    let status = Command::new("docker")
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

async fn image_present(tag: &str) -> Result<bool> {
    let out = Command::new("docker")
        .args(["image", "inspect", tag])
        .output()
        .await?;
    Ok(out.status.success())
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
