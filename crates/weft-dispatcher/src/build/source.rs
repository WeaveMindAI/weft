//! A version's files, back on a disk the compiler can read: fetched from the
//! tenant's assets, checked against the hash the manifest names, and
//! laid out under a fresh directory, through this replica's
//! [`BlobCache`] so a build of mostly unchanged files fetches almost none. The version is the whole project,
//! its `nodes/base_catalog/` included, so nothing comes from this install.

use std::path::{Component, Path, PathBuf};

use anyhow::{anyhow, bail, Context, Result};
use weft_core::project::hash::Manifest;

use futures::{StreamExt, TryStreamExt};

use super::blob_cache::BlobCache;
use super::ProjectStorage;

/// Where a manifest path may land: relative, forward slashes, no `..`, no
/// empty or `.` segment. The manifest comes from the client, so a path that
/// would climb out of the build's directory (or name it) is refused.
pub fn safe_relative_path(path: &str) -> Result<PathBuf> {
    if path.is_empty() || path.starts_with('/') || path.contains('\\') || path.contains('\0') {
        bail!("the version names a file at '{path}', which is not a project-relative path");
    }
    let rel = PathBuf::from(path);
    for component in rel.components() {
        match component {
            Component::Normal(part) if !part.is_empty() => {}
            _ => bail!("the version names a file at '{path}', which is not a project-relative path"),
        }
    }
    if path.split('/').any(|seg| seg.is_empty() || seg == "." || seg == "..") {
        bail!("the version names a file at '{path}', which is not a project-relative path");
    }
    Ok(rel)
}

/// How many files [`materialize`] fetches at once.
const FETCHES_IN_FLIGHT: usize = 16;

/// Lay every file of `manifest` out under `root`, each checked against its
/// hash: from this replica's `cache` when it kept the bytes, otherwise
/// fetched from the tenant's assets (several at once) and kept.
pub async fn materialize(
    storage: &dyn ProjectStorage,
    cache: &BlobCache,
    tenant: &str,
    manifest: &Manifest,
    root: &Path,
) -> Result<()> {
    let scope = weft_core::storage::key::KeyScope::Asset;
    let mut files = Vec::with_capacity(manifest.len());
    for (path, hash) in manifest {
        let rel = safe_relative_path(path)?;
        if !weft_core::storage::is_content_hash(hash) {
            bail!("the version names '{path}' by '{hash}', which is not a content hash");
        }
        files.push((path, hash, root.join(rel)));
    }
    // The futures are made up front (they run only when polled): a stream
    // that maps through a closure here is not provably `Send` to axum.
    let fetches: Vec<_> =
        files.into_iter().map(|(path, hash, dest)| lay_out(storage, cache, tenant, &scope, path, hash, dest)).collect();
    futures::stream::iter(fetches)
        .buffer_unordered(FETCHES_IN_FLIGHT)
        .try_collect::<()>()
        .await?;
    cache.evict()
}

/// One file of [`materialize`]: its bytes from the cache or the store,
/// written at `dest`.
async fn lay_out(
    storage: &dyn ProjectStorage,
    cache: &BlobCache,
    tenant: &str,
    scope: &weft_core::storage::key::KeyScope,
    path: &str,
    hash: &str,
    dest: PathBuf,
) -> Result<()> {
    let bytes = match cache.get(tenant, hash)? {
        Some(bytes) => bytes,
        None => {
            let scope_key = weft_core::storage::key::scope_key(scope, hash).map_err(|e| anyhow!("{e}"))?;
            let bytes = storage
                .read(&format!("{tenant}/{scope_key}"))
                .await
                .with_context(|| format!("fetch '{path}' of the version"))?;
            let got = weft_core::project::hash::sha256_hex(&bytes);
            if got != hash {
                bail!("'{path}' came back as {got}, and the version names {hash}; the store holds other bytes under that name");
            }
            cache.put(tenant, hash, &bytes)?;
            bytes
        }
    };
    if let Some(parent) = dest.parent() {
        std::fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
    }
    std::fs::write(&dest, bytes).with_context(|| format!("write {}", dest.display()))
}

/// Compile the materialized project: the definition, its catalog, and the
/// project handle the plan reads. Blocking. A compile failure renders every
/// diagnostic, one per line, the way the CLI prints its own.
pub fn compile(
    root: &Path,
) -> Result<(weft_core::ProjectDefinition, weft_catalog::FsCatalog, weft_compiler::project::Project)> {
    let project = weft_compiler::project::Project::load(root).map_err(|e| anyhow!("{e}"))?;
    let (definition, catalog) = weft_compiler::hash::load_enriched_project_with_diagnostics(&project)
        .map_err(|e| anyhow!("compile failed:\n{e}"))?;
    let diags = weft_compiler::validate::validate_with_mode(
        &definition,
        &catalog,
        weft_compiler::validate::ValidationMode::Structural,
    );
    if diags.iter().any(|d| matches!(d.severity, weft_compiler::Severity::Error)) {
        bail!("compile failed:\n{}", weft_compiler::render_diagnostics(&diags));
    }
    Ok((definition, catalog, project))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_project_relative_paths_are_taken() {
        for good in ["weft.toml", "src/main.weft", "nodes/mine/mod.rs", "assets/a b.png"] {
            assert!(safe_relative_path(good).is_ok(), "{good}");
        }
        for bad in ["", "/etc/passwd", "../x", "src/../../x", "src//x", "./x", "src/.", "a\\b", "a\0b"] {
            assert!(safe_relative_path(bad).is_err(), "{bad:?}");
        }
    }
}
