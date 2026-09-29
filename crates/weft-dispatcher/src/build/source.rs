//! A version's files, back on a disk the compiler can read: fetched from the
//! project's asset plane, checked against the hash the manifest names, and
//! laid out under a fresh directory with the catalog this install ships.

use std::path::{Component, Path, PathBuf};

use anyhow::{anyhow, bail, Context, Result};
use weft_core::project::hash::{Manifest, WEFT_ENTRY_PREFIX};

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

/// The weft entry a manifest carries, and the one this install would write
/// for the same catalog, compared: a version written against another weft
/// or another catalog cannot be rebuilt from this install's, and is refused
/// with the one command that fixes it.
pub fn check_weft_entry(manifest: &Manifest, own: &str) -> Result<()> {
    let entries: Vec<&String> = manifest.keys().filter(|k| k.starts_with(WEFT_ENTRY_PREFIX)).collect();
    let [entry] = entries.as_slice() else {
        bail!("the version carries {} weft entries; a version names exactly one", entries.len());
    };
    if *entry == own {
        return Ok(());
    }
    let version = |e: &str| e.trim_start_matches(WEFT_ENTRY_PREFIX).split(':').next().unwrap_or("").to_string();
    let (theirs, ours) = (version(entry), version(own));
    if theirs != ours {
        bail!(
            "this version was written by weft {theirs}, and this install runs weft {ours}: its \
             standard library is another one. Install the CLI of the install's version ({ours}), \
             or update the install to {theirs} (merge upstream into the fork and rerun its \
             install workflow)"
        );
    }
    bail!(
        "this version's `nodes/base_catalog/` is not the one weft {ours} ships (edited in place, \
         or seeded by another build of weft). Run `weft catalog update` in the project and try again"
    )
}

/// Fetch every file of `manifest` from the project's asset plane into
/// `root`, each checked against its hash, then seed the catalog this
/// install ships and check it is the one the version names.
pub async fn materialize(
    storage: &dyn ProjectStorage,
    tenant: &str,
    project_id: uuid::Uuid,
    manifest: &Manifest,
    root: &Path,
) -> Result<()> {
    let scope = weft_core::storage::key::KeyScope::Asset { project_id: project_id.to_string() };
    for (path, hash) in manifest.iter().filter(|(p, _)| !p.starts_with(WEFT_ENTRY_PREFIX)) {
        let rel = safe_relative_path(path)?;
        if !weft_core::storage::is_content_hash(hash) {
            bail!("the version names '{path}' by '{hash}', which is not a content hash");
        }
        let scope_key = weft_core::storage::key::scope_key(&scope, hash).map_err(|e| anyhow!("{e}"))?;
        let bytes = storage
            .read(&format!("{tenant}/{scope_key}"))
            .await
            .with_context(|| format!("fetch '{path}' of the version"))?;
        let got = weft_core::project::hash::sha256_hex(&bytes);
        if got != *hash {
            bail!("'{path}' came back as {got}, and the version names {hash}; the store holds other bytes under that name");
        }
        let dest = root.join(&rel);
        if let Some(parent) = dest.parent() {
            std::fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
        }
        std::fs::write(&dest, bytes).with_context(|| format!("write {}", dest.display()))?;
    }
    let root_owned = root.to_path_buf();
    let own_entry = tokio::task::spawn_blocking(move || {
        weft_compiler::project::seed_base_catalog(&root_owned)?;
        weft_compiler::project::weft_entry(&root_owned)
    })
    .await
    .context("the catalog seed panicked")?
    .map_err(|e| anyhow!("seed the standard library: {e}"))?;
    check_weft_entry(manifest, &own_entry)
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

    #[test]
    fn a_version_from_another_weft_or_catalog_is_refused_by_name() {
        let own = "weft:0.2.0:aaa";
        let with = |entry: &str| Manifest::from([("weft.toml".into(), "h".into()), (entry.into(), String::new())]);
        assert!(check_weft_entry(&with(own), own).is_ok());
        let other_version = check_weft_entry(&with("weft:0.1.9:aaa"), own).unwrap_err().to_string();
        assert!(other_version.contains("weft 0.1.9") && other_version.contains("weft 0.2.0"), "{other_version}");
        let other_catalog = check_weft_entry(&with("weft:0.2.0:bbb"), own).unwrap_err().to_string();
        assert!(other_catalog.contains("weft catalog update"), "{other_catalog}");
        let none = Manifest::from([("weft.toml".into(), "h".into())]);
        assert!(check_weft_entry(&none, own).is_err());
    }
}
