//! A version's files, back on a disk the compiler can read: fetched from the
//! tenant's assets, checked against the hash the manifest names, and
//! laid out under a fresh directory, through this replica's
//! [`BlobCache`] so a build of mostly unchanged files fetches almost none. The version is the whole project,
//! its `nodes/base_catalog/` included, so nothing comes from this install.
//!
//! Everything under `assets/` (the project's media, which can be large)
//! stays in the store: the compile reads one only when a `@file(...)`
//! names it, and [`VersionFiles`] fetches that one then.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex};

use anyhow::{anyhow, bail, Context, Result};
use weft_catalog::{NodeTreeEntryKind, SourceTree};
use weft_core::project::hash::Manifest;

use futures::{StreamExt, TryStreamExt};

use super::blob_cache::BlobCache;
use super::ProjectStorage;

/// Whether the manifest path `path` is one of the project's `assets/`
/// files, which [`materialize`] leaves in the store.
fn is_asset(path: &str) -> bool {
    path.starts_with("assets/")
}

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

/// Lay every file of `manifest` out under `root` except those under
/// `assets/`, each checked against its hash: from this replica's `cache`
/// when it kept the bytes, otherwise streamed from the tenant's assets
/// (several at once) and kept.
pub async fn materialize(
    storage: &dyn ProjectStorage,
    cache: &BlobCache,
    tenant: &str,
    manifest: &Manifest,
    root: &Path,
) -> Result<()> {
    let scope = weft_core::storage::key::KeyScope::Asset;
    // Grown as each path is checked, never sized from the client's count.
    let mut files = Vec::new();
    for (path, hash) in manifest {
        let rel = safe_relative_path(path)?;
        if !weft_core::storage::is_content_hash(hash) {
            bail!("the version names '{path}' by '{hash}', which is not a content hash");
        }
        if !is_asset(path) {
            files.push((path, hash, root.join(rel)));
        }
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

/// One file of [`materialize`], written at `dest`: copied from the cache,
/// or streamed from the store, and either way hashed from the disk once
/// written, so what the build reads (and names its images after) is the
/// version's bytes. A cached copy that does not hash to its name is
/// dropped from the cache and fetched again; it is kept only once it
/// checks. A file that fails on the way, its hash included, is removed.
async fn lay_out(
    storage: &dyn ProjectStorage,
    cache: &BlobCache,
    tenant: &str,
    scope: &weft_core::storage::key::KeyScope,
    path: &str,
    hash: &str,
    dest: PathBuf,
) -> Result<()> {
    if let Some(parent) = dest.parent() {
        std::fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
    }
    if cache.get_into(tenant, hash, &dest)? {
        if hash_of(&dest)? == hash {
            return Ok(());
        }
        tracing::warn!(target: "weft_dispatcher::build", %path, %hash, "a cached copy of a version file did not hash to its name; fetching it from the store again");
        std::fs::remove_file(&dest).with_context(|| format!("remove {}", dest.display()))?;
        cache.forget(tenant, hash)?;
    }
    let fetched = fetch_checked(storage, tenant, scope, path, hash, &dest).await;
    if fetched.is_err() {
        let _ = std::fs::remove_file(&dest);
    }
    fetched?;
    cache.put_from(tenant, hash, &dest)
}

/// Stream `path`'s stored bytes to `dest` and check they hash to `hash`.
async fn fetch_checked(
    storage: &dyn ProjectStorage,
    tenant: &str,
    scope: &weft_core::storage::key::KeyScope,
    path: &str,
    hash: &str,
    dest: &Path,
) -> Result<()> {
    let scope_key = weft_core::storage::key::scope_key(scope, hash).map_err(|e| anyhow!("{e}"))?;
    storage
        .download(&format!("{tenant}/{scope_key}"), dest)
        .await
        .with_context(|| format!("fetch '{path}' of the version"))?;
    check_hash(path, hash, &hash_of(dest)?)
}

/// The sha256 of the file at `path`, read from the disk.
fn hash_of(path: &Path) -> Result<String> {
    let file = std::fs::File::open(path).with_context(|| format!("read {}", path.display()))?;
    let (got, _) = weft_assets::hash_reader(std::io::BufReader::new(file)).with_context(|| format!("hash {}", path.display()))?;
    Ok(got)
}

/// Refuse bytes that hash to `got` when the version names `path` by `hash`.
fn check_hash(path: &str, hash: &str, got: &str) -> Result<()> {
    if got != hash {
        bail!("'{path}' came back as {got}, and the version names {hash}; the store holds other bytes under that name");
    }
    Ok(())
}

/// One materialized version as the compiler reads it: every path the
/// manifest names under `root`, and nothing else. A file [`materialize`]
/// laid out reads from the disk; an `assets/` file it left in the store is
/// fetched the first time the compile reads it (a `@file(...)` naming it),
/// checked against its hash, and kept in memory for any later read.
///
/// A version holds no symlinks, so every answer comes from the manifest's
/// paths and [`SourceTree::canonicalize`] is lexical. Only ever used from a
/// blocking thread (`spawn_blocking`): a fetch blocks on the runtime
/// captured when it was made.
pub struct VersionFiles {
    root: PathBuf,
    manifest: Manifest,
    /// Every directory the manifest's paths sit under, `""` for `root`.
    dirs: BTreeSet<String>,
    storage: Arc<dyn ProjectStorage>,
    tenant: String,
    handle: tokio::runtime::Handle,
    /// The `assets/` files fetched so far, by path.
    fetched: Mutex<BTreeMap<String, Vec<u8>>>,
}

impl VersionFiles {
    /// Made on the runtime, whose handle a fetch blocks on.
    pub fn new(root: PathBuf, manifest: Manifest, storage: Arc<dyn ProjectStorage>, tenant: String) -> Self {
        let mut dirs = BTreeSet::from([String::new()]);
        for path in manifest.keys() {
            let mut at = path.as_str();
            while let Some((parent, _)) = at.rsplit_once('/') {
                dirs.insert(parent.to_string());
                at = parent;
            }
        }
        Self {
            root,
            manifest,
            dirs,
            storage,
            tenant,
            handle: tokio::runtime::Handle::current(),
            fetched: Mutex::new(BTreeMap::new()),
        }
    }

    /// `path` as a manifest path (`""` for the root itself), `..` and `.`
    /// worked out lexically; `None` when it is not under `root`.
    fn key(&self, path: &Path) -> Option<String> {
        let rel = path.strip_prefix(&self.root).ok()?;
        let mut parts: Vec<&str> = Vec::new();
        for component in rel.components() {
            match component {
                Component::CurDir => {}
                Component::ParentDir => {
                    parts.pop()?;
                }
                Component::Normal(part) => parts.push(part.to_str()?),
                Component::RootDir | Component::Prefix(_) => return None,
            }
        }
        Some(parts.join("/"))
    }

    /// The bytes of the `assets/` file at `rel`, fetched from the store the
    /// first time.
    fn asset(&self, rel: &str, hash: &str) -> std::io::Result<Vec<u8>> {
        if let Some(bytes) = self.fetched.lock().expect("version files lock").get(rel) {
            return Ok(bytes.clone());
        }
        let fetch = async {
            let scope_key = weft_core::storage::key::scope_key(&weft_core::storage::key::KeyScope::Asset, hash)
                .map_err(|e| anyhow!("{e}"))?;
            let bytes = self
                .storage
                .read(&format!("{}/{scope_key}", self.tenant))
                .await
                .with_context(|| format!("fetch '{rel}' of the version"))?;
            check_hash(rel, hash, &weft_core::project::hash::sha256_hex(&bytes))?;
            Ok::<_, anyhow::Error>(bytes)
        };
        let bytes = self.handle.block_on(fetch).map_err(|e| std::io::Error::other(format!("{e:#}")))?;
        self.fetched.lock().expect("version files lock").insert(rel.to_string(), bytes.clone());
        Ok(bytes)
    }
}

fn not_in_version(path: &Path) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::NotFound, format!("{} is not in this version", path.display()))
}

impl SourceTree for VersionFiles {
    fn read_dir(&self, dir: &Path) -> std::io::Result<Vec<PathBuf>> {
        let rel = self.key(dir).filter(|rel| self.dirs.contains(rel)).ok_or_else(|| not_in_version(dir))?;
        let prefix = if rel.is_empty() { String::new() } else { format!("{rel}/") };
        let children: BTreeSet<&str> = self
            .manifest
            .range(prefix.clone()..)
            .map(|(path, _)| path)
            .take_while(|path| path.starts_with(prefix.as_str()))
            .map(|path| {
                let rest = &path[prefix.len()..];
                rest.split('/').next().unwrap_or(rest)
            })
            .collect();
        Ok(children.into_iter().map(|child| dir.join(child)).collect())
    }

    fn entry_kind(&self, path: &Path) -> std::io::Result<NodeTreeEntryKind> {
        match self.key(path) {
            Some(rel) if self.manifest.contains_key(&rel) => Ok(NodeTreeEntryKind::File),
            Some(rel) if self.dirs.contains(&rel) => Ok(NodeTreeEntryKind::Dir),
            _ => Err(not_in_version(path)),
        }
    }

    fn read(&self, path: &Path) -> std::io::Result<Vec<u8>> {
        let rel = self.key(path).ok_or_else(|| not_in_version(path))?;
        let hash = self.manifest.get(&rel).ok_or_else(|| not_in_version(path))?;
        if is_asset(&rel) {
            self.asset(&rel, hash)
        } else {
            std::fs::read(self.root.join(&rel))
        }
    }

    fn canonicalize(&self, path: &Path) -> std::io::Result<PathBuf> {
        match self.key(path) {
            Some(rel) if self.manifest.contains_key(&rel) || self.dirs.contains(&rel) => {
                Ok(if rel.is_empty() { self.root.clone() } else { self.root.join(rel) })
            }
            _ => Err(not_in_version(path)),
        }
    }
}

/// Compile the materialized project: the definition, the value of each
/// file content it names (`weft_core::content`), its catalog, and the
/// project handle the plan reads. Blocking, every file read through
/// `files` (the `weft.toml` from `root`, where [`materialize`] laid it
/// out). What does not compile stays in the definition as its broken parts
/// (`weft_compiler::compile_program`): the version registers, and a run
/// that needs one is refused when it is cut. A failure of the program as a
/// whole renders every diagnostic, one per line, the way the CLI prints its
/// own.
pub fn compile(
    root: &Path,
    files: Arc<VersionFiles>,
) -> Result<(weft_core::ProjectDefinition, weft_core::content::ContentValues, weft_catalog::FsCatalog, weft_compiler::project::Project)> {
    let project = weft_compiler::project::Project::load(root).map_err(|e| anyhow!("{e}"))?;
    let (definition, catalog, contents) = weft_compiler::hash::load_program_in(&project, files)
        // SYNC: "compile failed:" <-> crates/weft-cli/src/commands/ensure.rs compile_failure
        .map_err(|e| anyhow!("compile failed:\n{e}"))?;
    Ok((definition, contents, catalog, project))
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
