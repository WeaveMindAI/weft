//! Keeping a folder and its working copy the same, both ways: what the
//! local runtime does each time the folder or the working copy moves, and
//! what the CLI does before a command (so a command acts on what the
//! folder holds even with no runtime watching it, as on a cloud install).
//!
//! One pass reads the working copy and the folder, decides with
//! [`weft_core::working_copy::plan_sync`], and carries the plan out: the
//! files only the folder changed go up (their blobs first), the files only
//! the working copy changed are written to disk, and a file both changed
//! keeps the folder's bytes while the incoming ones are written beside it
//! as a conflict copy. The report lands all or nothing; when somebody moved
//! a path it read in between, the install answers `Stale` and the pass is
//! made again from a fresh read.
//!
//! A file is written only while the disk still holds what the pass read:
//! it is hashed again right before it is moved into place, so an edit
//! saved in the meantime is never overwritten (the next pass sees both
//! sides moved, and keeps it). What this syncer wrote itself is
//! remembered across its passes, so a pass made again after a `Stale`
//! does not mistake its own writes for an edit.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use weft_core::project::hash::Manifest;
use weft_core::working_copy::{plan_sync, Applied, Conflict, SyncReport, WorkingCopy, WorkingCopyId, WorkingState};

use crate::tree::{self, HashCache};
use crate::{AssetSource, AssetStore};

/// The working copy, as a syncer reaches it.
#[async_trait::async_trait]
pub trait WorkingCopyPort: Send + Sync {
    /// The working copy and its files, read now.
    async fn state(&self) -> Result<WorkingState>;
    /// Land one pass's report, or hear it is stale.
    async fn sync(&self, report: &SyncReport) -> Result<Applied>;
}

/// The tenant's stored files, as a syncer reads them.
#[async_trait::async_trait]
pub trait BlobFetch: Send + Sync {
    /// Write the blob `hash` into `dest`, a file that is there and empty,
    /// streaming it.
    async fn fetch(&self, hash: &str, dest: &Path) -> Result<()>;
}

/// What one [`sync_folder`] did.
#[derive(Debug, Clone)]
pub struct Synced {
    /// The working copy as it is now.
    pub copy: WorkingCopy,
    /// The files that went up to the working copy, and the files written
    /// to the folder (`None`: removed), each project-relative.
    pub pushed: Vec<String>,
    pub written: Vec<String>,
    /// The paths both sides changed in this pass.
    pub conflicts: Vec<Conflict>,
}

/// How many passes a sync makes when every one comes back stale before it
/// says so: the working copy is being changed faster than the folder can
/// follow, which is somebody else's writing, never this syncer's.
const PASSES: usize = 8;

/// Make the folder at `root` and its working copy the same (see the module
/// doc). `cache` remembers the folder's hashes between calls; the caller
/// keeps it and saves it.
pub async fn sync_folder(
    root: &Path,
    port: &dyn WorkingCopyPort,
    fetch: &dyn BlobFetch,
    store: &dyn AssetStore,
    cache: &mut HashCache,
) -> Result<Synced> {
    // What this syncer put on disk itself, path by path, across passes.
    let mut wrote: BTreeMap<String, Option<String>> = BTreeMap::new();
    let mut pushed = Vec::new();
    let mut written = Vec::new();
    let mut conflicts = Vec::new();
    for _ in 0..PASSES {
        let state = port.state().await.context("read the working copy")?;
        let disk = blocking(|| tree::disk_manifest(root, cache))?;
        let synced = with_own_writes(&state.synced, &disk.manifest, &wrote);
        let plan = plan_sync(&disk.manifest, &synced, &state.working);
        if plan.is_empty() && disk.conflicts == state.copy.conflicts {
            return Ok(Synced { copy: state.copy, pushed, written, conflicts });
        }
        // Up: the folder's new contents first, so the working copy never
        // names a blob the store lacks.
        let up: Vec<String> = plan.push.iter().filter(|c| c.hash.is_some()).map(|c| c.path.clone()).collect();
        if !up.is_empty() {
            let source = Folder(root);
            let hashed = blocking(|| crate::hash_files(&up, &source))?;
            if hashed.iter().any(|file| disk.manifest.get(&file.path) != Some(&file.hash)) {
                // Saved again since the pass read it: decide again over
                // the new bytes.
                continue;
            }
            crate::publish_hashed(&hashed, &source, store).await.context("upload the folder's changed files")?;
        }
        // Down: only over a file that still holds what the pass read.
        let mut stale_disk = false;
        for (path, incoming) in &plan.write {
            if !holds(root, path, disk.manifest.get(path))? {
                stale_disk = true;
                continue;
            }
            match incoming {
                Some(hash) => place(root, path, hash, fetch).await?,
                None => tree::remove_in_place(root, path)?,
            }
            wrote.insert(path.clone(), incoming.clone());
            written.push(path.clone());
        }
        let mut copies = disk.conflicts.clone();
        for conflict in &plan.conflicts {
            if let (Some(hash), Some(copy)) = (&conflict.incoming, &conflict.copy_path) {
                place(root, copy, hash, fetch).await?;
                if !copies.contains(copy) {
                    copies.push(copy.clone());
                }
            }
        }
        copies.sort();
        if stale_disk {
            // The folder moved under the pass: decide again over what it
            // holds now, before telling the working copy anything.
            continue;
        }
        let report = SyncReport { changes: plan.push.clone(), synced: plan.synced.clone(), conflicts: copies };
        match port.sync(&report).await.context("send the folder's changes")? {
            Applied::Applied { copy } => {
                pushed.extend(plan.push.iter().map(|c| c.path.clone()));
                conflicts.extend(plan.conflicts);
                return Ok(Synced { copy, pushed, written, conflicts });
            }
            Applied::Stale { .. } => continue,
        }
    }
    bail!(
        "the working copy of {} kept changing while it was being synced with the folder ({PASSES} passes); \
         something else is writing into it: run the command again once it is done",
        root.display()
    )
}

/// Run `work` (walking and hashing the folder) where blocking the thread
/// holds up nothing else: a first pass over a large project reads every
/// file, and on the runtime's worker threads that would stall every task
/// queued behind it. Needs the multi-threaded runtime, which the CLI and
/// the runtime both run on.
fn blocking<T>(work: impl FnOnce() -> T) -> T {
    tokio::task::block_in_place(work)
}

/// `synced` with what this syncer wrote itself folded in, for the paths
/// whose disk still holds exactly that: a pass made again after a `Stale`
/// knows those files as synced, not as edits.
fn with_own_writes(synced: &Manifest, disk: &Manifest, wrote: &BTreeMap<String, Option<String>>) -> Manifest {
    let mut out = synced.clone();
    for (path, content) in wrote {
        if disk.get(path) == content.as_ref() {
            match content {
                Some(hash) => out.insert(path.clone(), hash.clone()),
                None => out.remove(path),
            };
        }
    }
    out
}

/// Whether the file at `path` still holds `expected` (`None`: no file).
fn holds(root: &Path, path: &str, expected: Option<&String>) -> Result<bool> {
    let now = match tree::open_covered(root, path)? {
        Some(file) => Some(crate::hash_reader(file).with_context(|| format!("read {}", root.join(path).display()))?.0),
        None => None,
    };
    Ok(now.as_ref() == expected)
}

/// Write the blob `hash` at `path`: fetched into a private file first, so
/// the path holds the old file or the new one and never part of either.
async fn place(root: &Path, path: &str, hash: &str, fetch: &dyn BlobFetch) -> Result<()> {
    let temp = tree::temp_for(root, path)?;
    fetch.fetch(hash, temp.path()).await.with_context(|| format!("download {path}"))?;
    let (got, _) = crate::hash_reader(std::fs::File::open(temp.path())?)?;
    if got != hash {
        bail!("{path} came down as {got}, and the working copy names {hash}; the store holds other bytes under that name");
    }
    tree::write_in_place(root, path, temp.path())
}

/// A folder's files, by their project-relative path.
struct Folder<'a>(&'a Path);

impl AssetSource for Folder<'_> {
    fn open(&self, path: &str) -> Result<Box<dyn std::io::Read + Send>> {
        let full = self.0.join(path);
        Ok(Box::new(std::fs::File::open(&full).with_context(|| format!("read {}", full.display()))?))
    }
}

// ----- the folders a machine keeps in sync ------------------------------

/// One folder working copy a machine keeps in sync, and its folder.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FolderCopy {
    pub id: WorkingCopyId,
    pub root: PathBuf,
}

/// What one pass of [`sync_folder`] over a folder goes through: its
/// working copy, its tenant's stored files, and where the folder's new
/// files go up.
pub struct Reach<'a> {
    pub port: Box<dyn WorkingCopyPort + 'a>,
    pub fetch: Box<dyn BlobFetch + 'a>,
    pub store: Box<dyn AssetStore + 'a>,
}

/// What the install tells about its working copies.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Notice {
    /// This working copy changed, or went.
    Changed(WorkingCopyId),
    /// A working copy was made: there may be a new folder to keep.
    Added,
    /// Notices may have gone unheard: look at every folder again.
    Recheck,
}

/// The install's notices, one at a time.
#[async_trait::async_trait]
pub trait Notices: Send {
    /// The next notice. An error is the install no longer telling
    /// anything, which nothing could wake a folder from.
    async fn next(&mut self) -> Result<Notice>;
}

/// The install's folder working copies, as the machine that keeps them in
/// sync with their folders reaches them (the local runtime's folder
/// service).
#[async_trait::async_trait]
pub trait FolderCopies: Send + Sync {
    /// Start hearing the install's notices. Asked before the first look
    /// at [`FolderCopies::folders`], so a change made in between is heard.
    fn notices(&self) -> Result<Box<dyn Notices>>;
    /// Every folder working copy this machine keeps in sync.
    async fn folders(&self) -> Result<Vec<FolderCopy>>;
    /// What a pass over `copy` goes through.
    async fn reach(&self, copy: &FolderCopy) -> Result<Reach<'_>>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    use weft_core::working_copy::{apply_changes, fold_synced, Head, WorkingCopyKind};

    /// A working copy and a store in memory: the dumbest thing that lands
    /// a report the way the install does (all or nothing, stale when a
    /// change does not find what it expected).
    #[derive(Default)]
    struct Install {
        working: Mutex<Manifest>,
        synced: Mutex<Manifest>,
        conflicts: Mutex<Vec<String>>,
        blobs: Mutex<BTreeMap<String, Vec<u8>>>,
        /// Changes somebody else lands right before the next report.
        meanwhile: Mutex<Vec<(String, Option<String>)>>,
    }

    impl Install {
        fn copy(&self) -> WorkingCopy {
            WorkingCopy {
                id: uuid::Uuid::nil(),
                kind: WorkingCopyKind::Folder,
                host: None,
                path: None,
                head: Head::default(),
                working_version: weft_core::project::hash::manifest_version_id(&self.working.lock().unwrap()),
                revision: 0,
                conflicts: self.conflicts.lock().unwrap().clone(),
                updated_at: 0,
            }
        }

        fn put(&self, bytes: &[u8]) -> String {
            let hash = weft_core::project::hash::sha256_hex(bytes);
            self.blobs.lock().unwrap().insert(hash.clone(), bytes.to_vec());
            hash
        }
    }

    #[async_trait::async_trait]
    impl WorkingCopyPort for Install {
        async fn state(&self) -> Result<WorkingState> {
            Ok(WorkingState { copy: self.copy(), working: self.working.lock().unwrap().clone(), synced: self.synced.lock().unwrap().clone() })
        }

        async fn sync(&self, report: &SyncReport) -> Result<Applied> {
            for (path, hash) in self.meanwhile.lock().unwrap().drain(..) {
                let mut working = self.working.lock().unwrap();
                match hash {
                    Some(hash) => working.insert(path, hash),
                    None => working.remove(&path),
                };
            }
            let landed = apply_changes(&self.working.lock().unwrap(), &report.changes);
            match landed {
                Err(paths) => Ok(Applied::Stale { paths }),
                Ok(working) => {
                    *self.working.lock().unwrap() = working;
                    let synced = fold_synced(&self.synced.lock().unwrap(), &report.synced);
                    *self.synced.lock().unwrap() = synced;
                    *self.conflicts.lock().unwrap() = report.conflicts.clone();
                    Ok(Applied::Applied { copy: self.copy() })
                }
            }
        }
    }

    #[async_trait::async_trait]
    impl BlobFetch for Install {
        async fn fetch(&self, hash: &str, dest: &Path) -> Result<()> {
            let bytes = self.blobs.lock().unwrap().get(hash).cloned().context("no such blob")?;
            std::fs::write(dest, bytes)?;
            Ok(())
        }
    }

    #[async_trait::async_trait]
    impl AssetStore for Install {
        async fn held(&self, hashes: &[String]) -> Result<BTreeMap<String, String>> {
            let blobs = self.blobs.lock().unwrap();
            Ok(hashes.iter().filter(|h| blobs.contains_key(*h)).map(|h| (h.clone(), format!("asset/{h}"))).collect())
        }

        async fn upload(&self, hash: &str, _mime: &str, _filename: &str, _size: u64, bytes: &mut (dyn std::io::Read + Send)) -> Result<String> {
            let mut all = Vec::new();
            bytes.read_to_end(&mut all)?;
            anyhow::ensure!(weft_core::project::hash::sha256_hex(&all) == hash, "not its hash");
            self.blobs.lock().unwrap().insert(hash.to_string(), all);
            Ok(format!("asset/{hash}"))
        }

        fn interrupted(&self, _error: &anyhow::Error) -> bool {
            false
        }
    }

    fn write(root: &Path, rel: &str, text: &str) {
        let path = root.join(rel);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, text).unwrap();
    }

    async fn sync(root: &Path, install: &Install) -> Synced {
        let mut cache = HashCache::default();
        sync_folder(root, install, install, install, &mut cache).await.unwrap()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_save_goes_up_and_a_program_write_comes_down() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        write(root, "main.weft", "a = Text");
        let install = Install::default();
        let synced = sync(root, &install).await;
        assert_eq!(synced.pushed, vec!["main.weft"]);
        let main = install.working.lock().unwrap()["main.weft"].clone();
        assert_eq!(install.blobs.lock().unwrap()[&main], b"a = Text");

        // A program writes a prompt into the working copy: it lands on disk.
        let prompt = install.put(b"be kind");
        install.working.lock().unwrap().insert("prompts/tone.txt".into(), prompt);
        let synced = sync(root, &install).await;
        assert_eq!(synced.written, vec!["prompts/tone.txt"]);
        assert_eq!(std::fs::read_to_string(root.join("prompts/tone.txt")).unwrap(), "be kind");
        // And the next pass has nothing to do: its own write is no edit.
        let again = sync(root, &install).await;
        assert!(again.pushed.is_empty() && again.written.is_empty(), "{again:?}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn an_edit_on_both_sides_keeps_the_folders_file_and_writes_the_other_beside_it() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        write(root, "prompts/tone.txt", "one");
        let install = Install::default();
        sync(root, &install).await;
        write(root, "prompts/tone.txt", "mine");
        let theirs = install.put(b"theirs");
        install.working.lock().unwrap().insert("prompts/tone.txt".into(), theirs.clone());
        let synced = sync(root, &install).await;
        assert_eq!(synced.conflicts.len(), 1);
        assert_eq!(std::fs::read_to_string(root.join("prompts/tone.txt")).unwrap(), "mine", "the folder's edit is kept");
        let copy = weft_core::project::coverage::conflict_copy_name("prompts/tone.txt", &theirs);
        assert_eq!(std::fs::read_to_string(root.join(&copy)).unwrap(), "theirs", "the incoming one is beside it");
        assert_eq!(*install.conflicts.lock().unwrap(), vec![copy], "the working copy knows the copy is there");
        let mine = weft_core::project::hash::sha256_hex(b"mine");
        assert_eq!(install.working.lock().unwrap()["prompts/tone.txt"], mine, "and holds what the person sees");
    }

    /// Build output beside a node (cargo's `target` next to its
    /// `Cargo.toml`, installed `node_modules`) never goes up as part of the
    /// working copy, and the working copy moving to another version (a
    /// branch back) removes the source files it no longer holds and leaves
    /// the build output on disk; a source folder merely called `target`
    /// is source and goes up.
    #[tokio::test(flavor = "multi_thread")]
    async fn build_output_never_goes_up_and_a_branch_back_leaves_it_alone() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        write(root, "main.weft", "a = Text");
        write(root, "nodes/pkg/Cargo.toml", "[package]");
        write(root, "nodes/pkg/target/debug/build.log", "stale build output");
        write(root, "nodes/pkg/node_modules/left-pad/index.js", "module.exports = 1;");
        write(root, "prompts/target/tone.txt", "a prompt folder called target");
        let install = Install::default();
        let mut pushed = sync(root, &install).await.pushed;
        pushed.sort();
        assert_eq!(pushed, ["main.weft", "nodes/pkg/Cargo.toml", "prompts/target/tone.txt"]);

        install.working.lock().unwrap().remove("prompts/target/tone.txt");
        let synced = sync(root, &install).await;
        assert_eq!(synced.written, vec!["prompts/target/tone.txt"]);
        assert!(!root.join("prompts/target/tone.txt").exists(), "a source file the version does not hold goes");
        assert!(root.join("nodes/pkg/target/debug/build.log").is_file() && root.join("nodes/pkg/node_modules/left-pad/index.js").is_file(),
            "build output stays where it was");
        assert!(synced.pushed.is_empty(), "{synced:?}");
    }

    /// Somebody changes the file this pass was pushing: the install answers
    /// stale, and the next pass finds both sides moved.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_stale_report_is_decided_again() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        write(root, "main.weft", "one");
        let install = Install::default();
        sync(root, &install).await;
        write(root, "main.weft", "two");
        let other = install.put(b"three");
        install.meanwhile.lock().unwrap().push(("main.weft".into(), Some(other.clone())));
        let synced = sync(root, &install).await;
        assert_eq!(synced.conflicts.len(), 1, "{synced:?}");
        assert_eq!(std::fs::read_to_string(root.join("main.weft")).unwrap(), "two");
    }
}
