//! `weft branch <version|color>`: restore that version's files (a color
//! means its version) and move head. A color sets head's run, so the
//! next `--seed` inherits from it; a version clears it. Refuses on a
//! dirty tree naming the files; `--discard` overrides.

use anyhow::{bail, Context};

use super::versions::{fetch_tree, local_manifest, manifest_files, resolve_run, resolve_version, short, Manifest};
use super::Ctx;

pub async fn run(ctx: Ctx, reference: String, discard: bool) -> anyhow::Result<()> {
    let project = ctx.project()?;
    let client = ctx.client();
    let id = project.id().to_string();
    let tree = fetch_tree(&client, &id).await?;

    // Dirty: the files differ from head's version. Nothing to compare
    // against before the first checkpoint or run.
    if !discard {
        if let Some(head) = tree.head.head_version.as_deref().and_then(|h| tree.versions.iter().find(|v| v.id == h)) {
            let local = local_manifest(project)?;
            let dirty = dirty_files(&head.manifest, &local);
            if !dirty.is_empty() {
                bail!(
                    "the tree has changes since head {}:\n  {}\n`weft checkpoint` keeps them as a version, or `--discard` throws them away",
                    short(&head.id),
                    dirty.join("\n  ")
                );
            }
        }
    }

    // A color means its version and becomes head's run.
    let (body, version_id) = match resolve_run(&tree, &reference) {
        Ok(run) => (serde_json::json!({ "run": run.color }), run.version_id.clone()),
        Err(_) => {
            let version = resolve_version(&tree, &reference)?;
            (serde_json::json!({ "version": version.id.clone() }), version.id.clone())
        }
    };
    let manifest: Manifest = tree
        .versions
        .iter()
        .find(|v| v.id == version_id)
        .map(|v| v.manifest.clone())
        .with_context(|| format!("version {} is not in this project's tree", short(&version_id)))?;

    // FILES FIRST, head after.
    //
    // Neither order is atomic: `restore` writes one file at a time, so a
    // download that fails or a full disk leaves the tree part way either
    // way. What the order decides is what the person is told and what
    // they can do about it. Head moved first meant the dirty check (which
    // measures against head) was already satisfied by the half-restored
    // tree, so the next `weft branch` refused and listed half the tree as
    // changed, with `--discard` the only way out. Head moved last leaves
    // it where it was, and both failures below name the command that
    // finishes the job.
    restore(&client, project, &manifest).await.with_context(|| {
        format!(
            "the tree is part way to version {}; `weft branch {} --discard` finishes restoring it",
            short(&version_id),
            short(&version_id)
        )
    })?;
    // SYNC: body <-> crates/weft-dispatcher/src/api/versions.rs HeadRequest
    let resp = client
        .put_json(&format!("/projects/{id}/versions/head"), &body)
        .await
        .with_context(|| {
            format!(
                "the files on disk ARE version {} now, but head could not be moved there. \
                 `weft branch {reference} --discard` finishes the job, and it names {reference} \
                 again rather than the version, because that is what you asked for and a color \
                 also sets head's run. Plain `weft branch {reference}` will refuse, because head \
                 is still the old version and reads the restored files as your own uncommitted \
                 changes; `--discard` overwrites the files with this version, which is what they \
                 already are unless you have edited them since",
                short(&version_id)
            )
        })?;
    // The response's version is the contract, not a nicety: answering
    // without one means the shape drifted, and printing the version we
    // ASKED for would hide that behind a confident line.
    let version = resp
        .get("version")
        .and_then(|v| v.as_str())
        .context("head response missing version")?
        .to_string();

    if ctx.json_out(&resp)? {
        return Ok(());
    }
    match resp.get("run").and_then(|v| v.as_str()) {
        Some(run) => println!("head is now run {} on version {}", short(run), short(&version)),
        None => println!("head is now version {}", short(&version)),
    }
    Ok(())
}

/// The covered paths whose bytes differ between head's manifest and
/// the disk (added, removed, or changed), sorted. The pseudo-entry
/// (the installed weft) is ignored: a different weft is not an edit.
pub fn dirty_files(head: &Manifest, local: &Manifest) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for (path, hash) in manifest_files(local) {
        if head.get(path) != Some(hash) {
            out.push(path.clone());
        }
    }
    for (path, _) in manifest_files(head) {
        if !local.contains_key(path) {
            out.push(path.clone());
        }
    }
    out.sort();
    out.dedup();
    out
}

/// Make the disk match `manifest`: download every blob whose bytes
/// differ, remove every covered file the manifest does not name.
async fn restore(client: &crate::client::DispatcherClient, project: &weft_compiler::project::Project, manifest: &Manifest) -> anyhow::Result<()> {
    let local = local_manifest(project)?;
    let project_id = Some(project.id().to_string());
    // Every path is checked BEFORE anything is written, so a bad entry
    // halfway down the manifest cannot leave the tree half restored.
    //
    // The path is as untrusted as the hash beside it, and for the same
    // reason: both came off the wire. `Path::join` with an absolute path
    // THROWS THE ROOT AWAY, and a `..` segment walks out of it, so an
    // entry naming one would be written outside the project, where the
    // removal loop below (which only knows the paths it read from disk)
    // could never undo it. The dispatcher refuses such an entry when a
    // version is recorded; this is the other end of the same rule,
    // because where a client writes is not a server's word to take.
    // EXACTLY the rule the dispatcher applies when a version is recorded
    // (`api/versions.rs` `validate_manifest`), and no more. Asking
    // `covers()` as well was stricter than the recording rule, so an
    // entry weft had legitimately stored but that today's coverage rule
    // would not record turned a branch into a total refusal whose message
    // claimed weft would never have recorded it.
    let bad: Vec<&String> = manifest_files(manifest)
        .filter(|(path, _)| {
            path.is_empty()
                || path.starts_with('/')
                || path.contains('\\')
                || path.split('/').any(|seg| seg.is_empty() || seg == "." || seg == "..")
        })
        .map(|(path, _)| path)
        .collect();
    if !bad.is_empty() {
        bail!(
            "this version lists {}, which weft would never record as a project file; nothing was \
             restored",
            bad.iter().map(|p| format!("'{p}'")).collect::<Vec<_>>().join(", ")
        );
    }
    validate_destinations(&project.root, manifest)?;
    // Download before changing the working tree. Deleting obsolete files first
    // then permits a path to change between a file and a directory.
    let staged = tempfile::tempdir()?;
    for (path, hash) in manifest_files(manifest) {
        if local.get(path) == Some(hash) {
            continue;
        }
        // Through the key grammar's own constructor: the hash comes
        // off a manifest the dispatcher sent, so it is not this side's
        // to trust. Named by PATH, which is the file the person can
        // actually look at.
        let scope = weft_core::storage::key::KeyScope::Asset { project_id: project.id().to_string() };
        let key = weft_core::storage::key::scope_key(&scope, hash).map_err(|e| {
            anyhow::anyhow!("this version lists '{path}' as '{hash}', which is not a storable address ({e})")
        })?;
        let bytes = super::files::download_bytes(client, &key, &project_id)
            .await
            .with_context(|| format!("download {path} ({hash})"))?;
        let full = staged.path().join(path);
        if let Some(parent) = full.parent() {
            std::fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
        }
        std::fs::write(&full, bytes).with_context(|| format!("write {}", full.display()))?;
    }
    // From here the working tree changes. Nothing above touched it, so a
    // failure above leaves the person exactly where they were; a failure
    // below may leave a mixture of two versions, and says which.
    let mut touched = false;
    apply_restore(&project.root, &local, manifest, staged.path(), &mut touched).with_context(|| {
        if touched {
            "the working tree is now partly restored (some files are from the version you asked for, \
             the rest from where you were); fix the cause and run the same weft branch again to finish"
        } else {
            "nothing was restored; the working tree is as it was"
        }
    })
}

fn validate_destinations(root: &std::path::Path, manifest: &Manifest) -> anyhow::Result<()> {
    for (path, _) in manifest_files(manifest) {
        let mut full = root.to_path_buf();
        for part in std::path::Path::new(path).components() {
            full.push(part);
            match std::fs::symlink_metadata(&full) {
                Ok(metadata) if metadata.file_type().is_symlink() => {
                    bail!("cannot restore '{path}': {} is a symbolic link", full.display());
                }
                Ok(_) => {}
                Err(error) if matches!(error.kind(), std::io::ErrorKind::NotFound | std::io::ErrorKind::NotADirectory) => break,
                Err(error) => return Err(error).with_context(|| format!("inspect {}", full.display())),
            }
        }
    }
    Ok(())
}

/// Make `root` hold `manifest`, given it holds `local` now and every file
/// that has to change is downloaded under `staged`. Obsolete files go
/// first, so a path can change between a file and a directory. Not
/// atomic: `touched` turns true at the first change to the tree, so the
/// caller can say whether a failure left a mixture.
fn apply_restore(root: &std::path::Path, local: &Manifest, manifest: &Manifest, staged: &std::path::Path, touched: &mut bool) -> anyhow::Result<()> {
    // Checked again right before the first write: the downloads above
    // took time, and a symlink planted meanwhile would be written through.
    validate_destinations(root, manifest)?;
    for (path, _) in manifest_files(local) {
        if !manifest.contains_key(path) {
            let full = root.join(path);
            std::fs::remove_file(&full).with_context(|| format!("remove {}", full.display()))?;
            *touched = true;
            // And the directories it emptied, up to the project root: a
            // branch back past the creation of `nodes/my-pkg/` otherwise
            // left the folder sitting there, so the tree on disk did not
            // look like the version the person just asked for.
            let mut parent = full.parent().map(std::path::Path::to_path_buf);
            while let Some(dir) = parent {
                if dir == root || !dir.starts_with(root) {
                    break;
                }
                if std::fs::remove_dir(&dir).is_err() {
                    break; // not empty, which is the ordinary case
                }
                parent = dir.parent().map(std::path::Path::to_path_buf);
            }
        }
    }
    for (path, hash) in manifest_files(manifest) {
        if local.get(path) == Some(hash) { continue; }
        let full = root.join(path);
        if full.is_dir() {
            // Never erase untracked contents to make room for a file.
            std::fs::remove_dir(&full).with_context(|| format!("replace directory {} with a file", full.display()))?;
            *touched = true;
        }
        if let Some(parent) = full.parent().filter(|parent| !parent.is_dir()) {
            std::fs::create_dir_all(parent).with_context(|| format!("create {} for {path}", parent.display()))?;
            *touched = true;
        }
        std::fs::copy(staged.join(path), &full).with_context(|| format!("restore {}", full.display()))?;
        *touched = true;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn m(entries: &[(&str, &str)]) -> Manifest {
        entries.iter().map(|(p, h)| (p.to_string(), h.to_string())).collect()
    }

    #[test]
    fn dirty_files_names_added_removed_and_changed_but_not_the_weft_entry() {
        let head = m(&[("main.weft", "1"), ("gone.weft", "2"), ("weft:0.1:abc", "")]);
        let local = m(&[("main.weft", "X"), ("new.weft", "3"), ("weft:0.2:def", "")]);
        assert_eq!(dirty_files(&head, &local), vec!["gone.weft", "main.weft", "new.weft"]);
        assert!(dirty_files(&head, &head).is_empty());
    }

    #[test]
    fn restore_changes_files_to_directories_and_back() {
        let root = tempfile::tempdir().unwrap();
        let staged = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("node"), "old").unwrap();
        std::fs::create_dir(staged.path().join("node")).unwrap();
        std::fs::write(staged.path().join("node/main.rs"), "new").unwrap();
        let file = m(&[("node", "old")]);
        let directory = m(&[("node/main.rs", "new")]);
        let mut touched = false;
        apply_restore(root.path(), &file, &directory, staged.path(), &mut touched).unwrap();
        assert!(touched);
        assert_eq!(std::fs::read_to_string(root.path().join("node/main.rs")).unwrap(), "new");
        let staged_file = tempfile::tempdir().unwrap();
        std::fs::write(staged_file.path().join("node"), "restored").unwrap();
        let mut touched = false;
        apply_restore(root.path(), &directory, &file, staged_file.path(), &mut touched).unwrap();
        assert!(touched);
        assert_eq!(std::fs::read_to_string(root.path().join("node")).unwrap(), "restored");
    }

    /// A restore that fails before its first write says so truthfully:
    /// nothing is marked touched, and nothing on disk changed.
    #[test]
    fn a_restore_that_fails_at_once_reports_an_untouched_tree() {
        let root = tempfile::tempdir().unwrap();
        let staged = tempfile::tempdir().unwrap(); // holds no file for `main.weft`
        std::fs::write(root.path().join("main.weft"), "old").unwrap();
        let mut touched = false;
        let error = apply_restore(root.path(), &m(&[("main.weft", "old")]), &m(&[("main.weft", "new")]), staged.path(), &mut touched)
            .unwrap_err();
        assert!(error.to_string().contains("restore"), "{error:#}");
        assert!(!touched);
        assert_eq!(std::fs::read_to_string(root.path().join("main.weft")).unwrap(), "old");
    }

    #[cfg(unix)]
    #[test]
    fn restore_refuses_hidden_symlink_ancestors() {
        let root = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        std::os::unix::fs::symlink(outside.path(), root.path().join(".cache")).unwrap();
        assert!(validate_destinations(root.path(), &m(&[(".cache/payload", "hash")])).is_err());
        assert!(!outside.path().join("payload").exists());
    }
}
