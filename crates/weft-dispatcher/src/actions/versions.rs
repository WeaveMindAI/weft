//! The version tree's actions and its working copies' (see
//! `crate::versions` and `crate::working_copies` for the model): stamping
//! a working copy into a version, checkpointing, switching, the tree,
//! pruning, and writing a working copy's files.
//!
//! **Stamping** is how a working copy's files become a version: its
//! working manifest is recorded under its head (content-addressed, so
//! stamping unchanged code finds the version it already is and records
//! nothing) and its head moves there. Every action that does something
//! with code stamps first and then acts on the version: a build, a
//! checkpoint, a switch (which stamps where it leaves, so nothing is
//! lost). Every tree write holds the project's transition lock
//! ([`super::with_tree_lock`]).
//!
//! **References follow the tree.** Every blob a version or a working copy
//! names is one of the project's asset references, so the store never
//! expires it ([`publish_references`]): published after a working copy's
//! files change, after a stamp, a switch or a prune, and when a build
//! registers. What the program of a run not yet ended reads is kept too,
//! so a run waiting on a build since replaced still finds it.

use std::collections::{BTreeMap, BTreeSet};

use axum::http::StatusCode;
use weft_core::project::hash::Manifest;
use weft_core::versions::{
    manifest_diff, CheckpointRequest, PruneResponse, RunUpdate, RunningSource, SweepResponse, VersionRun, VersionSummary, VersionTree,
    VersionUpsert,
};
use weft_core::working_copy::{
    Applied, ChangeFiles, Forgotten, Head, RegisterFolder, SwitchRequest, Switched, SyncReport, WorkingCopy, WorkingCopyId, WorkingCopyKind,
    WorkingCopyRef, WorkingState,
};
use weft_core::RunId;

use super::{internal, operator_only, with_tree_lock, Actor, Refused};
use crate::state::DispatcherState;
use crate::versions::{plan_prune, version_id, Kept, VersionRow};
use crate::working_copies::{WorkingCopyRow, Write, Written};

/// This area's actions ([`super::names`]).
pub const NAMES: &[(&str, super::Reach)] = &[
    ("weft.working_copies.list", super::Reach::Everyone),
    ("weft.working_copies.read", super::Reach::Everyone),
    ("weft.working_copies.change", super::Reach::Everyone),
    ("weft.working_copies.read_file", super::Reach::Everyone),
    ("weft.working_copies.write", super::Reach::Everyone),
    ("weft.versions.checkpoint", super::Reach::Everyone),
    ("weft.versions.switch", super::Reach::Everyone),
    ("weft.working_copies.forget", super::Reach::Everyone),
    ("weft.versions.tree", super::Reach::Everyone),
    ("weft.versions.prune", super::Reach::Everyone),
    ("weft.versions.update_run", super::Reach::Everyone),
    ("weft.versions.running", super::Reach::Everyone),
    ("weft.builds.build", super::Reach::Everyone),
    ("weft.builds.state", super::Reach::Everyone),
    // A folder lives on a machine, and only the person's own CLI (or the
    // runtime watching that machine) can say what it holds.
    ("weft.working_copies.register_folder", super::Reach::Operator("a folder is on a person's machine; a program writes into a working copy")),
    ("weft.working_copies.sync", super::Reach::Operator("only the syncer of a folder knows what the folder holds")),
];

// ----- which working copy ------------------------------------------------

/// The working copy `wanted` names in `project`, or the project's default
/// ([`crate::working_copies::default_copy`]); a project with none gets its
/// program copy now, carrying on from its newest version. Never one of
/// another project, whatever id is sent.
pub(crate) async fn copy_of(state: &DispatcherState, project: uuid::Uuid, wanted: WorkingCopyRef) -> Result<WorkingCopyRow, Refused> {
    if let Some(id) = wanted.working_copy {
        return state
            .working_copies
            .get(id)
            .await
            .map_err(|e| internal("working copy", e))?
            .filter(|row| row.project_id == project)
            .ok_or((StatusCode::NOT_FOUND, format!("this project has no working copy {id}")));
    }
    let copies = state.working_copies.list(project).await.map_err(|e| internal("working copies", e))?;
    if let Some(copy) = crate::working_copies::default_copy(&copies) {
        return Ok(copy.clone());
    }
    program_copy(state, project, &copies).await
}

/// The project's program copy, made now when it has none: on the head
/// of its most recently changed working copy, else on its newest
/// version, with that version's files.
async fn program_copy(state: &DispatcherState, project: uuid::Uuid, copies: &[WorkingCopyRow]) -> Result<WorkingCopyRow, Refused> {
    if let Some(copy) = copies.iter().find(|c| c.kind == WorkingCopyKind::Program) {
        return Ok(copy.clone());
    }
    let mut head = crate::working_copies::adopted_head(copies);
    if head.head_version.is_none() {
        let versions = state.versions.versions(project).await.map_err(|e| internal("versions", e))?;
        head.head_version = versions.last().map(|v| v.id.clone());
    }
    let working = match head.head_version.as_deref() {
        Some(version) => state.versions.version(project, version).await.map_err(|e| internal("version", e))?.map(|v| v.manifest).unwrap_or_default(),
        None => Manifest::new(),
    };
    state.working_copies.program_copy(project, &working, &head, now()).await.map_err(|e| internal("make the program copy", e))
}

fn now() -> u64 {
    crate::lease::now_unix() as u64
}

// ----- stamping ----------------------------------------------------------

/// Stamp `copy`: record its files as a version under its head (or as a
/// new tree root), label it, and move its head there. Holds the tree lock
/// for its few writes; answers the version and the copy as it is now.
pub(crate) async fn stamp(
    state: &DispatcherState,
    project: uuid::Uuid,
    copy: uuid::Uuid,
    label: Option<&str>,
    root: bool,
) -> Result<(VersionUpsert, WorkingCopyRow), Refused> {
    let stamped = with_tree_lock(state, project, || async { stamp_locked(state, project, copy, label, root).await }).await?;
    publish_references(state, project, &[]).await.map_err(crate::api::after_writes)?;
    Ok(stamped)
}

/// [`stamp`] for a caller that holds the tree lock.
pub(crate) async fn stamp_locked(
    state: &DispatcherState,
    project: uuid::Uuid,
    copy: uuid::Uuid,
    label: Option<&str>,
    root: bool,
) -> Result<(VersionUpsert, WorkingCopyRow), Refused> {
    // Read inside the lock: the files and the head are the ones the
    // version is recorded from and parented on.
    let row = state
        .working_copies
        .get(copy)
        .await
        .map_err(|e| internal("working copy", e))?
        .filter(|row| row.project_id == project)
        .ok_or((StatusCode::NOT_FOUND, format!("this project has no working copy {copy}")))?;
    if row.working.is_empty() {
        return Err((
            StatusCode::CONFLICT,
            format!("{} holds no files yet, so there is nothing to record as a version", crate::versions::copy_name(&row)),
        ));
    }
    let upsert = commit_version(state, project, &row.working, label, root, row.head.head_version.as_deref()).await?;
    let to = head_after_stamp(&row.head, &upsert.version);
    if to == row.head {
        return Ok((upsert, row));
    }
    move_head_or_conflict(state, &row, &to).await?;
    announce(state, project, row.id).await;
    Ok((upsert, WorkingCopyRow { head: to, ..row }))
}

/// Where a working copy's head goes once its files are recorded as
/// `version`. Head's run stays while the code is the version head already
/// is: a stamp of unchanged code changes nothing, and the next `--seed`
/// still inherits from that run.
fn head_after_stamp(head: &Head, version: &str) -> Head {
    match head.head_version.as_deref() == Some(version) {
        true => head.clone(),
        false => Head { head_version: Some(version.to_string()), head_run: None },
    }
}

/// Tell whoever watches the project (the editor's view of the tree) that
/// one of its working copies changed.
pub(crate) async fn announce(state: &DispatcherState, project: uuid::Uuid, copy: uuid::Uuid) {
    state.events.publish(crate::events::DispatcherEvent::WorkingCopyChanged { project_id: project, working_copy: copy }).await;
}

/// Write the version row itself, parented on `parent` (or on nothing
/// under `root`), unless the project already has it. A label is set on a
/// new version and replaces an existing one's.
pub(crate) async fn commit_version(
    state: &DispatcherState,
    project: uuid::Uuid,
    manifest: &Manifest,
    label: Option<&str>,
    root: bool,
    parent: Option<&str>,
) -> Result<VersionUpsert, Refused> {
    validate_manifest(manifest)?;
    let id = version_id(manifest);
    let parent = if root { None } else { parent.filter(|p| *p != id).map(str::to_string) };
    let row = VersionRow {
        id: id.clone(),
        project_id: project,
        parent_id: parent.clone(),
        manifest: manifest.clone(),
        label: label.map(str::to_string),
        created_at: now(),
    };
    let created = state.versions.upsert_version(&row).await.map_err(|e| internal("record version", e))?;
    let recorded = if created { None } else { state.versions.version(project, &id).await.map_err(|e| internal("version", e))? };
    if !created && root {
        if let Some(parent) = recorded.as_ref().and_then(|v| v.parent_id.clone()) {
            return Err(root_conflict(&id, &parent));
        }
    }
    if !created {
        if let Some(label) = label {
            state.versions.set_label(project, &id, Some(label)).await.map_err(|e| internal("label", e))?;
        }
    }
    let parent = if created { parent } else { recorded.and_then(|v| v.parent_id) };
    Ok(VersionUpsert { version: id, created, parent })
}

/// `--root` on code the project already holds.
///
/// A version IS its code, so this exact code is already a version, and it
/// already has a parent: content addressed, it cannot have two. Answering
/// the existing version instead would hand the caller the old tree when
/// they asked for a new one.
pub(crate) fn root_conflict(id: &str, parent: &str) -> Refused {
    (
        StatusCode::CONFLICT,
        format!(
            "a version IS its code, and this exact code is already version {id}, recorded under \
             {parent}. It cannot also be a root, so there is nothing `--root` can do here: change \
             something and run again, or drop `--root` to record this run under the version it \
             already has"
        ),
    )
}

/// Record the registered program's files as a version, while the caller
/// holds the project's transition lock: the version a run the dispatcher
/// starts itself (a setup run, a paused run moved onto a new build) is
/// recorded under. A build stamped that version before registering it, so
/// it is normally there already; a build an older weft registered may
/// have none, and then it becomes a tree root.
pub(crate) async fn record_program_source_locked(
    state: &DispatcherState,
    project: uuid::Uuid,
    program: &weft_core::project::hash::ProgramIdentity,
) -> Result<String, Refused> {
    let manifest = state.projects.program_source(project, program).await.map_err(|e| (StatusCode::CONFLICT, e.to_string()))?;
    let id = version_id(&manifest);
    if state.versions.version(project, &id).await.map_err(|e| internal("version", e))?.is_some() {
        return Ok(id);
    }
    Ok(commit_version(state, project, &manifest, None, true, None).await?.version)
}

/// [`record_program_source_locked`], taking the lock.
pub(crate) async fn record_program_source(
    state: &DispatcherState,
    project: uuid::Uuid,
    program: &weft_core::project::hash::ProgramIdentity,
) -> Result<String, Refused> {
    with_tree_lock(state, project, || async { record_program_source_locked(state, project, program).await }).await
}

/// Refuse a manifest that could not be stored or addressed.
///
/// A manifest comes from a client and its values become storage keys
/// ([`blob_keys`]), so an entry carrying a `/` or a `..` would produce a
/// key the storage surface refuses, and because the asset reference
/// publish spans EVERY version of the project, one such row would break
/// that project's publish from then on, durably. The gate belongs where a
/// manifest enters the install: a version, and a working copy's files.
pub(crate) fn validate_manifest(manifest: &Manifest) -> Result<(), Refused> {
    for (path, hash) in manifest {
        validate_entry(path, Some(hash))?;
    }
    Ok(())
}

/// One manifest entry ([`validate_manifest`]): a project-relative path,
/// and a sha256 when it names content.
fn validate_entry(path: &str, hash: Option<&String>) -> Result<(), Refused> {
    if path.is_empty()
        || path.starts_with('/')
        || path.contains('\\')
        || path.split('/').any(|seg| seg.is_empty() || seg == "." || seg == "..")
    {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("'{}' is not a project-relative path", weft_core::truncate_user_string(path, 256)),
        ));
    }
    if let Some(hash) = hash {
        if hash.len() != 64 || !hash.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err((
                StatusCode::BAD_REQUEST,
                format!(
                    "'{}' names '{}', which is not a sha256",
                    weft_core::truncate_user_string(path, 256),
                    weft_core::truncate_user_string(hash, 128)
                ),
            ));
        }
    }
    Ok(())
}

/// Move `copy`'s head from where it was read to `to`, or refuse: it moved
/// in between, and the decision (which version to parent on, which run to
/// seed from) was made against the old one.
async fn move_head_or_conflict(state: &DispatcherState, copy: &WorkingCopyRow, to: &Head) -> Result<(), Refused> {
    let moved = state.working_copies.move_head(copy.id, &copy.head, to, now()).await.map_err(|e| internal("set head", e))?;
    if !moved {
        // Every tree write holds the project's lock, and this
        // compare-and-set runs inside it. Head changing under a locked
        // call means the row itself moved on: a `weft clean` clearing the
        // run head named (it takes no tree lock), or the project being
        // removed. A retry helps in the first case and never in the
        // second, so both are named.
        return Err((
            StatusCode::CONFLICT,
            "head changed under this call: either the run head pointed at was cleaned, or the \
             project was removed. Try again, and if it refuses again check `weft ps` for the \
             project"
                .into(),
        ));
    }
    Ok(())
}

// ----- checkpoint, switch ------------------------------------------------

/// Record a working copy's files as a version (no run, no build) and move
/// its head there. Identical code answers the existing version.
pub async fn checkpoint(state: &DispatcherState, actor: &Actor, project: uuid::Uuid, request: &CheckpointRequest) -> Result<VersionUpsert, Refused> {
    actor.may_act_on(state, project).await?;
    let copy = copy_of(state, project, request.working_copy).await?;
    Ok(stamp(state, project, copy.id, request.label.as_deref(), request.root).await?.0)
}

/// Move a working copy to a version (or to a run: its version, with the
/// run as head's run). Its files are stamped first, so the work it holds
/// is a version too and switching back to it brings it back; then its
/// files become the version's, which a folder's syncer writes to disk.
pub async fn switch(state: &DispatcherState, actor: &Actor, project: uuid::Uuid, request: &SwitchRequest) -> Result<Switched, Refused> {
    actor.may_act_on(state, project).await?;
    let to = &request.to;
    let copy = copy_of(state, project, request.working_copy).await?;
    let switched = with_tree_lock(state, project, || async {
        // Resolve the destination under the same lock as prune and the
        // move, so a valid destination cannot disappear between its read
        // and its use.
        let (version_id, run) = match (to.run, to.version.as_deref()) {
            (Some(run_id), _) => {
                let run = state
                    .versions
                    .run(run_id)
                    .await
                    .map_err(|e| internal("run", e))?
                    .filter(|r| r.project_id == project)
                    .ok_or((StatusCode::NOT_FOUND, format!("run {run_id} is not recorded in this project's tree")))?;
                (run.version_id, Some(run_id))
            }
            (None, Some(version)) => (version.to_string(), None),
            (None, None) => return Err((StatusCode::BAD_REQUEST, "name a version or a run".into())),
        };
        let target = state
            .versions
            .version(project, &version_id)
            .await
            .map_err(|e| internal("version", e))?
            .ok_or((StatusCode::NOT_FOUND, format!("no version {version_id} in this project")))?;
        // Where it leaves is a version before anything moves. A copy with
        // no files yet (a folder that was empty) has nothing to keep.
        let (saved, row) = if copy.working.is_empty() {
            let row = state.working_copies.get(copy.id).await.map_err(|e| internal("working copy", e))?.ok_or((StatusCode::NOT_FOUND, "the working copy is gone".to_string()))?;
            (row.head.head_version.clone(), row)
        } else {
            let (saved, row) = stamp_locked(state, project, copy.id, None, false).await?;
            (Some(saved.version), row)
        };
        let head = Head { head_version: Some(target.id.clone()), head_run: run };
        let moved = state
            .working_copies
            .replace(row.id, row.revision, &target.manifest, &head, now())
            .await
            .map_err(|e| internal("switch the working copy", e))?
            .ok_or((
                StatusCode::CONFLICT,
                "the working copy's files changed while it was being switched (a save, or a program writing); \
                 nothing moved, and the files it held are kept as a version: switch again"
                    .to_string(),
            ))?;
        Ok(Switched { saved, copy: moved.summary() })
    })
    .await?;
    announce(state, project, switched.copy.id).await;
    publish_references(state, project, &[]).await.map_err(crate::api::after_writes)?;
    Ok(switched)
}

/// Record a working copy's files as a version and remove the copy: a
/// folder that is gone (whose head would otherwise keep its versions from
/// every prune), or a program copy nothing writes into. The files are kept
/// the way a switch keeps where it leaves ([`Forgotten::saved`]). A folder
/// that is still there registers again the next time a command runs in
/// it; a program copy comes back the next time a call names no working
/// copy of a project with none.
pub async fn forget(state: &DispatcherState, actor: &Actor, project: uuid::Uuid, copy: WorkingCopyId) -> Result<Forgotten, Refused> {
    actor.may_act_on(state, project).await?;
    let row = copy_of(state, project, WorkingCopyRef { working_copy: Some(copy) }).await?;
    let forgotten = with_tree_lock(state, project, || async {
        let (saved, row) = if row.working.is_empty() {
            let row = state.working_copies.get(row.id).await.map_err(|e| internal("working copy", e))?.ok_or((StatusCode::NOT_FOUND, "the working copy is gone".to_string()))?;
            (None, row)
        } else {
            let (saved, row) = stamp_locked(state, project, row.id, None, false).await?;
            (Some(saved.version), row)
        };
        if !state.working_copies.remove(row.id, row.revision).await.map_err(|e| internal("forget the working copy", e))? {
            return Err((
                StatusCode::CONFLICT,
                "the working copy's files changed while it was being forgotten (a save, or a program writing); \
                 it is still there, and the files it held are kept as a version: forget it again"
                    .to_string(),
            ));
        }
        Ok(Forgotten { saved })
    })
    .await?;
    announce(state, project, copy).await;
    publish_references(state, project, &[]).await.map_err(crate::api::after_writes)?;
    Ok(forgotten)
}

// ----- working copies ----------------------------------------------------

/// Every working copy of the project.
pub async fn working_copies(state: &DispatcherState, actor: &Actor, project: uuid::Uuid) -> Result<Vec<WorkingCopy>, Refused> {
    actor.may_act_on(state, project).await?;
    Ok(state.working_copies.list(project).await.map_err(|e| internal("working copies", e))?.iter().map(WorkingCopyRow::summary).collect())
}

/// A working copy with its files, as a syncer reads it before deciding
/// anything (and a program reads what code a project holds).
pub async fn working_state(state: &DispatcherState, actor: &Actor, project: uuid::Uuid, wanted: WorkingCopyRef) -> Result<WorkingState, Refused> {
    actor.may_act_on(state, project).await?;
    let row = copy_of(state, project, wanted).await?;
    Ok(WorkingState { copy: row.summary(), working: row.working, synced: row.synced })
}

/// The folder working copy at `path` on `host`, if the project has one.
pub async fn find_folder(state: &DispatcherState, actor: &Actor, project: uuid::Uuid, host: &str, path: &str) -> Result<Option<WorkingState>, Refused> {
    operator_only(actor, "weft.working_copies.register_folder")?;
    actor.may_act_on(state, project).await?;
    let row = state.working_copies.folder(project, host, path).await.map_err(|e| internal("working copy", e))?;
    Ok(row.map(|row| WorkingState { copy: row.summary(), working: row.working, synced: row.synced }))
}

/// Register a folder as a working copy of the project: the first time,
/// with the folder's own files (every blob already stored) and the head
/// of wherever somebody last worked on the project; after that, the copy
/// it already is. A local install runs on the person's machine and keeps
/// the folder in sync from there, so it refuses a folder it could never
/// hear changes in ([`weft_core::working_copy::hears_no_changes`]).
pub async fn register_folder(state: &DispatcherState, actor: &Actor, project: uuid::Uuid, request: &RegisterFolder) -> Result<WorkingState, Refused> {
    operator_only(actor, "weft.working_copies.register_folder")?;
    actor.may_act_on(state, project).await?;
    if request.host.is_empty() || !request.path.starts_with('/') {
        return Err((StatusCode::BAD_REQUEST, format!("'{}' on '{}' is not a folder's absolute path on a machine", request.path, request.host)));
    }
    let local = state.install_info.cloud.is_none();
    if weft_core::working_copy::hears_no_changes(&request.path, local && weft_core::working_copy::on_wsl()) {
        return Err((
            StatusCode::BAD_REQUEST,
            format!(
                "{} is on a Windows drive, and WSL tells Linux nothing of the changes made there, \
                 so weft cannot keep this folder in sync with its working copy. Move the project into \
                 the Linux filesystem (under ~/, for example) and run the command again from there",
                request.path
            ),
        ));
    }
    validate_manifest(&request.manifest)?;
    if let Some(row) = state.working_copies.folder(project, &request.host, &request.path).await.map_err(|e| internal("working copy", e))? {
        return Ok(WorkingState { copy: row.summary(), working: row.working, synced: row.synced });
    }
    // Every blob it names must be stored, and stays: referenced before
    // the row names it, so a missing upload is refused now rather than
    // failing the first build.
    publish_references(state, project, &keys_of(&request.manifest)?).await?;
    let copies = state.working_copies.list(project).await.map_err(|e| internal("working copies", e))?;
    let head = crate::working_copies::adopted_head(&copies);
    let row = state
        .working_copies
        .register_folder(project, &request.host, &request.path, &request.manifest, &head, now())
        .await
        .map_err(|e| internal("register the folder", e))?;
    Ok(WorkingState { copy: row.summary(), working: row.working, synced: row.synced })
}

/// What a folder's syncer found in one pass ([`weft_core::working_copy::plan_sync`]):
/// the folder's changes to the working manifest, what the folder holds
/// now, and its conflict copies. All or nothing; a change that does not
/// find what it expected is answered `Stale`, and the syncer decides
/// again from a fresh read.
pub async fn sync(state: &DispatcherState, actor: &Actor, project: uuid::Uuid, copy: uuid::Uuid, report: &SyncReport) -> Result<Applied, Refused> {
    operator_only(actor, "weft.working_copies.sync")?;
    actor.may_act_on(state, project).await?;
    let row = copy_of(state, project, WorkingCopyRef { working_copy: Some(copy) }).await?;
    if row.kind != WorkingCopyKind::Folder {
        return Err((StatusCode::BAD_REQUEST, "only a folder's working copy is synced with a folder".into()));
    }
    for path in report.synced.keys().chain(report.conflicts.iter()) {
        validate_entry(path, None)?;
    }
    write(state, project, &row, Write { changes: &report.changes, synced: Some(&report.synced), conflicts: Some(&report.conflicts) }).await
}

/// Change files of a working copy: a program writing code, or a client
/// pushing a folder it syncs itself. Every blob a change names must be
/// stored already.
pub async fn change_files(state: &DispatcherState, actor: &Actor, project: uuid::Uuid, request: &ChangeFiles) -> Result<Applied, Refused> {
    actor.may_act_on(state, project).await?;
    let row = copy_of(state, project, request.working_copy).await?;
    write(state, project, &row, Write { changes: &request.changes, ..Default::default() }).await
}

/// One file of a working copy as a stored file (its blob, under the path's
/// name), or none when the copy holds no file there.
pub async fn read_file(
    state: &DispatcherState,
    actor: &Actor,
    project: uuid::Uuid,
    wanted: WorkingCopyRef,
    path: &str,
) -> Result<Option<weft_core::storage::StoredFile>, Refused> {
    actor.may_act_on(state, project).await?;
    let row = copy_of(state, project, wanted).await?;
    let Some(hash) = row.working.get(path) else { return Ok(None) };
    let tenant = actor.tenant();
    let meta = crate::storage::file_meta(state, &format!("{}/{}", tenant.as_str(), asset_key(hash)?))
        .await
        .map_err(|e| crate::api::storage::storage_err(e).into_refused())?;
    let mut file = weft_core::storage::StoredFile::from(&meta);
    file.filename = path.to_string();
    Ok(Some(file))
}

/// Write files into a working copy from their bytes (a program writing
/// code): each one's bytes stored in the tenant's assets first, then every
/// change landed together, or none when a path does not stand as its write
/// expected. A stored file that is one of the tenant's assets already is
/// named by its hash as it is: nothing is read or stored again, and
/// [`write`] refuses one the store does not hold.
pub async fn write_files(
    state: &DispatcherState,
    actor: &Actor,
    project: uuid::Uuid,
    request: &weft_core::working_copy::WriteFiles,
) -> Result<Applied, Refused> {
    use weft_core::working_copy::{FileContent, WorkingChange, MAX_WRITTEN_TEXT_BYTES};
    actor.may_act_on(state, project).await?;
    let tenant = actor.tenant().clone();
    let staged = tempfile::tempdir().map_err(|e| internal("a folder for the written files", e))?;
    let mut sources: BTreeMap<String, std::path::PathBuf> = BTreeMap::new();
    let mut held: BTreeMap<&str, String> = BTreeMap::new();
    for (i, file) in request.files.iter().enumerate() {
        validate_entry(&file.path, None)?;
        let at = staged.path().join(i.to_string());
        match &file.content {
            FileContent::Text { text } => {
                if text.len() > MAX_WRITTEN_TEXT_BYTES {
                    return Err((
                        StatusCode::PAYLOAD_TOO_LARGE,
                        format!(
                            "'{}' is {} bytes of text, more than one write carries ({MAX_WRITTEN_TEXT_BYTES}): store it with \
                             `ctx.storage(..)` first and write it by its key",
                            file.path,
                            text.len()
                        ),
                    ));
                }
                std::fs::write(&at, text).map_err(|e| internal("stage a written file", e))?;
            }
            FileContent::Stored { key } => {
                // Only a file of the program's own owner, whatever key it
                // names.
                let anchored = format!("{}/", tenant.as_str());
                let key = if key.starts_with(&anchored) { key.clone() } else { format!("{anchored}{key}") };
                if let Ok(weft_core::storage::key::ParsedKey { scope: weft_core::storage::key::KeyScope::Asset, id, .. }) =
                    weft_core::storage::key::parse_key(&key)
                {
                    held.insert(file.path.as_str(), id);
                    continue;
                }
                crate::storage::download_file(state, &key, &at)
                    .await
                    .map_err(|e| crate::api::storage::storage_err(e).into_refused())?;
            }
            FileContent::Removed => continue,
        }
        sources.insert(file.path.clone(), at);
    }
    let paths: Vec<String> = sources.keys().cloned().collect();
    let source = Staged(sources);
    let hashed = weft_assets::hash_files(&paths, &source).map_err(|e| internal("hash the written files", e))?;
    let store = crate::storage::asset_store(state, tenant.as_str());
    weft_assets::publish_hashed(&hashed, &source, &store).await.map_err(|e| internal("store the written files", e))?;
    let hash_of: BTreeMap<&str, &str> =
        hashed.iter().map(|h| (h.path.as_str(), h.hash.as_str())).chain(held.iter().map(|(path, hash)| (*path, hash.as_str()))).collect();
    let changes: Vec<WorkingChange> = request
        .files
        .iter()
        .map(|file| WorkingChange {
            path: file.path.clone(),
            hash: hash_of.get(file.path.as_str()).map(|h| h.to_string()),
            expected: file.expected.clone(),
        })
        .collect();
    change_files(state, actor, project, &ChangeFiles { working_copy: request.working_copy, changes }).await
}

/// Written files staged on the install's disk, by the path they are
/// written to.
struct Staged(BTreeMap<String, std::path::PathBuf>);

impl weft_assets::AssetSource for Staged {
    fn open(&self, path: &str) -> anyhow::Result<Box<dyn std::io::Read + Send>> {
        let at = self.0.get(path).ok_or_else(|| anyhow::anyhow!("'{path}' is no file this write stages"))?;
        Ok(Box::new(std::fs::File::open(at)?))
    }
}

async fn write(state: &DispatcherState, project: uuid::Uuid, row: &WorkingCopyRow, write: Write<'_>) -> Result<Applied, Refused> {
    for change in write.changes {
        validate_entry(&change.path, change.hash.as_ref())?;
        if !weft_core::project::coverage::covers(&change.path) {
            return Err((
                StatusCode::BAD_REQUEST,
                format!("'{}' is not a file a version holds, so a working copy does not hold it either", change.path),
            ));
        }
    }
    let added: Vec<String> = write.changes.iter().filter_map(|c| c.hash.clone()).collect::<BTreeSet<_>>().into_iter().collect();
    if !added.is_empty() {
        // Referenced before the row names them: a blob that is not
        // stored refuses the write, and one that is never expires while a
        // working copy holds it.
        let keys = added.iter().map(|hash| asset_key(hash)).collect::<Result<Vec<_>, _>>()?;
        publish_references(state, project, &keys).await?;
    }
    match state.working_copies.write(row.id, write, now()).await.map_err(|e| internal("write the working copy", e))? {
        Written::Landed(row) => {
            announce(state, project, row.id).await;
            Ok(Applied::Applied { copy: row.summary() })
        }
        Written::Stale(paths) => Ok(Applied::Stale { paths }),
    }
}

// ----- the tree ----------------------------------------------------------

pub async fn tree(state: &DispatcherState, actor: &Actor, project: uuid::Uuid) -> Result<VersionTree, Refused> {
    actor.may_act_on(state, project).await?;
    let copies = state.working_copies.list(project).await.map_err(|e| internal("working copies", e))?;
    let activated_versions = state.versions.activated_versions(project).await.map_err(|e| internal("activated versions", e))?;
    let versions = state.versions.versions(project).await.map_err(|e| internal("versions", e))?;
    let runs = state.versions.runs(project).await.map_err(|e| internal("runs", e))?;
    let by_id: BTreeMap<&str, &VersionRow> = versions.iter().map(|v| (v.id.as_str(), v)).collect();
    let trigger_runs = state.versions.trigger_runs(project).await.map_err(|e| internal("trigger runs", e))?;
    let version_summaries = versions
        .iter()
        .map(|v| VersionSummary {
            id: v.id.clone(),
            parent_id: v.parent_id.clone(),
            label: v.label.clone(),
            created_at: v.created_at,
            diff: v.parent_id.as_deref().and_then(|p| by_id.get(p)).map(|p| manifest_diff(&p.manifest, &v.manifest)).unwrap_or_default(),
            manifest: v.manifest.clone(),
            trigger_runs: trigger_runs.get(&v.id).map_or(0, |fired| fired.runs),
            last_trigger_run: trigger_runs.get(&v.id).map(|fired| fired.last),
        })
        .collect();
    // ONE read for every run's status. Asked per run, a project with a
    // thousand runs started by hand did a thousand point lookups on every
    // `weft tree` and every refresh of the editor's version sidebar.
    let run_ids: Vec<RunId> = runs.iter().map(|r| r.run_id).collect();
    let mut summaries = state.runs.run_summaries(&run_ids).await.map_err(|e| internal("run summaries", e))?;
    let runs = runs
        .into_iter()
        .map(|r| VersionRun {
            listing: summaries.remove(&r.run_id),
            run_id: r.run_id,
            version_id: r.version_id,
            definition_hash: r.definition_hash,
            seed_run_id: r.seed_run_id,
            stale: r.stale,
            spec: r.spec,
            example: r.example,
            started_at: r.created_at,
        })
        .collect();
    Ok(VersionTree { working_copies: copies.iter().map(WorkingCopyRow::summary).collect(), activated_versions, versions: version_summaries, runs })
}

/// The files of the program the install last built (what its triggers and
/// runs use), as a manifest the caller downloads through the storage plane
/// like any version's: what `weft running-source` and the editor's install
/// switch read, so a person looking at another install sees the program it
/// holds rather than the files on their disk.
pub async fn running(state: &DispatcherState, actor: &Actor, project: uuid::Uuid) -> Result<RunningSource, Refused> {
    actor.may_act_on(state, project).await?;
    let identity = state
        .projects
        .running_program_identity(project)
        .await
        .map_err(|e| internal("running program", e))?
        .ok_or((StatusCode::NOT_FOUND, "this install holds no build of the project yet; `weft activate` puts one there".to_string()))?;
    let manifest = state.projects.program_source(project, &identity).await.map_err(|e| (StatusCode::CONFLICT, e.to_string()))?;
    Ok(RunningSource { version: version_id(&manifest), manifest })
}

/// Associate a recorded run with a saved example.
pub async fn update_run(state: &DispatcherState, actor: &Actor, project: uuid::Uuid, run_id: RunId, update: &RunUpdate) -> Result<(), Refused> {
    actor.may_act_on(state, project).await?;
    // Read only to hold the run to this project. What gets written is
    // exactly what the caller named: the store leaves an unmentioned
    // column alone, so two sessions writing different fields of one run
    // cannot put back each other's stale value.
    state
        .versions
        .run(run_id)
        .await
        .map_err(|e| internal("run", e))?
        .filter(|r| r.project_id == project)
        .ok_or((StatusCode::NOT_FOUND, format!("run {run_id} is not recorded in this project's tree")))?;
    if let Some(example) = &update.example {
        state.versions.set_run_example(run_id, example.as_deref()).await.map_err(|e| internal("update run", e))?;
    }
    Ok(())
}

// ----- prune and sweep ---------------------------------------------------

/// What keeps a project's versions now ([`Kept`]): its working copies, the
/// versions its triggers listen on, and the version of the build it runs.
struct KeptRows {
    copies: Vec<WorkingCopyRow>,
    activated: Vec<String>,
    running: Option<String>,
}

impl KeptRows {
    async fn read(state: &DispatcherState, project: uuid::Uuid) -> Result<Self, Refused> {
        let copies = state.working_copies.list(project).await.map_err(|e| internal("working copies", e))?;
        let activated = state.versions.activated_versions(project).await.map_err(|e| internal("activated versions", e))?;
        let running = state.versions.registered_source(project).await.map_err(|e| internal("registered source", e))?.map(|source| version_id(&source));
        Ok(Self { copies, activated, running })
    }

    fn kept(&self) -> Kept<'_> {
        Kept { copies: &self.copies, activated: &self.activated, running: self.running.as_deref() }
    }
}

/// Delete a version subtree and every run under it, or (with `plan`) say
/// what that would remove. The blobs only the pruned versions held drop
/// out of the project's references and expire under the storage rule.
pub async fn prune(
    state: &DispatcherState,
    actor: &Actor,
    project: uuid::Uuid,
    version: &str,
    frozen: &[String],
    plan_only: bool,
) -> Result<PruneResponse, Refused> {
    actor.may_act_on(state, project).await?;
    // Plan and delete under the project's tree lock: without it a run
    // started between the two lands under a version this call is about to
    // delete, and the plan's "nothing here is still running" was true only
    // of a snapshot.
    let (plan, deleted) = with_tree_lock(state, project, || async {
        let kept = KeptRows::read(state, project).await?;
        let versions = state.versions.versions(project).await.map_err(|e| internal("versions", e))?;
        let runs = state.versions.runs(project).await.map_err(|e| internal("runs", e))?;
        let in_flight = in_flight_run_ids(state, project).await?;
        let mut plan = plan_prune(version, &versions, &runs, kept.kept(), frozen, |c| in_flight.contains(&c))
            .map_err(|r| (StatusCode::CONFLICT, r.reasons.join("\n")))?;
        let protected = state.versions.versions_in_use(project, false).await.map_err(|e| internal("source references", e))?;
        if plan.versions.iter().any(|id| protected.contains(id)) {
            return Err((StatusCode::CONFLICT, crate::versions::VERSIONS_IN_USE_REFUSAL.into()));
        }
        if let Some(source) = state.versions.registered_source(project).await.map_err(|e| internal("registered source", e))? {
            let retained: BTreeSet<_> = source.values().collect();
            plan.blobs.retain(|hash| !retained.contains(hash));
        }
        if plan_only {
            return Ok((plan, false));
        }
        // The TREE first, then the journals: a failure here leaves
        // journals nobody's tree names, which `weft clean` still reaches
        // by run, rather than tree rows naming journals that are gone
        // (which read as runs in flight for ever).
        state.versions.delete_versions(project, &plan.versions).await.map_err(|e| internal("delete versions", e))?;
        Ok((plan, true))
    })
    .await?;
    if !deleted {
        return Ok(PruneResponse { plan, deleted });
    }
    publish_references(state, project, &[]).await.map_err(crate::api::after_writes)?;
    // The journals go OUTSIDE the lock: their version rows are already
    // gone, so nothing can decide anything about these runs any more, and
    // a storage wipe per run would hold a pooled connection while every
    // run start of the project queued behind it.
    let mut failed: Vec<String> = Vec::new();
    for run_id in &plan.runs {
        match crate::api::run::clean_run(state, actor.tenant(), *run_id).await {
            Ok(_) => {}
            // A journal that is already gone is already clean.
            Err(e) if e.status() == StatusCode::NOT_FOUND => {}
            Err(e) => failed.push(format!("{run_id} ({}: {})", e.status(), e.message())),
        }
    }
    if !failed.is_empty() {
        return Err(internal(
            "prune",
            format!(
                "the versions were pruned, but these runs' journals could not be cleaned: {}. \
                 They are no longer in the tree; `weft clean <run_id>` removes each one.",
                failed.join(", ")
            ),
        ));
    }
    Ok(PruneResponse { plan, deleted })
}

/// Drop the versions a bulk clean left bare.
pub async fn sweep(state: &DispatcherState, actor: &Actor, project: uuid::Uuid) -> Result<SweepResponse, Refused> {
    actor.may_act_on(state, project).await?;
    Ok(SweepResponse { swept: sweep_bare_versions(state, project).await? })
}

/// Drop every bare version of one project (no runs, no descendants, no
/// label, no working copy's head, not activated), and answer which.
///
/// Called by the sweep and by the removal of a single run, which is the
/// act that makes a version bare. It lives here, on the server, rather
/// than in whoever asked: the CLI, the editor and a prune's own per-run
/// cleanup all delete runs, and a sweep each client has to remember is a
/// sweep the next client forgets.
pub(crate) async fn sweep_bare_versions(state: &DispatcherState, project: uuid::Uuid) -> Result<Vec<String>, Refused> {
    with_tree_lock(state, project, || async {
        let mut swept: Vec<String> = Vec::new();
        // A sweep can free a parent: loop until nothing is bare, reading
        // what keeps versions again each pass.
        loop {
            let kept = KeptRows::read(state, project).await?;
            let versions = state.versions.versions(project).await.map_err(|e| internal("versions", e))?;
            let runs = state.versions.runs(project).await.map_err(|e| internal("runs", e))?;
            let protected = state.versions.versions_in_use(project, true).await.map_err(|e| internal("source references", e))?;
            let bare: Vec<_> = crate::versions::sweepable_versions(&versions, &runs, kept.kept())
                .into_iter()
                .filter(|id| !protected.contains(id))
                .collect();
            if bare.is_empty() {
                break;
            }
            state.versions.delete_versions(project, &bare).await.map_err(|e| internal("delete versions", e))?;
            swept.extend(bare);
        }
        Ok(swept)
    })
    .await
}

/// The runs of a project that are still going: queued or being driven.
async fn in_flight_run_ids(state: &DispatcherState, project: uuid::Uuid) -> Result<std::collections::HashSet<RunId>, Refused> {
    Ok(state
        .runs
        .going_run_ids_for_project(project)
        .await
        .map_err(|e| internal("runs going", e))?
        .into_iter()
        .map(|(run_id, _)| run_id)
        .collect())
}

// ----- references --------------------------------------------------------

/// The asset key of the blob `hash`.
fn asset_key(hash: &str) -> Result<String, Refused> {
    weft_core::storage::key::scope_key(&weft_core::storage::key::KeyScope::Asset, hash)
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("'{hash}' is not a storable address ({e})")))
}

fn keys_of(manifest: &Manifest) -> Result<Vec<String>, Refused> {
    manifest.values().collect::<BTreeSet<_>>().into_iter().map(|hash| asset_key(hash)).collect()
}

/// The distinct blob keys the given manifests name: every entry is a
/// file, so every entry names one.
pub fn blob_keys<'a>(manifests: impl Iterator<Item = (&'a str, &'a BTreeMap<String, String>)>) -> anyhow::Result<Vec<String>> {
    let mut keys: BTreeSet<String> = BTreeSet::new();
    let scope = weft_core::storage::key::KeyScope::Asset;
    for (version, manifest) in manifests {
        for (path, hash) in manifest {
            keys.insert(weft_core::storage::key::scope_key(&scope, hash).map_err(|e| {
                // Every stored manifest passed `validate_manifest`, so this
                // cannot happen for a row written since that gate existed.
                // A row from before it can still be here, and dropping it
                // silently would quietly stop keeping that blob alive until
                // it expired: data loss with no signal. So it fails, naming
                // the one version the person has to deal with and the verb
                // that deals with it.
                anyhow::anyhow!(
                    "version {version} lists '{path}' as '{hash}', which is not a storable \
                     address ({e}), so this project's asset references cannot be published. \
                     `weft tree` shows where that version sits; getting rid of it is `weft \
                     prune {version}`, which first needs every working copy's head (and every \
                     activated version) off it, every run under it settled, and any frozen \
                     example that came from it re-frozen."
                )
            })?);
        }
    }
    Ok(keys.into_iter().collect())
}

/// The keys the project's registered build reads, from the resolutions
/// it was built with (`BuildInputs::assets`): every stored file a
/// resolution names, the content a text resolution names
/// (`weft_core::content`), which its workers read, and every `@asset`
/// that names a stored key itself (its source file is what a later build
/// reads again).
fn program_asset_keys(inputs: &weft_core::builds::BuildInputs) -> Vec<String> {
    let mut keys = BTreeSet::new();
    for (key, value) in &inputs.assets {
        let path = key.path.as_str();
        if path.starts_with("asset/") && weft_core::storage::key::is_scope_key(path) {
            keys.insert(path.to_string());
        }
        asset_keys_within(value, &mut keys);
    }
    keys.into_iter().collect()
}

/// The keys of the tenant's assets `program`'s workers read: the content
/// each value it carries by name stands for (`weft_core::content`), a
/// version's file or a text a build stored alike, and every stored file
/// of the tenant's assets a value names.
pub fn asset_keys_read_by(program: &weft_core::ProjectDefinition) -> BTreeSet<String> {
    let mut keys = BTreeSet::new();
    for value in program.written_values() {
        asset_keys_within(value, &mut keys);
    }
    keys
}

/// Add to `keys` the content `value` names and the stored files of the
/// tenant's assets it names, at any depth, tenant-less.
fn asset_keys_within(value: &serde_json::Value, keys: &mut BTreeSet<String>) {
    for content in weft_core::content::refs_in_value(value) {
        keys.insert(content.asset_key());
    }
    for file in weft_core::storage::stored_files_within(value) {
        if weft_core::storage::key::parse_key(&file.key).is_ok_and(|k| k.scope == weft_core::storage::key::KeyScope::Asset) {
            // Stored with its tenant; the publish anchors every key on
            // the tenant again.
            keys.insert(file.key.split_once('/').map(|(_, rest)| rest.to_string()).unwrap_or(file.key));
        }
    }
}

/// The keys the programs of the project's unfinished runs read
/// ([`asset_keys_read_by`]): a run waiting on a build the project has
/// since replaced still needs what that build stored. Read as the
/// references are published, never as a run starts, pauses or ends: a
/// run that ended lets go of them at the project's next publish.
async fn unfinished_runs_asset_keys(state: &DispatcherState, project: uuid::Uuid) -> Result<BTreeSet<String>, Refused> {
    let hashes = state.runs.unfinished_programs(project).await.map_err(|e| internal("unfinished runs' programs", e))?;
    let mut keys = BTreeSet::new();
    for hash in hashes {
        let program = state
            .program(project, &hash)
            .await
            .map_err(|e| internal("an unfinished run's program", e))?
            .ok_or_else(|| internal("an unfinished run's program", anyhow::anyhow!("a run of project {project} runs program {hash}, which was never recorded")))?;
        keys.extend(asset_keys_read_by(&program));
    }
    Ok(keys)
}

/// Publish the project's asset references: every blob any of its
/// versions or working copies names, every file its registered build
/// reads, and every file the programs of its unfinished runs read, are
/// kept; `required` must be stored, or nothing changes and the
/// call is refused (a write naming a blob that never landed). What no
/// project of the tenant references any more starts expiring.
///
/// A version's blob is KEPT, not required: one that expired or was
/// removed is that version's loss (it cannot be switched back to), never
/// a reason the next write cannot land; the warning names the versions.
pub(crate) async fn publish_references(state: &DispatcherState, project: uuid::Uuid, required: &[String]) -> Result<Vec<String>, Refused> {
    let tenant = state
        .projects
        .tenant_for(project)
        .await
        .map_err(|e| internal("project owner", e))?
        .ok_or((StatusCode::NOT_FOUND, crate::authenticator::NO_SUCH_PROJECT.to_string()))?;
    let blob_err = |e: anyhow::Error| internal("version blobs", e);
    let versions = state.versions.versions(project).await.map_err(blob_err)?;
    let copies = state.working_copies.list(project).await.map_err(|e| internal("working copies", e))?;
    let source = state.versions.registered_source(project).await.map_err(blob_err)?;
    // Which versions name each blob, so a missing one is reported as the
    // version's loss, by the id `weft tree` shows.
    let mut named_by: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for version in &versions {
        for key in blob_keys(std::iter::once((version.id.as_str(), &version.manifest))).map_err(blob_err)? {
            named_by.entry(key).or_default().push(version.id[..8].to_string());
        }
    }
    let mut kept: BTreeSet<String> = named_by.keys().cloned().collect();
    for copy in &copies {
        kept.extend(blob_keys(std::iter::once((copy.id.to_string().as_str(), &copy.working))).map_err(blob_err)?);
    }
    if let Some(source) = &source {
        kept.extend(blob_keys(std::iter::once(("registered sources", source))).map_err(blob_err)?);
    }
    if let Some(build) = state.projects.running_build(project).await.map_err(|e| internal("running build", e))? {
        kept.extend(program_asset_keys(&build.inputs));
    }
    kept.extend(unfinished_runs_asset_keys(state, project).await?);
    let anchor = |key: &str| format!("{tenant}/{key}");
    let request = weft_core::storage::AssetReferencesRequest {
        project: project.to_string(),
        keys: required.iter().map(|k| anchor(k)).collect::<BTreeSet<_>>().into_iter().collect(),
        kept: kept.iter().map(|k| anchor(k)).collect(),
    };
    let outcome = crate::storage::set_asset_references(state, &tenant, request).await.map_err(|e| {
        if e.downcast_ref::<crate::storage::StorageNotFound>().is_some() {
            return (StatusCode::CONFLICT, format!("a file this names was never stored, so nothing changed: {e:#}. Upload it again"));
        }
        crate::api::storage::storage_err(e).into_refused()
    })?;
    let named_by: BTreeMap<String, Vec<String>> = named_by.into_iter().map(|(k, v)| (anchor(&k), v)).collect();
    Ok(outcome
        .missing
        .iter()
        .map(|key| {
            let versions = named_by.get(key).map(|v| v.join(", ")).unwrap_or_else(|| "a working copy, the registered build or an unfinished run".into());
            tracing::warn!(%project, key, %versions, "a version names a stored file that no longer exists");
            format!(
                "{versions} name(s) a stored file that no longer exists in storage ({}); a version that lost it cannot be \
                 switched back to, and `weft prune` drops it",
                key.rsplit('/').next().unwrap_or(key)
            )
        })
        .collect())
}

// ----- a program's calls on code ------------------------------------------

/// Carry out one call of `actor` on project `project`'s code.
pub(crate) async fn program_call(
    state: &DispatcherState,
    actor: &Actor,
    project: uuid::Uuid,
    call: &weft_core::program::versions::VersionCall,
) -> Result<weft_core::program::ProgramCallOutcome, Refused> {
    use weft_core::program::versions::VersionCall;
    match call {
        VersionCall::WorkingCopies => super::answered(&working_copies(state, actor, project).await?),
        VersionCall::WorkingState { copy } => super::answered(&working_state(state, actor, project, *copy).await?),
        VersionCall::ChangeFiles { request } => super::answered(&change_files(state, actor, project, request).await?),
        VersionCall::ReadFile { copy, path } => super::answered(&read_file(state, actor, project, *copy, path).await?),
        VersionCall::WriteFiles { request } => super::answered(&write_files(state, actor, project, request).await?),
        VersionCall::Checkpoint { request } => super::answered(&checkpoint(state, actor, project, request).await?),
        VersionCall::Switch { request } => super::answered(&switch(state, actor, project, request).await?),
        VersionCall::Forget { copy } => super::answered(&forget(state, actor, project, *copy).await?),
        VersionCall::Tree => super::answered(&tree(state, actor, project).await?),
        VersionCall::Prune { version, frozen, plan } => super::answered(&prune(state, actor, project, version, frozen, *plan).await?),
        VersionCall::UpdateRun { run, update } => {
            update_run(state, actor, project, *run, update).await?;
            super::answered(&serde_json::json!({}))
        }
        VersionCall::Running => super::answered(&running(state, actor, project).await?),
        VersionCall::Build { request } => {
            let answer = match super::build::build(state, actor, project, request).await.map_err(|f| (f.status, f.message))? {
                super::build::Built::Registered(program) => weft_core::builds::BuildAnswer::Registered { program },
                super::build::Built::Underway(underway) => weft_core::builds::BuildAnswer::Underway { underway },
            };
            super::answered(&answer)
        }
        VersionCall::BuildState { build } => {
            let found = crate::build::waiting::state(&state.pg_pool, project, *build)
                .await
                .map_err(|e| crate::api::unavailable_or_internal("read where the build is", e))?
                .ok_or((StatusCode::NOT_FOUND, format!("no build {build} of this project")))?;
            super::answered(&found)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A registered build keeps alive the content a text resolution names
    /// (`weft_core::content`), which its workers read, beside the stored
    /// files a file resolution names.
    #[test]
    fn a_build_keeps_the_content_its_text_resolutions_name() {
        let text = weft_core::weft_type::WeftType::primitive(weft_core::weft_type::WeftPrimitive::String);
        let (content, _) = weft_core::content::ContentRef::of_text("terms", &text, "https://example.com/terms.txt").unwrap();
        let inputs = weft_core::builds::BuildInputs {
            node_set: weft_core::builds::NodeSet::Referenced,
            assets: [(
                weft_core::project::ResolutionKey { path: "https://example.com/terms.txt".into(), ty: "String".into() },
                content.to_value(),
            )]
            .into(),
        };
        assert_eq!(program_asset_keys(&inputs), vec![content.asset_key()]);
    }

    /// A checkpoint of the code head is already on leaves head where it
    /// is, its run included, so the next `--seed` still inherits from that
    /// run; a checkpoint of changed code moves head to the new version,
    /// with no run on it yet.
    #[test]
    fn a_checkpoint_of_unchanged_code_keeps_heads_run() {
        let run = uuid::Uuid::from_u128(7);
        let on_v1 = Head { head_version: Some("v1".into()), head_run: Some(run) };
        assert_eq!(head_after_stamp(&on_v1, "v1"), on_v1);
        assert_eq!(head_after_stamp(&on_v1, "v2"), Head { head_version: Some("v2".into()), head_run: None });
        assert_eq!(head_after_stamp(&Head::default(), "v1"), Head { head_version: Some("v1".into()), head_run: None });
    }

    /// A hash two versions share makes one key.
    #[test]
    fn blob_keys_dedupe() {
        let (aaa, bbb) = ("a".repeat(64), "b".repeat(64));
        let a: BTreeMap<String, String> = [("main.weft".to_string(), aaa.clone())].into_iter().collect();
        let b: BTreeMap<String, String> = [("main.weft".to_string(), aaa.clone()), ("weft.toml".to_string(), bbb.clone())].into_iter().collect();
        let keys = blob_keys([("v1", &a), ("v2", &b)].into_iter()).expect("both are addressable");
        assert_eq!(keys, vec![format!("asset/{aaa}"), format!("asset/{bbb}")], "one key for the shared hash");
    }

    /// A version stored while a manifest still named the installed weft
    /// by a `weft:<version>:<catalog hash>` entry with no hash: that entry
    /// is no file, so the publish fails naming it rather than skipping it.
    #[test]
    fn a_stored_entry_with_no_hash_fails_by_name() {
        let old: BTreeMap<String, String> = [("main.weft".to_string(), "a".repeat(64)), ("weft:0.5.0:cat".to_string(), String::new())].into_iter().collect();
        let err = blob_keys([("v1", &old)].into_iter()).unwrap_err().to_string();
        assert!(err.contains("version v1") && err.contains("weft:0.5.0:cat"), "{err}");
    }

    /// A manifest is client-supplied, and its values become storage
    /// keys. One entry that cannot be addressed would break the asset
    /// reference publish for every version of the project at once, so
    /// it is refused where a manifest enters the install.
    #[test]
    fn a_manifest_that_cannot_be_addressed_is_refused() {
        let good = "a".repeat(64);
        let ok: Manifest = [("main.weft".to_string(), good.clone()), ("nodes/base_catalog/a/mod.rs".to_string(), good.clone())].into_iter().collect();
        validate_manifest(&ok).expect("an ordinary manifest passes");
        for bad in [
            [("main.weft".to_string(), "aaa/../../etc".to_string())],
            [("main.weft".to_string(), "aaa".to_string())],
            [("../escape.weft".to_string(), good.clone())],
            [("/absolute.weft".to_string(), good.clone())],
            [("main.weft".to_string(), String::new())],
            [("weft:0.5.0:cat".to_string(), String::new())],
        ] {
            let m: Manifest = bad.into_iter().collect();
            assert!(validate_manifest(&m).is_err(), "{m:?} must be refused");
        }
    }

    /// The registered build keeps every stored file its resolutions name,
    /// and the stored key a text-typed `@asset` named, without its tenant.
    #[test]
    fn the_registered_builds_assets_are_read_off_its_resolutions() {
        let (pic, prompt) = ("1".repeat(64), "2".repeat(64));
        let file = weft_core::storage::StoredFile {
            key: format!("local/asset/{pic}"),
            mime_type: "image/png".into(),
            size_bytes: 3,
            filename: "assets/pic.png".into(),
            version: weft_core::storage::FIRST_FILE_VERSION,
            link_secs: None,
        };
        let image = weft_core::weft_type::WeftType::parse("Image").unwrap();
        let inputs = weft_core::builds::BuildInputs {
            node_set: weft_core::builds::NodeSet::Full,
            assets: [
                (
                    weft_core::project::ResolutionKey { path: "assets/pic.png".into(), ty: image.wire_string() },
                    weft_core::storage::typed_file_value(&file, &image),
                ),
                (weft_core::project::ResolutionKey { path: format!("asset/{prompt}"), ty: "String".into() }, serde_json::json!("be kind")),
            ]
            .into(),
        };
        assert_eq!(program_asset_keys(&inputs), vec![format!("asset/{pic}"), format!("asset/{prompt}")]);
    }
}
