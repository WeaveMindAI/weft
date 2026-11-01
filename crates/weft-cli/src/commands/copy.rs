//! This folder's working copy: the install's copy of the project that the
//! folder a command runs from is kept the same as.
//!
//! The install's version tree is where a project's code lives, and a
//! folder is one place it is edited (`weft_core::working_copy`). Before a
//! command acts on the project, [`this_copy`] makes the folder and its
//! working copy agree both ways: the folder's edits go up, what changed in
//! the install (a program writing code, a switch) is written down, and a
//! file both sides changed keeps the folder's bytes while the install's
//! are written beside it as a conflict copy (`weft_assets::sync`). Every
//! command then acts on that working copy, so the same code runs against
//! a local install (whose runtime also keeps the folder in sync as it
//! changes) and a remote one (where nothing else does).
//!
//! The folder is known to the install by the machine it is on (its host
//! name) and its path. The first command run from a folder registers it,
//! with the folder's own files as the working copy's.

use anyhow::{Context, Result};
use weft_assets::sync::{BlobFetch, Synced, WorkingCopyPort};
use weft_assets::tree::HashCache;
use weft_core::working_copy::{Applied, RegisterFolder, SyncReport, WorkingCopy, WorkingState, NO_WORKING_COPY_HEADER};

use super::Ctx;
use crate::client::DispatcherClient;
use crate::progress::{elapsed_text, while_waiting, Progress, SAY_AGAIN_EVERY};

/// This folder's working copy, synced with the folder.
pub struct FolderCopy {
    /// The project's id, as its routes name it.
    pub project_id: String,
    pub client: DispatcherClient,
    /// The working copy as the sync left it.
    pub copy: WorkingCopy,
}

impl FolderCopy {
    /// The working copy as a request names it.
    pub fn named(&self) -> weft_core::working_copy::WorkingCopyRef {
        weft_core::working_copy::WorkingCopyRef { working_copy: Some(self.copy.id) }
    }
}

/// Make this folder and its working copy the same (see the module doc),
/// registering the folder first when the install does not know it yet,
/// and answer the working copy. That it is syncing is said when it
/// starts, and again every [`SAY_AGAIN_EVERY`] while it goes on (a busy or
/// far install can take minutes). What the sync did is said through
/// `progress` when the command rides it, else printed (and nothing is said
/// under `--json`, whose one line is the command's answer: the conflict
/// copies are in the working copy it carries).
pub async fn this_copy(ctx: &Ctx, progress: Option<&Progress>) -> Result<FolderCopy> {
    let tell = !ctx.json();
    let human = progress.map_or(tell, |progress| !progress.json);
    if human {
        println!("syncing this folder with the install");
    }
    let (project_id, client, synced) = while_waiting(sync_folder(ctx, tell), SAY_AGAIN_EVERY, async |elapsed| {
        if human {
            println!("  still syncing this folder with the install ({} so far)", elapsed_text(elapsed.as_secs()));
        }
    })
    .await?;
    let (said, conflicts) = sync_lines(&synced, &|verb| ctx.weft(verb));
    match progress {
        Some(progress) => {
            if !progress.json {
                said.iter().for_each(|line| println!("{line}"));
            }
            conflicts.iter().for_each(|line| progress.warn(line));
        }
        None if tell => {
            said.iter().for_each(|line| println!("{line}"));
            conflicts.iter().for_each(|line| eprintln!("warning: {line}"));
        }
        None => {}
    }
    Ok(FolderCopy { project_id, client, copy: synced.copy })
}

/// This folder's working copy as the install has it, read without syncing
/// or registering anything: for a verb that only reads, or acts on
/// another working copy.
pub struct FolderSeen {
    /// The project's id, as its routes name it.
    pub project_id: String,
    pub client: DispatcherClient,
    /// The working copy, `None` when the install does not know this folder.
    pub copy: Option<WorkingCopy>,
}

impl FolderSeen {
    /// The working copy's id, when the install knows this folder.
    pub fn id(&self) -> Option<weft_core::working_copy::WorkingCopyId> {
        self.copy.as_ref().map(|copy| copy.id)
    }
}

/// This folder's working copy as the install has it ([`FolderSeen`]),
/// without [`this_copy`]'s sync: nothing is sent, registered or written.
pub async fn look_up_this_copy(ctx: &Ctx) -> Result<FolderSeen> {
    let project = ctx.project()?;
    let client = ctx.client()?;
    let project_id = project.id().to_string();
    let (host, path) = folder_named(project.root.as_path())?;
    let copy = find_folder(&client, &project_id, &host, &path).await?.map(|found| found.copy);
    Ok(FolderSeen { project_id, client, copy })
}

/// How the install names the folder at `root`: this machine and the
/// folder's path.
fn folder_named(root: &std::path::Path) -> Result<(String, String)> {
    let path = root
        .to_str()
        .with_context(|| format!("the project's folder {} is not a UTF-8 path, which a working copy cannot be named by", root.display()))?
        .to_string();
    let host = weft_core::working_copy::this_machine().map_err(anyhow::Error::msg)?;
    Ok((host, path))
}

/// Sync this folder with its working copy (see [`this_copy`]): the
/// project's id, the client it was synced through, and what the sync did.
/// `tell` says each file sent, when the command prints.
async fn sync_folder(ctx: &Ctx, tell: bool) -> Result<(String, DispatcherClient, Synced)> {
    super::ensure::ensure_project_known(ctx).await?;
    let project = ctx.project()?;
    let client = ctx.client()?;
    let project_id = project.id().to_string();
    let root = project.root.as_path();
    let (host, path) = folder_named(root)?;

    let copy = match find_folder(&client, &project_id, &host, &path).await? {
        Some(found) => found.copy,
        None => register(&client, &project_id, root, host, path, tell).await?.copy,
    };
    let port = InstallCopy { client: &client, project_id: &project_id, copy: copy.id };
    let store = if tell { super::assets::DispatcherStore::telling(&client) } else { super::assets::DispatcherStore::new(&client) };
    let mut cache = HashCache::load(root);
    let synced = weft_assets::sync::sync_folder(root, &port, &InstallBlobs(&client), &store, &mut cache)
        .await
        .with_context(|| format!("sync {} with its working copy", root.display()))?;
    cache.save(root)?;
    Ok((project_id, client, synced))
}

/// The folder's working copy, or `None` when the install does not know
/// the folder yet.
async fn find_folder(client: &DispatcherClient, project_id: &str, host: &str, path: &str) -> Result<Option<WorkingState>> {
    let query: String = url::form_urlencoded::Serializer::new(String::new()).append_pair("host", host).append_pair("path", path).finish();
    let found = client
        .get_json_unless_marked(&format!("/projects/{project_id}/working-copies/folder?{query}"), NO_WORKING_COPY_HEADER)
        .await
        .context("look up this folder's working copy")?;
    found.map(|state| serde_json::from_value(state).context("read the folder's working copy")).transpose()
}

/// Register the folder at `root` as a working copy, with its own files:
/// every one is stored first (the install refuses a manifest naming a
/// blob it lacks).
async fn register(client: &DispatcherClient, project_id: &str, root: &std::path::Path, host: String, path: String, tell: bool) -> Result<WorkingState> {
    let source = super::assets::DiskSource::new(root.to_path_buf());
    let hashed = weft_assets::hash_files(&weft_assets::tree::covered_paths(root)?, &source)?;
    let store = if tell { super::assets::DispatcherStore::telling(client) } else { super::assets::DispatcherStore::new(client) };
    weft_assets::publish_hashed(&hashed, &source, &store).await.context("upload the folder's files")?;
    let manifest = hashed.into_iter().map(|file| (file.path, file.hash)).collect();
    let body = RegisterFolder { host, path, manifest };
    let answer = client
        .post_json(&format!("/projects/{project_id}/working-copies/folder"), &serde_json::to_value(&body)?)
        .await
        .context("register this folder as a working copy")?;
    serde_json::from_value(answer).context("read the registered working copy")
}

/// What a sync did, as lines: what it moved, then (apart, since they are
/// what a person has to act on) each conflict, naming both files and the
/// command that settles it either way.
fn sync_lines(synced: &Synced, weft: &dyn Fn(&str) -> String) -> (Vec<String>, Vec<String>) {
    let mut said = Vec::new();
    if !synced.pushed.is_empty() {
        said.push(format!("sent your changes to the install: {}", synced.pushed.join(", ")));
    }
    if !synced.written.is_empty() {
        said.push(format!("brought the install's changes into this folder: {}", synced.written.join(", ")));
    }
    let conflicts = synced
        .conflicts
        .iter()
        .map(|conflict| match &conflict.copy_path {
            Some(copy) => {
                let (what, yours) = match conflict.folder {
                    Some(_) => ("changed both here and in the install: your file is kept", "keeps yours"),
                    None => ("was removed here and changed in the install: it stays removed", "keeps it removed"),
                };
                format!(
                    "{} {what}, and the install's is written beside it as {copy}; `{}` puts the install's in place, `{}` {yours} \
                     and drops the copy",
                    conflict.path,
                    weft(&format!("resolve {} --keep incoming", conflict.path)),
                    weft(&format!("resolve {} --keep folder", conflict.path))
                )
            }
            None => format!(
                "{} changed here and was removed in the install: your file is kept; delete it if you want the removal",
                conflict.path
            ),
        })
        .collect();
    (said, conflicts)
}

/// The folder's working copy, over the install's routes.
struct InstallCopy<'a> {
    client: &'a DispatcherClient,
    project_id: &'a str,
    copy: weft_core::working_copy::WorkingCopyId,
}

#[async_trait::async_trait]
impl WorkingCopyPort for InstallCopy<'_> {
    async fn state(&self) -> Result<WorkingState> {
        let answer = self.client.get_json(&format!("/projects/{}/working-copies/state?workingCopy={}", self.project_id, self.copy)).await?;
        serde_json::from_value(answer).context("read the working copy")
    }

    async fn sync(&self, report: &SyncReport) -> Result<Applied> {
        let answer = self
            .client
            .post_json(&format!("/projects/{}/working-copies/{}/sync", self.project_id, self.copy), &serde_json::to_value(report)?)
            .await?;
        serde_json::from_value(answer).context("read what the sync landed")
    }
}

/// The tenant's stored files, downloaded from the install's store.
pub(crate) struct InstallBlobs<'a>(pub &'a DispatcherClient);

#[async_trait::async_trait]
impl BlobFetch for InstallBlobs<'_> {
    async fn fetch(&self, hash: &str, dest: &std::path::Path) -> Result<()> {
        // Through the key grammar's own constructor: the hash came off a
        // manifest the install sent, so it is not this side's to trust.
        let key = weft_core::storage::key::scope_key(&weft_core::storage::key::KeyScope::Asset, hash)
            .map_err(|e| anyhow::anyhow!("'{hash}' is not a storable address ({e})"))?;
        super::files::download_to(self.0, &key, dest).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use weft_core::working_copy::{Conflict, Head, WorkingCopyKind};

    #[test]
    fn a_sync_says_what_it_moved_and_each_conflict_by_both_files() {
        let copy = WorkingCopy {
            id: uuid::Uuid::nil(),
            kind: WorkingCopyKind::Folder,
            host: None,
            path: None,
            head: Head::default(),
            working_version: "v".into(),
            revision: 0,
            conflicts: vec![],
            updated_at: 0,
        };
        let quiet = Synced { copy: copy.clone(), pushed: vec![], written: vec![], conflicts: vec![] };
        let weft = |verb: &str| format!("weft {verb}");
        assert_eq!(sync_lines(&quiet, &weft), (vec![], vec![]));
        let busy = Synced {
            copy,
            pushed: vec!["main.weft".into(), "prompts/tone.txt".into()],
            written: vec!["nodes/a.rs".into()],
            conflicts: vec![
                Conflict {
                    path: "prompts/tone.txt".into(),
                    folder: Some("f".into()),
                    incoming: Some("h".into()),
                    copy_path: Some("prompts/tone.weft-conflict-1.txt".into()),
                },
                Conflict { path: "gone.weft".into(), folder: Some("f".into()), incoming: None, copy_path: None },
                Conflict { path: "cut.weft".into(), folder: None, incoming: Some("h".into()), copy_path: Some("cut.weft-conflict-1.weft".into()) },
            ],
        };
        let (said, conflicts) = sync_lines(&busy, &weft);
        assert_eq!(
            said,
            vec![
                "sent your changes to the install: main.weft, prompts/tone.txt",
                "brought the install's changes into this folder: nodes/a.rs",
            ]
        );
        assert_eq!(
            conflicts,
            vec![
                "prompts/tone.txt changed both here and in the install: your file is kept, and the install's is written beside it as \
                 prompts/tone.weft-conflict-1.txt; `weft resolve prompts/tone.txt --keep incoming` puts the install's in place, \
                 `weft resolve prompts/tone.txt --keep folder` keeps yours and drops the copy",
                "gone.weft changed here and was removed in the install: your file is kept; delete it if you want the removal",
                "cut.weft was removed here and changed in the install: it stays removed, and the install's is written beside it as \
                 cut.weft-conflict-1.weft; `weft resolve cut.weft --keep incoming` puts the install's in place, \
                 `weft resolve cut.weft --keep folder` keeps it removed and drops the copy",
            ]
        );
    }
}
