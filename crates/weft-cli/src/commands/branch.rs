//! `weft branch <version|run_id>`: move this folder to that version (a run
//! means its version) and write its files here. A run becomes the head's
//! run, so the next `--seed` inherits from it; a version clears it. With
//! `--copy`, another working copy moves instead (the program copy, or a
//! folder elsewhere), and nothing is written here.
//!
//! `weft forget-copy <copy>`: keep a working copy's files as a version and
//! remove the copy, for a folder that is gone or the program copy, whose
//! head would otherwise keep its versions from every prune.
//!
//! Nothing is lost on the way: the folder is synced first, and the install
//! stamps where its working copy was as a version before it moves
//! (`POST /projects/{id}/versions/switch`), so `weft branch <that version>`
//! comes back. The folder is synced again after, which writes the
//! version's files to disk; a file edited here meanwhile is kept, with the
//! version's written beside it as a conflict copy.

use anyhow::{bail, Context};
use weft_core::versions::VersionTree;
use weft_core::working_copy::{Forgotten, SwitchRequest, SwitchTo, Switched, WorkingCopy, WorkingCopyId, WorkingCopyKind, WorkingCopyRef};

use super::versions::{fetch_tree, resolve_run, resolve_version, short};
use super::Ctx;

pub async fn run(ctx: Ctx, reference: String, copy: Option<String>) -> anyhow::Result<()> {
    let here = super::copy::this_copy(&ctx, None).await?;
    let tree = fetch_tree(&here.client, &here.project_id).await?;
    // A run means its version and becomes the head's run.
    let to = match resolve_run(&tree, &reference) {
        Ok(run) => SwitchTo { run: Some(run.run_id), version: None },
        Err(_) => SwitchTo { version: Some(resolve_version(&tree, &reference)?.id.clone()), run: None },
    };
    let other = match &copy {
        Some(named) => Some(resolve_copy(&tree, named)?).filter(|c| c.id != here.copy.id),
        None => None,
    };
    if let Some(other) = other {
        let body = SwitchRequest { working_copy: WorkingCopyRef { working_copy: Some(other.id) }, to };
        let answer = here
            .client
            .post_json(&format!("/projects/{}/versions/switch", here.project_id), &serde_json::to_value(&body)?)
            .await
            .with_context(|| format!("switch {}", copy_name(other)))?;
        let switched: Switched = serde_json::from_value(answer).context("read the switch answer")?;
        if ctx.json_out(&switched)? {
            return Ok(());
        }
        let at = switched.copy.head.head_version.as_deref().map(short).unwrap_or("no version");
        println!("{} is now at version {at}", copy_name(other));
        if let Some(saved) = &switched.saved {
            println!("its files as they were are version {}", short(saved));
        }
        return Ok(());
    }
    let body = SwitchRequest { working_copy: here.named(), to };
    let answer = here
        .client
        .post_json(&format!("/projects/{}/versions/switch", here.project_id), &serde_json::to_value(&body)?)
        .await
        .context("switch this folder's working copy")?;
    let switched: Switched = serde_json::from_value(answer).context("read the switch answer")?;
    // The working copy moved; this writes its files here.
    let landed = super::copy::this_copy(&ctx, None).await.with_context(|| {
        let back = match &switched.saved {
            Some(saved) => format!(", and `{}` goes back to where you were", ctx.weft(&format!("branch {}", short(saved)))),
            None => String::new(),
        };
        format!(
            "this folder's working copy moved, but its files could not all be written here; `{}` finishes writing them{back}",
            ctx.weft("tree")
        )
    })?;
    let done = Switched { saved: switched.saved, copy: landed.copy };
    if ctx.json_out(&done)? {
        return Ok(());
    }
    for line in moved_lines(&done, &|verb| ctx.weft(verb)) {
        println!("{line}");
    }
    Ok(())
}

pub async fn forget(ctx: Ctx, named: String) -> anyhow::Result<()> {
    let here = super::copy::this_copy(&ctx, None).await?;
    let tree = fetch_tree(&here.client, &here.project_id).await?;
    let copy = copy_to_forget(&tree, &named, here.copy.id)?;
    let answer = here
        .client
        .delete_json(&format!("/projects/{}/working-copies/{}", here.project_id, copy.id))
        .await
        .with_context(|| format!("forget {}", copy_name(copy)))?;
    let forgotten: Forgotten = serde_json::from_value(answer).context("read the answer")?;
    if ctx.json_out(&forgotten)? {
        return Ok(());
    }
    match &forgotten.saved {
        Some(saved) => println!(
            "forgot {}; its files are version {}, and `{}` brings them to this folder",
            copy_name(copy),
            short(saved),
            ctx.weft(&format!("branch {}", short(saved)))
        ),
        None => println!("forgot {}, which held no files", copy_name(copy)),
    }
    Ok(())
}

/// The working copy `named` names, for `weft forget-copy` run from the
/// folder whose own copy is `here`: never that one, which every command
/// run in the folder registers again at once.
fn copy_to_forget<'a>(tree: &'a VersionTree, named: &str, here: WorkingCopyId) -> anyhow::Result<&'a WorkingCopy> {
    let copy = resolve_copy(tree, named)?;
    if copy.id == here {
        bail!("{named} is this folder's working copy, which every command run here registers again at once; run this from another folder");
    }
    Ok(copy)
}

/// The working copy `named` names: `program` (the program copy), a
/// folder's path, or a working copy's id or the start of one.
fn resolve_copy<'a>(tree: &'a VersionTree, named: &str) -> anyhow::Result<&'a WorkingCopy> {
    let matches: Vec<&WorkingCopy> = if named == "program" {
        tree.working_copies.iter().filter(|c| c.kind == WorkingCopyKind::Program).collect()
    } else if named.starts_with('/') {
        let path = named.trim_end_matches('/');
        tree.working_copies.iter().filter(|c| c.path.as_deref() == Some(path)).collect()
    } else {
        tree.working_copies.iter().filter(|c| c.id.to_string().starts_with(named)).collect()
    };
    match matches.as_slice() {
        [one] => Ok(one),
        [] if named == "program" => bail!("this project has no program copy"),
        [] => bail!("no working copy of this project is {named}; `weft tree` lists them"),
        many => bail!(
            "{named} names {} working copies (on different machines); name one by its id: {}",
            many.len(),
            many.iter().map(|c| c.id.to_string()).collect::<Vec<_>>().join(", ")
        ),
    }
}

/// How a line names a working copy that is not this folder's.
fn copy_name(copy: &WorkingCopy) -> String {
    match (&copy.path, &copy.host) {
        (Some(path), Some(host)) => format!("the working copy at {path} on {host}"),
        (Some(path), None) => format!("the working copy at {path}"),
        (None, _) => "the program copy".to_string(),
    }
}

/// What a switch says: where the folder is now, and the line that comes
/// back to where it was (none when it held no file, so nothing was kept).
fn moved_lines(done: &Switched, weft: &dyn Fn(&str) -> String) -> Vec<String> {
    let at = done.copy.head.head_version.as_deref().map(short).unwrap_or("no version");
    let here = match done.copy.head.head_run {
        Some(run) => format!("this folder is now at run {} on version {at}", short(&run.to_string())),
        None => format!("this folder is now at version {at}"),
    };
    let mut lines = vec![here];
    if !done.copy.conflicts.is_empty() {
        lines.push(format!("conflict copies to look at: {}", done.copy.conflicts.join(", ")));
    }
    if let Some(saved) = &done.saved {
        lines.push(format!("your files as they were are version {}; `{}` comes back to them", short(saved), weft(&format!("branch {}", short(saved)))));
    }
    lines
}

#[cfg(test)]
mod tests {
    use super::*;
    use weft_core::working_copy::{Head, WorkingCopy, WorkingCopyKind};

    fn switched(run: Option<uuid::Uuid>, conflicts: &[&str]) -> Switched {
        Switched {
            saved: Some("aaaabbbbcccc".into()),
            copy: WorkingCopy {
                id: uuid::Uuid::nil(),
                kind: WorkingCopyKind::Folder,
                host: None,
                path: None,
                head: Head { head_version: Some("11112222ffff".into()), head_run: run },
                working_version: "11112222ffff".into(),
                revision: 1,
                conflicts: conflicts.iter().map(|c| c.to_string()).collect(),
                updated_at: 0,
            },
        }
    }

    #[test]
    fn a_switch_says_where_the_folder_is_and_how_to_come_back() {
        let weft = |verb: &str| format!("weft {verb}");
        assert_eq!(
            moved_lines(&switched(None, &[]), &weft),
            vec!["this folder is now at version 11112222", "your files as they were are version aaaabbbb; `weft branch aaaabbbb` comes back to them"]
        );
        let run: uuid::Uuid = "c2000000-0000-0000-0000-000000000000".parse().unwrap();
        let lines = moved_lines(&switched(Some(run), &["main.weft-conflict-1"]), &weft);
        assert_eq!(lines[0], "this folder is now at run c2000000 on version 11112222");
        assert_eq!(lines[1], "conflict copies to look at: main.weft-conflict-1");
        let empty = Switched { saved: None, ..switched(None, &[]) };
        assert_eq!(moved_lines(&empty, &weft), vec!["this folder is now at version 11112222"], "nothing was kept, so there is nothing to come back to");
    }

    /// `weft forget-copy` refuses this folder's own working copy, by
    /// whatever name it is given, and forgets any other.
    #[test]
    fn forgetting_this_folders_own_copy_is_refused() {
        let folder = |id: u128, path: &str| WorkingCopy {
            id: uuid::Uuid::from_u128(id),
            path: Some(path.into()),
            ..switched(None, &[]).copy
        };
        let tree = VersionTree {
            working_copies: vec![folder(1, "/srv/here"), folder(2, "/srv/gone")],
            activated_versions: Vec::new(),
            versions: Vec::new(),
            runs: Vec::new(),
        };
        let here = uuid::Uuid::from_u128(1);
        for named in ["/srv/here", "/srv/here/", "00000000-0000-0000-0000-000000000001"] {
            let why = copy_to_forget(&tree, named, here).unwrap_err().to_string();
            assert!(why.contains("this folder's working copy") && why.contains("from another folder"), "{named}: {why}");
        }
        assert_eq!(copy_to_forget(&tree, "/srv/gone", here).unwrap().id, uuid::Uuid::from_u128(2));
    }
}
