//! `weft resolve [<file>...] --keep <folder|incoming>`: settle the conflict
//! copies in this folder, one way or the other.
//!
//! When a file changed both in the folder and in its working copy, the sync
//! keeps the folder's file and writes the working copy's beside it as a
//! conflict copy (`weft_core::project::coverage::conflict_copy_name`), so
//! nothing is lost and nothing waits. This picks a side for good:
//! `--keep folder` deletes the conflict copies, `--keep incoming` moves
//! each one over its file. The folder is synced after, which sends the
//! result to the working copy.

use std::collections::BTreeMap;

use anyhow::{bail, Context, Result};
use weft_core::project::coverage::conflict_copy_origin;

use super::Ctx;

/// Which side of a conflict stays.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Keep {
    /// The folder's file: the conflict copy is deleted.
    Folder,
    /// The working copy's: the conflict copy replaces the folder's file.
    Incoming,
}

pub async fn run(ctx: Ctx, files: Vec<String>, keep: Keep) -> Result<()> {
    // Synced first, so the list of conflict copies is what the folder holds.
    let here = super::copy::this_copy(&ctx, None).await?;
    let root = ctx.project()?.root.clone();
    let picked = pick(&here.copy.conflicts, &files, keep)?;
    if picked.is_empty() {
        if !ctx.json_out(&serde_json::json!({ "keep": keep, "resolved": [], "copy": here.copy }))? {
            println!("this folder has no conflict copies");
        }
        return Ok(());
    }
    settle(&root, &picked, keep)?;
    let landed = super::copy::this_copy(&ctx, None).await.context("send the result to the install")?;
    let mut resolved: Vec<&str> = picked.iter().map(|(_, origin)| origin.as_str()).collect();
    resolved.sort();
    resolved.dedup();
    if ctx.json_out(&serde_json::json!({ "keep": keep, "resolved": resolved, "copy": landed.copy }))? {
        return Ok(());
    }
    let side = match keep {
        Keep::Folder => "kept this folder's",
        Keep::Incoming => "took the install's",
    };
    println!("{side}: {}", resolved.join(", "));
    if !landed.copy.conflicts.is_empty() {
        println!("conflict copies still here: {}", landed.copy.conflicts.join(", "));
    }
    Ok(())
}

/// Settle each `(copy, origin)` of `picked` in the folder at `root`: the
/// conflict copy deleted (`Keep::Folder`), or moved over its file
/// (`Keep::Incoming`).
fn settle(root: &std::path::Path, picked: &[(String, String)], keep: Keep) -> Result<()> {
    for (copy, origin) in picked {
        let copy_at = root.join(copy);
        match keep {
            Keep::Folder => std::fs::remove_file(&copy_at).with_context(|| format!("delete {copy}"))?,
            Keep::Incoming => std::fs::rename(&copy_at, root.join(origin)).with_context(|| format!("move {copy} over {origin}"))?,
        }
    }
    Ok(())
}

/// The conflict copies `files` names (a file, or one of its conflict copies;
/// none: every one), each with the file it was written beside. Taking the
/// incoming side needs one copy per file, so a file with several is
/// refused until one of its copies is named.
fn pick(conflicts: &[String], files: &[String], keep: Keep) -> Result<Vec<(String, String)>> {
    let all: Vec<(String, String)> =
        conflicts.iter().filter_map(|copy| conflict_copy_origin(copy).map(|origin| (copy.clone(), origin))).collect();
    let picked: Vec<(String, String)> = if files.is_empty() {
        all
    } else {
        let mut picked = Vec::new();
        for file in files {
            let file = file.trim_start_matches("./");
            let of: Vec<&(String, String)> = all.iter().filter(|(copy, origin)| copy == file || origin == file).collect();
            if of.is_empty() {
                bail!("{file} has no conflict copy in this folder; `weft tree` lists the ones there are");
            }
            picked.extend(of.into_iter().cloned());
        }
        picked.sort();
        picked.dedup();
        picked
    };
    if keep == Keep::Incoming {
        let mut by_file: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
        for (copy, origin) in &picked {
            by_file.entry(origin.as_str()).or_default().push(copy.as_str());
        }
        if let Some((file, copies)) = by_file.iter().find(|(_, copies)| copies.len() > 1) {
            bail!("{file} has {} conflict copies ({}); name the one to put in place", copies.len(), copies.join(", "));
        }
    }
    Ok(picked)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn copies() -> Vec<String> {
        vec![
            "main.weft-conflict-aaaa1111.weft".to_string(),
            "prompts/tone.weft-conflict-bbbb2222.txt".to_string(),
            "prompts/tone.weft-conflict-cccc3333.txt".to_string(),
        ]
    }

    #[test]
    fn a_file_or_one_of_its_copies_picks_what_is_resolved() {
        let all = pick(&copies(), &[], Keep::Folder).unwrap();
        assert_eq!(all.len(), 3, "no file named: every copy");
        let main = pick(&copies(), &["main.weft".into()], Keep::Incoming).unwrap();
        assert_eq!(main, vec![("main.weft-conflict-aaaa1111.weft".to_string(), "main.weft".to_string())]);
        let one = pick(&copies(), &["prompts/tone.weft-conflict-cccc3333.txt".into()], Keep::Incoming).unwrap();
        assert_eq!(one, vec![("prompts/tone.weft-conflict-cccc3333.txt".to_string(), "prompts/tone.txt".to_string())]);
        let why = pick(&copies(), &["nope.weft".into()], Keep::Folder).unwrap_err().to_string();
        assert!(why.contains("nope.weft has no conflict copy"), "{why}");
    }

    #[test]
    fn taking_the_incoming_side_needs_one_copy_per_file() {
        let why = pick(&copies(), &["prompts/tone.txt".into()], Keep::Incoming).unwrap_err().to_string();
        assert!(why.contains("prompts/tone.txt has 2 conflict copies") && why.contains("name the one"), "{why}");
        assert_eq!(pick(&copies(), &["prompts/tone.txt".into()], Keep::Folder).unwrap().len(), 2, "both copies go");
    }

    /// Taking the incoming side puts the conflict copy's bytes in its
    /// file's place and the copy goes; keeping the folder's leaves the
    /// file as it is and deletes the copy.
    #[test]
    fn settling_a_conflict_moves_or_drops_the_copy_on_disk() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        std::fs::create_dir_all(root.join("prompts")).unwrap();
        let file = "prompts/greeting.txt";
        let copy = "prompts/greeting.weft-conflict-aaaa1111.txt";
        let both = |mine: &str| {
            std::fs::write(root.join(file), mine).unwrap();
            std::fs::write(root.join(copy), "hello").unwrap();
            pick(&[copy.to_string()], &[file.to_string()], Keep::Incoming).unwrap()
        };
        settle(root, &both("mine"), Keep::Incoming).unwrap();
        assert_eq!(std::fs::read_to_string(root.join(file)).unwrap(), "hello");
        assert!(!root.join(copy).exists());
        settle(root, &both("mine again"), Keep::Folder).unwrap();
        assert_eq!(std::fs::read_to_string(root.join(file)).unwrap(), "mine again");
        assert!(!root.join(copy).exists());
    }
}
