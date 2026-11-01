//! `weft migrate connections|files --to <target>`: copy what the install
//! this command acts on keeps (the branch's, the default one, or `--on`'s)
//! to another install, so moving an app from your machine to the cloud
//! never means making every connection again. It copies; the install it
//! copies from keeps everything.
//!
//! A selection is always named: `--all`, or any of `--id`, `--since`,
//! `--until`, `--service`/`--project` (connections) and `--prefix`
//! (files). `--dry-run` says what would copy without copying.
//!
//! Connections travel sealed to the target: it makes a key pair, the
//! source seals the picked connections to it, and only the target opens
//! them, so no secret is ever in the clear on the way. Each keeps its id;
//! the source's picks are printed for you to set on the target. Files keep
//! their key (past the tenant, which every install names `local`), version
//! and lifetime, so a value naming one still finds it; their bytes go
//! through this machine.

use std::collections::BTreeMap;

use anyhow::Context;
use serde::Serialize;
use weft_core::migrate::{
    Candidate, ExportConnections, ExportedConnections, ImportConnections, ImportedConnections, Outcome, Selection, TransferKey,
};
use weft_core::storage::{FileImportBeginRequest, StoredFileMeta};

use super::Ctx;
use crate::client::DispatcherClient;

/// What to copy, as the flags every `weft migrate` verb takes spell it.
#[derive(clap::Args, Debug, Clone)]
pub struct Picks {
    /// The target to copy to (a `[targets.<name>]` of the project).
    #[arg(long, value_name = "target")]
    pub to: String,
    /// Copy everything.
    #[arg(long)]
    pub all: bool,
    /// Copy this one (a connection id, or a file key as `weft files
    /// inspect` takes it: `project/<id>/<file id>`). Repeatable.
    #[arg(long = "id", value_name = "id")]
    pub ids: Vec<String>,
    /// Only what was made at or after this moment: a date (`2026-10-01`,
    /// midnight UTC) or a time (`2026-10-01T14:00:00Z`).
    #[arg(long, value_parser = parse_moment)]
    pub since: Option<chrono::DateTime<chrono::Utc>>,
    /// Only what was made before this moment, written like `--since`.
    #[arg(long, value_parser = parse_moment)]
    pub until: Option<chrono::DateTime<chrono::Utc>>,
    /// Say what would be copied, and copy nothing.
    #[arg(long)]
    pub dry_run: bool,
}

#[derive(clap::Subcommand, Debug)]
pub enum MigrateAction {
    /// Copy saved connections (keys, sign-ins) to another install. Their
    /// secrets travel sealed to it; each keeps its id.
    Connections {
        #[command(flatten)]
        picks: Picks,
        /// Only connections of this service (`slack`, `google`).
        /// Repeatable.
        #[arg(long = "service", value_name = "service")]
        services: Vec<String>,
        /// Only connections of this project (its id). Repeatable.
        #[arg(long = "project", value_name = "project-id")]
        projects: Vec<uuid::Uuid>,
    },
    /// Copy stored files to another install, keeping each one's key,
    /// version and lifetime.
    Files {
        #[command(flatten)]
        picks: Picks,
        /// Only files whose key starts with this (`project/<id>/`,
        /// `shared/<name>/`). Repeatable.
        #[arg(long = "prefix", value_name = "prefix")]
        prefixes: Vec<String>,
    },
}

/// A moment as `--since` and `--until` take it.
fn parse_moment(raw: &str) -> Result<chrono::DateTime<chrono::Utc>, String> {
    if let Ok(at) = chrono::DateTime::parse_from_rfc3339(raw) {
        return Ok(at.with_timezone(&chrono::Utc));
    }
    chrono::NaiveDate::parse_from_str(raw, "%Y-%m-%d")
        .map(|day| day.and_hms_opt(0, 0, 0).expect("midnight exists").and_utc())
        .map_err(|_| format!("'{raw}' is neither a date (2026-10-01) nor a time (2026-10-01T14:00:00Z)"))
}

impl Picks {
    fn selection(&self, services: Vec<String>, projects: Vec<uuid::Uuid>, prefixes: Vec<String>) -> anyhow::Result<Selection> {
        let selection = Selection { all: self.all, ids: self.ids.clone(), since: self.since, until: self.until, services, projects, prefixes };
        selection.validate().map_err(anyhow::Error::msg)?;
        Ok(selection)
    }
}

pub async fn run(ctx: Ctx, action: MigrateAction) -> anyhow::Result<()> {
    let (picks, selection, what) = match action {
        MigrateAction::Connections { picks, services, projects } => {
            let mut selection = picks.selection(services, projects, Vec::new())?;
            // A connection id is a uuid, matched as one however it is typed.
            selection.ids = selection
                .ids
                .iter()
                .map(|id| id.parse::<uuid::Uuid>().map(|id| id.to_string()).map_err(|_| anyhow::anyhow!("--id {id} is not a connection id")))
                .collect::<anyhow::Result<_>>()?;
            (picks, selection, What::Connections)
        }
        MigrateAction::Files { picks, prefixes } => {
            let selection = picks.selection(Vec::new(), Vec::new(), prefixes)?;
            (picks, selection, What::Files)
        }
    };
    let source = ctx.client()?;
    let target = ctx.client_for_target(&picks.to)?;
    // An install answers at its own address and at its domain: asked
    // which it is, so the same one under two names is caught too.
    let service_url = |client: &DispatcherClient| {
        let client = client.clone();
        async move {
            let info: weft_core::install::InstallInfo = serde_json::from_value(client.get_json("/install").await?)
                .with_context(|| {
                    format!(
                        "{} answered /install in a shape this CLI does not read; it may run another weft than this CLI (update it, \
                         or use the CLI of its weft)",
                        client.base_url()
                    )
                })?;
            anyhow::Ok(info.service_url)
        }
    };
    let (from, to) = tokio::try_join!(service_url(&source), service_url(&target))?;
    anyhow::ensure!(
        from != to,
        "--to {} is the install this command copies from ({from}); name another one, or copy from another with --on",
        picks.to,
    );
    match what {
        What::Connections => connections(&ctx, &source, &target, &picks, selection).await,
        What::Files => files(&ctx, &source, &target, &picks, selection).await,
    }
}

enum What {
    Connections,
    Files,
}

/// One line of what a migrate did, or would do, with one item.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct Line {
    id: String,
    what: String,
    #[serde(flatten)]
    outcome: Outcome,
}

/// The ids `selection` names that no item has: said, so a typo is not
/// mistaken for something already copied.
fn not_found<'a>(selection: &'a Selection, found: &[Line]) -> Vec<&'a str> {
    selection.ids.iter().filter(|id| !found.iter().any(|line| &line.id == *id)).map(String::as_str).collect()
}

/// How a line reads.
fn say(line: &Line, dry_run: bool) -> String {
    match (&line.outcome, dry_run) {
        (Outcome::Copied, true) => format!("would copy {} ({})", line.id, line.what),
        (Outcome::Copied, false) => format!("copied {} ({})", line.id, line.what),
        (Outcome::AlreadyThere, _) => format!("already there: {} ({})", line.id, line.what),
        (Outcome::Skipped { why }, _) => format!("skipped {} ({}): {why}", line.id, line.what),
    }
}

// SYNC: connections <-> crates/weft-core/src/migrate.rs TransferKey/ExportConnections/ImportConnections, crates/weft-dispatcher/src/api/access.rs migrate_key/migrate_export/migrate_import, crates/weft-broker/src/access_admin.rs migrate_key/migrate_export/migrate_import
async fn connections(ctx: &Ctx, source: &DispatcherClient, target: &DispatcherClient, picks: &Picks, selection: Selection) -> anyhow::Result<()> {
    let key: Option<TransferKey> = match picks.dry_run {
        true => None,
        false => Some(
            serde_json::from_value(target.waiting_on_its_work().post_json("/access/migrate/key", &serde_json::json!({})).await?)
                .with_context(|| format!("read {}'s transfer key", picks.to))?,
        ),
    };
    let request = ExportConnections { selection, public_key: key.as_ref().map(|k| k.public_key.clone()) };
    let exported: ExportedConnections = serde_json::from_value(source.waiting_on_its_work().post_json("/access/migrate/export", &serde_json::to_value(&request)?).await?)
        .context("read the connections to copy")?;
    let outcomes: BTreeMap<uuid::Uuid, Outcome> = match (key, &exported.sealed) {
        (Some(key), Some(sealed)) => {
            let request = ImportConnections { key_id: key.key_id, sealed: sealed.clone() };
            let imported: ImportedConnections = serde_json::from_value(target.waiting_on_its_work().post_json("/access/migrate/import", &serde_json::to_value(&request)?).await?)
                .with_context(|| format!("read what {} did with the connections", picks.to))?;
            imported.connections.into_iter().map(|c| (c.id, c.outcome)).collect()
        }
        _ => BTreeMap::new(),
    };
    let lines: Vec<Line> = exported
        .connections
        .iter()
        .map(|c| Line {
            id: c.id.to_string(),
            what: match &c.label {
                Some(label) => format!("{}, {label}", c.service),
                None => c.service.clone(),
            },
            outcome: match (&c.skipped, outcomes.get(&c.id)) {
                (Some(why), _) => Outcome::Skipped { why: why.clone() },
                (None, Some(outcome)) => outcome.clone(),
                // A dry run copies nothing: what is not skipped is what
                // would be copied.
                (None, None) => Outcome::Copied,
            },
        })
        .collect();
    let picks_to_set: Vec<_> = exported
        .picks
        .iter()
        .filter(|p| lines.iter().any(|l| l.id == p.grant_id.to_string() && matches!(l.outcome, Outcome::Copied | Outcome::AlreadyThere)))
        .collect();
    let missing = not_found(&request.selection, &lines);
    if ctx.json_out(&serde_json::json!({ "connections": lines, "picks": picks_to_set, "notFound": missing }))? {
        return Ok(());
    }
    for id in &missing {
        eprintln!("no connection {id} on this install");
    }
    if lines.is_empty() {
        println!("no connection matches; nothing to copy");
        return Ok(());
    }
    for line in &lines {
        println!("{}", say(line, picks.dry_run));
    }
    if picks.dry_run {
        println!("(a dry run: nothing was copied; {} may still turn some down, saying why)", picks.to);
        return Ok(());
    }
    // A sign-in renews itself with a refresh token both installs now hold.
    // Most providers keep accepting it; one that replaces it on every use
    // breaks whichever install renews second, so it is said, never refused.
    let shared_sign_ins: Vec<&str> = exported
        .connections
        .iter()
        .filter(|c| c.sign_in && lines.iter().any(|l| l.id == c.id.to_string() && matches!(l.outcome, Outcome::Copied | Outcome::AlreadyThere)))
        .map(|c| c.service.as_str())
        .collect();
    if !shared_sign_ins.is_empty() {
        println!(
            "the sign-ins copied ({}) now renew from both installs with the same refresh token. Most providers allow that; one \
             that replaces its token on every renewal breaks whichever install renews second. If a copied sign-in stops working \
             on {}, connect it again there (`weft connect` in the project's folder, with --on {}), and use each install's own",
            shared_sign_ins.join(", "),
            picks.to,
            picks.to
        );
    }
    if !picks_to_set.is_empty() {
        println!("these nodes use the copied connections here; to pick them on {} too, run in each project's folder:", picks.to);
        for p in picks_to_set {
            println!("  project {}: weft connect --node {} --grant {} --on {}   (its field {})", p.project_id, p.node, p.grant_id, picks.to, p.field);
        }
    }
    Ok(())
}

/// Why a file is not copied, when it is not.
fn not_copied(meta: &StoredFileMeta, source: &str) -> Option<String> {
    match weft_core::storage::key::parse_key(&meta.key) {
        Err(e) => Some(format!("its key is not one weft makes ({e})")),
        Ok(parsed) => match parsed.scope {
            weft_core::storage::key::KeyScope::Run { .. } if !meta.keep => {
                Some(format!("a run's file goes with its run, which stays on {source} (a kept one would be copied)"))
            }
            _ => None,
        },
    }
}

async fn files(ctx: &Ctx, source: &DispatcherClient, target: &DispatcherClient, picks: &Picks, selection: Selection) -> anyhow::Result<()> {
    let from = ctx.on().unwrap_or("this install").to_string();
    let listed = super::files::list_files(source).await?;
    let picked: Vec<StoredFileMeta> = listed
        .into_iter()
        .filter(|meta| {
            let key = super::files::scope_key(&meta.key);
            let created_at = chrono::DateTime::from_timestamp(meta.created_at_unix, 0).unwrap_or_default();
            selection.picks(&Candidate { id: key, created_at, service: None, project: None, key: Some(key) })
        })
        .collect();
    let content = weft_assets::upload::ContentStore::new(super::assets::DispatcherPlane(target));
    // What the target holds already, by key past the account: a dry run
    // says "already there" for the same version and size, as the copy
    // would.
    let there: BTreeMap<String, (u64, u64)> = match picks.dry_run {
        false => BTreeMap::new(),
        true => super::files::list_files(target)
            .await?
            .into_iter()
            .map(|meta| (super::files::scope_key(&meta.key).to_string(), (meta.version, meta.size_bytes)))
            .collect(),
    };
    let scratch = tempfile::tempdir().context("make a folder for the files on their way")?;
    let mut lines = Vec::with_capacity(picked.len());
    for meta in &picked {
        let id = super::files::scope_key(&meta.key).to_string();
        let what = format!("{}, {}", meta.filename, super::files::fmt_size(meta.size_bytes));
        let outcome = match (not_copied(meta, &from), picks.dry_run) {
            (Some(why), _) => Outcome::Skipped { why },
            (None, true) if there.get(&id) == Some(&(meta.version, meta.size_bytes)) => Outcome::AlreadyThere,
            (None, true) => Outcome::Copied,
            (None, false) => match copy_file(source, &content, meta, scratch.path()).await.with_context(|| format!("copy {id}"))? {
                true => Outcome::AlreadyThere,
                false => Outcome::Copied,
            },
        };
        let line = Line { id, what, outcome };
        if !ctx.json() {
            println!("{}", say(&line, picks.dry_run));
        }
        lines.push(line);
    }
    let missing = not_found(&selection, &lines);
    if ctx.json_out(&serde_json::json!({ "files": lines, "notFound": missing }))? {
        return Ok(());
    }
    for id in &missing {
        eprintln!("no stored file {id} on this install");
    }
    if lines.is_empty() {
        println!("no file matches; nothing to copy");
    } else if picks.dry_run {
        println!("(a dry run: nothing was copied)");
    }
    Ok(())
}

/// Copy one file: download it here, then upload it to the target, under
/// the key it keeps. Answers whether it was there already.
async fn copy_file(
    source: &DispatcherClient,
    content: &weft_assets::upload::ContentStore<super::assets::DispatcherPlane<'_>>,
    meta: &StoredFileMeta,
    scratch: &std::path::Path,
) -> anyhow::Result<bool> {
    use weft_assets::AssetStore as _;
    let parsed = weft_core::storage::key::parse_key(&meta.key).map_err(anyhow::Error::msg)?;
    let is_asset = parsed.scope == weft_core::storage::key::KeyScope::Asset;
    // An asset is found by its content, under the same hash anywhere: one
    // the target holds needs no bytes.
    if is_asset && content.held(std::slice::from_ref(&parsed.id)).await?.contains_key(&parsed.id) {
        return Ok(true);
    }
    let path = scratch.join("file");
    let size = super::files::download_to(source, &meta.key, &path).await?;
    anyhow::ensure!(size == meta.size_bytes, "{} came down as {size} bytes where the install lists {}", meta.key, meta.size_bytes);
    let mut bytes = std::fs::File::open(&path).with_context(|| format!("open {}", path.display()))?;
    let already_there = match is_asset {
        true => {
            content.upload(&parsed.id, &meta.mime_type, &meta.filename, meta.size_bytes, &mut bytes).await?;
            false
        }
        false => content.copy(FileImportBeginRequest::of(meta), &mut bytes).await?.already_there,
    };
    std::fs::remove_file(&path).with_context(|| format!("remove {}", path.display()))?;
    Ok(already_there)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_moment_is_a_date_or_a_time() {
        assert_eq!(parse_moment("2026-10-01").unwrap().to_rfc3339(), "2026-10-01T00:00:00+00:00");
        assert_eq!(parse_moment("2026-10-01T14:00:00+02:00").unwrap().to_rfc3339(), "2026-10-01T12:00:00+00:00");
        assert!(parse_moment("yesterday").unwrap_err().contains("neither a date"));
    }

    fn meta(key: &str, keep: bool) -> StoredFileMeta {
        StoredFileMeta {
            key: key.into(),
            mime_type: "text/plain".into(),
            size_bytes: 1,
            filename: "f".into(),
            keep,
            expires_at_unix: None,
            keep_ttl_secs: None,
            created_at_unix: 0,
            version: 1,
        }
    }

    #[test]
    fn a_runs_file_stays_unless_kept() {
        let run = format!("t/run/{}/f", uuid::Uuid::nil());
        assert!(not_copied(&meta(&run, false), "local").unwrap().contains("stays on local"));
        assert_eq!(not_copied(&meta(&run, true), "local"), None);
        assert_eq!(not_copied(&meta(&format!("t/project/{}/f", uuid::Uuid::nil()), false), "local"), None);
    }

    #[test]
    fn a_line_says_what_happened() {
        let line = |outcome| Line { id: "project/p/f".into(), what: "f, 1 B".into(), outcome };
        assert_eq!(say(&line(Outcome::Copied), true), "would copy project/p/f (f, 1 B)");
        assert_eq!(say(&line(Outcome::Copied), false), "copied project/p/f (f, 1 B)");
        assert!(say(&line(Outcome::Skipped { why: "no".into() }), false).ends_with(": no"));
    }
}
