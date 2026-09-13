//! What the version-tree verbs share: the snapshot of a project's files,
//! the run spec on disk, the tree as the dispatcher answers it, and the
//! pure comparison of two runs' wires. The verbs themselves are one
//! file each (`checkpoint`, `branch`, `tree`, `diff`, `freeze`,
//! `examples`, `prune`, `wake`); `run` and `activate` snapshot
//! through here too.
//!
//! A snapshot covers the files that make the program: every `*.weft`
//! (an `@include`d file is program text), `weft.toml`, `nodes/**` except
//! the seeded `base_catalog/` (that one is the installed weft's, and is
//! covered by one pseudo-entry naming the weft version and the
//! catalog's content hash), `prompts/**`, `scripts/**`, `sql/**`,
//! `assets/**`, and `examples/**` (a branch back restores the examples
//! that existed then). NOT `layouts/`: a canvas drag is not a version,
//! for the same reason the definition hash ignores it. The blobs go
//! through `weft_assets::publish_files` into the project's asset plane,
//! so a file identical to one any earlier version held costs nothing.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use serde_json::Value;
use weft_compiler::project::Project;
use weft_core::run_spec::{Expected, ExpectedWire, PortValues, RunSpec};

use crate::client::DispatcherClient;

pub use weft_core::project::hash::{Manifest, WEFT_ENTRY_PREFIX};

/// Whether a project-relative path (forward slashes) is part of a
/// version. The one rule; the walk and the dirty check both read it.
pub fn covers(rel: &str) -> bool {
    let first = rel.split('/').next().unwrap_or("");
    // A HIDDEN top-level entry is tooling, never a version's files:
    // `.weft` and `.git`, and equally `.github`, `.vscode`, `.idea`,
    // `.gitignore`, `.env`. Said as one rule about the whole entry,
    // hidden files included, for two reasons. The prefix spellings it
    // replaces each covered one name and let the next one through
    // (`.git/` did not exclude `.github/x.weft`, so the rule and the
    // walk disagreed about a folder real projects have). And asking it
    // only of folders let `.weft` itself back in through the
    // `ends_with(".weft")` rule below, and made the same directory
    // answer differently spelled with and without its trailing slash.
    if first.starts_with('.') {
        return false;
    }
    if rel.starts_with("target/") || rel.starts_with("layouts/") {
        return false;
    }
    // `node_modules` is never source, at any depth: a node that carries
    // a JS package has its own, and an ordinary `pnpm install` inside a
    // node used to put thousands of files through the hash, the
    // publish and `weft branch`'s dirty check, with a branch back
    // deleting them one by one. The name is npm's, so nothing else is
    // called that.
    //
    // `target` is excluded at the ROOT only, just above: the project
    // root is where weft's own build puts a Cargo workspace, so a
    // top-level `target/` is build output every time. Deeper down it is
    // an ordinary word, and a node package, a prompts folder or a
    // scripts folder may legitimately be called it, so it is NOT
    // excluded by name there: silently dropping source from a version
    // loses it on a branch back. The walk tells the two apart, because
    // it can see whether a `Cargo.toml` sits beside the directory.
    if rel.split('/').any(|seg| seg == "node_modules") {
        return false;
    }
    if rel.starts_with("nodes/base_catalog/") {
        return false;
    }
    if rel.ends_with(".weft") || rel == "weft.toml" {
        return true;
    }
    matches!(first, "nodes" | "prompts" | "scripts" | "sql" | "assets" | "examples")
}

/// Every covered file under `root`, project-relative with forward
/// slashes, sorted.
pub fn covered_paths(root: &Path) -> Result<Vec<String>> {
    let mut out = Vec::new();
    walk(root, root, &mut out)?;
    out.sort();
    Ok(out)
}

fn walk(root: &Path, dir: &Path, out: &mut Vec<String>) -> Result<()> {
    for entry in std::fs::read_dir(dir).with_context(|| format!("read {}", dir.display()))? {
        let entry = entry?;
        let path = entry.path();
        let raw = path.strip_prefix(root).expect("under root");
        let Some(rel) = raw.to_str() else {
            // A path whose bytes are not UTF-8 cannot be a manifest key.
            // `to_string_lossy` would have spelled it with replacement
            // characters, and that spelling decides what is IN the
            // version: the file would be published under a name that is
            // not its name, read as both added and removed by every
            // dirty check (the disk name never equals the lossy one), and
            // restored beside the original rather than over it.
            //
            // Refused by name, but only when a version would actually
            // take it. Such a name in a folder no version reads is not
            // this command's business, and failing every snapshot over a
            // file that would never be recorded is worse than passing it
            // over.
            let lossy = raw.to_string_lossy().replace('\\', "/");
            let wanted = if path.is_dir() { !prunes_whole_tree(&lossy) } else { covers(&lossy) };
            if wanted {
                anyhow::bail!(
                    "'{}' is not a name weft can record in a version (its bytes are not UTF-8); \
                     rename it, or move it out of the project",
                    path.display()
                );
            }
            continue;
        };
        let rel = rel.replace('\\', "/");
        let is_symlink = entry.file_type()?.is_symlink();
        // `is_dir` FOLLOWS a symlink, so this has to be asked before any
        // question about what the entry is.
        if path.is_dir() && !is_symlink {
            // Prune whole trees the rule never covers, so a large
            // `.weft/target` is never walked.
            let name = path.file_name().map(|n| n.to_string_lossy().to_string()).unwrap_or_default();
            // A `target/` directory is Cargo's output exactly when a
            // `Cargo.toml` sits beside it. That is a fact about the
            // disk, not about the path, which is why the pure `covers`
            // rule cannot make this call and the walk does.
            let cargo_output = name == "target" && dir.join("Cargo.toml").is_file();
            // Pruning asks `covers` about the directory as a path, so
            // the two cannot disagree: spelled out again here, `.weft`
            // without its slash also pruned a folder called
            // `.weft-notes` that `covers` would have included, and only
            // the walk's opinion reaches the disk. The one thing `covers`
            // cannot answer is `cargo_output`, which is a fact about
            // what sits next to the directory rather than about its path.
            if !(cargo_output || prunes_whole_tree(&rel)) {
                walk(root, &path, out)?;
            }
        } else if is_symlink {
            // A link where a version's own file or folder would be is
            // refused by name. Followed, a link pointing at a folder above
            // it is an endless walk (a stack overflow, with no message),
            // and one pointing OUT of the project pulls somebody else's
            // files in under a path inside it, which `weft branch` would
            // later write back THROUGH.
            //
            // A link anywhere else is nobody's business here, and most
            // links in a project are exactly that: a symlinked `README.md`
            // or `docker-compose.yml` at the root is ordinary, and no
            // version would ever take it. So the question asked matches
            // what the link IS. `path.is_dir()` follows the link, which is
            // safe to ASK (it recurses nothing) and is the only way to know
            // which of the two questions applies.
            let refuse = if path.is_dir() { !prunes_whole_tree(&rel) } else { covers(&rel) };
            if refuse {
                anyhow::bail!(
                    "'{rel}' is a symlink, and a version is the files themselves: a link can \
                     point out of the project or back at a folder above it, and branching back \
                     would write through it. Replace it with the file, or move it out of the \
                     project"
                );
            }
        } else if covers(&rel) {
            out.push(rel);
        }
    }
    Ok(())
}

/// Whether a DIRECTORY holds nothing a version covers, so the walk can
/// skip it whole.
///
/// Asks [`covers`] rather than restating it: a path under this directory
/// can only be covered if the directory itself could be, and the two
/// spellings drifting apart is how a folder ends up silently in or out
/// of a version.
fn prunes_whole_tree(rel: &str) -> bool {
    // A directory's own path is not a file path, so `covers` is asked
    // about a file inside it. `.weft` is covered nowhere; `nodes` is
    // covered only through its contents.
    !covers(&format!("{rel}/x.weft")) && !covers(&format!("{rel}/x"))
}

/// The content hash of the seeded base catalog: every file under
/// `nodes/base_catalog/`, path and bytes, sorted.
fn base_catalog_hash(root: &Path) -> Result<String> {
    use sha2::Digest;
    let dir = root.join("nodes").join("base_catalog");
    let mut files: Vec<PathBuf> = Vec::new();
    if dir.is_dir() {
        collect_files(&dir, &mut files)?;
    }
    files.sort();
    let mut hasher = sha2::Sha256::new();
    for f in files {
        let rel = f.strip_prefix(&dir).expect("under the catalog").to_string_lossy().replace('\\', "/");
        hasher.update(rel.as_bytes());
        hasher.update(b"\n");
        hasher.update(std::fs::read(&f).with_context(|| format!("read {}", f.display()))?);
        hasher.update(b"\n");
    }
    Ok(weft_core::project::hash::hex(&hasher.finalize()))
}

fn collect_files(dir: &Path, out: &mut Vec<PathBuf>) -> Result<()> {
    for entry in std::fs::read_dir(dir).with_context(|| format!("read {}", dir.display()))? {
        let entry = entry?;
        let path = entry.path();
        let name = entry.file_name().to_string_lossy().to_string();
        // Installed dependencies and build output are NOT part of what
        // weft is: they differ per machine and per install, and this hash
        // ends up inside the manifest, which IS the version id. The
        // catalog ships a pnpm package, so one `pnpm install` in there
        // folded thousands of local files into the id and two machines
        // then disagreed about the name of identical code, with the dirty
        // check reporting drift nobody had caused.
        //
        // The SAME list the seed copies through
        // (`weft_catalog::NODE_TREE_EXCLUDE`), because this hash has to
        // describe what the catalog actually is: a name excluded here and
        // copied there, or the other way round, means the hash answers
        // for files the build never staged. Installed dependencies and
        // build output differ per machine and per install, and this hash
        // ends up inside the manifest, which IS the version id, so one
        // `pnpm install` in the catalog once folded thousands of local
        // files into it and two machines disagreed about the name of
        // identical code.
        if weft_catalog::is_node_tree_excluded(&name) {
            continue;
        }
        // A symlink is refused rather than skipped. Skipping left the
        // file out of the hash in silence, so a catalog differing only by
        // a link hashed the same as one without it; and following it
        // would read something that is not a file of the catalog. The
        // catalog is generated by `weft catalog update` from weft's own
        // installed copy, so a link in here is weft's to fix, not the
        // project's.
        if entry.file_type()?.is_symlink() {
            bail!(
                "{} is a symlink, and the catalog's hash is taken over real files only, so \
                 this project cannot be hashed. The catalog comes from the installed weft: \
                 report this, and `weft catalog update` once it ships without the link",
                path.display()
            );
        }
        if path.is_dir() {
            collect_files(&path, out)?;
        } else {
            out.push(path);
        }
    }
    Ok(())
}

/// The manifest of the project as it is on disk, without publishing
/// anything: what `branch` compares against head to see a dirty tree.
pub fn local_manifest(project: &Project) -> Result<Manifest> {
    let paths = covered_paths(&project.root)?;
    let source = crate::commands::assets::DiskSource::new(project.root.clone());
    let mut manifest = Manifest::new();
    for hashed in weft_assets::hash_files(&paths, &source)? {
        manifest.insert(hashed.path, hashed.hash);
    }
    manifest.insert(weft_entry(project)?, String::new());
    Ok(manifest)
}

fn weft_entry(project: &Project) -> Result<String> {
    Ok(format!("{WEFT_ENTRY_PREFIX}{}:{}", env!("CARGO_PKG_VERSION"), base_catalog_hash(&project.root)?))
}

/// Snapshot the project: publish every covered file into the asset
/// plane and answer the manifest a version records.
pub async fn snapshot(client: &DispatcherClient, project: &Project) -> Result<Manifest> {
    let paths = covered_paths(&project.root)?;
    let source = crate::commands::assets::DiskSource::new(project.root.clone());
    let store = crate::commands::assets::DispatcherStore::new(client, project.id().to_string());
    let published = weft_assets::publish_files(&paths, &source, &store).await.context("publish the version's files")?;
    let mut manifest: Manifest = published.into_iter().map(|(path, p)| (path, p.hash)).collect();
    manifest.insert(weft_entry(project)?, String::new());
    Ok(manifest)
}

/// The files of `manifest` that are not the pseudo-entry.
pub fn manifest_files(manifest: &Manifest) -> impl Iterator<Item = (&String, &String)> {
    manifest.iter().filter(|(path, _)| !path.starts_with(WEFT_ENTRY_PREFIX))
}

// ----- the tree as the dispatcher answers it -----------------------------

// SYNC: Tree, Head, VersionSummary, RunSummary, ManifestDiff <-> crates/weft-dispatcher/src/api/versions.rs TreeResponse, VersionSummary, RunSummary and crates/weft-dispatcher/src/versions.rs Head, ManifestDiff, extension-vscode/src/sidebar/version-tree.ts TreeJson, VersionSummary, RunSummary
#[derive(Debug, Clone, serde::Deserialize)]
pub struct Tree {
    pub head: Head,
    pub versions: Vec<VersionSummary>,
    pub runs: Vec<RunSummary>,
}

#[derive(Debug, Clone, Default, serde::Deserialize)]
pub struct Head {
    pub head_version: Option<String>,
    pub head_run: Option<String>,
    pub activation_version: Option<String>,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct VersionSummary {
    pub id: String,
    pub parent_id: Option<String>,
    pub label: Option<String>,
    pub created_at: u64,
    pub diff: ManifestDiff,
    pub manifest: Manifest,
}

#[derive(Debug, Clone, Default, serde::Deserialize)]
pub struct ManifestDiff {
    pub added: Vec<String>,
    pub removed: Vec<String>,
    pub changed: Vec<String>,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct RunSummary {
    pub color: String,
    pub version_id: String,
    pub definition_hash: String,
    pub seed_color: Option<String>,
    pub stale: Vec<String>,
    pub spec: Option<RunSpec>,
    pub example: Option<String>,
    pub status: String,
    pub started_at: u64,
    pub completed_at: Option<u64>,
}

pub async fn fetch_tree(client: &DispatcherClient, project_id: &str) -> Result<Tree> {
    Ok(fetch_tree_raw(client, project_id).await?.0)
}

/// The tree, parsed, alongside the exact document the dispatcher sent.
///
/// `weft tree --json` forwards the server's own answer with one field
/// added, so it needs both. Fetching twice (once parsed, once raw) meant
/// two round trips whose answers could disagree: the emitted document
/// could list a version the added field was computed without.
pub async fn fetch_tree_raw(client: &DispatcherClient, project_id: &str) -> Result<(Tree, Value)> {
    let value = client
        .get_json(&format!("/projects/{project_id}/versions/tree"))
        .await
        .context("read the version tree")?;
    let tree: Tree = serde_json::from_value(value.clone()).context("parse the version tree")?;
    Ok((tree, value))
}

/// The first characters of a version id or a color, for a line.
pub fn short(id: &str) -> &str {
    &id[..id.len().min(8)]
}

/// The run `ref` names: a whole color, or the start of one among the
/// tree's runs.
pub fn resolve_run<'a>(tree: &'a Tree, reference: &str) -> Result<&'a RunSummary> {
    let matches: Vec<&RunSummary> = tree.runs.iter().filter(|r| r.color.starts_with(reference)).collect();
    match matches.as_slice() {
        [one] => Ok(one),
        [] => bail!("no run starts with {reference} in this project's tree; `weft tree` lists them"),
        many => bail!(
            "{reference} names {} runs ({}); give more characters",
            many.len(),
            many.iter().map(|r| short(&r.color)).collect::<Vec<_>>().join(", ")
        ),
    }
}

/// The version `ref` names: a checkpoint label, a whole id, or the
/// start of one. A label wins over an id prefix, since a label is a
/// word somebody chose and an id is hex.
pub fn resolve_version<'a>(tree: &'a Tree, reference: &str) -> Result<&'a VersionSummary> {
    let labelled: Vec<&VersionSummary> = tree.versions.iter().filter(|v| v.label.as_deref() == Some(reference)).collect();
    match labelled.as_slice() {
        [one] => return Ok(one),
        [] => {}
        many => bail!(
            "{reference} labels {} versions ({}); name one by its id",
            many.len(),
            many.iter().map(|v| short(&v.id)).collect::<Vec<_>>().join(", ")
        ),
    }
    let matches: Vec<&VersionSummary> = tree.versions.iter().filter(|v| v.id.starts_with(reference)).collect();
    match matches.as_slice() {
        [one] => Ok(one),
        [] => bail!("no version is labelled or starts with {reference} in this project's tree; `weft tree` lists them"),
        many => bail!(
            "{reference} names {} versions ({}); give more characters",
            many.len(),
            many.iter().map(|v| short(&v.id)).collect::<Vec<_>>().join(", ")
        ),
    }
}

// ----- the spec on disk --------------------------------------------------

pub fn examples_dir(project: &Project) -> PathBuf {
    project.root.join("examples")
}

pub fn spec_path(project: &Project, name: &str) -> PathBuf {
    examples_dir(project).join(format!("{name}.json"))
}

/// Read a saved spec: `examples/<name>.json` for a bare name, or the
/// file itself for anything ending in `.json` (a path, relative to the
/// project root or absolute).
pub fn read_spec(project: &Project, name: &str) -> Result<RunSpec> {
    let path = spec_file_path(project, name);
    read_spec_if_present(project, name)?
        .with_context(|| format!("no spec at {}", path.display()))
}

/// The rows of a run's journal, as the replay endpoint answers them.
///
/// One reader, because three commands hand-rolled the same four steps
/// (`GET .../replay`, take the array, fold it into wires, resolve the
/// stored media) and the ones that also need the raw rows had to keep
/// their own copy of the first half.
pub async fn replay_rows(
    client: &crate::client::DispatcherClient,
    color: &str,
) -> Result<Vec<Value>> {
    let rows = client
        .get_json(&format!("/executions/{color}/replay"))
        .await
        .context("read the run")?;
    serde_json::from_value(rows).context("execution replay must be an array of events")
}

/// What every wire of a run carried, with stored media resolved to its
/// bytes so two runs' values compare as values.
pub async fn output_wires(
    client: &crate::client::DispatcherClient,
    project_id: &str,
    color: &str,
) -> Result<Expected> {
    let mut expected: Expected = serde_json::from_value(client.get_json(&format!("/executions/{color}/outputs")).await?)
        .context("decode complete output history")?;
    normalize_media(client, project_id, &mut expected.wires).await?;
    Ok(expected)
}

/// Where a spec NAME lives: `examples/<name>.json`, or the path itself
/// when it already names a file. One place, because the accepted spellings
/// are one rule and two copies of it drift the moment a spelling changes.
fn spec_file_path(project: &Project, name: &str) -> PathBuf {
    let path = if name.ends_with(".json") { PathBuf::from(name) } else { spec_path(project, name) };
    if path.is_absolute() { path } else { project.root.join(path) }
}

/// The spec of that name, or `None` when there is no file there.
///
/// Only a missing file is an absence. A file that exists and does not
/// read or parse is an error: treating it as "nothing here" lets the
/// caller write over a spec whose only problem was a typo in it.
pub fn read_spec_if_present(project: &Project, name: &str) -> Result<Option<RunSpec>> {
    let path = spec_file_path(project, name);
    let text = match std::fs::read_to_string(&path) {
        Ok(t) => t,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e).with_context(|| format!("read {}", path.display())),
    };
    let mut spec: RunSpec = serde_json::from_str(&text).with_context(|| format!("parse {}", path.display()))?;
    spec.name = path.file_stem().context("example filename")?.to_string_lossy().into_owned();
    Ok(Some(spec))
}

/// An example NAME, not a path.
///
/// The reader accepts a path form (`examples/chain.json`, an absolute
/// path) because a person may point at a file; the writer always writes
/// `examples/<name>.json`. Letting a path reach the writer produced
/// `examples/examples/chain.json.json`, so the rule lives on the writer
/// where both `weft freeze` and `weft run --save` pass through it.
// SYNC: validate_example_name <-> packages/weft-graph/src/run-spec.ts exampleNameProblem
pub fn validate_example_name(name: &str) -> Result<()> {
    // A leading dash is refused because the name is typed back as an
    // argument (`weft run <name>`, `weft freeze <name>`), where the
    // argument parser reads it as a flag and refuses before this function
    // is ever reached. Said here so the refusal names the real problem,
    // and so the editor (which shares this rule) never writes a file
    // whose name nothing can then run.
    if name.starts_with('-') {
        bail!("an example cannot start with a dash: `{name}` reads as a flag, not a name");
    }
    let looks_like_a_path = name.contains('/') || name.contains('\\') || name.ends_with(".json");
    if !looks_like_a_path && !name.is_empty() && name != "." && name != ".." {
        return Ok(());
    }
    // The suggestion has to survive this same rule, or the person
    // follows it and is refused again: `...json` strips to `..`, and
    // `notes.json.json` strips once to `notes.json`, which this rule
    // refuses in its turn. So strip every `.json` off the end, and
    // check the result against the same conditions.
    let mut suggestion = name.rsplit(['/', '\\']).next().unwrap_or("");
    while let Some(shorter) = suggestion.strip_suffix(".json") {
        suggestion = shorter;
    }
    let usable = !suggestion.is_empty() && suggestion != "." && suggestion != "..";
    if usable {
        bail!("an example is named, not a path: use `{suggestion}`");
    }
    bail!("an example needs a name (a word, not a path or a directory): `weft freeze <name>`")
}

pub fn write_spec(project: &Project, spec: &RunSpec) -> Result<PathBuf> {
    validate_example_name(&spec.name)?;
    let path = spec_path(project, &spec.name);
    std::fs::create_dir_all(examples_dir(project))?;
    let mut text = serde_json::to_string_pretty(spec)?;
    text.push('\n');
    std::fs::write(&path, text).with_context(|| format!("write {}", path.display()))?;
    Ok(path)
}

/// Every spec in `examples/`, by name, sorted, plus the files in there
/// that do not read as one.
///
/// Return unreadable files so listings can explain each missing example
/// and pruning can still operate without parsing a half-written file.
pub fn list_specs(project: &Project) -> Result<(Vec<RunSpec>, Vec<String>)> {
    let dir = examples_dir(project);
    let mut out = Vec::new();
    let mut unreadable = Vec::new();
    if !dir.is_dir() {
        return Ok((out, unreadable));
    }
    for entry in std::fs::read_dir(&dir)? {
        let path = entry?.path();
        if path.extension().and_then(|e| e.to_str()) != Some("json") {
            continue;
        }
        // A file that will not READ is the same answer as one that will
        // not PARSE, and it is handed back the same way. Letting the read
        // error out instead meant a directory called `examples/notes.json`,
        // or one file with the wrong permissions, stopped `weft prune`
        // dead with a bare "Is a directory (os error 21)" and no path.
        match std::fs::read_to_string(&path).map_err(|e| e.to_string()).and_then(|text| {
            serde_json::from_str::<RunSpec>(&text).map_err(|e| format!("is not a spec ({e})"))
        }) {
            Ok(mut spec) => {
                // The filename is the runnable name; an edited display name
                // inside the JSON must not point the listing at another file.
                spec.name = path.file_stem().context("example filename")?.to_string_lossy().into_owned();
                out.push(spec);
            }
            Err(why) => unreadable.push(format!("{} {why}", path.display())),
        }
    }
    out.sort_by(|a, b| a.name.cmp(&b.name));
    unreadable.sort();
    Ok((out, unreadable))
}

/// The flags `weft run` turns into a spec.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct RunFlags {
    pub from: Vec<String>,
    pub target: Vec<String>,
    pub before: Vec<String>,
    pub group: Option<String>,
    pub fire: Vec<String>,
    pub emit: Vec<String>,
    pub clear: Vec<String>,
}

impl RunFlags {
    pub fn is_empty(&self) -> bool {
        self.from.is_empty()
            && self.target.is_empty()
            && self.before.is_empty()
            && self.group.is_none()
            && self.fire.is_empty()
            && self.emit.is_empty()
            && self.clear.is_empty()
    }
}

/// Parse a node and JSON payload. Dots belong to the node id.
fn parse_node_flag(flag: &str, verb: &str) -> Result<(String, Value)> {
    let (node, json) = flag.split_once('=')
        .ok_or_else(|| anyhow::anyhow!("{verb} wants node=<json>, got '{flag}'"))?;
    if node.is_empty() { bail!("{verb} requires a node name"); }
    let value = serde_json::from_str(json)
        .with_context(|| format!("{verb} {node}: the value must be JSON"))?;
    Ok((node.into(), value))
}

/// Repeated flags merge distinct ports; repeating a port is an error.
fn parse_port_flags(flags: &[String], verb: &str, allow_bare: bool) -> Result<PortValues> {
    let mut values: PortValues = BTreeMap::new();
    for flag in flags {
        let (node, json) = match flag.split_once('=') {
            Some(parts) => parts,
            None if allow_bare => (flag.as_str(), "{}"),
            None => bail!("{verb} wants node=<json>, got '{flag}'"),
        };
        if node.is_empty() { bail!("{verb} requires a node name"); }
        let ports = weft_core::run_spec::parse_port_object(json)
            .with_context(|| format!("{verb} {node}: expected an object mapping ports to values; the value must be JSON"))?;
        let target = values.entry(node.into()).or_default();
        for (port, value) in ports {
            if target.contains_key(&port) { bail!("{verb}: duplicate port '{node}.{port}'"); }
            target.insert(port, value);
        }
    }
    Ok(values)
}

/// The spec the flags describe, named `name`.
pub fn spec_from_flags(name: &str, flags: &RunFlags) -> Result<RunSpec> {
    apply_run_flags(&RunSpec::whole(name), flags)
}

/// Edit one loaded spec before graph validation. Clearing is explicit;
/// supplying a new start replaces the saved start set and its payloads.
pub fn apply_run_flags(base: &RunSpec, flags: &RunFlags) -> Result<RunSpec> {
    if flags.fire.len() > 1 { bail!("a run can fire only one trigger"); }
    let mut spec = base.clone();
    for field in &flags.clear {
        match field.as_str() {
            "from" => spec.from.clear(),
            "emit" => spec.emit.clear(),
            "target" => spec.target.clear(),
            "before" => spec.before.clear(),
            "group" => spec.group = None,
            "fire" => spec.fire = None,
            _ => bail!("--clear: unknown setting '{field}'; use from, emit, target, before, group, or fire"),
        }
    }
    if !flags.from.is_empty() { spec.from = parse_port_flags(&flags.from, "--from", true)?; }
    if !flags.target.is_empty() { spec.target = flags.target.clone(); }
    if !flags.before.is_empty() { spec.before = flags.before.clone(); }
    if let Some(group) = &flags.group {
        spec.group = parse_port_flags(std::slice::from_ref(group), "group", true)?.into_iter().next();
    }
    if let Some(fire) = flags.fire.first() { spec.fire = Some(parse_node_flag(fire, "--fire")?); }
    for (node, ports) in parse_port_flags(&flags.emit, "--emit", false)? {
        spec.emit.entry(node).or_default().extend(ports);
    }
    Ok(spec)
}

// ----- wires: what a run put on every output port ------------------------

/// The frames of a replay row.
fn frames_of(row: &Value) -> weft_core::frames::LoopFrames {
    row.get("frames")
        .and_then(|f| serde_json::from_value(f.clone()).ok())
        .unwrap_or_default()
}

pub fn frames_key(frames: &weft_core::frames::LoopFrames) -> String {
    frames.iter().map(|f| f.index.to_string()).collect::<Vec<_>>().join(".")
}

/// The outside facts a replay shows: every kick payload, every answer
/// a person gave (with the question the node showed them, which is
/// its firing input), and every live caller message.
#[derive(Debug, Default, Clone)]
pub struct OutsideFacts {
    pub answers: Vec<weft_core::run_spec::Answer>,
    pub caller: Vec<Value>,
}

pub fn outside_facts(rows: &[Value]) -> OutsideFacts {
    let mut facts = OutsideFacts::default();
    // node -> frames key -> the firing input the person saw.
    let mut inputs: BTreeMap<(String, String), Value> = BTreeMap::new();
    for row in rows {
        match row.get("kind").and_then(|k| k.as_str()) {
            Some("node_started") => {
                if let Some(node) = row.get("node").and_then(|n| n.as_str()) {
                    inputs.insert((node.to_string(), frames_key(&frames_of(row))), row.get("input").cloned().unwrap_or(Value::Null));
                }
            }
            Some("node_resumed") => {
                let Some(node) = row.get("node").and_then(|n| n.as_str()) else { continue };
                if row.get("token").is_some_and(|t| !t.is_null()) {
                    let frames = frames_of(row);
                    facts.answers.push(weft_core::run_spec::Answer {
                        node: node.to_string(),
                        payload: row.get("value").cloned().unwrap_or(Value::Null),
                        question: inputs.get(&(node.to_string(), frames_key(&frames))).cloned(),
                        frames,
                    });
                }
            }
            Some("caller_inbound") => facts.caller.push(row.clone()),
            _ => {}
        }
    }
    facts
}

/// One wire's difference between two runs.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct WireDiff {
    pub node: String,
    pub port: String,
    pub frames: weft_core::frames::LoopFrames,
    pub ordinal: u64,
    pub left: Option<ExpectedWire>,
    pub right: Option<ExpectedWire>,
}

/// The comparison of two wire sets: every wire that differs, and the
/// count of wires compared. Both sides go through [`normalize_media`]
/// first so a media value compares by its bytes.
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize)]
pub struct WiresDiff {
    pub compared: usize,
    pub differing: Vec<WireDiff>,
}

impl WiresDiff {
    pub fn is_same(&self) -> bool {
        self.differing.is_empty()
    }

    /// Per node: how many of its wires differ, out of how many.
    pub fn per_node(&self, all: &[ExpectedWire]) -> Vec<(String, usize, usize)> {
        let mut totals: BTreeMap<String, usize> = BTreeMap::new();
        let keys: BTreeSet<_> = all.iter().map(|wire| (wire.node.clone(), frames_key(&wire.frames), wire.port.clone(), wire.ordinal))
            .chain(self.differing.iter().map(|wire| (wire.node.clone(), frames_key(&wire.frames), wire.port.clone(), wire.ordinal))).collect();
        for (node, _, _, _) in keys {
            *totals.entry(node).or_default() += 1;
        }
        let mut diffs: BTreeMap<String, usize> = BTreeMap::new();
        for d in &self.differing {
            *diffs.entry(d.node.clone()).or_default() += 1;
        }
        diffs.into_iter().map(|(node, n)| { let total = totals.get(&node).copied().unwrap_or(n); (node, n, total) }).collect()
    }
}

pub fn diff_wires(left: &[ExpectedWire], right: &[ExpectedWire]) -> WiresDiff {
    let key = |w: &ExpectedWire| (w.node.clone(), frames_key(&w.frames), w.port.clone(), w.ordinal);
    let l: BTreeMap<_, &ExpectedWire> = left.iter().map(|w| (key(w), w)).collect();
    let r: BTreeMap<_, &ExpectedWire> = right.iter().map(|w| (key(w), w)).collect();
    let keys: std::collections::BTreeSet<_> = l.keys().chain(r.keys()).cloned().collect();
    let mut out = WiresDiff { compared: keys.len(), differing: Vec::new() };
    for k in keys {
        let (lw, rw) = (l.get(&k), r.get(&k));
        let same = match (lw, rw) {
            (Some(a), Some(b)) => a == b,
            _ => false,
        };
        if !same {
            let any = lw.or(rw).expect("a key comes from one side");
            out.differing.push(WireDiff {
                node: any.node.clone(),
                port: any.port.clone(),
                frames: any.frames.clone(),
                ordinal: any.ordinal,
                left: lw.map(|w| (*w).clone()),
                right: rw.map(|w| (*w).clone()),
            });
        }
    }
    out
}

/// Replace every stored-media reference in a wire's value by the
/// sha256 of its bytes (`{"__weft_media_sha256__": "<hash>"}`), so two
/// runs that wrote the same picture under two keys compare equal and
/// two different pictures under look-alike keys do not. A key whose id
/// segment already is a content hash (an asset) needs no download.
pub async fn normalize_media(client: &DispatcherClient, project_id: &str, wires: &mut [ExpectedWire]) -> Result<()> {
    // One cache across the whole slice: a file that flows through ten
    // nodes used to be downloaded once per wire, ten times. The key IS
    // the content's address, so two wires naming it name the same bytes.
    let mut hashes: BTreeMap<String, String> = BTreeMap::new();
    for w in wires.iter_mut() {
        w.value = normalize_value(client, project_id, std::mem::take(&mut w.value), &mut hashes).await?;
    }
    Ok(())
}

async fn normalize_value(
    client: &DispatcherClient,
    project_id: &str,
    value: Value,
    hashes: &mut BTreeMap<String, String>,
) -> Result<Value> {
    let mut keys: Vec<String> = Vec::new();
    collect_stored_keys(&value, &mut keys);
    keys.sort();
    keys.dedup();
    for key in keys {
        if hashes.contains_key(&key) {
            continue;
        }
        let hash = media_hash(client, project_id, &key).await?;
        hashes.insert(key, hash);
    }
    Ok(replace_stored(value, hashes))
}

/// Every stored-file key inside `value`, markers included at any depth.
fn collect_stored_keys(value: &Value, out: &mut Vec<String>) {
    if let Some(key) = stored_key(value) {
        out.push(key);
        return;
    }
    match value {
        Value::Object(map) => map.values().for_each(|v| collect_stored_keys(v, out)),
        Value::Array(items) => items.iter().for_each(|v| collect_stored_keys(v, out)),
        _ => {}
    }
}

/// `value` with every stored-file marker replaced by its bytes' hash.
fn replace_stored(value: Value, hashes: &BTreeMap<String, String>) -> Value {
    if let Some(key) = stored_key(&value) {
        return serde_json::json!({ "__weft_media_sha256__": hashes[&key] });
    }
    match value {
        Value::Object(map) => Value::Object(map.into_iter().map(|(k, v)| (k, replace_stored(v, hashes))).collect()),
        Value::Array(items) => Value::Array(items.into_iter().map(|v| replace_stored(v, hashes)).collect()),
        other => other,
    }
}

/// The storage key a stored-file marker carries, if `value` is one.
fn stored_key(value: &Value) -> Option<String> {
    if let Ok(weft_core::storage::media::MediaSlotContent::Stored {
        handle: weft_core::storage::FileHandle::Key(key), ..
    }) = weft_core::storage::media::classify_media_slot(value)
    {
        return Some(key);
    }
    None
}

async fn media_hash(client: &DispatcherClient, project_id: &str, key: &str) -> Result<String> {
    if let Ok(parsed) = weft_core::storage::key::parse_key(key) {
        if weft_core::storage::is_content_hash(&parsed.id) {
            return Ok(parsed.id);
        }
    }
    let bytes = crate::commands::files::download_bytes(client, key, &Some(project_id.to_string()))
        .await
        .with_context(|| format!("download {key} to compare it"))?;
    Ok(weft_core::project::hash::sha256_hex(&bytes))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn a_renamed_example_is_run_and_listed_by_its_filename() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("weft.toml"), "[package]\nname = 'test'\nid = '00000000-0000-0000-0000-000000000001'\n").unwrap();
        let project = Project::load(dir.path()).unwrap();
        std::fs::create_dir_all(examples_dir(&project)).unwrap();
        std::fs::write(spec_path(&project, "renamed"), r#"{"name":"old","from":{"start":{"value":7}}}"#).unwrap();
        let spec = read_spec(&project, "examples/renamed.json").unwrap();
        assert_eq!(spec.name, "renamed");
        assert_eq!(spec.from["start"]["value"], json!(7));
        let (listed, errors) = list_specs(&project).unwrap();
        assert!(errors.is_empty());
        assert_eq!(listed, vec![spec]);
    }

    fn version(id: &str, label: Option<&str>) -> VersionSummary {
        VersionSummary {
            id: format!("{id}00000000"),
            parent_id: None,
            label: label.map(str::to_string),
            created_at: 0,
            diff: ManifestDiff::default(),
            manifest: Default::default(),
        }
    }

    #[test]
    fn a_version_is_found_by_label_before_id_prefix() {
        let tree = Tree { head: Head::default(), versions: vec![version("aaaa", Some("try-x")), version("bbbb", Some("dup")), version("cccc", Some("dup"))], runs: vec![] };
        assert_eq!(resolve_version(&tree, "try-x").unwrap().id, "aaaa00000000");
        assert_eq!(resolve_version(&tree, "aaa").unwrap().id, "aaaa00000000");
        assert!(resolve_version(&tree, "dup").unwrap_err().to_string().contains("labels 2 versions"));
        assert!(resolve_version(&tree, "zzz").unwrap_err().to_string().contains("labelled or starts with"));
    }

    /// Every refusal has to suggest something this same rule accepts,
    /// or the person follows the message and is refused again.
    #[test]
    fn an_example_name_refusal_suggests_something_that_passes() {
        validate_example_name("chain").expect("a plain word is a name");
        validate_example_name("angry-customer").expect("so is a hyphenated one");
        // Generated rather than listed, so the property is checked
        // across the shapes rather than the handful somebody thought
        // of: every combination of a directory prefix, a stem and
        // repeated extensions.
        let mut bads: Vec<String> = Vec::new();
        for dir in ["", "d/", "d\\", "a/b/"] {
            for stem in ["", ".", "..", "x", "notes"] {
                for ext in ["", ".json", ".json.json", ".json.json.json"] {
                    let candidate = format!("{dir}{stem}{ext}");
                    if validate_example_name(&candidate).is_err() {
                        bads.push(candidate);
                    }
                }
            }
        }
        assert!(bads.len() > 20, "the generator should produce plenty of bad names: {bads:?}");
        for bad in &bads {
            let err = match validate_example_name(bad) {
                Ok(()) => panic!("{bad} must not be accepted as a name"),
                Err(e) => e.to_string(),
            };
            let quoted = err.split('`').nth(1).unwrap_or("");
            // What a usable name would be, worked out here independently
            // of the rule under test. When there is one, the message has
            // to offer THAT, not the generic form: checking only "any
            // suggestion it happens to make is accepted" passes
            // vacuously against a rule that stopped suggesting anything.
            let mut stem = bad.rsplit(['/', '\\']).next().unwrap_or("");
            while let Some(shorter) = stem.strip_suffix(".json") {
                stem = shorter;
            }
            if stem.is_empty() || stem == "." || stem == ".." {
                assert_eq!(quoted, "weft freeze <name>", "{bad} has no usable name to suggest: {err}");
            } else {
                assert_eq!(quoted, stem, "{bad} should be told to use `{stem}`: {err}");
                validate_example_name(quoted)
                    .unwrap_or_else(|_| panic!("{bad} suggested `{quoted}`, which is refused too"));
            }
        }
    }

    #[test]
    fn a_version_covers_program_files_and_not_layouts_or_the_seeded_catalog() {
        for yes in [
            "main.weft",
            "triage.weft",
            "weft.toml",
            "nodes/mine/mod.rs",
            "nodes/pkg/metadata.json",
            "prompts/p.txt",
            "scripts/s.py",
            "sql/q.sql",
            "assets/pic.png",
            "examples/angry.json",
            "sub/dir/inc.weft",
            // `target` is an ordinary word: a node, a prompt folder or
            // a script folder may be called it, and dropping those
            // would lose source on a branch back. Cargo's own output
            // is told apart by the `Cargo.toml` beside it, which the
            // walk checks and this pure rule cannot.
            "nodes/target/mod.rs",
            "prompts/target/tone.txt",
        ] {
            assert!(covers(yes), "{yes} is part of a version");
        }
        for no in [
            "layouts/main.json",
            "nodes/base_catalog/x/mod.rs",
            ".weft/target/x",
            ".git/HEAD",
            // Every hidden top-level folder, not just those two: a
            // `.weft` file under `.github` is somebody's CI, and the walk
            // pruned `.github` while this rule used to answer that the
            // file belonged to the version.
            ".github/workflows/ci.weft",
            ".vscode/settings.json",
            "target/debug/x",
            "README.md",
            "notes.txt",
            // Build output inside a node is build output too.
            "nodes/bridge/node_modules/left-pad/index.js",
        ] {
            assert!(!covers(no), "{no} is not part of a version");
        }
    }

    /// The walk's one disk-aware call: a `target/` is cargo's exactly
    /// when a `Cargo.toml` sits beside it. Told wrong in either
    /// direction it costs real files: thousands of build artifacts
    /// hashed and re-deleted on a branch back, or somebody's writing
    /// dropped out of the version that was supposed to keep it.
    #[test]
    fn a_target_folder_is_build_output_only_next_to_a_cargo_toml() {
        let dir = tempfile::tempdir().expect("tempdir");
        let root = dir.path();
        let write = |rel: &str| {
            let path = root.join(rel);
            std::fs::create_dir_all(path.parent().expect("parent")).expect("mkdir");
            std::fs::write(&path, "x").expect("write");
        };
        write("weft.toml");
        write("nodes/pkg/Cargo.toml");
        write("nodes/pkg/target/debug/build.log");
        write("nodes/pkg/node_modules/left-pad/index.js");
        write("prompts/target/tone.txt");

        let paths = covered_paths(root).expect("walk");
        assert!(paths.contains(&"prompts/target/tone.txt".to_string()), "{paths:?}");
        assert!(paths.contains(&"nodes/pkg/Cargo.toml".to_string()), "{paths:?}");
        assert!(!paths.iter().any(|p| p.starts_with("nodes/pkg/target/")), "{paths:?}");
        assert!(!paths.iter().any(|p| p.contains("node_modules")), "{paths:?}");
    }

    /// The walk prunes exactly what `covers` excludes. Two spellings of
    /// one rule is how a folder ends up silently in or out of a version,
    /// and only the walk's opinion reaches the disk.
    #[test]
    fn the_walk_prunes_what_the_rule_excludes_and_nothing_else() {
        // Every hidden top-level folder is tooling, not a version's files.
        for rel in [".weft", ".git", ".github", ".vscode", ".weft-notes", "layouts", "nodes/base_catalog", "target", "nodes/a/node_modules"] {
            assert!(prunes_whole_tree(rel), "{rel} holds nothing a version covers");
        }
        for rel in ["nodes", "prompts", "scripts", "sql", "assets", "examples"] {
            assert!(!prunes_whole_tree(rel), "{rel} can hold covered files");
        }
    }

    /// A symlink is refused by name rather than followed: one pointing at
    /// a folder above it is an endless walk, and one pointing out of the
    /// project pulls files in under a path inside it, which a branch back
    /// would write through.
    #[test]
    fn a_symlink_in_the_tree_is_refused_by_name() {
        let dir = tempfile::tempdir().expect("tempdir");
        let root = dir.path();
        std::fs::write(root.join("weft.toml"), "x").expect("write");
        std::fs::create_dir_all(root.join("prompts")).expect("mkdir");
        std::fs::write(root.join("prompts/real.txt"), "x").expect("write");
        #[cfg(unix)]
        {
            // A link a version would never take is nobody's business here,
            // and most links in a project are exactly that: refusing them
            // would refuse an ordinary project.
            std::fs::create_dir_all(root.join(".weft")).expect("mkdir");
            std::os::unix::fs::symlink(root, root.join(".weft/loop")).expect("symlink");
            std::os::unix::fs::symlink(root.join("weft.toml"), root.join("README.md")).expect("symlink");
            std::fs::create_dir_all(root.join("layouts")).expect("mkdir");
            std::os::unix::fs::symlink(root.join("weft.toml"), root.join("layouts/notes.txt"))
                .expect("symlink");
            covered_paths(root).expect("a link a version would not take is passed over");

            std::os::unix::fs::symlink(root, root.join("prompts/loop")).expect("symlink");
            let err = covered_paths(root).expect_err("a symlink is refused");
            assert!(err.to_string().contains("prompts/loop"), "{err}");
            assert!(err.to_string().contains("symlink"), "{err}");
        }
    }

    #[test]
    fn starting_values_and_fire_flags_parse_and_refuse_plainly() {
        let p = parse_port_flags(&[r#"classify={"text":"hi"}"#.into(), r#"g.inner={"count":3}"#.into(), "bare".into()], "--from", true).unwrap();
        assert_eq!(p["classify"]["text"], json!("hi"));
        assert_eq!(p["g.inner"]["count"], json!(3));
        assert!(p["bare"].is_empty());
        assert!(parse_port_flags(&["classify=hi".into()], "--from", true).unwrap_err().to_string().contains("must be JSON"));
        assert!(parse_port_flags(&["classify=1".into()], "--from", true).unwrap_err().to_string().contains("expected an object"));
        assert!(parse_port_flags(&[r#"a={"x":1}"#.into(), r#"a={"x":2}"#.into()], "--from", true).unwrap_err().to_string().contains("duplicate port"));
        assert!(parse_port_flags(&["bare".into()], "--emit", false).is_err());
        let k = parse_node_flag(r#"inbound={"a":1}"#, "--fire").unwrap();
        assert_eq!(k, ("inbound".into(), json!({"a": 1})));
        assert!(parse_node_flag("inbound", "--fire").is_err());
    }

    #[test]
    fn flags_become_a_spec_and_no_flags_become_a_whole_run() {
        let flags = RunFlags { from: vec![r#"a={"in":1}"#.into()], ..RunFlags::default() };
        let spec = spec_from_flags("one-off", &flags).unwrap();
        assert_eq!(spec.from.keys().collect::<Vec<_>>(), vec!["a"]);
        assert_eq!(spec.from["a"]["in"], json!(1));
        let spec = spec_from_flags("plain", &RunFlags::default()).unwrap();
        assert_eq!(spec, RunSpec::whole("plain"));
        let group = spec_from_flags("batch", &RunFlags { group: Some(r#"batch={"items":[1,2]}"#.into()), ..Default::default() }).unwrap();
        assert_eq!(group.group, Some(("batch".into(), [("items".into(), json!([1,2]))].into())));
        let bare = spec_from_flags("batch", &RunFlags { group: Some("batch".into()), ..Default::default() }).unwrap();
        assert_eq!(bare.group, Some(("batch".into(), BTreeMap::new())));
    }

    #[test]
    fn loaded_start_replacement_preserves_other_bounds_and_does_not_move_old_values() {
        let saved: RunSpec = serde_json::from_value(json!({
            "name":"case", "from":{"old":{"text":"saved"}}, "target":["end"],
            "before":["publish"], "caller":["history"]
        })).unwrap();
        let edited = apply_run_flags(&saved, &RunFlags { from:vec!["renamed".into()], ..Default::default() }).unwrap();
        assert_eq!(edited.from, PortValues::from([("renamed".into(), BTreeMap::new())]));
        assert_eq!(edited.target, saved.target);
        assert_eq!(edited.before, saved.before);
        assert_eq!(edited.caller, saved.caller);
        assert_eq!(saved.from["old"]["text"], json!("saved"));
    }

    #[test]
    fn explicit_clearing_happens_before_replacement_and_rejects_unknown_settings() {
        let saved: RunSpec = serde_json::from_value(json!({
            "name":"case", "from":{"old":{}}, "target":["deleted"], "group":["old_group",{}],
            "emit":{"stub":{"out":1}}, "fire":["trigger",null]
        })).unwrap();
        let edited = apply_run_flags(&saved, &RunFlags {
            clear: vec!["from", "target", "group", "emit", "fire"].into_iter().map(String::from).collect(),
            from: vec![r#"new={"text":"now"}"#.into()], ..Default::default()
        }).unwrap();
        assert_eq!(edited.from["new"]["text"], json!("now"));
        assert!(edited.target.is_empty() && edited.emit.is_empty());
        assert!(edited.group.is_none() && edited.fire.is_none());
        for field in ["input", "scope", "expected", "misspelled"] {
            assert!(apply_run_flags(&saved, &RunFlags { clear:vec![field.into()], ..Default::default() }).is_err());
        }
    }

    #[test]
    fn emitted_port_edits_merge_with_saved_values_but_duplicate_new_ports_are_errors() {
        let saved: RunSpec = serde_json::from_value(json!({"name":"case","emit":{"a":{"x":1,"y":2},"b":{"z":3}}})).unwrap();
        let edited = apply_run_flags(&saved, &RunFlags { emit:vec![r#"a={"x":9}"#.into()], ..Default::default() }).unwrap();
        assert_eq!(serde_json::to_value(&edited.emit).unwrap(), json!({"a":{"x":9,"y":2},"b":{"z":3}}));
        assert!(apply_run_flags(&saved, &RunFlags {
            emit:vec![r#"a={"x":9}"#.into(), r#"a={"x":10}"#.into()], ..Default::default()
        }).is_err());
    }

    #[test]
    fn diff_names_changed_missing_and_extra_wires_and_summarises_per_node() {
        let wire = |node: &str, port: &str, value| ExpectedWire { node: node.into(), port: port.into(), value, ..Default::default() };
        let left = vec![wire("a", "out", json!(1)), wire("a", "keep", json!(0)), wire("b", "out", json!(1))];
        let right = vec![wire("a", "out", json!(2)), wire("a", "keep", json!(0)), wire("c", "out", json!(1))];
        let diff = diff_wires(&left, &right);
        assert_eq!(diff.compared, 4);
        let names: Vec<String> = diff.differing.iter().map(|d| format!("{}.{}", d.node, d.port)).collect();
        assert_eq!(names, vec!["a.out", "b.out", "c.out"]);
        assert_eq!(diff.differing[1].right, None, "missing on the right");
        assert_eq!(diff.differing[2].left, None, "missing on the left");
        assert_eq!(diff.per_node(&left), vec![("a".to_string(), 1, 2), ("b".to_string(), 1, 1), ("c".to_string(), 1, 1)]);
        assert!(diff_wires(&left, &left).is_same());
    }

    #[test]
    fn outside_facts_pair_each_answer_with_the_question_the_node_showed() {
        let rows = vec![
            json!({ "kind": "node_started", "node": "review", "frames": [], "input": { "prompt": "ok?" } }),
            json!({ "kind": "node_suspended", "node": "review", "frames": [], "token": "t" }),
            json!({ "kind": "node_resumed", "node": "review", "frames": [], "token": "t", "value": { "answer": "yes" } }),
            json!({ "kind": "node_resumed", "node": "crashed", "frames": [], "token": null, "value": null }),
            json!({ "kind": "caller_inbound", "offset": 0, "payload": "hi" }),
        ];
        let facts = outside_facts(&rows);
        assert_eq!(facts.answers.len(), 1, "a crash re-dispatch is not an answer");
        assert_eq!(facts.answers[0].payload, json!({ "answer": "yes" }));
        assert_eq!(facts.answers[0].question, Some(json!({ "prompt": "ok?" })));
        assert_eq!(facts.caller.len(), 1);
    }

    #[test]
    fn a_stored_media_marker_is_recognised_and_a_plain_value_is_not() {
        let marker = json!({ "__weft_image__": { "key": "t/exec/c/pic", "mimeType": "image/png", "sizeBytes": 3, "filename": "pic.png" } });
        assert_eq!(stored_key(&marker), Some("t/exec/c/pic".to_string()));
        assert_eq!(stored_key(&json!({ "key": "x" })), None);
    }
}
