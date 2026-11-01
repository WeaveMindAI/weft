//! What the version-tree verbs share: the run spec on disk, the tree as
//! the dispatcher answers it, and the pure comparison of two runs'
//! wires. The verbs themselves are one file each (`checkpoint`,
//! `branch`, `tree`, `diff`, `freeze`, `examples`, `prune`, `wake`). A
//! version is made by the install, from the working copy this folder is
//! kept in sync with (`copy::this_copy`).

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use anyhow::{bail, Context, Result};
use serde_json::Value;
use weft_compiler::project::Project;
use weft_core::versions::{VersionRun, VersionSummary, VersionTree};
use weft_core::live_event::{DispatcherEvent, IdentifiedEvent};

/// An event of a run's history with its values written in, as the replay
/// answers `with_values=true`.
pub type ValuedEvent = IdentifiedEvent<DispatcherEvent<Value>>;
use weft_core::run_spec::{Expected, ExpectedWire, RunSpec};

use crate::client::DispatcherClient;

// ----- the tree as the dispatcher answers it -----------------------------

pub async fn fetch_tree(client: &DispatcherClient, project_id: &str) -> Result<VersionTree> {
    Ok(fetch_tree_raw(client, project_id).await?.0)
}

/// The tree, parsed, alongside the exact document the dispatcher sent.
///
/// `weft tree --json` forwards the server's own answer with one field
/// added, so it needs both. Fetching twice (once parsed, once raw) meant
/// two round trips whose answers could disagree: the emitted document
/// could list a version the added field was computed without.
pub async fn fetch_tree_raw(client: &DispatcherClient, project_id: &str) -> Result<(VersionTree, Value)> {
    let value = client
        .get_json(&format!("/projects/{project_id}/versions/tree"))
        .await
        .context("read the version tree")?;
    let tree: VersionTree = serde_json::from_value(value.clone()).context("parse the version tree")?;
    Ok((tree, value))
}

/// The first characters of a version id or a run, for a line.
pub fn short(id: &str) -> &str {
    &id[..id.len().min(8)]
}

/// The run `ref` names: a whole run, or the start of one among the
/// tree's runs.
pub fn resolve_run<'a>(tree: &'a VersionTree, reference: &str) -> Result<&'a VersionRun> {
    let matches: Vec<&VersionRun> = tree.runs.iter().filter(|r| r.run_id.to_string().starts_with(reference)).collect();
    match matches.as_slice() {
        [one] => Ok(one),
        [] => bail!("no run starts with {reference} in this project's tree; `weft tree` lists them"),
        many => bail!(
            "{reference} names {} runs ({}); give more characters",
            many.len(),
            some_ids(many.iter().map(|r| r.run_id.to_string()))
        ),
    }
}

/// How many ambiguous matches an error names before it stops. An empty
/// or one-character reference matches every run in the tree, and a
/// hundred ids scroll the instruction that follows them off the screen.
const AMBIGUOUS_SHOWN: usize = 5;

/// The first few ids, shortened, with an ellipsis when there are more.
/// One definition, because all three resolvers below report the same
/// way and a list that is short in one of them is short in all.
fn some_ids<S: AsRef<str>>(ids: impl Iterator<Item = S>) -> String {
    let mut shown: Vec<S> = Vec::new();
    let mut total = 0usize;
    for id in ids {
        total += 1;
        if shown.len() < AMBIGUOUS_SHOWN {
            shown.push(id);
        }
    }
    let list = shown.iter().map(|id| short(id.as_ref())).collect::<Vec<_>>().join(", ");
    if total > AMBIGUOUS_SHOWN { format!("{list}, ...") } else { list }
}

/// The version `ref` names: a checkpoint label, a whole id, or the
/// start of one. A label wins over an id prefix, since a label is a
/// word somebody chose and an id is hex.
pub fn resolve_version<'a>(tree: &'a VersionTree, reference: &str) -> Result<&'a VersionSummary> {
    let labelled: Vec<&VersionSummary> = tree.versions.iter().filter(|v| v.label.as_deref() == Some(reference)).collect();
    match labelled.as_slice() {
        [one] => return Ok(one),
        [] => {}
        many => bail!(
            "{reference} labels {} versions ({}); name one by its id",
            many.len(),
            some_ids(many.iter().map(|v| v.id.as_str()))
        ),
    }
    let matches: Vec<&VersionSummary> = tree.versions.iter().filter(|v| v.id.starts_with(reference)).collect();
    match matches.as_slice() {
        [one] => Ok(one),
        [] => bail!("no version is labelled or starts with {reference} in this project's tree; `weft tree` lists them"),
        many => bail!(
            "{reference} names {} versions ({}); give more characters",
            many.len(),
            some_ids(many.iter().map(|v| v.id.as_str()))
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
    read_spec_if_present(project, name)?
        .with_context(|| format!("no spec at {}", spec_file_path(project, name).display()))
}

/// The rows of a run's journal, as the replay endpoint answers them, with
/// their values written in.
///
/// One reader, because three commands hand-rolled the same four steps
/// (`GET .../replay`, take the array, fold it into wires, resolve the
/// stored media) and the ones that also need the raw rows had to keep
/// their own copy of the first half.
pub async fn replay_rows(
    client: &crate::client::DispatcherClient,
    run_id: &str,
) -> Result<Vec<ValuedEvent>> {
    let rows = client
        .get_json(&format!("/runs/{run_id}/replay?with_values=true"))
        .await
        .context("read the run")?;
    crate::client::read_answer(rows, "the run's events")
}

/// What every wire of a run carried, with stored media resolved to its
/// bytes so two runs' values compare as values. `expect` names the places
/// whose outputs a diff puts first, each read against the program the run
/// ran and answered in `focus` (`weft freeze --expect`).
pub async fn output_wires(
    client: &crate::client::DispatcherClient,
    run_id: &str,
    expect: &[String],
) -> Result<Expected> {
    let query = match expect {
        [] => String::new(),
        names => format!("?expect={}", super::runs::query_escaped(&names.join(","))),
    };
    let mut expected: Expected = serde_json::from_value(client.get_json(&format!("/runs/{run_id}/outputs{query}")).await?)
        .context("decode complete output history")?;
    normalize_media(client, &mut expected.wires).await?;
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
///
/// A file an older weft wrote reads as today's spec, and the file is left
/// as it is ([`RunSpec::parse_file`]): reading an example never changes
/// the project. It takes today's shape when weft writes it ([`write_spec`]).
///
/// The spec is named after its file.
pub fn read_spec_if_present(project: &Project, name: &str) -> Result<Option<RunSpec>> {
    let path = spec_file_path(project, name);
    let text = match std::fs::read_to_string(&path) {
        Ok(t) => t,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e).with_context(|| format!("read {}", path.display())),
    };
    let mut spec = RunSpec::parse_file(&text).map_err(|e| anyhow::anyhow!("parse {}: {e}", path.display()))?;
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

/// Write `spec` as `examples/<name>.json`, answering where.
pub fn write_spec(project: &Project, spec: &RunSpec) -> Result<PathBuf> {
    validate_example_name(&spec.name)?;
    let path = spec_path(project, &spec.name);
    std::fs::create_dir_all(examples_dir(project))?;
    let text = spec.file_text().map_err(|e| anyhow::anyhow!("write {}: {e}", path.display()))?;
    std::fs::write(&path, &text).with_context(|| format!("write {}", path.display()))?;
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
        match std::fs::read_to_string(&path)
            .map_err(|e| e.to_string())
            .and_then(|text| RunSpec::parse_file(&text).map_err(|e| format!("is not a spec ({e})")))
        {
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

// ----- wires: what a run put on every output port ------------------------

pub fn frames_key(frames: &weft_core::frames::LoopFrames) -> String {
    weft_core::frames::frames_text(frames)
}

/// The outside facts a replay shows: how every wait ended (a person's
/// answer or skip, or the run giving it up, with the question the node
/// showed, which is its firing input), and every live caller message.
#[derive(Debug, Default, Clone)]
pub struct OutsideFacts {
    pub answers: Vec<weft_core::run_spec::Answer>,
    pub caller: Vec<Value>,
}

pub fn outside_facts(rows: &[ValuedEvent]) -> Result<OutsideFacts> {
    let mut facts = OutsideFacts::default();
    // The program's messages the caller never received, and why: a
    // window names those a caller reading too slowly lost (possibly ones
    // an earlier window carried), the row saying the caller left names
    // those still waiting then. All are known before any message is
    // written down.
    let dropped: BTreeMap<u64, &str> = rows
        .iter()
        .flat_map(|row| match &row.event {
            DispatcherEvent::CallerWindow { dropped, .. } => dropped.iter().map(|o| (*o, "caller_too_slow")).collect(),
            DispatcherEvent::CallerDisconnected { dropped, .. } => dropped.iter().map(|o| (*o, "exchange_ended")).collect(),
            _ => Vec::new(),
        })
        .collect();
    // node -> frames key -> the firing input the person saw.
    let mut inputs: BTreeMap<(String, String), Value> = BTreeMap::new();
    for row in rows {
        match &row.event {
            DispatcherEvent::NodeStarted { node, frames, input, .. } => {
                inputs.insert((node.clone(), frames_key(frames)), serde_json::to_value(input).context("write down a firing's input")?);
            }
            // Every wait, the ones held in the worker included (those
            // never suspend or resume the node, so only this event sees
            // them): a replay ends each the same way.
            DispatcherEvent::WaitEnded { node, frames, ended, .. } => {
                facts.answers.push(weft_core::run_spec::Answer {
                    node: node.clone(),
                    ended: ended.clone(),
                    question: inputs.get(&(node.clone(), frames_key(frames))).cloned(),
                    frames: frames.clone(),
                });
            }
            // BOTH directions of the exchange, one entry per message.
            // The journal folds a window of the conversation into a
            // single row, so the messages come out of that row's
            // `messages` list rather than one row each; each carries
            // its own `direction`, which is what makes the answer
            // readable beside the request. Recording the window rows
            // whole instead would put the journal's batching into a
            // file a person reads. A message the caller never received
            // says so and why (`"dropped": "caller_too_slow"` or
            // `"exchange_ended"`).
            DispatcherEvent::CallerWindow { messages, .. } => {
                for message in messages {
                    let mut written = serde_json::to_value(message).context("write down a caller message")?;
                    if let Some(why) = dropped.get(&message.offset) {
                        written["dropped"] = Value::String((*why).to_string());
                    }
                    facts.caller.push(written);
                }
            }
            _ => {}
        }
    }
    Ok(facts)
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
pub async fn normalize_media(client: &DispatcherClient, wires: &mut [ExpectedWire]) -> Result<()> {
    // One cache across the whole slice: a file that flows through ten
    // nodes used to be downloaded once per wire, ten times. The key IS
    // the content's address, so two wires naming it name the same bytes.
    let mut hashes: BTreeMap<String, String> = BTreeMap::new();
    for w in wires.iter_mut() {
        w.value = normalize_value(client, std::mem::take(&mut w.value), &mut hashes).await?;
    }
    Ok(())
}

async fn normalize_value(
    client: &DispatcherClient,
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
        let hash = media_hash(client, &key).await?;
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
        handle: weft_core::storage::FileHandle::Key { key, .. }, ..
    }) = weft_core::storage::media::classify_media_slot(value)
    {
        return Some(key);
    }
    None
}

async fn media_hash(client: &DispatcherClient, key: &str) -> Result<String> {
    if let Ok(parsed) = weft_core::storage::key::parse_key(key) {
        if weft_core::storage::is_content_hash(&parsed.id) {
            return Ok(parsed.id);
        }
    }
    let bytes = crate::commands::files::download_bytes(client, key)
        .await
        .with_context(|| format!("download {key} to compare it"))?;
    Ok(weft_core::project::hash::sha256_hex(&bytes))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use weft_core::versions::ManifestDiff;
    use weft_core::primitive::AwaitEnd;

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

    /// A spec an older weft saved, with keys spelled the old way, reads
    /// as today's everywhere it is read (a run, `weft examples`, `weft
    /// prune`, `weft diff`), and reading leaves the file as it is: the
    /// project's files, and so its version, do not change. The file takes
    /// today's shape when weft writes it (`weft run --save`, `weft freeze`).
    #[test]
    fn a_spec_an_older_weft_saved_reads_as_todays_and_is_left_as_it_is() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("weft.toml"), "[package]\nname = 'test'\nid = '00000000-0000-0000-0000-000000000001'\n").unwrap();
        let project = Project::load(dir.path()).unwrap();
        std::fs::create_dir_all(examples_dir(&project)).unwrap();
        let path = spec_path(&project, "old");
        let old = r#"{"name":"old","frozen_from":{"version":"v","execution_id":"00000000-0000-0000-0000-000000000002","definition_hash":"h"},"settings":{"keep_for":"1d"}}"#;
        std::fs::write(&path, old).unwrap();
        let spec = read_spec(&project, "old").unwrap();
        let frozen = spec.frozen_from.as_ref().expect("the frozen run carried over");
        assert_eq!(frozen.definition_hash, "h");
        let (listed, unreadable) = list_specs(&project).unwrap();
        assert!(unreadable.is_empty(), "{unreadable:?}");
        assert_eq!(listed, vec![spec.clone()]);
        assert_eq!(read_spec_if_present(&project, "old").unwrap().as_ref(), Some(&spec));
        assert_eq!(std::fs::read_to_string(&path).unwrap(), old, "reading never rewrites the file");

        // Read-only, the file still reads: a read needs no write access.
        let mut readonly = std::fs::metadata(&path).unwrap().permissions();
        readonly.set_readonly(true);
        std::fs::set_permissions(&path, readonly.clone()).unwrap();
        assert_eq!(list_specs(&project).unwrap().0, vec![spec.clone()]);
        #[allow(clippy::permissions_set_readonly_false)]
        readonly.set_readonly(false);
        std::fs::set_permissions(&path, readonly).unwrap();

        assert_eq!(write_spec(&project, &spec).unwrap(), path);
        let written = std::fs::read_to_string(&path).unwrap();
        assert_eq!(written, spec.file_text().unwrap(), "the file now holds today's shape");
        assert!(!written.contains("frozen_from") && written.contains("\"specVersion\""), "{written}");
        assert_eq!(read_spec(&project, "old").unwrap(), spec);
    }

    /// An example file that is there but does not parse is an error naming
    /// the file, never an absence: `weft freeze` and `weft run --save`
    /// write over an absent example, and would destroy one whose only
    /// problem was a typo. Reading leaves the file as the person left it.
    #[test]
    fn an_example_that_does_not_parse_is_an_error_and_is_left_alone() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("weft.toml"), "[package]\nname = 'test'\nid = '00000000-0000-0000-0000-000000000001'\n").unwrap();
        let project = Project::load(dir.path()).unwrap();
        std::fs::create_dir_all(examples_dir(&project)).unwrap();
        let path = spec_path(&project, "chain");
        let typo = "{ \"name\": \"chain\", \"target\": [\"out\"],, }";
        std::fs::write(&path, typo).unwrap();
        let why = format!("{:#}", read_spec_if_present(&project, "chain").unwrap_err());
        assert!(why.contains("parse") && why.contains("chain.json"), "{why}");
        assert_eq!(std::fs::read_to_string(&path).unwrap(), typo);

        std::fs::write(&path, "{ \"name\": \"chain\" }").unwrap();
        assert_eq!(read_spec_if_present(&project, "chain").unwrap().map(|spec| spec.name).as_deref(), Some("chain"));
        assert!(read_spec_if_present(&project, "absent").unwrap().is_none(), "only a missing file is an absence");
    }

    fn version(id: &str, label: Option<&str>) -> VersionSummary {
        VersionSummary {
            id: format!("{id}00000000"),
            parent_id: None,
            label: label.map(str::to_string),
            created_at: 0,
            diff: ManifestDiff::default(),
            manifest: Default::default(),
            trigger_runs: 0,
            last_trigger_run: None,
        }
    }

    #[test]
    fn a_version_is_found_by_label_before_id_prefix() {
        let tree = VersionTree { working_copies: vec![], activated_versions: vec![], versions: vec![version("aaaa", Some("try-x")), version("bbbb", Some("dup")), version("cccc", Some("dup"))], runs: vec![] };
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
    fn outside_facts_record_how_each_wait_ended_with_the_question_the_node_showed() {
        // Each row as the replay answers it: the run's own fields around
        // the kind's.
        let row = |mut fields: Value| -> ValuedEvent {
            let fields_of = fields.as_object_mut().unwrap();
            fields_of.insert("eventId".into(), json!("journal:1:0:0"));
            fields_of.insert("runId".into(), json!(uuid::Uuid::nil()));
            fields_of.insert("projectId".into(), json!(uuid::Uuid::nil()));
            fields_of.insert("atUnix".into(), json!(1));
            serde_json::from_value(fields).unwrap()
        };
        let rows = vec![
            row(json!({ "kind": "node_started", "node": "review", "frames": [], "input": { "prompt": "ok?" }, "closedPorts": [] })),
            row(json!({ "kind": "node_suspended", "node": "review", "frames": [], "token": "t" })),
            row(json!({ "kind": "wait_ended", "node": "review", "frames": [], "token": "t", "ended": { "type": "answered", "value": { "answer": "yes" } } })),
            row(json!({ "kind": "node_resumed", "node": "review", "frames": [], "token": "t" })),
            row(json!({ "kind": "node_resumed", "node": "crashed", "frames": [], "token": null })),
            row(json!({ "kind": "wait_ended", "node": "skipped", "frames": [], "token": "t3", "ended": { "type": "skipped" } })),
            // A wait held in the worker that gave up: no suspend, no
            // resume, only its ending and the node failing with its error.
            row(json!({ "kind": "node_started", "node": "held", "frames": [], "input": { "prompt": "still there?" }, "closedPorts": [] })),
            row(json!({ "kind": "wait_ended", "node": "held", "frames": [], "token": "t2", "ended": { "type": "gave_up", "error": "node 'held' gave up its wait: quiet" } })),
            row(json!({ "kind": "node_failed", "node": "held", "frames": [], "error": "node 'held' gave up its wait: quiet" })),
            // One window row carrying the whole short conversation,
            // which is how the journal writes it: the messages come out
            // of the row, not one row each.
            row(json!({ "kind": "caller_window", "firstOffset": 0, "lastOffset": 1, "messages": [
                { "offset": 0, "direction": "inbound", "payload": { "kind": "json", "data": "hi" }, "payloadByteSize": 4, "atUnix": 1 },
                { "offset": 1, "direction": "outbound", "payload": { "kind": "json", "data": "bye" }, "payloadByteSize": 5, "terminal": true, "atUnix": 1 },
            ], "totals": [] })),
            // A later window whose chunk pushed the first one out before
            // the caller read it.
            row(json!({ "kind": "caller_window", "firstOffset": 2, "lastOffset": 3, "messages": [
                { "offset": 2, "direction": "outbound", "payload": { "kind": "json", "data": "late" }, "payloadByteSize": 6, "atUnix": 1 },
                { "offset": 3, "direction": "outbound", "payload": { "kind": "json", "data": "later" }, "payloadByteSize": 7, "atUnix": 1 },
            ], "totals": [], "dropped": [1] })),
            // The caller left with the last one still waiting to go out.
            row(json!({ "kind": "caller_disconnected", "offset": 4, "reason": "caller hung up", "dropped": [3] })),
        ];
        let facts = outside_facts(&rows).unwrap();
        assert_eq!(facts.answers.len(), 3, "one per wait that ended; a boundary re-fire is none");
        assert_eq!(facts.answers[0].ended, AwaitEnd::Answered { value: json!({ "answer": "yes" }) });
        assert_eq!(facts.answers[0].question, Some(json!({ "prompt": "ok?" })));
        assert_eq!(facts.answers[1].node, "skipped");
        assert_eq!(facts.answers[1].ended, AwaitEnd::Skipped);
        assert_eq!(facts.answers[2].node, "held");
        assert_eq!(facts.answers[2].ended, AwaitEnd::GaveUp { error: "node 'held' gave up its wait: quiet".into() });
        assert_eq!(facts.answers[2].question, Some(json!({ "prompt": "still there?" })), "a wait given up keeps the question it asked");
        assert_eq!(facts.caller.len(), 4, "both halves of the exchange, request AND answer");
        let directions: Vec<&str> =
            facts.caller.iter().filter_map(|m| m["direction"].as_str()).collect();
        assert_eq!(directions, vec!["inbound", "outbound", "outbound", "outbound"], "each message says which way it went");
        let dropped: Vec<Option<&str>> = facts.caller.iter().map(|m| m.get("dropped").and_then(Value::as_str)).collect();
        assert_eq!(
            dropped,
            vec![None, Some("caller_too_slow"), None, Some("exchange_ended")],
            "a message the record names as never received says so, and why"
        );
    }

    #[test]
    fn a_stored_media_marker_is_recognised_and_a_plain_value_is_not() {
        let marker = json!({ "__weft_image__": { "key": "t/run/c/pic", "mimeType": "image/png", "sizeBytes": 3, "filename": "pic.png" } });
        assert_eq!(stored_key(&marker), Some("t/run/c/pic".to_string()));
        assert_eq!(stored_key(&json!({ "key": "x" })), None);
    }
}
