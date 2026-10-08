//! What the install's version-tree endpoints answer and the CLI reads
//! (`/projects/{id}/versions/...`): one Rust type per message, so the
//! two ends cannot drift.

use serde::{Deserialize, Serialize};

use crate::exec::CancelCause;
use crate::project::hash::Manifest;
use crate::run_spec::RunSpec;
use crate::ExecutionId;

/// The project's head: where the next version parents and the next
/// seed comes from, plus the versions the triggers are activated on.
// SYNC: Head <-> extension-vscode/src/sidebar/version-tree.ts TreeJson.head
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Head {
    pub head_version: Option<String>,
    pub head_run: Option<ExecutionId>,
    /// Every version some activation's listeners run (one per trigger
    /// and owner at most, deduplicated, sorted). Empty while nothing
    /// listens.
    #[serde(default)]
    pub activated_versions: Vec<String>,
}

/// The paths that differ between two versions: added, removed, and
/// changed, sorted. What `weft tree` prints next to a version.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestDiff {
    pub added: Vec<String>,
    pub removed: Vec<String>,
    pub changed: Vec<String>,
}

/// What changed from `parent` to `child`.
pub fn manifest_diff(parent: &Manifest, child: &Manifest) -> ManifestDiff {
    let mut diff = ManifestDiff::default();
    for (path, hash) in child {
        match parent.get(path) {
            None => diff.added.push(path.clone()),
            Some(h) if h != hash => diff.changed.push(path.clone()),
            Some(_) => {}
        }
    }
    for path in parent.keys() {
        if !child.contains_key(path) {
            diff.removed.push(path.clone());
        }
    }
    diff
}

// SYNC: VersionSummary <-> extension-vscode/src/sidebar/version-tree.ts VersionSummary
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VersionSummary {
    pub id: String,
    pub parent_id: Option<String>,
    pub label: Option<String>,
    pub created_at: u64,
    /// What changed against the parent (empty on a root).
    pub diff: ManifestDiff,
    pub manifest: Manifest,
    /// How many runs the project's triggers started on this version (each
    /// one too many to list), and the newest.
    #[serde(default)]
    pub trigger_runs: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_trigger_run: Option<ExecutionId>,
}

// SYNC: RunSummary <-> extension-vscode/src/sidebar/version-tree.ts RunSummary
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunSummary {
    pub execution_id: ExecutionId,
    pub version_id: String,
    /// The program this run executed. `weft freeze` records it on the
    /// example so a later reader can tell which definition the frozen
    /// wires came from.
    pub definition_hash: String,
    pub seed_execution_id: Option<ExecutionId>,
    pub stale: Vec<String>,
    pub spec: Option<RunSpec>,
    pub example: Option<String>,
    /// The execution's status as the executions list reports it, or
    /// `None` when its journal is gone.
    // SYNC: RunSummary.status <-> extension-vscode/src/sidebar/version-tree.ts RunSummary.status
    pub status: Option<crate::program::RunStatus>,
    pub started_at: u64,
    pub completed_at: Option<u64>,
    /// For a cancelled run, who or what stopped it.
    #[serde(default)]
    pub cancel_cause: Option<CancelCause>,
    /// Node firings the run skipped because something upstream closed:
    /// the number a person wants when a run says it completed and the
    /// thing they were waiting for never happened.
    #[serde(default)]
    pub skipped_nodes: u64,
}

/// `GET /projects/{id}/versions/tree`.
// SYNC: VersionTree <-> extension-vscode/src/sidebar/version-tree.ts TreeJson
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VersionTree {
    pub head: Head,
    pub versions: Vec<VersionSummary>,
    pub runs: Vec<RunSummary>,
}

/// `GET /projects/{id}/versions/running`: the files of the program this
/// install holds, and the version they are.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunningSource {
    pub version: String,
    pub manifest: Manifest,
}

/// What a started run answers (`POST /projects/{id}/versions/run`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunStarted {
    pub execution_id: ExecutionId,
    pub version: String,
    pub seed: Option<ExecutionId>,
    /// Nodes taken from the seed, sorted.
    pub inherited: Vec<String>,
    /// Nodes this run executes, sorted.
    pub ran: Vec<String>,
    pub warnings: Vec<String>,
}

/// `POST /projects/{id}/versions`: record the files as a version under
/// head (no run, no build) and move head there.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointRequest {
    pub manifest: Manifest,
    #[serde(default)]
    pub label: Option<String>,
    /// Start a new tree: the version parents on nothing.
    #[serde(default)]
    pub root: bool,
}

/// What a checkpoint answers: the version the files are.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VersionUpsert {
    pub version: String,
    /// `false` when the project already had this exact code.
    pub created: bool,
    pub parent: Option<String>,
}

/// `POST /projects/{id}/versions/runs`: the run that records itself.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VersionRunRequest {
    pub manifest: Manifest,
    /// The definition the CLI just registered; the run refuses if the
    /// project's running definition moved since.
    #[serde(rename = "definitionHash")]
    pub definition_hash: String,
    #[serde(rename = "binaryHash")]
    pub binary_hash: String,
    #[serde(default)]
    pub seed: bool,
    #[serde(default, rename = "seedUntil")]
    pub seed_until: Vec<String>,
    #[serde(default, rename = "seedBefore")]
    pub seed_before: Vec<String>,
    #[serde(default)]
    pub root: bool,
    /// `None` is a plain whole-graph run.
    #[serde(default)]
    pub spec: Option<RunSpec>,
    /// The saved example whose starting parameters this run uses, if any.
    #[serde(default)]
    pub example: Option<String>,
}

/// `PUT /projects/{id}/versions/head`: move head to a run (its version,
/// with the run as the next seed) or to a bare version. A run wins when
/// both are named.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct HeadRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run: Option<ExecutionId>,
}

/// Where head went, with that version's files so `weft branch` can
/// restore them.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeadResponse {
    pub version: String,
    pub run: Option<ExecutionId>,
    pub manifest: Manifest,
}

/// `PUT /projects/{id}/versions/runs/{execution_id}`: change what a
/// recorded run remembers. A field left out is kept.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunUpdate {
    /// Absent: keep it. `null`: clear it. A name: the saved example
    /// this run was frozen as.
    #[serde(default, deserialize_with = "present", skip_serializing_if = "Option::is_none")]
    pub example: Option<Option<String>>,
}

/// A field the caller may leave alone, set, or clear.
///
/// `None` is "the key was absent, keep what is recorded"; `Some(None)`
/// is an explicit `null`, "clear it".
fn present<'de, D, T>(d: D) -> Result<Option<T>, D::Error>
where
    T: Deserialize<'de>,
    D: serde::Deserializer<'de>,
{
    Ok(Some(T::deserialize(d)?))
}

/// What `weft prune <version>` removes: the subtree's versions, every
/// run under them, and the blobs no surviving manifest names.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrunePlan {
    pub versions: Vec<String>,
    pub runs: Vec<ExecutionId>,
    /// `sha256` of every blob only the pruned versions held.
    pub blobs: Vec<String>,
}

/// `DELETE /projects/{id}/versions/{version}`: the plan, and whether it
/// was carried out (`false` under `?plan=true`). After a delete the plan
/// is what was actually removed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PruneResponse {
    pub plan: PrunePlan,
    pub deleted: bool,
}

/// `POST /projects/{id}/versions/sweep`: the bare versions it dropped.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SweepResponse {
    pub swept: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manifest_diff_names_every_kind_of_change() {
        let manifest = |entries: &[(&str, &str)]| -> Manifest {
            entries.iter().map(|(p, h)| (p.to_string(), h.to_string())).collect()
        };
        let parent = manifest(&[("a", "1"), ("b", "2"), ("c", "3")]);
        let child = manifest(&[("a", "1"), ("b", "X"), ("d", "4")]);
        let diff = manifest_diff(&parent, &child);
        assert_eq!(diff.added, vec!["d"]);
        assert_eq!(diff.removed, vec!["c"]);
        assert_eq!(diff.changed, vec!["b"]);
    }

    #[test]
    fn a_head_move_names_only_where_it_goes() {
        let run = ExecutionId::nil();
        let body = serde_json::to_value(HeadRequest { run: Some(run), version: None }).unwrap();
        assert_eq!(body, serde_json::json!({ "run": run }));
        let back: HeadRequest = serde_json::from_value(serde_json::json!({ "version": "v1" })).unwrap();
        assert_eq!((back.version.as_deref(), back.run), (Some("v1"), None));
    }

    #[test]
    fn a_run_update_tells_keep_from_clear_from_set() {
        let read = |v: serde_json::Value| serde_json::from_value::<RunUpdate>(v).unwrap().example;
        assert_eq!(read(serde_json::json!({})), None);
        assert_eq!(read(serde_json::json!({ "example": null })), Some(None));
        assert_eq!(read(serde_json::json!({ "example": "x" })), Some(Some("x".into())));
        let keep = serde_json::to_value(RunUpdate { example: None }).unwrap();
        assert_eq!(keep, serde_json::json!({}));
        let set = serde_json::to_value(RunUpdate { example: Some(Some("x".into())) }).unwrap();
        assert_eq!(set, serde_json::json!({ "example": "x" }));
    }

    #[test]
    fn a_run_request_round_trips_its_camel_case_keys() {
        let body = VersionRunRequest {
            manifest: Manifest::new(),
            definition_hash: "d".into(),
            binary_hash: "b".into(),
            seed: true,
            seed_until: vec!["n".into()],
            seed_before: Vec::new(),
            root: false,
            spec: None,
            example: Some("e".into()),
        };
        let wire = serde_json::to_value(&body).unwrap();
        for key in ["definitionHash", "binaryHash", "seedUntil", "seedBefore"] {
            assert!(wire.get(key).is_some(), "{key} in {wire}");
        }
        let back: VersionRunRequest = serde_json::from_value(wire).unwrap();
        assert_eq!((back.definition_hash.as_str(), back.seed_until.len(), back.example.as_deref()), ("d", 1, Some("e")));
    }
}
