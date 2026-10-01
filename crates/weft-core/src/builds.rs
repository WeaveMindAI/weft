//! What the install's build endpoint (`POST /projects/{id}/builds`)
//! reads and answers, and the CLI sends and reads: one Rust type per
//! message, so the two ends cannot drift.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// What a build produced: the compiled program, its three hashes and the
/// implementations its worker carries, and the infra image every infra
/// place resolves to. The install registers all of it in one transaction
/// and answers it; downstream verbs name the build by its hashes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BuiltProgram {
    pub definition: crate::ProjectDefinition,
    /// The worker image's identity.
    #[serde(rename = "binaryHash")]
    pub binary_hash: String,
    /// The runtime shape; a resync compares it.
    #[serde(rename = "definitionHash")]
    pub definition_hash: String,
    /// The infra closure; an upgrade compares it.
    #[serde(rename = "infraHash")]
    pub infra_hash: String,
    pub implementations: BTreeMap<String, String>,
    /// `place -> { image name -> image ref }`, keyed the way each infra
    /// place's row is (`one.db`), which is how the supervisor reads it.
    #[serde(rename = "infraImages")]
    pub infra_images: BTreeMap<String, BTreeMap<String, String>>,
    /// The image refs this build had to build (absent from the registry
    /// when it began); empty when every image was already there.
    #[serde(rename = "builtImages")]
    pub built_images: Vec<String>,
    /// The infra places whose images differ from the ones the build
    /// before registered: a new copy of one starts on the new image, while
    /// a copy already running keeps its own until it is upgraded (`weft
    /// infra upgrade`). Filled at registration, which knows what came
    /// before.
    #[serde(rename = "replacedInfraImages", default)]
    pub replaced_infra_images: Vec<String>,
}

impl BuiltProgram {
    /// This build, as a later verb names it.
    pub fn named(&self) -> BuildHashes {
        BuildHashes {
            binary_hash: Some(self.binary_hash.clone()),
            definition_hash: Some(self.definition_hash.clone()),
            infra_hash: Some(self.infra_hash.clone()),
        }
    }
}

/// The build a verb names (an activate, an infra sync), flattened into its
/// body. The install runs what the last build registered; a hash named
/// here only checks the caller means that build, and the verb is refused
/// when it names another (a teammate's build landed in between). A hash
/// left out is not checked, and a body naming none applies the registered
/// build as it is.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BuildHashes {
    #[serde(default, rename = "binaryHash", skip_serializing_if = "Option::is_none")]
    pub binary_hash: Option<String>,
    #[serde(default, rename = "definitionHash", skip_serializing_if = "Option::is_none")]
    pub definition_hash: Option<String>,
    #[serde(default, rename = "infraHash", skip_serializing_if = "Option::is_none")]
    pub infra_hash: Option<String>,
}

/// Which catalog node types a worker binary compiles in.
///
/// `Referenced` is the ordinary build: the types the program names, so
/// the binary is as small as the program. `Full` compiles every node
/// in the catalog, so a program edit that starts using a node the
/// previous program did not never rebuilds the image: the build is
/// content-addressed either way (the compiler's `compute_binary_hash` folds the same
/// set plus this choice), so a full image rebuilds only when a node
/// source was added, removed, or edited. CLI builds default to `Full`;
/// `--referenced` selects only the graph's types.
/// On the wire (the build request) as `"referenced"` / `"full"`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeSet {
    Referenced,
    Full,
}

impl NodeSet {
    /// The line the binary hash folds so a full image and a referenced
    /// image of one program never share a tag.
    pub fn hash_marker(self) -> &'static str {
        match self {
            NodeSet::Referenced => "referenced",
            NodeSet::Full => "full",
        }
    }
}

/// `POST /projects/{id}/builds`: build this version of the project.
/// What the CLI sends and the install reads.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VersionBuildRequest {
    /// The project's name, as its `weft.toml` says.
    pub name: String,
    /// The version: every covered file's hash. Every blob is already in
    /// the tenant's assets (the CLI's snapshot put them there).
    pub manifest: crate::project::hash::Manifest,
    /// Compile every catalog node into the worker (`full`) or only the
    /// ones the program references (`referenced`).
    #[serde(rename = "nodeSet")]
    pub node_set: NodeSet,
    /// `@asset` resolutions the author's machine produced, by resolution
    /// key (`FileRef::resolution_key`).
    #[serde(default)]
    pub assets: BTreeMap<String, serde_json::Value>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_built_program_round_trips_under_its_wire_names() {
        let built = BuiltProgram {
            definition: serde_json::from_value(serde_json::json!({
                "id": "00000000-0000-0000-0000-000000000001", "nodes": [], "edges": []
            }))
            .unwrap(),
            binary_hash: "b".into(),
            definition_hash: "d".into(),
            infra_hash: "i".into(),
            implementations: [("Llm".to_string(), "h".to_string())].into(),
            infra_images: [("one.db".to_string(), [("db".to_string(), "r/db:1".to_string())].into())].into(),
            built_images: vec!["r/db:1".into()],
            replaced_infra_images: vec!["one.db".into()],
        };
        let wire = serde_json::to_value(&built).unwrap();
        for key in ["binaryHash", "definitionHash", "infraHash", "infraImages", "builtImages", "replacedInfraImages"] {
            assert!(wire.get(key).is_some(), "{key} missing from {wire}");
        }
        let back: BuiltProgram = serde_json::from_value(wire.clone()).unwrap();
        assert_eq!(serde_json::to_value(&back).unwrap(), wire);
    }
}
