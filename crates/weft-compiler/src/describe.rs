//! Catalog introspection for tooling (Tangle, VS Code extension,
//! dashboard). Reads `metadata.json` (and the optional sibling
//! `form_field_specs.json`) from every node available in the
//! project scope (stdlib + user `nodes/` + `nodes/vendor/`) and
//! emits a unified description.
//!
//! Must work on partially-written user nodes: if `metadata.json` is
//! absent or malformed, the node is skipped and a warning is added
//! to the result. The IDE / Tangle live-edit case is the sole
//! reason for this softness: a user mid-rename has a transient
//! `metadata.json` parse error that should not crash the editor's
//! catalog refresh. Build-time and CI paths surface the warnings to
//! the user; they are not silent.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::CompileResult;

/// One node, with the spec the form_builder editor needs to drive
/// its UI for nodes whose `features.has_form_schema` is true.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DescribedNode {
    pub metadata: weft_core::NodeMetadata,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub form_field_specs: Vec<weft_core::node::FormFieldSpec>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatalogDescription {
    pub nodes: Vec<DescribedNode>,
    pub warnings: Vec<String>,
}

/// Scan `{project_root}/nodes/` recursively for any subdirectory
/// containing `metadata.json` and collect those into a catalog.
/// Malformed metadata.json files are skipped with a warning.
pub fn describe_project(project_root: &Path) -> CompileResult<CatalogDescription> {
    let nodes_dir = project_root.join("nodes");
    if !nodes_dir.is_dir() {
        return Ok(CatalogDescription { nodes: Vec::new(), warnings: Vec::new() });
    }
    let mut nodes = Vec::new();
    let mut warnings = Vec::new();
    walk(&nodes_dir, &mut nodes, &mut warnings);
    Ok(CatalogDescription { nodes, warnings })
}

fn walk(dir: &Path, nodes: &mut Vec<DescribedNode>, warnings: &mut Vec<String>) {
    let metadata = dir.join("metadata.json");
    if metadata.is_file() {
        match std::fs::read_to_string(&metadata) {
            Ok(text) => match serde_json::from_str::<weft_core::NodeMetadata>(&text) {
                Ok(m) => {
                    let specs = load_form_specs(dir, warnings);
                    nodes.push(DescribedNode {
                        metadata: m,
                        form_field_specs: specs,
                    });
                }
                Err(err) => warnings.push(format!("{}: {}", metadata.display(), err)),
            },
            Err(err) => warnings.push(format!("{}: {}", metadata.display(), err)),
        }
        return;
    }
    // No metadata.json here; recurse into subdirectories.
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(err) => {
            warnings.push(format!("{}: {}", dir.display(), err));
            return;
        }
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            walk(&path, nodes, warnings);
        }
    }
}

/// Try the node dir first, then walk up to its parent (package
/// root) looking for `form_field_specs.json`. Mirrors the
/// FsCatalog loader so describe and stdlib catalog stay in sync.
fn load_form_specs(
    node_dir: &Path,
    warnings: &mut Vec<String>,
) -> Vec<weft_core::node::FormFieldSpec> {
    // Look in the node's own dir, then fall back to its parent (the
    // form group's `form_field_specs.json`). Fields shared across
    // sibling form variants live at the parent.
    let mut candidates: Vec<PathBuf> = vec![node_dir.join("form_field_specs.json")];
    if let Some(parent) = node_dir.parent() {
        candidates.push(parent.join("form_field_specs.json"));
    }
    for candidate in candidates {
        if !candidate.is_file() {
            continue;
        }
        match std::fs::read_to_string(&candidate) {
            Ok(text) => match serde_json::from_str(&text) {
                Ok(specs) => return specs,
                Err(err) => warnings.push(format!("{}: {}", candidate.display(), err)),
            },
            Err(err) => warnings.push(format!("{}: {}", candidate.display(), err)),
        }
    }
    Vec::new()
}
