//! Catalog introspection for tooling (Tangle, VS Code extension,
//! dashboard). Reads `metadata.json` (and the optional sibling
//! `form_field_specs.json`) from every node available in the
//! project scope (stdlib + user `nodes/` + `nodes/vendor/`) and
//! emits a unified description.
//!
//! Must work on partially-written user nodes: if `metadata.json`
//! is absent or malformed, best-effort fallback is to skip that
//! node but keep going. Tangle gets a warning flag, not a hard
//! failure.

use std::path::Path;

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
                    let specs = load_form_specs(dir);
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
    // No metadata.json here — recurse into subdirectories.
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
fn load_form_specs(node_dir: &Path) -> Vec<weft_core::node::FormFieldSpec> {
    let candidates = [
        node_dir.join("form_field_specs.json"),
        node_dir
            .parent()
            .map(|p| p.join("form_field_specs.json"))
            .unwrap_or_else(|| Path::new("/dev/null").to_path_buf()),
    ];
    for candidate in candidates {
        if candidate.is_file() {
            if let Ok(text) = std::fs::read_to_string(&candidate) {
                if let Ok(specs) = serde_json::from_str(&text) {
                    return specs;
                }
            }
        }
    }
    Vec::new()
}
