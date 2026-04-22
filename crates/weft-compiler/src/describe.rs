//! Catalog introspection for tooling (Tangle, VS Code extension,
//! dashboard). Reads `metadata.json` from every node available in the
//! project scope (stdlib + user `nodes/` + `nodes/vendor/`) and emits
//! a unified JSON description.
//!
//! Must work on partially-written user nodes: if `metadata.json` is
//! absent or malformed, best-effort fallback is to skip that node but
//! keep going. Tangle gets a warning flag, not a hard failure.

use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::error::CompileResult;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatalogDescription {
    pub nodes: Vec<weft_core::NodeMetadata>,
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

fn walk(dir: &Path, nodes: &mut Vec<weft_core::NodeMetadata>, warnings: &mut Vec<String>) {
    let metadata = dir.join("metadata.json");
    if metadata.is_file() {
        match std::fs::read_to_string(&metadata) {
            Ok(text) => match serde_json::from_str::<weft_core::NodeMetadata>(&text) {
                Ok(m) => nodes.push(m),
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
