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

pub fn describe_project(_project_root: &Path) -> CompileResult<CatalogDescription> {
    // Phase A2 target.
    Ok(CatalogDescription { nodes: Vec::new(), warnings: Vec::new() })
}
