//! Catalog introspection. Tooling (Tangle, VS Code extension, the ops
//! dashboard's node picker) fetches the per-project or global node
//! catalog from these endpoints.

use std::collections::BTreeMap;

use axum::{extract::{Path, Query, State}, Json};
use serde::{Deserialize, Serialize};
use weft_compiler::describe::describe_project;
use weft_core::{node::NodeMetadata, NodeCatalog};
use weft_stdlib::StdlibCatalog;

use crate::state::DispatcherState;

#[derive(Serialize)]
pub struct NodesResponse {
    /// Keyed by NodeMetadata.node_type so the VS Code webview can
    /// merge this directly into its NODE_TYPE_CONFIG registry.
    pub catalog: BTreeMap<String, NodeMetadata>,
    /// Soft errors from scanning project-local nodes (malformed
    /// metadata.json, unreadable files). Empty for the stdlib-only
    /// path.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
}

#[derive(Deserialize)]
pub struct NodesQuery {
    /// Absolute path to the project root. When present, we also
    /// walk `{root}/nodes/**/metadata.json` and merge those entries
    /// (project-local wins over stdlib on a name collision).
    pub project_root: Option<String>,
}

fn stdlib_catalog() -> BTreeMap<String, NodeMetadata> {
    StdlibCatalog
        .all()
        .into_iter()
        .filter_map(|t| StdlibCatalog.lookup(t).map(|n| (t.to_string(), n.metadata())))
        .collect()
}

/// Global catalog. When `?project_root=/path` is supplied we also
/// scan for project-local nodes under that root. The VS Code
/// extension calls this with the folder containing the active
/// .weft file so the palette lists both stdlib and user-defined
/// node types. Without the param the response is stdlib-only.
pub async fn nodes(
    State(_state): State<DispatcherState>,
    Query(q): Query<NodesQuery>,
) -> Json<NodesResponse> {
    let mut catalog = stdlib_catalog();
    let mut warnings = Vec::new();
    if let Some(root) = q.project_root.as_deref() {
        if let Ok(desc) = describe_project(std::path::Path::new(root)) {
            for m in desc.nodes {
                catalog.insert(m.node_type.clone(), m);
            }
            warnings.extend(desc.warnings);
        }
    }
    Json(NodesResponse { catalog, warnings })
}

/// Per-project catalog alias. Kept for tooling that already calls
/// this; behaves like `/describe/nodes?project_root=...` once we
/// thread project → path lookup through DispatcherState. For now
/// it mirrors the stdlib-only response.
pub async fn project_catalog(
    State(_state): State<DispatcherState>,
    Path(_id): Path<String>,
) -> Json<NodesResponse> {
    Json(NodesResponse { catalog: stdlib_catalog(), warnings: Vec::new() })
}
