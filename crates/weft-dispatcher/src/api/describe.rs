//! Catalog introspection. Tooling (Tangle, VS Code extension, the ops
//! dashboard's node picker) fetches the per-project or global node
//! catalog from these endpoints.

use std::collections::BTreeMap;

use axum::{extract::{Path, Query, State}, Json};
use serde::{Deserialize, Serialize};
use weft_catalog::stdlib_catalog as stdlib_fs_catalog;
use weft_compiler::describe::describe_project;
use weft_core::node::{FormFieldSpec, NodeMetadata};
use weft_core::MetadataCatalog;

use crate::state::DispatcherState;

/// Per-node entry exposed to the VS Code extension and other
/// tooling. Bundles metadata with the node's form field specs so
/// the form_builder editor (and downstream consumers) can drive
/// their UI from a single response.
#[derive(Serialize)]
pub struct NodeEntry {
    #[serde(flatten)]
    pub metadata: NodeMetadata,
    /// Form-field vocabulary the node owns. Empty if the node
    /// doesn't declare `features.has_form_schema`.
    #[serde(rename = "formFieldSpecs", default, skip_serializing_if = "Vec::is_empty")]
    pub form_field_specs: Vec<FormFieldSpec>,
}

#[derive(Serialize)]
pub struct NodesResponse {
    /// Keyed by NodeMetadata.node_type so the VS Code webview can
    /// merge this directly into its NODE_TYPE_CONFIG registry.
    pub catalog: BTreeMap<String, NodeEntry>,
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

/// Read stdlib metadata + per-node form_field_specs and return as
/// a map. Hidden nodes (Passthrough) are excluded from the palette.
fn stdlib_map() -> BTreeMap<String, NodeEntry> {
    let fs = match stdlib_fs_catalog() {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!(target: "weft_dispatcher::describe", "stdlib catalog: {e}");
            return BTreeMap::new();
        }
    };
    fs.all()
        .into_iter()
        .filter(|m| !m.features.hidden)
        .map(|m| {
            let specs = fs.form_field_specs(&m.node_type).to_vec();
            (
                m.node_type.clone(),
                NodeEntry { metadata: m.clone(), form_field_specs: specs },
            )
        })
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
    let mut catalog = stdlib_map();
    let mut warnings = Vec::new();
    if let Some(root) = q.project_root.as_deref() {
        if let Ok(desc) = describe_project(std::path::Path::new(root)) {
            for entry in desc.nodes {
                let specs = entry.form_field_specs;
                catalog.insert(
                    entry.metadata.node_type.clone(),
                    NodeEntry {
                        metadata: entry.metadata,
                        form_field_specs: specs,
                    },
                );
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
    Json(NodesResponse { catalog: stdlib_map(), warnings: Vec::new() })
}
