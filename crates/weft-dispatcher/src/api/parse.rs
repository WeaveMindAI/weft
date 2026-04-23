//! Interactive parse + validate endpoints for the VS Code extension.
//!
//! `/parse`: fast path. Stages 1+2+3 (lex, flatten, lenient enrich).
//! Returns the project + per-referenced-node catalog metadata + any
//! diagnostics surfaced during lenient enrich.
//!
//! `/validate`: slow path. Full compile pipeline including strict
//! enrich, generic validation, and per-node validators.

use std::collections::BTreeMap;

use axum::{extract::State, http::StatusCode, Json};
use serde::{Deserialize, Serialize};

use weft_catalog::{FsCatalog, stdlib_catalog};
use weft_compiler::Diagnostic;
use weft_core::{node::NodeMetadata, MetadataCatalog, ProjectDefinition};

use crate::state::DispatcherState;

#[derive(Debug, Deserialize)]
pub struct ParseRequest {
    pub source: String,
    #[serde(default)]
    pub project_id: Option<uuid::Uuid>,
}

/// Catalog metadata for one node type, wire format for the VS Code
/// extension. Mirrors the fields of `NodeMetadata` the webview needs
/// to render a node (icon, color, description, fields, features,
/// ports). The webview's `protocol.ts` has the matching TS type.
pub type CatalogEntry = NodeMetadata;

#[derive(Debug, Serialize)]
pub struct ParseResponse {
    pub project: ProjectDefinition,
    /// Per-node-type catalog entries. Keyed by `NodeDefinition.nodeType`.
    /// Scoped to the node types referenced in the project so the
    /// response stays small even as the catalog grows.
    pub catalog: BTreeMap<String, CatalogEntry>,
    pub diagnostics: Vec<Diagnostic>,
}

pub async fn parse(
    State(_state): State<DispatcherState>,
    Json(req): Json<ParseRequest>,
) -> Result<Json<ParseResponse>, (StatusCode, String)> {
    let project_id = req.project_id.unwrap_or_else(uuid::Uuid::nil);
    let catalog = stdlib_catalog()
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("catalog: {e}")))?;
    let (project, diagnostics) = weft_compiler::parse_only(&req.source, project_id, &catalog);
    let catalog_map = collect_catalog(&project, &catalog);
    Ok(Json(ParseResponse { project, catalog: catalog_map, diagnostics }))
}

/// For each node type present in the project, pull its catalog entry
/// (the `NodeMetadata` from metadata.json). Hidden node types (like
/// Passthrough) are omitted. Unknown types are skipped: `/parse` is
/// lenient, the webview renders them as placeholders.
fn collect_catalog(
    project: &ProjectDefinition,
    catalog: &FsCatalog,
) -> BTreeMap<String, CatalogEntry> {
    let mut out = BTreeMap::new();
    for node in &project.nodes {
        if out.contains_key(&node.node_type) {
            continue;
        }
        if let Some(meta) = catalog.lookup(&node.node_type) {
            if meta.features.hidden {
                continue;
            }
            out.insert(node.node_type.clone(), meta.clone());
        }
    }
    out
}

#[derive(Debug, Serialize)]
pub struct ValidateResponse {
    pub diagnostics: Vec<Diagnostic>,
}

pub async fn validate(
    State(_state): State<DispatcherState>,
    Json(req): Json<ParseRequest>,
) -> Result<Json<ValidateResponse>, (StatusCode, String)> {
    let project_id = req.project_id.unwrap_or_else(uuid::Uuid::nil);
    let mut diagnostics = Vec::new();

    let mut project = match weft_compiler::weft_compiler::compile(&req.source, project_id) {
        Ok(p) => p,
        Err(errors) => {
            for e in errors {
                diagnostics.push(Diagnostic {
                    line: e.line,
                    column: 0,
                    severity: weft_compiler::Severity::Error,
                    message: e.message,
                    code: Some("parse".into()),
                });
            }
            return Ok(Json(ValidateResponse { diagnostics }));
        }
    };

    let catalog = stdlib_catalog()
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("catalog: {e}")))?;
    if let Err(e) = weft_compiler::enrich::enrich(&mut project, &catalog) {
        diagnostics.push(Diagnostic {
            line: 0,
            column: 0,
            severity: weft_compiler::Severity::Error,
            message: format!("{e}"),
            code: Some("enrich".into()),
        });
    }

    diagnostics.extend(weft_compiler::validate::validate(&project, &catalog));

    // Per-node validators ran in v1 by calling `node_impl.validate`.
    // In v2, node Rust code lives in the project binary; the
    // dispatcher can't invoke it. For Phase A we rely on generic
    // validation rules expressible from metadata alone (enforced by
    // the compiler's validate.rs). Node-specific checks that need
    // Rust logic run when `weft build` invokes cargo (and cargo
    // compiles the project crate which includes the node).

    Ok(Json(ValidateResponse { diagnostics }))
}
