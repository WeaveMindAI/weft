//! Interactive parse + validate endpoints for the VS Code extension.
//!
//! `/parse`: fast path. Stages 1+2+3 (lex, flatten, lenient enrich).
//! Called on every text-change (debounced ~100ms) by the graph
//! webview. Never aborts: returns a (possibly partial) project plus
//! diagnostics.
//!
//! `/validate`: slow path. Full compile pipeline including strict
//! enrich + validation. Called on a longer debounce (~500ms) to
//! populate VS Code's Problems panel.

use axum::{extract::State, http::StatusCode, Json};
use serde::{Deserialize, Serialize};

use weft_compiler::Diagnostic;
use weft_core::ProjectDefinition;
use weft_stdlib::StdlibCatalog;

use crate::state::DispatcherState;

#[derive(Debug, Deserialize)]
pub struct ParseRequest {
    pub source: String,
    /// Project id to stamp into the returned ProjectDefinition. The
    /// VS Code extension should send the project's real id (from
    /// weft.toml). If absent, we mint a placeholder; the id has no
    /// runtime meaning for /parse.
    #[serde(default)]
    pub project_id: Option<uuid::Uuid>,
}

#[derive(Debug, Serialize)]
pub struct ParseResponse {
    pub project: ProjectDefinition,
    pub diagnostics: Vec<Diagnostic>,
}

pub async fn parse(
    State(_state): State<DispatcherState>,
    Json(req): Json<ParseRequest>,
) -> Result<Json<ParseResponse>, (StatusCode, String)> {
    let project_id = req.project_id.unwrap_or_else(uuid::Uuid::nil);
    let (project, diagnostics) = weft_compiler::parse_only(&req.source, project_id, &StdlibCatalog);
    Ok(Json(ParseResponse { project, diagnostics }))
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

    if let Err(e) = weft_compiler::enrich::enrich(&mut project, &StdlibCatalog) {
        diagnostics.push(Diagnostic {
            line: 0,
            column: 0,
            severity: weft_compiler::Severity::Error,
            message: format!("{e}"),
            code: Some("enrich".into()),
        });
    }

    // Stage 4: full validation pass. Every rule from the v1 audit.
    diagnostics.extend(weft_compiler::validate::validate(&project));

    Ok(Json(ValidateResponse { diagnostics }))
}

#[derive(Debug, Serialize)]
pub struct ValidateResponse {
    pub diagnostics: Vec<Diagnostic>,
}
